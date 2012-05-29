#!/usr/bin/env python
# Copyright 2012 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import collections
import datetime
import json
import os
import time

from google.appengine.api import taskqueue
from google.appengine.ext import ndb

from titan.common import utils
from titan.files import files

WINDOW_SIZE_SECONDS = 1
TASKQUEUE_NAME = 'titan-dirs'
TASKQUEUE_LEASE_SECONDS = 2 * WINDOW_SIZE_SECONDS
TASKQUEUE_LEASE_MAX_TASKS = 1000
TASKQUEUE_LEASE_ETA_BUFFER = TASKQUEUE_LEASE_SECONDS

_STATUS_AVAILABLE = 1
_STATUS_DELETED = 2

class DirManagerMixin(files.File):
  """Mixin to initiate directory update tasks when files change."""

  def Write(self, *args, **kwargs):
    result = super(DirManagerMixin, self).Write(*args, **kwargs)
    self.AddTitanDirUpdateTask(action=_STATUS_AVAILABLE)
    return result

  def Delete(self, *args, **kwargs):
    result = super(DirManagerMixin, self).Delete(*args, **kwargs)
    self.AddTitanDirUpdateTask(action=_STATUS_DELETED)
    return result

  def AddTitanDirUpdateTask(self, action):
    """Add a task to the pull queue about which path was modified and how."""
    now = time.time()
    window = _GetWindow(now)
    path_data = {
        'path': self.path,
        'modified': now,
        'action': action,
    }
    # Important: unlock tasks in the same window at the same time, and
    # after the window itself has passed.
    current_task_eta = datetime.datetime.utcfromtimestamp(
        window + TASKQUEUE_LEASE_ETA_BUFFER)
    task = taskqueue.Task(
        method='PULL',
        payload=json.dumps(path_data),
        tag=str(window),
        eta=current_task_eta)
    task.add(queue_name=TASKQUEUE_NAME)

class DirTaskConsumer(object):
  """Service which consumes and processes path-modification tasks."""

  def ProcessNextWindow(self):
    """Lease one window-worth of tasks and update the corresponding dirs.

    Returns:
      A list of ModifiedPaths.
    """
    queue = taskqueue.Queue(TASKQUEUE_NAME)
    # Don't specify a tag; this pulls the oldest tasks of the same tag.
    tasks = queue.lease_tasks_by_tag(lease_seconds=TASKQUEUE_LEASE_SECONDS,
                                     max_tasks=TASKQUEUE_LEASE_MAX_TASKS)
    if not tasks:
      return {}

    # Keep leasing similar tasks if we hit the per-request leasing max.
    have_all_tasks = True if len(tasks) < TASKQUEUE_LEASE_MAX_TASKS else False
    while not have_all_tasks:
      tasks_in_window = queue.lease_tasks_by_tag(
          lease_seconds=TASKQUEUE_LEASE_SECONDS,
          max_tasks=TASKQUEUE_LEASE_MAX_TASKS,
          tag=tasks[0].tag)
      tasks.extend(tasks_in_window)
      if len(tasks_in_window) < TASKQUEUE_LEASE_MAX_TASKS:
        have_all_tasks = True

    # Package each task's data into a ModifiedPath and pass it on.
    # Don't deal with ordering or chronologically collapsing paths here.
    modified_paths = []
    for task in tasks:
      path_data = json.loads(task.payload)
      modified_path = ModifiedPath(
          path=path_data['path'],
          modified=path_data['modified'],
          action=path_data['action'],
      )
      modified_paths.append(modified_path)

    # Compute the affected directories and then update them if needed.
    dir_service = DirService()
    affected_dirs = dir_service.ComputeAffectedDirs(modified_paths)
    dir_service.UpdateAffectedDirs(**affected_dirs)

    queue.delete_tasks(tasks)

    return modified_paths

class ModifiedPath(object):
  """Simple container for metadata about the type of path modification."""

  WRITE = 1
  DELETE = 2

  def __init__(self, path, modified, action):
    """Constructor.

    Args:
      path: Absolute path of modified file (including filename).
      modified: Unix timestamp float.
      action: One of ModifiedPath.WRITE or ModifiedPath.DELETE.
    """
    utils.ValidateFilePath(path)
    self.path = path
    self.modified = modified
    self.action = action

class DirService(object):
  """Service for managing directory entities."""

  def ComputeAffectedDirs(self, modified_paths):
    """Compute which dirs are affected by path modifications.

    Args:
      modified_paths: A list of ModifiedPath objects.
    Returns:
      A dictionary containing 'dirs_with_adds' and 'dirs_with_deletes',
      both of which are sets of strings containing the affect dir paths.
    """
    # First, merge file path modifications.
    # Perform an in-order pass to get the final modified state of each file.
    sorted_paths = sorted(modified_paths, key=lambda path: path.modified)
    new_modified_paths = {}
    for modified_path in sorted_paths:
      new_modified_paths[modified_path.path] = modified_path
    sorted_paths = sorted(new_modified_paths.values(),
                          key=lambda path: path.modified)

    # Second, generate the set of affected directory paths.
    # This does not need to collapse dirs which are added and then deleted,
    # the dir should be present in both lists if it is affected by both an
    # add and a delete.
    dirs_with_adds = set()
    dirs_with_deletes = set()
    for modified_path in sorted_paths:
      current_dirs = utils.SplitPath(modified_path.path)
      if modified_path.action == ModifiedPath.WRITE:
        dirs_with_adds = dirs_with_adds.union(set(current_dirs))
      elif modified_path.action == ModifiedPath.DELETE:
        dirs_with_deletes = dirs_with_deletes.union(set(current_dirs))

    # Ignore root dir; it's hard-coded elsewhere to always exist.
    dirs_with_adds.discard('/')
    dirs_with_deletes.discard('/')

    affected_dirs = {
        'dirs_with_adds': dirs_with_adds,
        'dirs_with_deletes': dirs_with_deletes,
    }
    return affected_dirs

  def UpdateAffectedDirs(self, dirs_with_adds, dirs_with_deletes):
    """Manage changes to _TitanDir entities computed by ComputeAffectedDirs."""
    # Order deletes by depth first. This isn't actually by depth, but all we
    # need to guarantee here is that paths with common subdirs are deleted
    # depth-first, which can be accomplished by sorting in reverse
    # alphabetical order.
    dirs_with_deletes = sorted(list(dirs_with_deletes), reverse=True)

    # For every directory which contained a deleted file (including children),
    # check if the directory should disappear. It should disappear if:
    #   1. There are no files in the directory, and...
    #   2. There are no child directories, and...
    #   3. The directory path is not present in dirs_with_adds.
    dirs_paths_to_delete = []
    for path in dirs_with_deletes:
      if path in dirs_with_adds or files.Files.List(path, limit=1):
        # The directory is marked for addition, or files still exist in it.
        continue
      subdirs = Dirs.List(path, limit=2)
      if len(subdirs) > 1:
        # Multiple subdirs exist, cannot delete dir.
        continue
      elif len(subdirs) == 1:
        # Handle the case where the only remaining subdir is marked for delete.
        if subdirs.values()[0].path not in dirs_paths_to_delete:
          continue
      dirs_paths_to_delete.append(path)

    # Batch get all directory entities, both added and deleted.
    dir_keys = [ndb.Key(_TitanDir, path) for path in dirs_paths_to_delete]
    dir_keys += [ndb.Key(_TitanDir, path) for path in dirs_with_adds]
    existing_dir_ents = ndb.get_multi(dir_keys)
    # Transform into a dictionary mapping paths to existing entities:
    existing_dirs = {}
    for ent in existing_dir_ents:
      if ent:
        existing_dirs[ent.path] = ent

    changed_dir_ents = []
    for path in dirs_paths_to_delete:
      if path in existing_dirs:
        # Existing directory, mark as deleted.
        ent = existing_dirs[path]
        ent.status = _STATUS_DELETED
      else:
        # Missing directory entity, create a new one and mark as deleted.
        ent = _TitanDir(
            id=path,
            name=os.path.basename(path),
            parent_path=os.path.dirname(path),
            parent_paths=utils.SplitPath(path),
            status=_STATUS_DELETED,
        )
      # Whitespace. Important.
      changed_dir_ents.append(ent)

    for path in dirs_with_adds:
      if path in existing_dirs:
        # Existing directory, make sure it's marked as available.
        ent = existing_dirs[path]
        ent.status = _STATUS_AVAILABLE
      else:
        # Missing directory entity, create a new one and mark as available.
        ent = _TitanDir(
            id=path,
            name=os.path.basename(path),
            parent_path=os.path.dirname(path),
            parent_paths=utils.SplitPath(path),
            status=_STATUS_AVAILABLE,
        )
      # Whitespace. Important.
      changed_dir_ents.append(ent)

    ndb.put_multi(changed_dir_ents)

class Dir(object):
  """A simple directory."""

  # TODO(user): implement Copy and Move methods, and properties.

  def __init__(self, path):
    Dir.ValidatePath(path)
    self.path = path

  @staticmethod
  def ValidatePath(path):
    return utils.ValidateDirPath(path)

class Dirs(collections.Mapping):
  """A mapping of directory paths to Dir objects."""

  def __init__(self, paths):
    """Constructor.

    Args:
      paths: An iterable of absolute directory paths.
    Raises:
      ValueError: If given invalid paths.
    """
    self._titan_dirs = {}
    if not hasattr(paths, '__iter__'):
      raise ValueError('"paths" must be an iterable.')
    for path in paths:
      self._titan_dirs[path] = Dir(path=path)

  def __delitem__(self, path):
    del self._titan_dirs[path]

  def __getitem__(self, path):
    return self._titan_dirs[path]

  def __setitem__(self, path, titan_dir):
    assert isinstance(titan_dir, Dir)
    self._titan_dirs[path] = titan_dir

  def __contains__(self, other):
    path = getattr(other, 'path', other)
    return path in self._titan_dirs

  def __iter__(self):
    for key in self._titan_dirs:
      yield key

  def __len__(self):
    return len(self._titan_dirs)

  def __eq__(self, other):
    if not isinstance(other, Dirs) or len(self) != len(other):
      return False
    for titan_dir in other.itervalues():
      if titan_dir not in self:
        return False
    return True

  def __repr__(self):
    return '<Dirs %r>' % self.keys()

  def update(self, other_titan_dirs):
    for titan_dir in other_titan_dirs.itervalues():
      self[titan_dir.path] = titan_dir

  @classmethod
  def List(cls, path, limit=None):
    """List the sub-directories of a directory.

    Args:
      path: An absolute directory path.
      limit: An integer limiting the number of sub-directories returned.
    Returns:
      A populated Dirs mapping.
    """
    Dir.ValidatePath(path)

    # Strip trailing slash.
    if path != '/' and path.endswith('/'):
      path = path[:-1]

    dirs_query = _TitanDir.query()
    dirs_query = dirs_query.filter(_TitanDir.parent_path == path)
    dirs_query = dirs_query.filter(_TitanDir.status == _STATUS_AVAILABLE)
    dir_keys = dirs_query.fetch(limit=limit, keys_only=True)
    titan_dirs = cls([key.id() for key in dir_keys])
    return titan_dirs

class _TitanDir(ndb.Expando):
  """Model for representing a dir; don't use directly outside of this module.

  This model is intentionally free of data methods.

  Attributes:
    id: Full directory path. Example: '/path/to/dir'
    name: Final directory name. Example: 'dir'
    parent_path: Full path to parent directory.
        Example: '/path/to'
    parent_paths: A list of parent directories.
        Example: ['/', '/path', '/path/to', '/path/to/dir']
    status: If the directory is available or deleted.
  """
  name = ndb.StringProperty()
  parent_path = ndb.StringProperty()
  parent_paths = ndb.StringProperty(repeated=True)
  status = ndb.IntegerProperty(
      default=_STATUS_AVAILABLE,
      choices=[_STATUS_AVAILABLE, _STATUS_DELETED])

  def __repr__(self):
    return '<_TitanDir: %s>' % self.key.id()

  @property
  def path(self):
    return self.key.id()

def _GetWindow(timestamp=None, window_size=WINDOW_SIZE_SECONDS):
  """Get the window for the given unix time and window size."""
  return int(window_size * round(float(timestamp) / window_size))
