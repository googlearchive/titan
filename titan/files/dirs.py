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
import gc
import json
import logging
import os
import time

from google.appengine import runtime
from google.appengine.api import taskqueue
from google.appengine.ext import deferred
from google.appengine.ext import ndb

from titan.common import utils
from titan.files import files

WINDOW_SIZE_SECONDS = 10
TASKQUEUE_NAME = 'titan-dirs'
TASKQUEUE_LEASE_SECONDS = 3 * WINDOW_SIZE_SECONDS  # 30 second leasing buffer.
TASKQUEUE_LEASE_MAX_TASKS = 1000
TASKQUEUE_LEASE_ETA_BUFFER = TASKQUEUE_LEASE_SECONDS
DEFAULT_CRON_RUNTIME_SECONDS = 60
INITIALIZER_BATCH_SIZE = 100
INITIALIZER_NUM_BATCHES = 50

_STATUS_AVAILABLE = 1
_STATUS_DELETED = 2

class Error(Exception):
  pass

class InvalidDirectoryError(Error):
  pass

class InvalidMetaError(Error):
  pass

class RespawnSignalError(Error):
  pass

class DirManagerMixin(files.File):
  """Mixin to initiate directory update tasks when files change."""

  def Write(self, *args, **kwargs):
    async = kwargs.pop('_dir_manager_async', True)
    result = super(DirManagerMixin, self).Write(*args, **kwargs)
    # Update parent dirs synchronously (the actual directory update RPC is
    # asynchronous, to effectively ignore write contention issues which will
    # rarely occur when many parent dirs don't exist and a large set of files
    # with common parent parents are concurrently created).
    self.UpdateTitanDirs(async=async)
    return result

  def Delete(self, *args, **kwargs):
    result = super(DirManagerMixin, self).Delete(*args, **kwargs)
    # Update dirs eventually.
    self.AddTitanDirDeleteTask()
    return result

  def UpdateTitanDirs(self, async=True):
    """Updates parent path directories to make sure they exist."""
    modified_path = ModifiedPath(
        path=self.path,
        modified=time.time(),
        action=_STATUS_AVAILABLE,
    )
    dir_service = DirService()
    affected_dirs_kwargs = dir_service.ComputeAffectedDirs([modified_path])

    # Evil, but really really convenient. If in test or dev_appserver, just
    # update the directory entities synchronously with the request.
    if os.environ.get('SERVER_SOFTWARE', '').startswith(('Dev', 'Test')):
      async = False

    affected_dirs_kwargs['async'] = async
    dir_service.UpdateAffectedDirs(**affected_dirs_kwargs)

  def AddTitanDirDeleteTask(self):
    """Add a task to the pull queue about which path was deleted."""
    now = time.time()
    window = _GetWindow(now)
    path_data = {
        'path': self.path,
        'modified': now,
        'action': _STATUS_DELETED,
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

    # Evil, but really really convenient. If in test or dev_appserver, just
    # update the directory entities synchronously with the request.
    if os.environ.get('SERVER_SOFTWARE', '').startswith(('Dev', 'Test')):
      dir_task_consumer = DirTaskConsumer()
      dir_task_consumer.ProcessNextWindow()

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

    for tasks_to_delete in utils.ChunkGenerator(tasks):
      queue.delete_tasks(tasks_to_delete)

    return modified_paths

  def ProcessWindowsWithBackoff(self, runtime=DEFAULT_CRON_RUNTIME_SECONDS):
    """Long-running function to process multiple windows.

    Args:
      runtime: How long to process data for.
    Returns:
      A list of results from ProcessNextWindow().
    """
    results = utils.RunWithBackoff(
        func=self.ProcessNextWindow,
        runtime=runtime,
        max_backoff=TASKQUEUE_LEASE_SECONDS)
    return results

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

  def Serialize(self):
    results = {
        'path': self.path,
        'modified': self.modified,
        'action': self.action,
    }
    return results

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
      if modified_path.path.startswith('/_titan/ver/'):
        # Small integration with versions mixin: don't create directories
        # for versioned file paths. See note in _CreateAllDirsWithRespawn.
        continue
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

  def UpdateAffectedDirs(self, dirs_with_adds, dirs_with_deletes, async=False):
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
      if path in dirs_with_adds or files.Files.List(path, limit=1,
                                                    _internal=True):
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
        if ent.status == _STATUS_DELETED:
          # Skip this entity entirely if it's already correct.
          continue
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
        if ent.status == _STATUS_AVAILABLE:
          # Skip this entity entirely if it's already correct.
          continue
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

    for dir_ents in utils.ChunkGenerator(changed_dir_ents, chunk_size=100):
      if not async:
        ndb.put_multi(dir_ents)
      else:
        ndb.put_multi_async(dir_ents)

class Dir(object):
  """A simple directory."""

  # TODO(user): implement Copy and Move methods, and properties.

  def __init__(self, path):
    Dir.ValidatePath(path)
    # Strip trailing slash.
    if path != '/' and path.endswith('/'):
      path = path[:-1]
    self._path = path
    self._name = os.path.basename(self.path)
    self._meta = None
    self._dir_ent = None

  @property
  def _dir(self):
    """Internal property that allows lazy-loading of the public properties."""
    if not self._dir_ent:
      self._dir_ent = _TitanDir.get_by_id(self.path)
      if not self._dir_ent or self._dir_ent.status == _STATUS_DELETED:
        raise InvalidDirectoryError('Directory does not exist: %s' % self.path)
    return self._dir_ent

  @property
  def path(self):
    return self._path

  @property
  def meta(self):
    """Dir meta data."""
    if self._meta:
      return self._meta
    meta = {}
    for key in self._dir.meta_properties:
      meta[key] = getattr(self._dir, key)
    self._meta = utils.DictAsObject(meta)
    return self._meta

  @property
  def exists(self):
    try:
      return bool(self._dir)
    except InvalidDirectoryError:
      return False

  @property
  def name(self):
    return self._name

  @staticmethod
  def ValidatePath(path):
    return utils.ValidateDirPath(path)

  def SetMeta(self, meta):
    _TitanDir.ValidateMetaProperties(meta)
    for key, value in meta.iteritems():
      setattr(self._dir, key, value)
    self._dir.put()

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
      A lazy Dirs mapping.
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

def InitializeDirsFromCurrentState(taskqueue_name='default'):
  """Spawns task to create all directories from current file state.

  This is potentially expensive because it must touch every Titan File in
  existence.

  Args:
    taskqueue_name: The name of the taskqueue which will be used.
  """
  deferred.defer(_CreateAllDirsWithRespawn, _queue=taskqueue_name)

def _CreateAllDirsWithRespawn(cursor=None, include_versioned_dirs=False):
  """A long-running, dir-creation task which respawns until completion."""
  now = datetime.datetime.now()
  # Turn off the in-context cache to avoid OOM issues (since query results are
  # cached by default).
  ctx = ndb.get_context()
  ctx.set_cache_policy(False)
  try:
    count = 0
    more = True
    dir_service = DirService()
    query = files._TitanFile.query()

    while more:
      if not cursor:
        # First run.
        ents, cursor, more = query.fetch_page(INITIALIZER_BATCH_SIZE)
      else:
        ents, cursor, more = query.fetch_page(
            INITIALIZER_BATCH_SIZE, start_cursor=cursor)
      modified_paths = []
      for ent in ents:
        if not include_versioned_dirs and ent.path.startswith('/_titan/ver/'):
          # Skip versioned paths since there is already much metadata around
          # discovering file versions in a changeset, and also because
          # microversions can create very numerous, duplicate, deep
          # directory trees.
          # NOTE: can't skip these entities by adding an inequality to the
          # query because of the cursor handling, so skip them manually here.
          continue
        try:
          modified_path = ModifiedPath(
              path=ent.path,
              modified=now,
              action=ModifiedPath.WRITE,
          )
        except ValueError:
          # Skip any old, invalid files which may end in a trailing slash. :(
          logging.error('Skipping invalid path: %s', repr(ent.path))
          continue
        modified_paths.append(modified_path)

      if modified_paths:
        affected_dirs = dir_service.ComputeAffectedDirs(modified_paths)
        if affected_dirs['dirs_with_adds']:
          dir_service.UpdateAffectedDirs(**affected_dirs)
          logging.info('Created directories: \n%s',
                       '\n'.join(sorted(list(affected_dirs['dirs_with_adds']))))

      # Save the second-to-last cursor, because if we exceed the deadline above,
      # we need to restart the last update.
      penultimate_cursor = cursor
      count += 1

      # Attempt to avoid OOM errors.
      gc.collect()
      if count >= INITIALIZER_NUM_BATCHES:
        raise RespawnSignalError
  except (runtime.DeadlineExceededError, RespawnSignalError):
    try:
      taskqueue_name = os.environ['HTTP_X_APPENGINE_QUEUENAME']
      deferred.defer(
          _CreateAllDirsWithRespawn,
          cursor=penultimate_cursor, _queue=taskqueue_name)
    except:
      logging.exception('Unable to finish creating directories!')

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

  BASE_PROPERTIES = frozenset((
      'name',
      'parent_path',
      'parent_paths',
      'status',
  ))

  def __repr__(self):
    return '<_TitanDir: %s>' % self.key.id()

  @property
  def path(self):
    return self.key.id()

  @property
  def meta_properties(self):
    """A dictionary containing any expando properties."""
    meta_properties = self.to_dict()
    for name in self.BASE_PROPERTIES:
      meta_properties.pop(name)
    return meta_properties

  @staticmethod
  def ValidateMetaProperties(meta):
    """Verify that meta properties are valid."""
    if not meta:
      return
    for key in meta:
      if key in _TitanDir.BASE_PROPERTIES:
        raise InvalidMetaError('Invalid name for meta property: "%s"' % key)

def _GetWindow(timestamp=None, window_size=WINDOW_SIZE_SECONDS):
  """Get the window for the given unix time and window size."""
  return int(window_size * round(float(timestamp) / window_size))
