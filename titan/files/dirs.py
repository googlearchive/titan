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
from titan import files
from titan.common import utils

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

class NamespaceMismatchError(Error):
  pass

class DirManagerMixin(files.File):
  """Mixin to initiate directory update tasks when files change."""

  def write(self, *args, **kwargs):
    async = kwargs.pop('_dir_manager_async', True)
    result = super(DirManagerMixin, self).write(*args, **kwargs)
    # Update parent dirs synchronously (the actual directory update RPC is
    # asynchronous, to effectively ignore write contention issues which will
    # rarely occur when many parent dirs don't exist and a large set of files
    # with common parent parents are concurrently created).
    self.update_titan_dirs(async=async)
    return result

  def delete(self, *args, **kwargs):
    result = super(DirManagerMixin, self).delete(*args, **kwargs)
    # Update dirs eventually.
    self.add_titan_dir_delete_task()
    return result

  def update_titan_dirs(self, async=True):
    """Updates parent path directories to make sure they exist."""
    modified_path = ModifiedPath(
        path=self.real_path,
        namespace=self.namespace,
        modified=time.time(),
        action=_STATUS_AVAILABLE,
    )
    dir_service = DirService()
    affected_dirs_kwargs = dir_service.compute_affected_dirs([modified_path])

    # Evil, but really really convenient. If in test or dev_appserver, just
    # update the directory entities synchronously with the request.
    server_software = os.environ.get('SERVER_SOFTWARE', '')
    if server_software.lower().startswith(('dev', 'test')):
      async = False

    affected_dirs_kwargs['async'] = async
    dir_service.update_affected_dirs(**affected_dirs_kwargs)

  def add_titan_dir_delete_task(self):
    """Add a task to the pull queue about which path was deleted."""
    now = time.time()
    window = _get_window(now)
    path_data = {
        'path': self.path,
        'namespace': self.namespace,
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
    server_software = os.environ.get('SERVER_SOFTWARE', '')
    if server_software.lower().startswith(('dev', 'test')):
      dir_task_consumer = DirTaskConsumer()
      dir_task_consumer.process_next_window()

class DirTaskConsumer(object):
  """Service which consumes and processes path-modification tasks."""

  def process_next_window(self):
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
          namespace=path_data['namespace'],
          modified=path_data['modified'],
          action=path_data['action'],
      )
      modified_paths.append(modified_path)

    # Compute the affected directories and then update them if needed.
    dir_service = DirService()
    affected_dirs = dir_service.compute_affected_dirs(modified_paths)
    dir_service.update_affected_dirs(**affected_dirs)

    for tasks_to_delete in utils.chunk_generator(tasks):
      queue.delete_tasks(tasks_to_delete)

    return modified_paths

  def process_windows_with_backoff(self, runtime=DEFAULT_CRON_RUNTIME_SECONDS):
    """Long-running function to process multiple windows.

    Args:
      runtime: How long to process data for.
    Returns:
      A list of results from process_next_window().
    """
    results = utils.run_with_backoff(
        func=self.process_next_window,
        runtime=runtime,
        max_backoff=TASKQUEUE_LEASE_SECONDS)
    return results

class ModifiedPath(object):
  """Simple container for metadata about the type of path modification."""

  WRITE = 1
  DELETE = 2

  def __init__(self, path, namespace, modified, action):
    """Constructor.

    Args:
      path: Absolute path of modified file (including filename).
      namespace: The namespace of the modified file.
      modified: Unix timestamp float.
      action: One of ModifiedPath.WRITE or ModifiedPath.DELETE.
    """
    Dir.validate_path(path, namespace=namespace)
    self.path = path
    self.namespace = namespace
    self.modified = modified
    self.action = action

  def serialize(self):
    results = {
        'path': self.path,
        'namespace': self.namespace,
        'modified': self.modified,
        'action': self.action,
    }
    return results

class DirService(object):
  """Service for managing directory entities."""

  def compute_affected_dirs(self, modified_paths):
    """Compute which dirs are affected by path modifications.

    Args:
      modified_paths: A list of ModifiedPath objects.
    Raises:
      NamespaceMismatchError: If mixing namespaces.
    Returns:
      A dictionary containing 'dirs_with_adds' and 'dirs_with_deletes',
      both of which are sets of strings containing the affect dir paths.
    """
    if modified_paths:
      namespace = modified_paths[0].namespace
    # First, merge file path modifications.
    # Perform an in-order pass to get the final modified state of each file.
    sorted_paths = sorted(modified_paths, key=lambda path: path.modified)
    new_modified_paths = {}
    for modified_path in sorted_paths:
      if modified_path.namespace != namespace:
        raise NamespaceMismatchError(
            'Namespace "{}" does not match namespace "{}".'.format(
                modified_path.namespace, namespace))
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
      current_dirs = utils.split_path(modified_path.path)
      if modified_path.action == ModifiedPath.WRITE:
        dirs_with_adds = dirs_with_adds.union(set(current_dirs))
      elif modified_path.action == ModifiedPath.DELETE:
        dirs_with_deletes = dirs_with_deletes.union(set(current_dirs))

    # Ignore root dir; it's hard-coded elsewhere to always exist.
    dirs_with_adds.discard('/')
    dirs_with_deletes.discard('/')

    affected_dirs = {
        'namespace': namespace,
        'dirs_with_adds': dirs_with_adds,
        'dirs_with_deletes': dirs_with_deletes,
    }
    return affected_dirs

  @ndb.toplevel
  def update_affected_dirs(self, dirs_with_adds, dirs_with_deletes,
                           namespace=None, async=False):
    """Manage changes to _TitanDir entities computed by compute_affected_dirs."""
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
      if path in dirs_with_adds or files.Files.list(
          path, namespace=namespace, limit=1, _internal=True):
        # The directory is marked for addition, or files still exist in it.
        continue
      subdirs = Dirs.list(path, limit=2)
      if len(subdirs) > 1:
        # Multiple subdirs exist, cannot delete dir.
        continue
      elif len(subdirs) == 1:
        # Handle the case where the only remaining subdir is marked for delete.
        if subdirs.values()[0].path not in dirs_paths_to_delete:
          continue
      dirs_paths_to_delete.append(path)

    # Batch get all directory entities, both added and deleted.
    ns = namespace
    dir_keys = [
        ndb.Key(_TitanDir, path, namespace=ns) for path in dirs_paths_to_delete]
    dir_keys += [
        ndb.Key(_TitanDir, path, namespace=ns) for path in dirs_with_adds]
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
            # NDB properties:
            id=path,
            namespace=namespace,
            # Model properties:
            name=os.path.basename(path),
            parent_path=os.path.dirname(path),
            parent_paths=utils.split_path(path),
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
            # NDB properties:
            id=path,
            namespace=namespace,
            # Model properties:
            name=os.path.basename(path),
            parent_path=os.path.dirname(path),
            parent_paths=utils.split_path(path),
            status=_STATUS_AVAILABLE,
        )
      # Whitespace. Important.
      changed_dir_ents.append(ent)

    for dir_ents in utils.chunk_generator(changed_dir_ents, chunk_size=100):
      if not async:
        ndb.put_multi(dir_ents)
      else:
        ndb.put_multi_async(dir_ents)

class Dir(object):
  """A simple directory."""

  def __init__(self, path, namespace=None, strip_prefix=None):
    Dir.validate_path(path)
    if strip_prefix:
      Dir.validate_path(strip_prefix)
      # Strip trailing slash.
      if strip_prefix.endswith('/'):
        strip_prefix = strip_prefix[:-1]

    # Strip trailing slash.
    if path != '/' and path.endswith('/'):
      path = path[:-1]
    self._path = path
    self._name = os.path.basename(path)
    self._meta = None
    self._dir_ent = None
    self._strip_prefix = strip_prefix
    self._namespace = namespace

  @property
  def _dir(self):
    """Internal property that allows lazy-loading of the public properties."""
    if not self._dir_ent:
      self._dir_ent = _TitanDir.get_by_id(self._path, namespace=self.namespace)
      if not self._dir_ent or self._dir_ent.status == _STATUS_DELETED:
        raise InvalidDirectoryError('Directory does not exist: %s' % self._path)
    return self._dir_ent

  @property
  def path(self):
    if self._strip_prefix:
      return self._path[len(self._strip_prefix):]
    return self._path

  @property
  def namespace(self):
    return self._namespace

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
  def validate_path(path, namespace=None):
    utils.validate_dir_path(path)
    if namespace is not None:
      utils.validate_namespace(namespace)

  def set_meta(self, meta):
    _TitanDir.validate_meta_properties(meta)
    for key, value in meta.iteritems():
      setattr(self._dir, key, value)
    self._dir.put()

  def serialize(self):
    data = {
        'meta': self._meta.serialize() if self.meta else None,
        'path': self.path,
        'name': self.name,
    }
    return data

class Dirs(collections.Mapping):
  """An ordered mapping of directory paths to Dir objects."""

  def __init__(self, paths=None, dirs=None, namespace=None, **kwargs):
    """Constructor.

    Args:
      paths: An iterable of absolute directory paths.
      dirs: An iterable of Dir objects.
      namespace: The filesystem namespace.
      **kwargs: Keyword arguments to pass through to Dir objects.
    Raises:
      ValueError: If given invalid paths.
      TypeError: If not given "paths" or "dirs".
    """
    self._titan_dirs = {}
    self._ordered_paths = []
    self._namespace = namespace
    if paths is not None and dirs is not None:
      raise TypeError('Either "paths" or "dirs" must be given.')
    if paths is not None and not hasattr(paths, '__iter__'):
      raise ValueError('"paths" must be an iterable.')
    if dirs is not None and not hasattr(dirs, '__iter__'):
      raise ValueError('"dirs" must be an iterable.')

    if paths is not None:
      for path in paths:
        self._add_dir(Dir(path=path, namespace=namespace, **kwargs))
    if dirs is not None:
      for titan_dir in dirs:
        self._add_dir(titan_dir)

  def _add_dir(self, titan_dir):
    if titan_dir.namespace != self.namespace:
      raise NamespaceMismatchError(
          'Dir namespace "{}" does not match Dirs namespace "{}".'.format(
              titan_dir.namespace, self.namespace))
    if titan_dir.path not in self._titan_dirs:
      self._ordered_paths.append(titan_dir.path)
    self._titan_dirs[titan_dir.path] = titan_dir

  def _remove_dir(self, path):
    self._ordered_paths.remove(path)
    del self._titan_dirs[path]

  def __delitem__(self, path):
    self._remove_dir(path)

  def __getitem__(self, path):
    return self._titan_dirs[path]

  def __setitem__(self, path, titan_dir):
    assert isinstance(titan_dir, Dir)
    if titan_dir.path not in self:
      self._ordered_paths.append(titan_dir.path)
    self._titan_dirs[path] = titan_dir

  def __contains__(self, other):
    path = getattr(other, 'path', other)
    return path in self._titan_dirs

  def __iter__(self):
    for key in self._ordered_paths:
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

  @property
  def namespace(self):
    return self._namespace

  def update(self, other_titan_dirs):
    if other_titan_dirs.namespace != self.namespace:
      raise NamespaceMismatchError(
          'Dirs namespace "{}" does not match Dirs namespace "{}".'.format(
              other_titan_dirs.namespace, self.namespace))
    for titan_dir in other_titan_dirs.itervalues():
      self[titan_dir.path] = titan_dir

  def sort(self):
    self._ordered_paths.sort()

  @classmethod
  def list(cls, path, namespace=None, limit=None, **kwargs):
    """List the sub-directories of a directory.

    Args:
      path: An absolute directory path.
      namespace: The filesystem namespace.
      limit: An integer limiting the number of sub-directories returned.
      **kwargs: Keyword arguments to pass through to Dir objects.
    Returns:
      A lazy Dirs mapping.
    """
    Dir.validate_path(path, namespace=namespace)

    # Strip trailing slash.
    if path != '/' and path.endswith('/'):
      path = path[:-1]

    dirs_query = _TitanDir.query(namespace=namespace)
    dirs_query = dirs_query.filter(_TitanDir.parent_path == path)
    dirs_query = dirs_query.filter(_TitanDir.status == _STATUS_AVAILABLE)
    dir_keys = dirs_query.fetch(limit=limit, keys_only=True)
    titan_dirs = cls(
        [key.id() for key in dir_keys], namespace=namespace, **kwargs)
    return titan_dirs

  def serialize(self):
    data = {}
    for titan_dir in self.itervalues():
      data[titan_dir.name] = titan_dir.serialize()
    return data

class _TitanDir(ndb.Expando):
  """Model for representing a dir; don't use directly outside of this module.

  This model is intentionally free of data methods.

  Attributes:
    name: Final directory name. Example: 'dir'
    path: Full directory path. Example: '/path/to/dir'
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
  def validate_meta_properties(meta):
    """Verify that meta properties are valid."""
    if not meta:
      return
    for key in meta:
      if key in _TitanDir.BASE_PROPERTIES:
        raise InvalidMetaError('Invalid name for meta property: "%s"' % key)

def _get_window(timestamp=None, window_size=WINDOW_SIZE_SECONDS):
  """Get the window for the given unix time and window size."""
  return int(window_size * round(float(timestamp) / window_size))
