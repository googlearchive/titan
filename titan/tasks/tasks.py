#!/usr/bin/env python
# Copyright 2013 Google Inc. All Rights Reserved.
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

"""Titan Tasks library for realtime feedback over batches of deferred tasks."""

import functools
import hashlib
import json
import os
import re
import time
from titan import channel
from titan import files
from titan.common import utils
from titan.tasks import deferred

__all__ = [
    # Constants.
    'GROUP_REGEX',
    'STATUS_QUEUED',
    'STATUS_RUNNING',
    'STATUS_SUCCESSFUL',
    'STATUS_FAILED',
    'DEFAULT_GROUP',
    'DEFAULT_QUEUE_NAME',
    # Errors.
    'Error',
    'InvalidTaskError',
    'InvalidTaskStatusError',
    'TaskFinalizedError',
    'DuplicateTaskError',
    'TaskError',
    'TaskManagerFinalizedError',
    'TaskManagerNotFinalizedError',
    'InvalidTaskManagerError',
    # Classes.
    'Task',
    'TaskManager',
]

_ROOT_DIR_PATH = '/_titan/tasks/'
_GROUP_REGEX_STRING = r'^[a-zA-Z0-9-]+$'
GROUP_REGEX = re.compile(_GROUP_REGEX_STRING)

DEFAULT_GROUP = 'default'
DEFAULT_QUEUE_NAME = 'default'

STATUS_QUEUED = 'queued'
STATUS_RUNNING = 'running'
STATUS_SUCCESSFUL = 'successful'
STATUS_FAILED = 'failed'

class Error(Exception):
  pass

class InvalidTaskError(Error):
  pass

class InvalidTaskStatusError(Error):
  pass

class TaskFinalizedError(Error):
  pass

class DuplicateTaskError(Error):
  pass

class TaskError(Error):
  """Raise this error from deferred tasks and its message will be saved."""

class TaskManagerNotFinalizedError(Error):
  pass

class TaskManagerFinalizedError(Error):
  pass

class InvalidTaskManagerError(Error):
  pass

class NoBroadcastChannelError(Error):
  pass

class VariableNotLoaded(object):
  """Used to distinguish between None and unset."""

def require_finalized(func):
  """Methods decorator which requires that the task manager is finalized."""

  @functools.wraps(func)
  def Wrapper(*args, **kwargs):
    func_self = args[0]
    if not func_self._finalized:
      raise TaskManagerNotFinalizedError('Task manager must be finalized.')
    return func(*args, **kwargs)

  return Wrapper

class TaskManager(object):
  """Manages groups of tasks and provides realtime feedback."""

  def __init__(self, key, group=DEFAULT_GROUP, _file=None):
    """Constructor.

    Args:
      key: A unique key for this task manager.
      group: An optional, arbitrary string for grouping task managers.
      _file: Internal-only optimization arg.
    Raises:
      ValueError: If group is invalid.
    """
    if not GROUP_REGEX.match(group):
      raise ValueError('"group" arg must match: %s' % _GROUP_REGEX_STRING)

    self._group = group
    self._key = key

    # First set by _Initialize, then from _data.
    self._description = VariableNotLoaded
    self._internal_queue = VariableNotLoaded
    self._internal_broadcast_channel_key = VariableNotLoaded
    self._internal_broadcast_channel = VariableNotLoaded

    # Memoized properties.
    self._internal_file = _file
    self._internal_dir_path = None
    self._internal_tasks_dir_path = None
    # Memoized properties from _data.
    self._internal_data = None
    self._status = None
    self._created_by = None
    self._task_keys = None
    self._internal_finalized = None
    self._num_total = None

  def __reduce__(self):
    # This method allows a TaskManager object to be efficiently pickled,
    # such as when it is passed to a deferred task.
    # http://docs.python.org/library/pickle.html#object.__reduce__
    return _unpickle_task_manager, (self.key, self.group), {}

  def _initialize(self, description, broadcast_channel_key, queue):
    self._description = description
    self._internal_broadcast_channel_key = broadcast_channel_key
    self._internal_queue = queue
    self._internal_finalized = False
    self._file.write(json.dumps(self.serialize(full=False)))

  def __eq__(self, other):
    if not hasattr(other, 'serialize'):
      return False
    return self.key == other.key and self.group == other.group

  def __repr__(self):
    return '<TaskManager group:%s key:%s>' % (self.group, self.key)

  @property
  def _data(self):
    if not self._internal_data:
      self._internal_data = json.loads(self._file.content)
    return self._internal_data

  @property
  def _file(self):
    if self._internal_file is None:
      self._internal_file = files.File(self._dir_path + '.json', _internal=True)
    return self._internal_file

  @property
  def _dir_path(self):
    # /_titan/tasks/<group>/<task_manager_key>
    if not self._internal_dir_path:
      self._internal_dir_path = utils.safe_join(
          _ROOT_DIR_PATH, self.group, self.key)
    return self._internal_dir_path

  @property
  def _tasks_dir_path(self):
    # /_titan/tasks/<group>/<task_manager_key>/tasks
    if not self._internal_tasks_dir_path:
      self._internal_tasks_dir_path = utils.safe_join(self._dir_path, 'tasks')
    return self._internal_tasks_dir_path

  @property
  def _broadcast_channel_key(self):
    if self._internal_broadcast_channel_key is VariableNotLoaded:
      self._internal_broadcast_channel_key = self._data['broadcast_channel_key']
    return self._internal_broadcast_channel_key

  @property
  def _broadcast_channel(self):
    if not self._broadcast_channel_key:
      raise NoBroadcastChannelError(
          'This task manager was not created with a broadcast_channel_key.')
    if self._internal_broadcast_channel is VariableNotLoaded:
      self._internal_broadcast_channel = channel.BroadcastChannel(
          self._broadcast_channel_key)
    return self._internal_broadcast_channel

  @property
  def _queue(self):
    if self._internal_queue is VariableNotLoaded:
      self._internal_queue = self._data['queue']
    return self._internal_queue

  @property
  def _finalized(self):
    if self._internal_finalized is None:
      # The 'finalized=False' item is dropped from the file when finalized.
      if not self.exists:
        self._internal_finalized = False
      else:
        self._internal_finalized = self._data.get('finalized', True)
    return self._internal_finalized

  @property
  def num_total(self):
    if self._num_total is None:
      self._num_total = self._data['num_total']
    return self._num_total

  @property
  @require_finalized
  def num_completed(self):
    return files.Files.count(self._tasks_dir_path)

  @property
  @require_finalized
  def num_successful(self):
    filters = [files.FileProperty('status') == STATUS_SUCCESSFUL]
    return files.Files.count(self._tasks_dir_path, filters=filters)

  @property
  @require_finalized
  def num_failed(self):
    filters = [files.FileProperty('status') == STATUS_FAILED]
    return files.Files.count(self._tasks_dir_path, filters=filters)

  @property
  @require_finalized
  def status(self):
    if self.num_completed != self.num_total:
      return STATUS_RUNNING
    elif self.num_failed:
      return STATUS_FAILED
    return STATUS_SUCCESSFUL

  @property
  def exists(self):
    return self._file.exists

  @property
  def key(self):
    return self._key

  @property
  def group(self):
    return self._group

  @property
  @require_finalized
  def successful_tasks(self):
    filters = [files.FileProperty('status') == STATUS_SUCCESSFUL]
    titan_files = files.Files.list(self._tasks_dir_path, filters=filters)
    tasks = set()
    for titan_file in titan_files.itervalues():
      tasks.add(Task(self, _internal_key=titan_file.name_clean))
    return tasks

  @property
  @require_finalized
  def failed_tasks(self):
    filters = [files.FileProperty('status') == STATUS_FAILED]
    titan_files = files.Files.list(self._tasks_dir_path, filters=filters)
    tasks = set()
    for titan_file in titan_files.itervalues():
      tasks.add(Task(self, _internal_key=titan_file.name_clean))
    return tasks

  @property
  def created(self):
    return self._file.created

  @property
  def created_by(self):
    return self._file.created_by

  @property
  def description(self):
    if self._description is VariableNotLoaded:
      self._description = self._data['description']
    return self._description

  @property
  @require_finalized
  def task_keys(self):
    if self._task_keys is None:
      self._task_keys = set(self._data['task_keys'])
    return self._task_keys.copy()

  @property
  @require_finalized
  def tasks(self):
    """Generator yielding lazy Task objects."""
    for task_key in self.task_keys:
      yield Task(self, task_key)

  def finalize(self):
    """Finalizes the TaskManager after all tasks have been deferred."""
    if self._finalized:
      raise TaskManagerFinalizedError('TaskManager has already been finalized.')

    # Save task manager state.
    self._internal_finalized = True
    self._file.write(json.dumps(self.serialize(full=False)))

  def serialize(self, full=False):
    """Returns serializable data for the task manager."""
    data = {
        'key': self.key,
        'group': self.group,
        'description': self.description,
        'broadcast_channel_key': self._broadcast_channel_key,
        'queue': self._queue,
    }

    if self._finalized:
      data.update({
          # These are stored after the task manager is finalized.
          'num_total': self.num_total,
          'task_keys': list(self.task_keys),
      })
    else:
      # Only store this if not finalized, leave it out of the final version.
      data['finalized'] = False

    if full:
      data.update({
          'created': self.created,
          'created_by': self.created_by.email if self.created_by else None,
          # These are calculated dynamically and not stored in the meta file.
          'status': self.status,
          'num_successful': self.num_successful,
          'num_failed': self.num_failed,
          'num_completed': self.num_completed,
      })
    return data

  def subscribe(self, client_id):
    """Adds an existing client to the broadcast channel.

    Can be called with any App Engine channel client_id, including ones created
    with the native API. This allows you to manage a single channel for
    connected clients which may or may not use BroadcastChannel messages.

    Args:
      client_id: A abitrary string which uniquely identifies the client channel.
          This MUST have been sent previously to channel.CreateChannel()
          and then used in a JavaScript client with goog.appengine.Channel,
          otherwise messages will not be received.
    """
    self._broadcast_channel.subscribe(client_id)

  def defer_task(self, task_key, callback, *args, **kwargs):
    """Runs the given callback in a task associated to the task manager.

    Args:
      task_key: A arbitrary, but unique key for this task. This is useful for
          linking individual deferred tasks to user-visible elements.
      callback: The function to defer which will be given *args and **kwargs.
    Raises:
      InvalidTaskManagerError: If the task manager does not exist.
      TaskManagerFinalizedError: If the task manager is already finalized.
      DuplicateTaskError: If the same task_key is used.
    """
    if not self.exists:
      raise InvalidTaskManagerError(
          'The task manager "%s" in group "%s" does not exist.'
          % (self.key, self.group))
    if self._finalized:
      raise TaskManagerFinalizedError(
          'The task manager has already been finalized, tasks can no longer '
          'be deferred using this task manager.')

    if self._task_keys is None:
      self._task_keys = set()
    if self._num_total is None:
      self._num_total = 0

    if task_key in self._task_keys:
      raise DuplicateTaskError(
          'Task with key "%s" has already been added.' % task_key)

    self._num_total += 1
    self._task_keys.add(task_key)

    # Extra data passed to the callback wrapper.
    kwargs['_task_data'] = {
        'task_manager_key': self.key,
        'task_manager_group': self.group,
        'task_key': task_key,
        'broadcast_channel_key': self._broadcast_channel_key,
    }
    kwargs['_queue'] = self._queue

    # Broadcast status of each task when it enters the queue.
    # Do this before the call to Defer, to ensure correct ordering.
    if self._broadcast_channel_key:
      _maybe_send_status_message(
          self._broadcast_channel, STATUS_QUEUED, task_key, self.key)

    deferred.defer(_callback_wrapper, callback, *args, **kwargs)

  @require_finalized
  def get_task(self, task_key):
    if not str(task_key) in self.task_keys:
      raise InvalidTaskError('Task "%s" in task manager "%s" not found.'
                             % (task_key, self.key))
    return Task(self, task_key)

  @classmethod
  def new(cls, group=DEFAULT_GROUP, description=None,
          broadcast_channel_key=None, queue=DEFAULT_QUEUE_NAME):
    """Create a new, unique TaskManager.

    Args:
      group: Optional, arbitrary string for grouping task managers.
      description: Arbitrary text stored with the TaskManager for any use.
      broadcast_channel_key: A unique key for a titan.channel.BroadcastChannel
          used for sending all task updates in realtime to connected clients.
      queue: Which task queue to use for deferred tasks. Uses the 'default'
          task queue by default.
    Returns:
      A new TaskManager.
    """
    # Make a ~unique key for the task manager instance by using the
    # current app server instance ID, request ID hash, and time.
    key = hashlib.md5()
    key.update(os.environ.get('INSTANCE_ID', ''))  # Not on dev_appserver. :(
    key.update(os.environ['REQUEST_ID_HASH'])
    key.update(str(time.time()))
    key = key.hexdigest()
    task_manager = cls(key=key, group=group)
    task_manager._initialize(description, broadcast_channel_key, queue)
    return task_manager

  @classmethod
  def list(cls, group=DEFAULT_GROUP, limit=None, offset=None):
    """Returns a list of TaskManager objects.

    Args:
      group: The group ID of where to list task managers.
      limit: An integer limit for the list query.
      offset: An integer offset for the list query.
    Returns:
      A list of TaskManager objects in descending chronological order.
    """
    titan_files = files.Files.list(
        dir_path=utils.safe_join(_ROOT_DIR_PATH, group),
        limit=limit, offset=offset, _internal=True)
    titan_files.load()

    # Sort in descending chronological order.
    ordered_files = sorted(titan_files.itervalues(), key=lambda f: f.created)
    ordered_files.reverse()
    task_manager_keys = [f.name_clean for f in ordered_files]

    # Create TaskManager objects and inject the preloaded Titan file.
    task_managers = []
    for i, task_manager_key in enumerate(task_manager_keys):
      task_manager = TaskManager(
          key=task_manager_key, group=group, _file=ordered_files[i])
      task_managers.append(task_manager)
    return task_managers

class Task(object):
  """An individual task wrapper.

  This object should only be created by the TaskManager or deferred task.
  This object memoizes many properties and is not meant to be long-lived.
  """

  def __init__(self, task_manager, key=None, _internal_key=None):
    if not key and not _internal_key or key and _internal_key:
      raise ValueError('Exactly one of "key" or "_internal_key" is required.')
    self._key = key
    self._task_manager = task_manager
    self._status = None
    self._internal_file = None

    if _internal_key:
      self._internal_key = _internal_key
    else:
      # Store the file at an "internal_key" location to avoid large free-form
      # filenames. This is safe to hash since it should be unique for each task
      # within a task manager. Salt it with the task manager key just for kicks.
      self._internal_key = hashlib.md5(self.key + self._task_manager.key)
      self._internal_key = self._internal_key.hexdigest()

  @property
  def _data(self):
    return json.loads(self._file.content)

  @property
  def _file(self):
    if self._internal_file is None:
      # /_titan/tasks/<group>/<task_manager_key>/tasks/<task_key_hash>.json
      filename = utils.safe_join(
          _ROOT_DIR_PATH, self._task_manager.group, self._task_manager.key,
          'tasks', self._internal_key + '.json')
      self._internal_file = files.File(filename, _internal=True)
    return self._internal_file

  @property
  def key(self):
    if not self._key:
      return self._data['key']
    return self._key

  @property
  def status(self):
    if not self._file:
      # If the file doesn't exist, the task hasn't completed.
      return self._status or STATUS_RUNNING
    return self._data['status']

  @property
  def error_message(self):
    return self._data.get('error', {}).get('message', None)

  def finalize(self, status, error_message=None):
    """Finalize the task after completion.

    This should only be called by the callback wrapper.

    Args:
      status: The final task status, either STATUS_SUCCESSFUL or STATUS_FAILED.
      error_message: Optional failure message which will be stored.
    Raises:
      TaskFinalizedError: If finalize is called after successful completion.
      InvalidTaskStatusError: If given status is invalid.
    """
    if self.status == STATUS_SUCCESSFUL:
      raise TaskFinalizedError('Task is already successfully finalized.')
    valid_statuses = (STATUS_SUCCESSFUL, STATUS_FAILED)
    if status not in valid_statuses:
      raise InvalidTaskStatusError(
          '"status" must be one of: %r' % valid_statuses)

    self._status = status
    data = self.serialize()
    if error_message:
      data['error'] = {'message': error_message}
    meta = {}
    meta['status'] = self.status
    self._file.write(json.dumps(data), meta=meta)

  def serialize(self):
    data = {
        'key': self.key,
        'status': self.status,
    }
    return data

# NOTE: Any changes you make to this function and its caller need to be
# backwards-compatible since the change will affect in-flight tasks.
# This must be module-level for pickling.
def _callback_wrapper(callback, *args, **kwargs):
  task_data = kwargs.pop('_task_data')
  task_key = task_data['task_key']
  task_manager_key = task_data['task_manager_key']
  task_manager_group = task_data['task_manager_group']
  broadcast_channel_key = task_data['broadcast_channel_key']
  task_manager = TaskManager(key=task_manager_key, group=task_manager_group)
  task = Task(task_manager, task_key)
  broadcast_channel = None
  if broadcast_channel_key:
    broadcast_channel = channel.BroadcastChannel(key=broadcast_channel_key)

  _maybe_send_status_message(
      broadcast_channel, STATUS_RUNNING, task_key, task_manager_key)
  try:
    callback(*args, **kwargs)
  except TaskFinalizedError:
    # There is a rare case where App Engine may run a given task more than
    # once. If this happens, let the task finish successfully.
    pass
  except TaskError as e:
    # Specialized error that allows tasks to store failure messages.
    task.finalize(STATUS_FAILED, error_message=e.message)
    _maybe_send_status_message(
        broadcast_channel, STATUS_FAILED, task_key, task_manager_key, e.message)
    raise
  except:
    task.finalize(STATUS_FAILED)
    _maybe_send_status_message(
        broadcast_channel, STATUS_FAILED, task_key, task_manager_key)
    raise
  else:
    task.finalize(STATUS_SUCCESSFUL)
    _maybe_send_status_message(
        broadcast_channel, STATUS_SUCCESSFUL, task_key, task_manager_key)

def _maybe_send_status_message(
    broadcast_channel, status, task_key, task_manager_key, error=None):
  if broadcast_channel is None:
    return
  message = {
      'status': status,
      'task_key': task_key,
      'task_manager_key': task_manager_key,
  }
  if status == STATUS_FAILED:
    message['error'] = error
  broadcast_channel.send_message(json.dumps(message))

def _unpickle_task_manager(key, group):
  return TaskManager(key=key, group=group)
