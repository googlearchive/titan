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

"""Wrapper around versions which auto-commits all file changes.

Documentation:
  http://googlecloudplatform.github.io/titan/files/microversions.html
"""

import cPickle as pickle
import logging
import time
from google.appengine.api import taskqueue
from titan import files
from titan import users
from titan.common import utils
from titan.files.mixins import versions

TASKQUEUE_NAME = 'titan-microversions'
TASKQUEUE_LEASE_SECONDS = 240  # 4 minutes.

DEFAULT_MAX_TASKS = 20
MAX_BACKOFF_SECONDS = 10

# This should should always be at least 1 minute so that there are no gaps
# between microversions consumer runs.
DEFAULT_PROCESSING_TIMEOUT_SECONDS = 60

class _Actions(object):
  WRITE = 'write'
  DELETE = 'delete'

class MicroversioningMixin(files.File):
  """Mixin to provide microversioning of all file actions."""

  @classmethod
  def should_apply_mixin(cls, **kwargs):
    # Disable if the versions mixin will be enabled, otherwise enable.
    # Microversioning can be limited to certain paths by making and
    # registering a subclass of this mixin and overriding this method.
    mixin_state = kwargs.get('_mixin_state')
    if ('changeset' in kwargs
        or mixin_state and mixin_state.get('is_versions_enabled')):
      return False
    if mixin_state is not None:
      mixin_state['is_microversions_enabled'] = True
    return True

  @utils.compose_method_kwargs
  def write(self, **kwargs):
    """Write method. See superclass docstring."""
    # If content is big enough, write the content to blobstore earlier and pass
    # the blob to both the superclass and to the pull task. This allows large
    # files to be microversioned AND to share the same exact blob between the
    # root file and the microversioned file.
    #
    # Duplicate some method calls from files.File.write:
    # If given unicode, encode it as UTF-8 and flag it for future decoding.
    kwargs['content'], kwargs['encoding'] = self._maybe_encode_content(
        kwargs['content'], kwargs['encoding'])
    # If big enough, store content in blobstore. Must come after encoding.
    kwargs['content'], kwargs['blob'] = self._maybe_write_to_blobstore(
        kwargs['content'], kwargs['blob'])

    kwargs['_delete_old_blob'] = False
    file_kwargs = self._original_kwargs.copy()
    file_kwargs.update({'path': self.path})

    # Defer microversion task.
    user = users.get_current_user()
    data = {
        'file_kwargs': file_kwargs,
        'method_kwargs': kwargs,
        'email': user.email if user else None,
        'action': _Actions.WRITE,
        'time': time.time(),
    }
    try:
      task = taskqueue.Task(method='PULL', payload=pickle.dumps(data))
    except taskqueue.TaskTooLargeError:
      # Different objects pickle to different sizes, so it is difficult to
      # know ahead of time if the content will bloat pickling or not.
      # If it does, force the file to save to blobstore and recreate the task.
      kwargs['content'], kwargs['blob'] = self._maybe_write_to_blobstore(
          kwargs['content'], kwargs['blob'], force_blobstore=True)
      task = taskqueue.Task(method='PULL', payload=pickle.dumps(data))
    task.add(queue_name=TASKQUEUE_NAME)

    return super(MicroversioningMixin, self).write(**kwargs)

  @utils.compose_method_kwargs
  def delete(self, **kwargs):
    """Delete method. See superclass docstring."""
    kwargs['_delete_old_blob'] = False
    file_kwargs = self._original_kwargs.copy()
    file_kwargs.update({'path': self.path})

    # Defer microversion task.
    user = users.get_current_user()
    data = {
        'file_kwargs': file_kwargs,
        'method_kwargs': kwargs,
        'email': user.email if user else None,
        'action': _Actions.DELETE,
        'time': time.time(),
    }
    task = taskqueue.Task(method='PULL', payload=pickle.dumps(data))
    task.add(queue_name=TASKQUEUE_NAME)

    return super(MicroversioningMixin, self).delete(**kwargs)

def process_data(max_tasks=DEFAULT_MAX_TASKS, allow_transient_errors=False):
  """Process a batch of microversions tasks and commit them."""
  vcs = versions.VersionControlService()
  queue = taskqueue.Queue(TASKQUEUE_NAME)

  # The size of this list will be O(max changes of the same file path).
  # A new changeset is added for each change to the same file, within the set
  # of leased tasks.
  changesets = [vcs.new_staging_changeset()]

  # Grab the oldest tasks and reorder in chronological order.
  # TODO(user): do pull queues guarantee native ordering already?
  try:
    tasks = queue.lease_tasks(
        lease_seconds=TASKQUEUE_LEASE_SECONDS, max_tasks=max_tasks)
    tasks = sorted(tasks, key=lambda t: pickle.loads(t.payload)['time'])
  except taskqueue.TransientError:
    if allow_transient_errors:
      return False
    raise

  results = []
  successful_tasks = []
  for task in tasks:
    level = 0
    microversion_data = pickle.loads(task.payload)
    microversion_data.pop('time')
    path = microversion_data['file_kwargs']['path']

    while True:
      if not path in changesets[level].associated_paths:
        break
      else:
        # We've seen this file change before, ascend one level of changesets.
        level += 1
        if level == len(changesets):
          changesets.append(vcs.new_staging_changeset())

    try:
      _write_microversion(changeset=changesets[level], **microversion_data)
      results.append({'path': path, 'changeset': changesets[level]})
      successful_tasks.append(task)
    except Exception as e:
      logging.exception('Microversion for "%s" failed.', path)
      results.append({'path': path, 'error': e})

  for changeset in changesets:
    if not changeset.associated_paths:
      # Every file might have failed, so we would have an empty changeset.
      # Allow this changeset to be orphaned.
      continue
    changeset.finalize_associated_files()
    vcs.commit(changeset, save_manifest=False)

  if successful_tasks:
    queue.delete_tasks(successful_tasks)

  # Exponential backoff will be triggered if this returns a null value.
  return results

def process_data_with_backoff(
    timeout_seconds=DEFAULT_PROCESSING_TIMEOUT_SECONDS,
    max_tasks=DEFAULT_MAX_TASKS):
  """Like process_data, but with exponential backoff."""
  results = utils.run_with_backoff(
      func=process_data,
      runtime=timeout_seconds,
      max_tasks=max_tasks,
      allow_transient_errors=True)
  for result in results:
    if result is not False and 'error' in result:
      logging.error('Microversion failed (will retry): %s - %s',
                    results['path'], results['error'])
  return results

def _write_microversion(changeset, file_kwargs, method_kwargs, email, action):
  """Task to enqueue for microversioning a file action."""
  # Set the _internal flag for all microversion operations.
  file_kwargs['_internal'] = True
  file_kwargs['changeset'] = changeset
  method_kwargs['_delete_old_blob'] = False
  if email:
    method_kwargs['created_by'] = users.TitanUser(email)
    method_kwargs['modified_by'] = users.TitanUser(email)
  if action == _Actions.WRITE:
    files.File(**file_kwargs).write(**method_kwargs)
  elif action == _Actions.DELETE:
    files.File(**file_kwargs).delete(**method_kwargs)
