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

http://code.google.com/p/titan-files/wiki/Microversions
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
  def ShouldApplyMixin(cls, **kwargs):
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

  @utils.ComposeMethodKwargs
  def Write(self, **kwargs):
    """Write method. See superclass docstring."""
    # If content is big enough, write the content to blobstore earlier and pass
    # the blob to both the superclass and to the pull task. This allows large
    # files to be microversioned AND to share the same exact blob between the
    # root file and the microversioned file.
    #
    # Duplicate some method calls from files.File.Write:
    # If given unicode, encode it as UTF-8 and flag it for future decoding.
    kwargs['content'], kwargs['encoding'] = self._MaybeEncodeContent(
        kwargs['content'], kwargs['encoding'])
    # If big enough, store content in blobstore. Must come after encoding.
    kwargs['content'], kwargs['blob'] = self._MaybeWriteToBlobstore(
        kwargs['content'], kwargs['blob'])

    kwargs['_delete_old_blob'] = False
    file_kwargs = self._original_kwargs.copy()
    file_kwargs.update({'path': self.path})

    # Defer microversion task.
    user = users.GetCurrentUser()
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
      kwargs['content'], kwargs['blob'] = self._MaybeWriteToBlobstore(
          kwargs['content'], kwargs['blob'], force_blobstore=True)
      task = taskqueue.Task(method='PULL', payload=pickle.dumps(data))
    task.add(queue_name=TASKQUEUE_NAME)

    return super(MicroversioningMixin, self).Write(**kwargs)

  @utils.ComposeMethodKwargs
  def Delete(self, **kwargs):
    """Delete method. See superclass docstring."""
    kwargs['_delete_old_blob'] = False
    file_kwargs = self._original_kwargs.copy()
    file_kwargs.update({'path': self.path})

    # Defer microversion task.
    user = users.GetCurrentUser()
    data = {
        'file_kwargs': file_kwargs,
        'method_kwargs': kwargs,
        'email': user.email if user else None,
        'action': _Actions.DELETE,
        'time': time.time(),
    }
    task = taskqueue.Task(method='PULL', payload=pickle.dumps(data))
    task.add(queue_name=TASKQUEUE_NAME)

    return super(MicroversioningMixin, self).Delete(**kwargs)

def ProcessData(max_tasks=DEFAULT_MAX_TASKS, allow_transient_errors=False):
  """Process a batch of microversions tasks and commit them."""
  vcs = versions.VersionControlService()
  queue = taskqueue.Queue(TASKQUEUE_NAME)

  # The size of this list will be O(max changes of the same file path).
  # A new changeset is added for each change to the same file, within the set
  # of leased tasks.
  changesets = [vcs.NewStagingChangeset()]

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
          changesets.append(vcs.NewStagingChangeset())

    try:
      _WriteMicroversion(changeset=changesets[level], **microversion_data)
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
    changeset.FinalizeAssociatedFiles()
    vcs.Commit(changeset)

  if successful_tasks:
    queue.delete_tasks(successful_tasks)

  # Exponential backoff will be triggered if this returns a null value.
  return results

def ProcessDataWithBackoff(timeout_seconds=DEFAULT_PROCESSING_TIMEOUT_SECONDS,
                           max_tasks=DEFAULT_MAX_TASKS):
  results = utils.RunWithBackoff(
      func=ProcessData,
      runtime=timeout_seconds,
      max_tasks=max_tasks,
      allow_transient_errors=True)
  for result in results:
    if result is not False and 'error' in result:
      logging.error('Microversion failed (will retry): %s - %s',
                    results['path'], results['error'])
  return results

def _WriteMicroversion(changeset, file_kwargs, method_kwargs, email, action):
  """Task to enqueue for microversioning a file action."""
  # Set the _internal flag for all microversion operations.
  file_kwargs['_internal'] = True
  file_kwargs['changeset'] = changeset
  if email:
    file_kwargs['_created_by_override'] = users.TitanUser(email)
    file_kwargs['_modified_by_override'] = users.TitanUser(email)
  if action == _Actions.WRITE:
    method_kwargs['_delete_old_blob'] = False
    files.File(**file_kwargs).Write(**method_kwargs)
  elif action == _Actions.DELETE:
    # Mark this file as deleted in the version control system.
    method_kwargs['delete'] = True
    method_kwargs['_delete_old_blob'] = False
    files.File(**file_kwargs).Write(**method_kwargs)

# DEPRECATED. Must remain until all in-flight microversions are processed.
def _CommitMicroversion(file_kwargs, method_kwargs, user, action):
  """Task to enqueue for microversioning a file action.

  Args:
    file_kwargs: A dictionary of file keyword args, including 'path'.
    method_kwargs: A dictionary (or None) of method keyword args.
    user: A users.TitanUser object.
    action: The action to perform from the _Actions enum.
  Returns:
    The final changeset.
  """
  vcs = versions.VersionControlService()
  changeset = vcs.NewStagingChangeset(created_by=user)

  # Setting the 'changeset' argument here is noticed by the factory and so
  # microversioning will not be mixed in, but versioning will.
  file_kwargs['changeset'] = changeset
  file_kwargs['_created_by_override'] = user
  file_kwargs['_modified_by_override'] = user

  if action == _Actions.WRITE:
    method_kwargs['_delete_old_blob'] = False
    files.File(**file_kwargs).Write(**method_kwargs)
  elif action == _Actions.DELETE:
    # Mark this file as deleted in the version control system.
    method_kwargs['delete'] = True
    method_kwargs['_delete_old_blob'] = False
    files.File(**file_kwargs).Write(**method_kwargs)

  # Indicate that this specific changeset object has been used for all file
  # operations and can be trusted for strong consistency guarantees.
  changeset.FinalizeAssociatedFiles()

  return vcs.Commit(changeset)
