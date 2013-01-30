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

from google.appengine.ext import deferred

from titan import files
from titan import users
from titan.common import utils
from titan.files.mixins import versions

TASKQUEUE_NAME = 'microversions'

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
    # the blob to both the superclass and to the deferred task. This allows
    # large files to be microversioned AND to share the same exact blob between
    # the root file and the microversioned file.
    #
    # Duplicate some method calls from files.File.Write:
    # If given unicode, encode it as UTF-8 and flag it for future decoding.
    kwargs['content'], kwargs['encoding'] = self._MaybeEncodeContent(
        kwargs['content'], kwargs['encoding'])
    # If big enough, store content in blobstore. Must come after encoding.
    kwargs['content'], kwargs['blob'] = self._MaybeWriteToBlobstore(
        kwargs['content'], kwargs['blob'])

    # Writes should go to the root tree, and versioning is deferred.
    kwargs['_delete_old_blob'] = False
    file_kwargs = self._original_kwargs.copy()
    file_kwargs.update({'path': self.path})
    deferred.defer(
        _CommitMicroversion,
        file_kwargs=file_kwargs,
        method_kwargs=kwargs.copy(),
        user=users.GetCurrentUser(),
        action=_Actions.WRITE,
        # The queue in which this task should run.
        _queue=TASKQUEUE_NAME)
    return super(MicroversioningMixin, self).Write(**kwargs)

  @utils.ComposeMethodKwargs
  def Delete(self, **kwargs):
    """Delete method. See superclass docstring."""
    kwargs['_delete_old_blob'] = False
    file_kwargs = self._original_kwargs.copy()
    file_kwargs.update({'path': self.path})
    deferred.defer(
        _CommitMicroversion,
        file_kwargs=file_kwargs,
        method_kwargs=kwargs.copy(),
        user=users.GetCurrentUser(),
        action=_Actions.DELETE,
        # The queue in which this task should run.
        _queue=TASKQUEUE_NAME)
    return super(MicroversioningMixin, self).Delete(**kwargs)

def _CommitMicroversion(file_kwargs, method_kwargs, user, action,
                        created_by=None):
  """Task to enqueue for microversioning a file action.

  Args:
    file_kwargs: A dictionary of file keyword args, including 'path'.
    method_kwargs: A dictionary (or None) of method keyword args.
    user: A users.TitanUser object.
    action: The action to perform from the _Actions enum.
    created_by: Legacy arg; see TODO below.
  Returns:
    The final changeset.
  """
  # TODO(user): This is backwards-compatibility for in-flight tasks after
  # the user arg change is first deployed. Feel free to remove this and the
  # created_by arg after old tasks have processed.
  if created_by:
    user = created_by

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
