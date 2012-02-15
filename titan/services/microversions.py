#!/usr/bin/env python
# Copyright 2011 Google Inc. All Rights Reserved.
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
  http://code.google.com/p/titan-files/wiki/MicroversionsService

  With this service enabled, each Titan write operation will automatically defer
  a task to create a changeset and commit the single file change. After the
  task is deferred, each operation will write-through the change to the root
  tree, keeping an up-to-date copy of the files at the root.

  Immediately, files.Get('/foo.html') will return the correct file.
  Eventually, once the task completes, the versions service can be used
  directly to retrieve versioning information.
"""

from google.appengine.api import users
from google.appengine.ext import deferred
from titan.common import hooks
from titan.files import files
from titan.services import versions

SERVICE_NAME = 'microversions'

# The "RegisterService" method is required for all Titan service plugins.
def RegisterService():
  hooks.RegisterHook(SERVICE_NAME, 'file-exists', hook_class=HookForExists)
  hooks.RegisterHook(SERVICE_NAME, 'file-get', hook_class=HookForGet)
  hooks.RegisterHook(SERVICE_NAME, 'file-write', hook_class=HookForWrite)
  hooks.RegisterHook(SERVICE_NAME, 'file-touch', hook_class=HookForTouch)
  hooks.RegisterHook(SERVICE_NAME, 'file-delete', hook_class=HookForDelete)
  hooks.RegisterHook(SERVICE_NAME, 'list-files', hook_class=HookForListFiles)

# Hooks for Titan Files require Pre and Post methods, take specific arguments,
# and return specific result structures. See here for more info:
# http://code.google.com/p/titan-files/wiki/Services

class HookForExists(hooks.Hook):
  """A hook for files.Exists()."""

  def Pre(self, **kwargs):
    # Skip microversioning for calls directed toward the versions service.
    if 'changeset' in kwargs:
      return _DisableService(SERVICE_NAME, **kwargs)
    # Reads should come directly from the root tree.
    return _DisableService(versions.SERVICE_NAME, **kwargs)

class HookForGet(hooks.Hook):
  """Hook for files.Get()."""

  def Pre(self, **kwargs):
    if 'changeset' in kwargs:
      return _DisableService(SERVICE_NAME, **kwargs)
    return _DisableService(versions.SERVICE_NAME, **kwargs)

class HookForWrite(hooks.Hook):
  """Hook for files.Write()."""

  def Pre(self, **kwargs):
    if 'changeset' in kwargs:
      return _DisableService(SERVICE_NAME, **kwargs)

    # For now, disable microversioning of large files to avoid task limits.
    # TODO(user): Intercept large uploads here and stream directly to
    # blobstore so that the deferred task arguments will be within limits.
    # This logic should be abstracted from its current place in files.Write().
    if kwargs['content'] and len(kwargs['content']) > files.MAX_CONTENT_SIZE:
      return _DisableService(versions.SERVICE_NAME, **kwargs)

    # Writes should go to the root tree, and versioning is deferred.
    created_by = users.get_current_user()
    deferred.defer(_CommitMicroversion, created_by=created_by, write=True,
                   _queue=SERVICE_NAME, **kwargs)
    changed_kwargs = _DisableService(versions.SERVICE_NAME, **kwargs)
    changed_kwargs['_delete_old_blob'] = False
    return changed_kwargs

class HookForTouch(hooks.Hook):
  """Hook for files.Touch()."""

  def Pre(self, **kwargs):
    if 'changeset' in kwargs:
      return _DisableService(SERVICE_NAME, **kwargs)
    created_by = users.get_current_user()
    deferred.defer(_CommitMicroversion, created_by=created_by, touch=True,
                   _queue=SERVICE_NAME, **kwargs)
    return _DisableService(versions.SERVICE_NAME, **kwargs)

class HookForDelete(hooks.Hook):
  """Hook for files.Delete()."""

  def Pre(self, **kwargs):
    # Make sure files.File objects are transformed into string paths here.
    kwargs['paths'] = files.ValidatePaths(kwargs['paths'])

    if 'changeset' in kwargs:
      return _DisableService(SERVICE_NAME, **kwargs)
    created_by = users.get_current_user()
    deferred.defer(_CommitMicroversion, created_by=created_by, delete=True,
                   _queue=SERVICE_NAME, **kwargs)
    changed_kwargs = _DisableService(versions.SERVICE_NAME, **kwargs)
    changed_kwargs['_delete_old_blobs'] = False
    return changed_kwargs

class HookForListFiles(hooks.Hook):
  """Hook for files.ListFiles()."""

  def Pre(self, **kwargs):
    if 'changeset' in kwargs:
      return _DisableService(SERVICE_NAME, **kwargs)
    return _DisableService(versions.SERVICE_NAME, **kwargs)

def _DisableService(service_name, **kwargs):
  """Get kwargs which will disable a service."""
  disabled_services = set(kwargs.get('disabled_services', []))
  disabled_services.add(service_name)
  return {'disabled_services': list(disabled_services)}

def _CommitMicroversion(created_by, write=False, touch=False, delete=False,
                        **kwargs):
  """Task to enqueue for microversioning of a file write operation.

  Args:
    created_by: A users.User object.
    write: True if a Write() operation.
    touch: True if a Touch() operation.
    delete: True if a Delete() operation.
    **kwargs: The keyword args passed to the Titan method.
  """
  vcs = versions.VersionControlService()
  changeset = vcs.NewStagingChangeset(created_by=created_by)

  # Skip microversioning, send command direct to versions service.
  kwargs['changeset'] = changeset
  kwargs.update(_DisableService(SERVICE_NAME, **kwargs))

  if write:
    # Write the file through the versions service (microversioning will be off).
    kwargs['_delete_old_blob'] = False
    files.Write(**kwargs)
  elif touch:
    # Touch the file.
    files.Touch(**kwargs)
  elif delete:
    # Mark this file as deleted in the version control system.
    kwargs['delete'] = True
    # Transform Delete(paths=[]) into multiple Write(path, delete=True) calls.
    paths = kwargs['paths']
    is_multiple = hasattr(paths, '__iter__')
    kwargs['_delete_old_blob'] = False
    del kwargs['paths']
    for path in paths if is_multiple else [paths]:
      files.Write(path=path, **kwargs)

  # Indicate that this specific changeset object has been used for all file
  # operations and can be trusted for strong consistency guarantees.
  changeset.FinalizeAssociatedPaths()

  return vcs.Commit(changeset)
