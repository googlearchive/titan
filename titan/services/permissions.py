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

"""Basic file permissions layer.

Documentation:
  http://code.google.com/p/titan-files/wiki/Services#Permissions_service

Usage:
  permissions = permissions.Permissions(read_users=['bob@example.com'],
                                        write_users=['alice@example.com'])
  files.Write('/some/file.html', 'content', permissions=permissions)
"""

from google.appengine.api import users
from titan.common import hooks
from titan.files import files

SERVICE_NAME = 'permissions'

class PermissionsError(IOError):
  pass

# The "RegisterService" method is required for all Titan service plugins.
def RegisterService():
  # TODO(user): Add allow_override=False functionality to service layer
  # hooks. Until then, don't expose services_override in handlers.py.
  hooks.RegisterHook(SERVICE_NAME, 'file-get', hook_class=HookForGet)
  hooks.RegisterHook(SERVICE_NAME, 'file-read', hook_class=HookForRead)
  hooks.RegisterHook(SERVICE_NAME, 'file-write', hook_class=HookForWrite)
  hooks.RegisterHook(SERVICE_NAME, 'file-touch', hook_class=HookForTouch)
  hooks.RegisterHook(SERVICE_NAME, 'file-delete', hook_class=HookForDelete)

class Permissions(object):
  """Container for holding sets of read and write usernames."""

  def __init__(self, read_users=None, write_users=None):
    self.read_users = set() if read_users is None else set(read_users)
    self.write_users = set() if write_users is None else set(write_users)

# Allow unused arguments since hook methods must support base method arguments.

class HookForGet(hooks.Hook):
  """Hook for files.Get()."""

  def Pre(self, paths, file_ents=None, user=None, **kwargs):
    new_kwargs = hooks.MakeKeywordArgs(locals(),
                                       base_args=['paths', 'file_ents'])
    self.user = user
    return new_kwargs

  def Post(self, file_objs):
    """For every File returned, verify that the user has read permissions."""
    _VerifyPermissions(file_objs, user=self.user, read=True)
    return file_objs

class HookForRead(hooks.Hook):
  """Hook for files.Read()."""

  def Pre(self, path, file_ent=None, user=None, **kwargs):
    """Pre hook to verify that a user has read permissions over a file."""
    new_kwargs = hooks.MakeKeywordArgs(locals(), base_args=['path', 'file_ent'])

    # Verify that the user has read permissions.
    path = files.ValidatePaths(path)
    if not file_ent:
      file_ent, _ = files._GetFiles(path)
    _VerifyPermissions(file_ent, user=user, read=True)

    # Pass the file entity to the next layer to avoid duplicate RPCs.
    new_kwargs['file_ent'] = file_ent
    return new_kwargs

class HookForWrite(hooks.Hook):
  """Hook for files.Write()."""

  def Pre(self, path, content=None, blobs=None, mime_type=None, meta=None,
          async=False, permissions=None, file_ent=None, user=None, **kwargs):
    """Pre hook for files.Write()."""
    base_args = ['path', 'content', 'blobs', 'mime_type', 'meta', 'async',
                 'file_ent']
    new_kwargs = hooks.MakeKeywordArgs(locals(), base_args=base_args)

    self.permissions = permissions

    # Check write permissions.
    path = files.ValidatePaths(path)
    if not file_ent:
      file_ent, _ = files._GetFiles(path)
    _VerifyPermissions(file_ent, user, write=True)

    # Pass the file entity to the next layer to avoid duplicate RPCs.
    new_kwargs['file_ent'] = file_ent

    # If changing permissions, update the permission meta properties.
    if permissions:
      new_kwargs['meta'] = {} if meta is None else meta
      new_kwargs['meta'].update({
          'permissions_read_users': list(permissions.read_users) or None,
          'permissions_write_users': list(permissions.write_users) or None,
      })
    return new_kwargs

class HookForDelete(hooks.Hook):
  """Hook for files.Delete()."""

  def Pre(self, paths, async=False, file_ents=None, update_subdir_caches=False,
          user=None, **kwargs):
    """Pre hook for files.Delete()."""
    base_args = ['paths', 'async', 'file_ents', 'update_subdir_caches']
    new_kwargs = hooks.MakeKeywordArgs(locals(), base_args=base_args)

    if not file_ents:
      file_ents, _ = files._GetFilesOrDie(paths)
    _VerifyPermissions(file_ents, user, write=True)

    # Pass the file entities to the next layer to avoid duplicate RPCs.
    new_kwargs['file_ents'] = file_ents
    return new_kwargs

class HookForTouch(hooks.Hook):
  """Hook for files.Touch()."""

  def Pre(self, paths, async=False, file_ents=None, user=None, **kwargs):
    """Pre hook for files.Touch()."""
    base_args = ['paths', 'async', 'file_ents']
    new_kwargs = hooks.MakeKeywordArgs(locals(), base_args=base_args)

    paths = files.ValidatePaths(paths)
    if not file_ents:
      file_ents, _ = files._GetFiles(paths)
    _VerifyPermissions(file_ents, user, write=True)

    # Pass the file entities to the next layer to avoid duplicate RPCs.
    new_kwargs['file_ents'] = file_ents
    return new_kwargs

def _VerifyPermissions(file_ents, user, read=False, write=False):
  """Check user access over file_ents, verifying ability read and/or write."""
  if not hasattr(file_ents, '__iter__'):
    file_ents = [file_ents]

  have_evaluated_user = False
  for file_ent in file_ents:
    if not file_ent:
      # If a file entity doesn't exist, default permissions are open.
      continue

    read_users = getattr(file_ent, 'permissions_read_users', None)
    write_users = getattr(file_ent, 'permissions_write_users', None)
    if read_users and write_users:
      read_users = set(read_users).union(set(write_users))

    check_permissions = bool(read and read_users or write and write_users)
    if not user and not have_evaluated_user and check_permissions:
      # We are checking permissions, fetch the current user.
      if users.is_current_user_admin():
        # Allow app-level admins to do anything.
        return
      user = users.get_current_user()
      user = user and user.email()
      have_evaluated_user = True

    if read and read_users and user not in read_users:
      raise PermissionsError(
          'Permission denied: "%s" to read Titan File "%s".'
          % (user, file_ent.path))
    if write and write_users and user not in write_users:
      raise PermissionsError(
          'Permission denied: "%s" to write Titan File: "%s".'
          % (user, file_ent.path))
