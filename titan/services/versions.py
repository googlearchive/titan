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

"""Titan file versioning, including atomic commits of Titan file changesets.

This service may be useful for:
- Versioning changes to Titan files.
- Changing a group of files instantaneously, especially if the files are
  in a serving path and changing them serially would cause an inconsistent
  user-visible state.
"""

from titan.common import hooks

SERVICE_NAME = 'versions'

# The "RegisterService" method is required for all Titan service plugins.
def RegisterService():
  hooks.RegisterHook(SERVICE_NAME, 'file-write', hook_class=HookForWrite)
  hooks.RegisterHook(SERVICE_NAME, 'file-get', hook_class=HookForGet)

class HookForWrite(hooks.Hook):
  """A hook for files.Write()."""

  def Pre(self, path, content=None, blobs=None, mime_type=None, meta=None,
          async=False, changeset=None, **kwargs):
    raise NotImplementedError

class HookForGet(hooks.Hook):
  """A hook for files.Get()."""

  def Pre(self, paths, **kwargs):
    raise NotImplementedError

