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

"""Temporarily use a different namespace to perform file operations.

Documentation:
  http://code.google.com/p/titan-files/wiki/NamespacesService

Usage:
  files.Write('/path/to/file.txt', 'tmp!', namespace='tmp')
  files.Get('/path/to/file.txt', namespace='tmp').content
    => 'tmp!'
"""

from google.appengine.api import namespace_manager
from titan.common import hooks

SERVICE_NAME = 'namespaces'
HOOK_METHODS = (
    'file-exists',
    'file-get',
    'file-write',
    'file-delete',
    'file-touch',
    'list-files',
    'list-dir',
    'dir-exists',
)

# The "RegisterService" method is required for all Titan service plugins.
def RegisterService():
  for method in HOOK_METHODS:
    hooks.RegisterHook(SERVICE_NAME, method, hook_class=NamespaceHook)

class NamespaceHook(hooks.Hook):
  """Base Hook class for tmp files."""

  def Pre(self, namespace=None, **kwargs):
    self.namespace = namespace
    if namespace:
      self.original_namespace = namespace_manager.get_namespace()
      namespace_manager.set_namespace(namespace)
    else:
      self.original_namespace = None

  def Post(self, results):
    self._RevertNamespace()
    return results

  def OnError(self, unused_error):
    self._RevertNamespace()

  def _RevertNamespace(self):
    if self.namespace:
      namespace_manager.set_namespace(self.original_namespace)
