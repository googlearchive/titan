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

"""Allow configuration of which services will run for specific paths.

WARNING: Avoid performing operations which affect mixed paths. For example,
if you configure a service to only run on '/foo', and then perform
files.Get(['/foo/bar', '/baz']), the service will also be run for the '/baz'
path and may have unintended consequences.

Documentation:
  http://code.google.com/p/titan-files/wiki/PathLimitingService
"""

from titan.common import hooks
from titan.files import files

SERVICE_NAME = 'titan-path-limiting'

def RegisterService():
  hooks.RegisterHook(SERVICE_NAME, 'file-exists', hook_class=SinglePathHook)
  hooks.RegisterHook(SERVICE_NAME, 'file-get', hook_class=MultiplePathsHook)
  hooks.RegisterHook(SERVICE_NAME, 'file-write', hook_class=SinglePathHook)
  hooks.RegisterHook(SERVICE_NAME, 'file-delete', hook_class=MultiplePathsHook)
  hooks.RegisterHook(SERVICE_NAME, 'file-touch', hook_class=MultiplePathsHook)
  hooks.RegisterHook(SERVICE_NAME, 'file-copy', hook_class=HookForCopy)
  hooks.RegisterHook(SERVICE_NAME, 'copy-dir', hook_class=HookForCopyDir)
  hooks.RegisterHook(SERVICE_NAME, 'list-files', hook_class=DirPathHook)
  hooks.RegisterHook(SERVICE_NAME, 'list-dir', hook_class=DirPathHook)
  hooks.RegisterHook(SERVICE_NAME, 'dir-exists', hook_class=DirPathHook)

class SinglePathHook(hooks.Hook):
  """Hook for all operations which pass "path"."""

  def Pre(self, **kwargs):
    """Pre-hook method."""
    return _ComposeDisabledServices(kwargs['path'], kwargs)

class MultiplePathsHook(hooks.Hook):
  """Hook for all operations which pass "paths"."""

  def Pre(self, **kwargs):
    """Pre-hook method."""
    return _ComposeDisabledServices(kwargs['paths'], kwargs)

class HookForCopy(hooks.Hook):
  """Hook for files.Copy()."""

  def Pre(self, **kwargs):
    """Pre-hook method."""
    paths = (kwargs['source_path'], kwargs['destination_path'])
    return _ComposeDisabledServices(paths, kwargs)

class HookForCopyDir(hooks.Hook):
  """Hook for files.CopyDir()."""

  def Pre(self, **kwargs):
    paths = (kwargs['source_dir_path'], kwargs['destination_dir_path'])
    return _ComposeDisabledServices(paths, kwargs)

class DirPathHook(hooks.Hook):
  """Hook for all operations which pass "dir_path"."""

  def Pre(self, **kwargs):
    """Pre-hook method."""
    return _ComposeDisabledServices(kwargs['dir_path'], kwargs)

def _ComposeDisabledServices(paths, original_kwargs):
  """Figure out which services to disable based on the given paths."""
  paths = paths if hasattr(paths, '__iter__') else [paths]
  paths = files.ValidatePaths(paths)
  disabled_services = set(original_kwargs.get('disabled_services', []))

  path_limits_config = hooks.GetServiceConfig(SERVICE_NAME)
  services_whitelist = path_limits_config['services_whitelist']

  for service_name, path_regex in services_whitelist.iteritems():
    any_paths_match = False
    for path in paths:
      if path_regex.match(path):
        any_paths_match = True
        break
    # If any paths matched the whitelist, we leave the service enabled.
    # Otherwise, if no paths matched, we disable it.
    if not any_paths_match:
      disabled_services.add(service_name)

  if disabled_services:
    return {'disabled_services': disabled_services}
