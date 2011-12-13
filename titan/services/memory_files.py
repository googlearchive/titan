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

"""Service to cache specific Titan files in appserver memory.

Documentation:
  http://code.google.com/p/titan-files/wiki/MemoryFilesService
"""

import os
from titan.common import datastructures
from titan.common import hooks
from titan.files import files

SERVICE_NAME = 'memory-files'

# The default size of the MRU dictionary. This is how many File objects will be
# allowed in the global cache at a time.
# TODO(user): Allow this to be customized by a service config setting.
DEFAULT_MRU_SIZE = 100

# Map of paths to File objects.
# This is used for request-local storage of File objects to prevent repeated
# RPCs, at the expense of only having request-level consistency of file read
# operations. Each new request clears this object (not really thread-safe).
# TODO(user): Figure out how to have true request-local storage in 2.7.
_global_file_objs = datastructures.MRUDict(max_size=DEFAULT_MRU_SIZE)
_global_current_request_hash = ''

# The "RegisterService" method is required for all Titan service plugins.
def RegisterService():
  hooks.RegisterHook(SERVICE_NAME, 'file-get', hook_class=HookForGet)
  hooks.RegisterHook(SERVICE_NAME, 'file-exists', hook_class=HookForExists)
  hooks.RegisterHook(SERVICE_NAME, 'file-write', hook_class=HookForWrites)
  hooks.RegisterHook(SERVICE_NAME, 'file-touch', hook_class=HookForWrites)
  hooks.RegisterHook(SERVICE_NAME, 'file-delete', hook_class=HookForWrites)
  hooks.RegisterHook(SERVICE_NAME, 'file-copy', hook_class=HookForCopy)

class HookForGet(hooks.Hook):
  """A hook for files.Get()."""

  def Pre(self, **kwargs):
    paths = files.ValidatePaths(kwargs['paths'])
    paths, is_final_result = _Get(paths)
    if is_final_result:
      return hooks.TitanMethodResult(paths)
    return {'paths': paths}

class HookForExists(hooks.Hook):
  """A hook for files.Exists()."""

  def Pre(self, **kwargs):
    path = files.ValidatePaths(kwargs['path'])
    path, is_final_result = _Get(path)
    if is_final_result:
      return hooks.TitanMethodResult(path)
    return {'path': path}

class HookForWrites(hooks.Hook):
  """A hook for files.Write(), files.Delete(), and files.Touch()."""

  def Pre(self, **kwargs):
    paths = files.ValidatePaths(kwargs.get('path', kwargs.get('paths')))
    _Clear(paths)

class HookForCopy(hooks.Hook):
  """A hook for files.Copy()."""

  def Pre(self, **kwargs):
    paths = files.ValidatePaths(kwargs['destination_path'])
    _Clear(paths)

def _Get(paths):
  """Get File objects from paths, and populate the global cache."""
  is_multiple = hasattr(paths, '__iter__')

  # Important: if this is the beginning of a new request, clear the previous
  # request's file_objs so we never use them.
  global _global_current_request_hash
  if _global_current_request_hash != os.environ['REQUEST_ID_HASH']:
    _global_file_objs.clear()
    _global_current_request_hash = os.environ['REQUEST_ID_HASH']

  paths_set = set(paths if is_multiple else [paths])
  cached_file_keys = set(_global_file_objs.keys())
  cached_paths = list(cached_file_keys & paths_set)
  uncached_paths = list(paths_set - cached_file_keys)

  if cached_paths and not uncached_paths:
    # All of the requested files are currently in the cache; return this subset.
    file_objs = dict(
        (k, _global_file_objs[k]) for k in cached_paths if _global_file_objs[k])
    return __NormalizeResult(file_objs, is_multiple)

  # Cache new files in the global cache.
  file_objs = files.Get(uncached_paths, disabled_services=True)
  _global_file_objs.update(file_objs)

  # Also store Nones, so that non-existent files are not re-fetched.
  for path in uncached_paths:
    if path not in file_objs:
      _global_file_objs[path] = None

  # Merge file objects which existed in the global cache into the result.
  for path in cached_paths:
    if path not in _global_file_objs:
      # Path may have been evicted by above sets, cached_paths is out of date.
      continue
    # This affects the MRUDict, so only grab the value once.
    value = _global_file_objs[path]
    if value:
      file_objs[path] = value

  return __NormalizeResult(file_objs, is_multiple)

def _Clear(paths):
  """Remove paths from the global cache."""
  is_multiple = hasattr(paths, '__iter__')
  paths_list = paths if is_multiple else [paths]
  for path in paths_list:
    if path in _global_file_objs:
      del _global_file_objs[path]

def __NormalizeResult(file_objs, is_multiple):
  """Handle all result cases including multiple paths and non-existent paths."""
  # Return should be compatible with the path arg to files.Get(), or
  # should give is_final_result=True to signal wrapper to use TitanMethodResult.
  is_final_result = False
  if is_multiple:
    result = file_objs.values()
  else:
    result = file_objs.values()[0] if file_objs else None
  if not result:
    # Non-existent result in cache or otherwise. Signal a short-circuit return.
    is_final_result = True
    result = {} if is_multiple else None
  return result, is_final_result
