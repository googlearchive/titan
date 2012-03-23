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
DEFAULT_MRU_SIZE = 300

_ENVIRON_FILES_STORE_NAME = 'titan-memory-files-store'

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
    file_objs, is_final_result = _Get(paths)
    if is_final_result:
      return hooks.TitanMethodResult(file_objs)
    return {'paths': file_objs}

class HookForExists(hooks.Hook):
  """A hook for files.Exists()."""

  def Pre(self, **kwargs):
    path = files.ValidatePaths(kwargs['path'])
    file_obj, is_final_result = _Get(path)
    if is_final_result:
      return hooks.TitanMethodResult(bool(file_obj))
    return {'path': file_obj}

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

def _GetRequestLocalFilesStore():
  """Returns a request-local MRUDict mapping paths to File objects."""
  # os.environ is replaced by the runtime environment with a request-local
  # object, allowing non-string types to be stored globally in the environment
  # and automatically cleaned up at the end of each request.
  if _ENVIRON_FILES_STORE_NAME not in os.environ:
    local_files_store = datastructures.MRUDict(max_size=DEFAULT_MRU_SIZE)
    os.environ[_ENVIRON_FILES_STORE_NAME] = local_files_store
  return os.environ[_ENVIRON_FILES_STORE_NAME]

def _Get(paths):
  """Get File objects from paths, and populate the global cache."""
  is_multiple = hasattr(paths, '__iter__')
  local_files_store = _GetRequestLocalFilesStore()

  paths_set = set(paths if is_multiple else [paths])
  cached_file_keys = set(local_files_store.keys())
  cached_paths = list(cached_file_keys & paths_set)
  uncached_paths = list(paths_set - cached_file_keys)

  if cached_paths and not uncached_paths:
    # All of the requested files are currently in the cache; return this subset.
    file_objs = dict(
        (k, local_files_store[k]) for k in cached_paths if local_files_store[k])
    return __NormalizeResult(file_objs, is_multiple)

  # Merge file objects which existed in the global cache into the result.
  # Order is important here: these need to be added before the uncached_paths
  # are merged in.
  file_objs = {}
  for path in cached_paths:
    # This affects the MRUDict, so only grab the value once.
    value = local_files_store[path]
    if value:  # The cached calue could be None, meaning the file doesn't exist.
      file_objs[path] = value

  new_file_objs = files.Get(uncached_paths, disabled_services=True)
  local_files_store.update(new_file_objs)
  file_objs.update(new_file_objs)

  # Also store Nones, so that non-existent files are not re-fetched.
  for path in uncached_paths:
    if path not in new_file_objs:
      local_files_store[path] = None

  return __NormalizeResult(file_objs, is_multiple)

def _Clear(paths):
  """Remove paths from the global cache."""
  is_multiple = hasattr(paths, '__iter__')
  local_files_store = _GetRequestLocalFilesStore()
  paths_list = paths if is_multiple else [paths]
  for path in paths_list:
    if path in local_files_store:
      del local_files_store[path]

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
