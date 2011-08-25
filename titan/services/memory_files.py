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

import logging
import os
from google.appengine.api import memcache
from titan.common import hooks
from titan.files import files

SERVICE_NAME = 'memory-files'
DEFAULT_MEMCACHE_NAMESPACE = 'titan-memory-files'

# Use a global to store a mapping of paths to files._File entities in local
# app server memory.
_global_file_ents = {}
_global_memory_files_version = 0

# The "RegisterService" method is required for all Titan service plugins.
def RegisterService():
  hooks.RegisterHook(SERVICE_NAME, 'file-get', hook_class=HookForGet)
  hooks.RegisterHook(SERVICE_NAME, 'file-read', hook_class=HookForRead)
  hooks.RegisterHook(SERVICE_NAME, 'file-write', hook_class=HookForWrite)
  hooks.RegisterHook(SERVICE_NAME, 'file-touch', hook_class=HookForTouch)
  hooks.RegisterHook(SERVICE_NAME, 'file-delete', hook_class=HookForDelete)

class HookForGet(hooks.Hook):
  """A hook for files.Get()."""

  def Pre(self, **kwargs):
    paths = files.ValidatePaths(kwargs['paths'])
    file_ents_map = _GetGlobalEntities(paths)

    # If no paths matched for serving from globals, continue as normal.
    if not file_ents_map:
      return

    # TODO(user): after refactor of Get() returns, this will need to change
    # to pass the core method a dictionary for the 'file_ents' arg.
    # For now, since we know this is only one entity, pull it out.
    file_ent = file_ents_map.values()[0]

    # Pass the global file_ents to the core Titan method, avoiding further RPCs.
    return {'file_ents': file_ent}

class HookForRead(hooks.Hook):
  """A hook for files.Read()."""

  def Pre(self, **kwargs):
    path = files.ValidatePaths(kwargs['path'])
    file_ents_map = _GetGlobalEntities(path)

    # If no paths matched for serving from globals, continue as normal.
    if not file_ents_map:
      return

    file_ent = file_ents_map.values()[0]
    return {'file_ent': file_ent}

class HookForWrite(hooks.Hook):
  """A hook for files.Write()."""

  def Pre(self, **kwargs):
    path = files.ValidatePaths(kwargs['path'])
    _EvictGlobalEntities(path)

class HookForDelete(hooks.Hook):
  """A hook for files.Delete()."""

  def Pre(self, **kwargs):
    paths = files.ValidatePaths(kwargs['paths'])
    _EvictGlobalEntities(paths)

class HookForTouch(hooks.Hook):
  """A hook for files.Touch()."""

  def Pre(self, **kwargs):
    paths = files.ValidatePaths(kwargs['paths'])
    _EvictGlobalEntities(paths)

def _EvictGlobalEntities(paths):
  """Evict the given paths from the globals cache."""
  if not _AnyPathMatchesConfig(paths):
    return
  # If version is not set (normal eviction), start it at 0 and increment
  # below. add() only has an effect if the item doesn't exist in memcache.
  memcache.add('version', 0, namespace=DEFAULT_MEMCACHE_NAMESPACE)

  # Update cache version number, signifying to other appservers that
  # their globals cache is stale and to clear their local cache.
  new_version = memcache.incr('version',
                              namespace=DEFAULT_MEMCACHE_NAMESPACE)

  # Also, clear the current appserver's local cache and version counter.
  _global_file_ents.clear()
  if new_version is not None:
    global _global_memory_files_version
    _global_memory_files_version = new_version
    os.environ['MEMORY_FILES_VERSION'] = str(new_version)
  else:
    logging.error('Unable to increment memory files version in memcache.')

def _AnyPathMatchesConfig(paths):
  """Check if any path in paths matches the config for using memory files."""
  paths = paths if hasattr(paths, '__iter__') else [paths]
  for path in paths:
    for regex in hooks.GetServiceConfig(SERVICE_NAME)['path_regexes']:
      if regex.match(path):
        return True
  return False

def _GetGlobalEntities(paths):
  """Return entities from in-memory cache if paths match the configured regexes.

  Will also fetch and store the entities if matched but not currently cached.

  Args:
    paths: An absolute path or iterable of absolute paths.
  Returns:
    A dictionary of {<path>: <file_ent>} for paths marked to come from globals.
  """
  paths_to_entities = {}

  # TODO(user): Add support for getting a mixed list of paths where
  # some of them are pulled from the globals and some are not.
  # For now, just return to follow the normal Get code path.
  if hasattr(paths, '__iter__'):
    return
  path = paths

  path_regexes = hooks.GetServiceConfig(SERVICE_NAME)['path_regexes']
  for regex in path_regexes:
    if not regex.match(path):
      continue
    if not _ValidateGlobalCache():
      return
    # This file is special. Get it from the global app memory, and if it
    # isn't there yet, put it there.
    file_ent = _global_file_ents.get(path, None)
    if not file_ent:
      file_ent, _ = files._GetFiles(path)
      _global_file_ents[path] = file_ent
    paths_to_entities[path] = file_ent
  return paths_to_entities

def _ValidateGlobalCache():
  """Make sure our globals cache is up to date and usable.

  Returns:
    True: if the globals cache is valid and up to date.
    False: if unknown state and globals shouldn't be used.
  """
  if 'MEMORY_FILES_VERSION' in os.environ:
    memory_files_version = int(os.environ['MEMORY_FILES_VERSION'])
  else:
    # This is the first Titan file read in the request path. Pull the current
    # version counter from memcache and store in an os.environ var to avoid
    # any further memcache calls for it. In the best case, this memcache call
    # is the only one ever needed and all file entities come from the globals.
    memory_files_version = memcache.get('version',
                                        namespace=DEFAULT_MEMCACHE_NAMESPACE)

    # If the memcache counter has been evicted, we are in an unknown state.
    # Return False to signify the global cache is stale and also shouldn't
    # be populated since we don't know which version to associate. This unknown
    # state will be fixed the next time a Write() of a memory file happens.
    if not memory_files_version:
      return False

  if memory_files_version != _global_memory_files_version:
    # This appserver has a stale global cache. Evict all the entities!
    _global_file_ents.clear()

  global _global_memory_files_version
  _global_memory_files_version = memory_files_version
  os.environ['MEMORY_FILES_VERSION'] = str(memory_files_version)

  return True
