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

"""A convenience wrapper for internal Titan memcache operations."""

import collections
import logging
import os
from google.appengine.api import memcache
from titan.common import sharded_cache

# Pseudo namespaces for memcache values.
FILE_MEMCACHE_PREFIX = 'titan-file:'
BLOB_MEMCACHE_PREFIX = 'titan-blob:'
DIR_MEMCACHE_PREFIX = 'titan-dir:'

# The flag to store in memcache signifying that a file doesn't exist.
_NO_FILE_FLAG = False

def GetFiles(paths):
  """Given paths, get _File entities (or Nones) if each file state is cached.

  Args:
    paths: Absolute filename or iterable of absolute filenames.
  Returns:
    On cache hit: A tuple of (<Entity or list of entities>, True).
    On cache miss: (None, False)
  """
  is_multiple = hasattr(paths, '__iter__')
  if is_multiple:
    cache_keys = [FILE_MEMCACHE_PREFIX + path for path in paths]

    try:
      file_ents = memcache.get_multi(cache_keys)
    except AttributeError:
      memcache.delete_multi(cache_keys)
      logging.exception('Possibly corrupt memcache values (%r).', cache_keys)
      return None, False

    # Cache miss: if less keys are returned than were sent.
    if len(file_ents) != len(paths):
      return None, False
    # Turn memcache dictionary back into correctly-ordered list,
    # and replace files flagged as non-existent with None.
    file_ents = [file_ents[key] or None for key in cache_keys]
  else:
    cache_key = FILE_MEMCACHE_PREFIX + paths
    try:
      file_ents = memcache.get(cache_key)
    except AttributeError:
      memcache.delete(cache_key)
      logging.exception('Possibly corrupt memcache value (%r).', cache_key)
      return None, False

    # Cache miss:
    if file_ents is None:
      return None, False
  # We can reliably return NoneType when a file is flagged in cache as
  # non-existent. Return a var to distinguish this from a cache miss.
  return file_ents, True

def StoreFiles(file_ents):
  """Store the given _File entities in memcache."""
  is_multiple = hasattr(file_ents, '__iter__')
  # Require that all entity objects exist before setting.
  if not is_multiple and not file_ents or is_multiple and not all(file_ents):
    raise ValueError('Attempting to set invalid entities. Got: %s' % file_ents)
  if is_multiple:
    data = {}
    for file_ent in file_ents:
      cache_key = FILE_MEMCACHE_PREFIX + file_ent.path
      data[cache_key] = file_ent
    return memcache.set_multi(data)
  else:
    cache_key = FILE_MEMCACHE_PREFIX + file_ents.path
    return memcache.set(cache_key, file_ents)

def StoreAll(data):
  """Store either file entities or flag files as non-existent.

  Args:
    data: a dictionary with path keys and either _File entity or None values.
        Paths with a None value will be marked as non-existent.
  Returns:
    The result of memcache.set_multi().
  """
  values = {}
  for key, value in data.iteritems():
    cache_key = FILE_MEMCACHE_PREFIX + key
    values[cache_key] = value if value else _NO_FILE_FLAG
  return memcache.set_multi(values)

def SetFileDoesNotExist(paths):
  """Set a flag signifying that the given _File entities do not exist."""
  is_multiple = hasattr(paths, '__iter__')
  if is_multiple:
    data = [(FILE_MEMCACHE_PREFIX + path, _NO_FILE_FLAG) for path in paths]
    data = dict(data)
    return memcache.set_multi(data)
  else:
    cache_key = FILE_MEMCACHE_PREFIX + paths
    return memcache.set(cache_key, _NO_FILE_FLAG)

def GetBlob(path):
  """Get a blob's content from the sharded cache."""
  cache_key = BLOB_MEMCACHE_PREFIX + path
  return sharded_cache.Get(cache_key)

def StoreBlob(path, content):
  """Set a blob's content in the sharded cache."""
  cache_key = BLOB_MEMCACHE_PREFIX + path
  return sharded_cache.Set(cache_key, content)

def ClearBlobsForFiles(file_ents):
  """Delete blobs from the sharded cache."""
  files_list = file_ents if hasattr(file_ents, '__iter__') else [file_ents]
  for file_ent in files_list:
    cache_key = BLOB_MEMCACHE_PREFIX + file_ent.path
    sharded_cache.Delete(cache_key)

def StoreSubdirs(data):
  """Store the full list of subdirectories for given directories.

  Args:
    data: A mapping of absolute directory paths to complete lists of subdirs.
        The subdir list should be strings of relative subdirectory names.
  Returns:
    The result of memcache.set_multi().
  """
  dir_cache_keys = [DIR_MEMCACHE_PREFIX + dir_path for dir_path in data]
  dir_caches = memcache.get_multi(dir_cache_keys)
  for key, value in data.iteritems():
    cache_key = DIR_MEMCACHE_PREFIX + key
    if cache_key not in dir_caches:
      dir_caches[cache_key] = {}
    dir_caches[cache_key]['subdirs'] = set(value)
  return memcache.set_multi(dir_caches)

def GetSubdirs(dir_path):
  """Get a set of subdirs in a directory."""
  dir_cache = memcache.get(DIR_MEMCACHE_PREFIX + dir_path)
  if dir_cache is None or 'subdirs' not in dir_cache:
    return
  return dir_cache['subdirs']

def UpdateSubdirsForFiles(file_ents):
  """For file entities, update appropriate subdir cache lists."""
  # Example:
  #   A file named "/foo/bar/index.html" is added. It has a paths list of
  #   ['/', '/foo', '/foo/bar']. For the cache entry "dir:/", we need make sure
  #   "foo" is in its value set, and same for "bar" in the "dir:/foo" cache.
  #
  # Get the dir caches, update their subdir lists, and multi set them back in.
  dir_cache_changes = _GetDirCacheChangesForFiles(file_ents)
  dir_caches = memcache.get_multi(dir_cache_changes.keys())
  for dir_cache_key, values_to_change in dir_cache_changes.iteritems():
    # Because we only have a subdir delta, only update subdir lists that are
    # currently cached. Otherwise, sibling subdirs will be lost.
    if dir_cache_key in dir_caches and 'subdirs' in dir_caches[dir_cache_key]:
      dir_caches[dir_cache_key]['subdirs'].update(values_to_change)
  if dir_caches:
    return memcache.set_multi(dir_caches)

def ClearSubdirsForFiles(file_ents):
  """Clears the affected subdir caches after file deletion."""
  dir_cache_changes = _GetDirCacheChangesForFiles(file_ents)
  dir_caches = memcache.get_multi(dir_cache_changes.keys())
  for dir_cache_key in dir_caches:
    try:
      del dir_caches[dir_cache_key]['subdirs']
    except KeyError:
      pass
  return memcache.set_multi(dir_caches)

def _GetDirCacheChangesForFiles(file_ents):
  """Makes a dictionary of dir cache keys to list of changed subdirs."""
  dir_cache_changes = collections.defaultdict(set)
  files_list = file_ents if hasattr(file_ents, '__iter__') else [file_ents]
  for file_ent in files_list:
    last_index = len(file_ent.paths) - 1
    for i, dir_path in enumerate(file_ent.paths):
      if i != last_index:
        subdir_name = os.path.split(file_ent.paths[i + 1])[1]
        dir_cache_changes[DIR_MEMCACHE_PREFIX + dir_path].add(subdir_name)
  return dir_cache_changes
