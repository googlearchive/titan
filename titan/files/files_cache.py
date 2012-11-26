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

"""Convenience functions for internal Titan memcache operations."""

import collections
import logging
import os
from google.appengine.api import memcache
from titan.common import sharded_cache

BLOB_MEMCACHE_PREFIX = 'titan-blob:'

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
