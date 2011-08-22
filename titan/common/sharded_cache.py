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

"""API for storing objects larger than 1MB in App Engine memcache.

This may be useful for relatively small blobstore entities which need to
be read into app memory often, but can't be cached due to size limits.

This module should not be used with very large objects, keeping in mind the
32 MB limit of memcache.set_multi. Also, this module is capable of caching
objects smaller than 1MB, but it is less efficient at it.
"""

import cPickle as pickle
import logging
from google.appengine.api import memcache

NAMESPACE = 'sharded'

# The default amount of time to cache data.
# Setting a default expiration on all data will automatically cleanup shards
# which have been orphaned (such as by eviction of the shard_map, a Set() with
# smaller content that gets fewer shards, etc.).
DEFAULT_EXPIRATION_SECONDS = 24 * 60 * 60

def Get(key, namespace=None):
  """Get a memcache entry, or None."""
  namespace = NAMESPACE + (namespace or '')
  shard_map = memcache.get(key, namespace=namespace)
  if not shard_map:
    # The shard_map was evicted or never set.
    return

  num_shards = shard_map['num_shards']
  keys = ['%s%d' % (key, i) for i in range(num_shards)]
  shards = memcache.get_multi(keys, namespace=namespace)
  if len(shards) != num_shards:
    # One or more content shards were evicted, delete map and content shards.
    memcache.delete_multi([key] + keys, namespace=namespace)
    return

  # All shards present, stitch contents back together and unpickle.
  shards = tuple([shards[key] for key in keys])
  value = '%s' * shard_map['num_shards']
  value = pickle.loads(value % shards)
  return value

def Set(key, value, time=DEFAULT_EXPIRATION_SECONDS, namespace=None):
  """Set a memcache entry."""
  namespace = NAMESPACE + (namespace or '')
  value = pickle.dumps(value)

  # The original key is used as the shard map.
  # The content shards are stored as '<key>0', '<key>1', etc.
  num_shards = (len(value) / memcache.MAX_VALUE_SIZE) + 1
  content_map = {}
  content_map[key] = {'num_shards': num_shards}
  for i in range(num_shards):
    # [0:1MB] first, [1MB:2MB] second, etc.
    begin_slice = i * memcache.MAX_VALUE_SIZE
    end_slice = begin_slice + memcache.MAX_VALUE_SIZE
    content_map[key + str(i)] = value[begin_slice:end_slice]

  # Set the shard map and all content shards.
  failed_keys = memcache.set_multi(content_map, time=time, namespace=namespace)
  if failed_keys:
    logging.error('Sharded cache set_multi failed. Keys: %r', failed_keys)
    if not memcache.delete_multi(failed_keys, namespace=namespace):
      logging.error('Sharded cache delete_multi failed, Keys: %r', failed_keys)
  return bool(failed_keys)

def Delete(key, seconds=0, namespace=None):
  """Delete a memcache entry."""
  namespace = NAMESPACE + (namespace or '')
  shard_map = memcache.get(key, namespace=namespace)
  if not shard_map:
    # The shard_map was evicted or never set.
    return memcache.DELETE_ITEM_MISSING
  keys = [key] + ['%s%d' % (key, i) for i in range(shard_map['num_shards'])]
  return memcache.delete_multi(keys, seconds=seconds, namespace=namespace)
