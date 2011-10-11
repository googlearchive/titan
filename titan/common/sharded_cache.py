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
32 MB limit of memcache.set_multi.
"""

import cPickle as pickle
import logging
from google.appengine.api import memcache

# Pseudo namespace for memcache values.
MEMCACHE_PREFIX = 'sharded:'

# The default amount of time to cache data.
# Setting a default expiration on all data will automatically cleanup shards
# which have been orphaned (such as by eviction of the shard_map, a Set() with
# smaller content that gets fewer shards, etc.).
DEFAULT_EXPIRATION_SECONDS = 24 * 60 * 60  # 1 day

# Cutoff for when content will be sharded into multiple memcache entries.
# Content less than this size will be stored with the shard_map dictionary and
# only use a single memcache item total. This value needs to leave room for the
# max number of bytes of the pickled shard_map dict (without content).
MIN_SHARDING_SIZE = memcache.MAX_VALUE_SIZE - 1000  # 999 KB

def Get(key):
  """Get a memcache entry, or None."""
  key = MEMCACHE_PREFIX + key
  shard_map = memcache.get(key)
  if not shard_map:
    # The shard_map was evicted or never set.
    return

  # If zero shards, the content was small enough and stored in the shard_map.
  num_shards = shard_map['num_shards']
  if num_shards == 0:
    return pickle.loads(shard_map['content'])

  keys = ['%s%d' % (key, i) for i in range(num_shards)]
  shards = memcache.get_multi(keys)
  if len(shards) != num_shards:
    # One or more content shards were evicted, delete map and content shards.
    memcache.delete_multi([key] + keys)
    return

  # All shards present, stitch contents back together and unpickle.
  shards = tuple([shards[key] for key in keys])
  value = '%s' * shard_map['num_shards']
  value = pickle.loads(value % shards)
  return value

def Set(key, value, time=DEFAULT_EXPIRATION_SECONDS):
  """Set a memcache entry."""
  key = MEMCACHE_PREFIX + key
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

  # Optimization: for small content, store the content in the shard_map
  # dictionary directly instead of actually sharding.
  if num_shards == 1 and len(value) < MIN_SHARDING_SIZE:
    content_map[key]['num_shards'] = num_shards = 0
    content_map[key]['content'] = value
    del content_map[key + '0']

  # Set the shard map and all content shards.
  failed_keys = memcache.set_multi(content_map, time=time)
  if failed_keys:
    logging.error('Sharded cache set_multi failed. Keys: %r', failed_keys)
    if not memcache.delete_multi(failed_keys):
      logging.error('Sharded cache delete_multi failed, Keys: %r', failed_keys)
  return not bool(failed_keys)

def Delete(key, seconds=0):
  """Delete a memcache entry."""
  key = MEMCACHE_PREFIX + key
  shard_map = memcache.get(key)
  if not shard_map:
    # The shard_map was evicted or never set.
    return memcache.DELETE_ITEM_MISSING
  keys = [key] + ['%s%d' % (key, i) for i in range(shard_map['num_shards'])]
  return memcache.delete_multi(keys, seconds=seconds)
