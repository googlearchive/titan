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

"""Tests for sharded_cache.py."""

import cPickle as pickle
from google.appengine.api import memcache
from google.appengine.ext import testbed
from titan.common.lib.google.apputils import basetest
from titan.common import sharded_cache

MAX_VALUE_SIZE = memcache.MAX_VALUE_SIZE

# 1KB -- Should be packed with the shard_map entry and use 0 real shards.
SMALL_CONTENT = 'a' * 1000

# 2MB -- Will be slightly >2MB when pickled, so should be in 3 shards.
LARGE_CONTENT = 'b' * MAX_VALUE_SIZE * 2
LARGE_CONTENT_PICKLED = pickle.dumps(LARGE_CONTENT)

class ShardedCacheTest(basetest.TestCase):

  def setUp(self):
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_memcache_stub()

  def tearDown(self):
    self.testbed.deactivate()

  def testGet(self):
    sharded_cache.Set('foo', SMALL_CONTENT)
    data = sharded_cache.Get('foo')
    self.assertEqual(len(SMALL_CONTENT), len(data))
    self.assertEqual(SMALL_CONTENT, data)

    # No shards have been evicted.
    sharded_cache.Set('foo', LARGE_CONTENT)
    data = sharded_cache.Get('foo')
    self.assertEqual(len(LARGE_CONTENT), len(data))
    self.assertEqual(LARGE_CONTENT, data)

    # Shard map was evicted.
    sharded_cache.Set('foo', LARGE_CONTENT)
    memcache.delete(sharded_cache.MEMCACHE_PREFIX + 'foo')
    self.assertEqual(None, sharded_cache.Get('foo'))

    # 1 content shard was evicted.
    sharded_cache.Set('foo', LARGE_CONTENT)
    memcache.delete(sharded_cache.MEMCACHE_PREFIX + 'foo1')
    self.assertEqual(None, sharded_cache.Get('foo'))
    # The shard map and unevicted shards should be deleted.
    cache_keys = ['foo', 'foo0', 'foo1', 'foo2', 'foo3']
    memcache_keys = [sharded_cache.MEMCACHE_PREFIX + key for key in cache_keys]
    content = memcache.get_multi(memcache_keys)
    self.assertFalse(any(content))

    # All content shards were evicted.
    sharded_cache.Set('foo', LARGE_CONTENT)
    cache_keys = ['foo0', 'foo1', 'foo2']
    memcache_keys = [sharded_cache.MEMCACHE_PREFIX + key for key in cache_keys]
    memcache.delete_multi(memcache_keys)
    self.assertEqual(None, sharded_cache.Get('foo'))

  def testSet(self):
    # Set object smaller than 1MB.
    sharded_cache.Set('foo', SMALL_CONTENT)
    shard_map = memcache.get(sharded_cache.MEMCACHE_PREFIX + 'foo')
    self.assertEqual(0, shard_map['num_shards'])
    self.assertEqual(SMALL_CONTENT, pickle.loads(shard_map['content']))
    first_shard = memcache.get(sharded_cache.MEMCACHE_PREFIX + 'foo0')
    self.assertEqual(None, first_shard)

    # Set object larger than 1MB.
    sharded_cache.Set('foo', LARGE_CONTENT)
    shard_map = memcache.get(sharded_cache.MEMCACHE_PREFIX + 'foo')
    cache_keys = ['foo0', 'foo1', 'foo2']
    memcache_keys = [sharded_cache.MEMCACHE_PREFIX + key for key in cache_keys]
    content = memcache.get_multi(memcache_keys)
    self.assertEqual(3, shard_map['num_shards'])
    keys = ['%sfoo%d' % (sharded_cache.MEMCACHE_PREFIX, i) for i in xrange(3)]
    expected_content_shards = {
        # 0 to 1MB.
        keys[0]: LARGE_CONTENT_PICKLED[0:MAX_VALUE_SIZE],
        # 1MB to 2MB.
        keys[1]: LARGE_CONTENT_PICKLED[MAX_VALUE_SIZE:MAX_VALUE_SIZE * 2],
        # 2MB to end.
        keys[2]: LARGE_CONTENT_PICKLED[MAX_VALUE_SIZE * 2:],
    }
    self.assertDictEqual(expected_content_shards, content)
    next_shard = memcache.get(sharded_cache.MEMCACHE_PREFIX + 'foo3')
    self.assertEqual(None, next_shard)

  def testDelete(self):
    # Delete small content with no sharding.
    sharded_cache.Set('foo', SMALL_CONTENT)
    sharded_cache.Delete('foo')
    shard_map = memcache.get(sharded_cache.MEMCACHE_PREFIX + 'foo')
    self.assertEqual(None, shard_map)

    # Delete content stored in multiple shards.
    sharded_cache.Set('foo', LARGE_CONTENT)
    sharded_cache.Delete('foo')
    shard_map = memcache.get(sharded_cache.MEMCACHE_PREFIX + 'foo')
    cache_keys = ['foo0', 'foo1', 'foo2']
    memcache_keys = [sharded_cache.MEMCACHE_PREFIX + key for key in cache_keys]
    content = memcache.get_multi(memcache_keys)
    self.assertEqual(None, shard_map)
    self.assertEqual(None, sharded_cache.Get('foo'))
    self.assertDictEqual({}, content)

  def testMaxValueSize(self):
    # If memcache max value size ever changes, I want to know.
    self.assertEqual(1000000, MAX_VALUE_SIZE)

if __name__ == '__main__':
  basetest.main()
