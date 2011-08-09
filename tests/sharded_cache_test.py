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

from tests import testing

import cPickle as pickle
from google.appengine.api import memcache
from titan.common.lib.google.apputils import basetest
from titan.common import sharded_cache

MAX_VALUE_SIZE = memcache.MAX_VALUE_SIZE

# 1KB -- Should be in 1 shard.
SMALL_CONTENT = 'a' * 1000

# 2MB -- Will be slightly >2MB when pickled, so should be in 3 shards.
LARGE_CONTENT = 'b' * MAX_VALUE_SIZE * 2
LARGE_CONTENT_PICKLED = pickle.dumps(LARGE_CONTENT)

class ShardedCacheTest(testing.BaseTestCase):

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
    memcache.delete('foo', namespace=sharded_cache.NAMESPACE)
    self.assertEqual(None, sharded_cache.Get('foo'))

    # 1 content shard was evicted.
    sharded_cache.Set('foo', LARGE_CONTENT)
    memcache.delete('foo1', namespace=sharded_cache.NAMESPACE)
    self.assertEqual(None, sharded_cache.Get('foo'))
    # The shard map and unevicted shards should be deleted.
    cache_keys = ['foo', 'foo0', 'foo1', 'foo2', 'foo3']
    content = memcache.get_multi(cache_keys, namespace=sharded_cache.NAMESPACE)
    self.assertFalse(any(content))

    # All content shards were evicted.
    sharded_cache.Set('foo', LARGE_CONTENT)
    memcache.delete_multi(['foo0', 'foo1', 'foo2'],
                          namespace=sharded_cache.NAMESPACE)
    self.assertEqual(None, sharded_cache.Get('foo'))

  def testSet(self):
    # Set object smaller than 1MB.
    sharded_cache.Set('foo', SMALL_CONTENT)
    shard_map = memcache.get('foo', namespace=sharded_cache.NAMESPACE)
    content = memcache.get('foo0', namespace=sharded_cache.NAMESPACE)
    self.assertEqual(1, shard_map['num_shards'])
    self.assertEqual(SMALL_CONTENT, pickle.loads(content))

    # Set object larger than 1MB.
    sharded_cache.Set('foo', LARGE_CONTENT)
    shard_map = memcache.get('foo', namespace=sharded_cache.NAMESPACE)
    cache_keys = ['foo0', 'foo1', 'foo2']
    content = memcache.get_multi(cache_keys, namespace=sharded_cache.NAMESPACE)
    self.assertEqual(3, shard_map['num_shards'])
    expected_content_shards = {
        # 0 to 1MB.
        'foo0': LARGE_CONTENT_PICKLED[0:MAX_VALUE_SIZE],
        # 1MB to 2MB.
        'foo1': LARGE_CONTENT_PICKLED[MAX_VALUE_SIZE:MAX_VALUE_SIZE * 2],
        # 2MB to end.
        'foo2': LARGE_CONTENT_PICKLED[MAX_VALUE_SIZE * 2:],
    }
    self.assertDictEqual(expected_content_shards, content)
    next_shard = memcache.get('foo3', namespace=sharded_cache.NAMESPACE)
    self.assertEqual(None, next_shard)

  def testDelete(self):
    # Delete content stored in single shard.
    sharded_cache.Set('foo', SMALL_CONTENT)
    sharded_cache.Delete('foo')
    shard_map = memcache.get('foo', namespace=sharded_cache.NAMESPACE)
    content = memcache.get('foo0', namespace=sharded_cache.NAMESPACE)
    self.assertEqual(None, shard_map)
    self.assertEqual(None, content)

    # Delete content stored in multiple shards.
    sharded_cache.Set('foo', LARGE_CONTENT)
    sharded_cache.Delete('foo')
    shard_map = memcache.get('foo', namespace=sharded_cache.NAMESPACE)
    cache_keys = ['foo0', 'foo1', 'foo2']
    content = memcache.get_multi(cache_keys, namespace=sharded_cache.NAMESPACE)
    self.assertEqual(None, shard_map)
    self.assertEqual(None, sharded_cache.Get('foo'))
    self.assertDictEqual({}, content)

  def testMaxValueSize(self):
    # If memcache max value size ever changes, I want to know.
    self.assertEqual(1000000, MAX_VALUE_SIZE)

if __name__ == '__main__':
  basetest.main()
