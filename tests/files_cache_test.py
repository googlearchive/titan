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

"""Tests for files_cache.py."""

from tests import testing

from google.appengine.api import memcache
from titan.common.lib.google.apputils import basetest
from titan.common import sharded_cache
from titan.files import files_cache
from titan.files import files

class FileCacheTestCase(testing.BaseTestCase):

  def testGetFiles(self):
    # Fake cache eviction:
    files.Write('/foo/bar', 'Bar')
    memcache.delete(files_cache.FILE_MEMCACHE_PREFIX + '/foo/bar')

    # Cache miss: any path doesn't exist in memcache.
    # Single non-cached path.
    cached_file = files_cache.GetFiles('/foo/bar')
    self.assertEqual((None, False), cached_file)
    # Multiple non-cached paths.
    cached_files = files_cache.GetFiles(['/foo/bar', '/foo/bar/baz'])
    self.assertEqual((None, False), cached_files)
    # Multiple paths, one of which is not cached.
    cached_files = files_cache.GetFiles(['/foo/bar', '/foo/bar/baz'])
    self.assertEqual((None, False), cached_files)

    # Cache hit: all given paths exist in memcache.
    # Single cached path.
    files.Write('/foo/bar', 'Bar')
    cached_file, cache_hit = files_cache.GetFiles('/foo/bar')
    file_ent = files._File.get_by_key_name('/foo/bar')
    self.assertTrue(cache_hit)
    self.assertEntityEqual(file_ent, cached_file)
    # Multiple cached paths.
    paths = ['/foo/bar', '/foo/bar/baz']
    files.Touch(paths)
    cached_files, cache_hit = files_cache.GetFiles(paths)
    self.assertTrue(cache_hit)
    file_ents = files._File.get_by_key_name(paths)
    self.assertEntitiesEqual(file_ents, cached_files)

  def testStoreFiles(self):
    # Store single _File entity.
    file_obj = files.Write('/foo/bar', 'Bar')
    result = files_cache.StoreFiles(file_obj._file)
    self.assertTrue(result)
    cache_item = memcache.get(files_cache.FILE_MEMCACHE_PREFIX + '/foo/bar')
    self.assertEntityEqual(file_obj._file, cache_item)

    # Store multiple _File entities.
    files.Write('/foo/bar', 'Bar')
    files.Write('/foo/bar/baz', 'Baz')
    file_ents = files._File.get_by_key_name(['/foo/bar', '/foo/bar/baz'])
    result = files_cache.StoreFiles(file_ents)
    self.assertEqual([], result)
    cache_item = memcache.get(files_cache.FILE_MEMCACHE_PREFIX + '/foo/bar')
    self.assertEntityEqual(file_ents[0], cache_item)
    cache_item = memcache.get(files_cache.FILE_MEMCACHE_PREFIX + '/foo/bar/baz')
    self.assertEntityEqual(file_ents[1], cache_item)

  def testStoreAll(self):
    # Every entity should exist in memcache, and every None should be flagged.
    file_obj = files.Touch('/foo/bar')
    data = {
        '/foo': None,
        '/foo/bar': file_obj._file,
    }
    result = files_cache.StoreAll(data)
    self.assertEqual([], result)
    cache_item = memcache.get(files_cache.FILE_MEMCACHE_PREFIX + '/foo')
    self.assertEqual(files_cache._NO_FILE_FLAG, cache_item)
    cache_item = memcache.get(files_cache.FILE_MEMCACHE_PREFIX + '/foo/bar')
    self.assertEntityEqual(file_obj._file, cache_item)

  def testSetFileDoesNotExist(self):
    # Set single path.
    files_cache.SetFileDoesNotExist('/foo/bar')
    cache_item = memcache.get(files_cache.FILE_MEMCACHE_PREFIX + '/foo/bar')
    self.assertEqual(files_cache._NO_FILE_FLAG, cache_item)

    # Set multiple paths.
    paths = ['/foo/bar', '/foo/bar/baz']
    cache_keys = [files_cache.FILE_MEMCACHE_PREFIX + path for path in paths]
    files_cache.SetFileDoesNotExist(paths)
    cache_items = memcache.get_multi(cache_keys)
    for key in cache_keys:
      self.assertEqual(files_cache._NO_FILE_FLAG, cache_items[key])

  def testGetBlob(self):
    sharded_cache.Set(files_cache.BLOB_MEMCACHE_PREFIX + '/foo.html', 'Test')
    self.assertEqual('Test', files_cache.GetBlob('/foo.html'))

  def testStoreBlob(self):
    result = files_cache.StoreBlob('/foo.html', 'Test')
    self.assertTrue(result)
    self.assertEqual('Test', sharded_cache.Get(
        files_cache.BLOB_MEMCACHE_PREFIX + '/foo.html'))

  def testClearBlobsForFiles(self):
    file_obj = files.Touch('/foo.html')

    # Clear single file.
    sharded_cache.Set('/foo.html', 'Test')
    files_cache.ClearBlobsForFiles(file_obj._file)
    cache_item = memcache.get('/foo.html')
    self.assertEqual(None, cache_item)

    # Clear multiple files.
    memcache.flush_all()
    sharded_cache.Set('/foo.html', 'Test')
    files_cache.ClearBlobsForFiles([file_obj._file])
    cache_item = memcache.get('/foo.html')
    self.assertEqual(None, cache_item)

  def testStoreSubdirs(self):
    memcache.set(files_cache.DIR_MEMCACHE_PREFIX + '/', {'old_key': 1})

    # Store new subdirs lists.
    files_cache.StoreSubdirs({
        '/': ['foo'],
        '/foo': set([]),
    })
    cache_item = memcache.get(files_cache.DIR_MEMCACHE_PREFIX + '/')
    self.assertEqual(set(['foo']), cache_item['subdirs'])
    # Verify that other cached dir data is not touched.
    self.assertTrue(cache_item['old_key'])
    cache_item = memcache.get(files_cache.DIR_MEMCACHE_PREFIX + '/foo')
    self.assertEqual(set([]), cache_item['subdirs'])

    # Update subdirs (should overwrite).
    files_cache.StoreSubdirs({'/': ['new_dir']})
    cache_item = memcache.get(files_cache.DIR_MEMCACHE_PREFIX + '/')
    self.assertEqual(set(['new_dir']), cache_item['subdirs'])

  def testGetSubdirs(self):
    cached_subdirs = files_cache.GetSubdirs('/')
    self.assertEqual(None, cached_subdirs)

    files_cache.StoreSubdirs({'/': ['foo', 'bar', 'baz']})
    cached_subdirs = files_cache.GetSubdirs('/')
    self.assertEqual(set(['foo', 'bar', 'baz']), cached_subdirs)

    # Directory caches that exist without a "subdir" key should return None.
    file_obj = files.Touch('/foo/bar/baz.html')
    files_cache.ClearSubdirsForFiles(file_obj._file)
    cached_subdirs = files_cache.GetSubdirs('/')
    self.assertEqual(None, cached_subdirs)

  def testUpdateSubdirsForFiles(self):
    file_objs = [files.Touch('/foo/bar/baz.html')]

    # Cleanup the dir caches for testing.
    preset_data = {'old_key': 1, 'subdirs': set([])}
    memcache.set(files_cache.DIR_MEMCACHE_PREFIX + '/', preset_data)
    memcache.set(files_cache.DIR_MEMCACHE_PREFIX + '/foo', preset_data)

    # Update single subdir set.
    files_cache.UpdateSubdirsForFiles(file_objs[0]._file)
    cache_item = memcache.get(files_cache.DIR_MEMCACHE_PREFIX + '/')
    self.assertEqual(set(['foo']), cache_item['subdirs'])
    self.assertTrue(cache_item['old_key'])
    cache_item = memcache.get(files_cache.DIR_MEMCACHE_PREFIX + '/foo')
    self.assertEqual(set(['bar']), cache_item['subdirs'])

    # Update multiple subdir sets.
    file_objs.append(files.Touch('/foo/qux/foo.html'))
    files_cache.UpdateSubdirsForFiles([file_objs[0]._file, file_objs[1]._file])
    cache_item = memcache.get(files_cache.DIR_MEMCACHE_PREFIX + '/')
    self.assertEqual(set(['foo']), cache_item['subdirs'])
    self.assertTrue(cache_item['old_key'])
    cache_item = memcache.get(files_cache.DIR_MEMCACHE_PREFIX + '/foo')
    self.assertEqual(set(['bar', 'qux']), cache_item['subdirs'])

  def testClearSubdirsForFiles(self):
    file_objs = files.Touch(['/foo/bar/baz.html', '/foo/qux/foo.html'])
    files.ListDir('/')
    self.assertTrue(memcache.get(files_cache.DIR_MEMCACHE_PREFIX + '/'))
    self.assertTrue(memcache.get(files_cache.DIR_MEMCACHE_PREFIX + '/foo'))
    files_cache.ClearSubdirsForFiles([f._file for f in file_objs])
    self.assertDictEqual(
        {}, memcache.get(files_cache.DIR_MEMCACHE_PREFIX + '/'))
    self.assertDictEqual(
        {}, memcache.get(files_cache.DIR_MEMCACHE_PREFIX + '/foo'))

if __name__ == '__main__':
  basetest.main()
