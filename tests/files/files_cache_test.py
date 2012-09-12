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

from tests.common import testing

from google.appengine.api import memcache
from titan.common.lib.google.apputils import basetest
from titan.common import sharded_cache
from titan.files import files_cache
from titan.files import files

class FileCacheTestCase(testing.BaseTestCase):

  def testGetBlob(self):
    sharded_cache.Set(files_cache.BLOB_MEMCACHE_PREFIX + '/foo.html', 'Test')
    self.assertEqual('Test', files_cache.GetBlob('/foo.html'))

  def testStoreBlob(self):
    result = files_cache.StoreBlob('/foo.html', 'Test')
    self.assertTrue(result)
    self.assertEqual('Test', sharded_cache.Get(
        files_cache.BLOB_MEMCACHE_PREFIX + '/foo.html'))

  def testClearBlobsForFiles(self):
    titan_file = files.File('/foo.html').Write('')

    # Clear single file.
    sharded_cache.Set('/foo.html', 'Test')
    files_cache.ClearBlobsForFiles(titan_file._file)
    cache_item = memcache.get('/foo.html')
    self.assertEqual(None, cache_item)

    # Clear multiple files.
    memcache.flush_all()
    sharded_cache.Set('/foo.html', 'Test')
    files_cache.ClearBlobsForFiles([titan_file._file])
    cache_item = memcache.get('/foo.html')
    self.assertEqual(None, cache_item)

if __name__ == '__main__':
  basetest.main()
