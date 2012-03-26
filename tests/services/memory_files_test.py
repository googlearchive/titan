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

"""Tests for memory_files.py."""

from tests.common import testing

import os
from google.appengine.api import memcache
from titan.common.lib.google.apputils import basetest
from titan.files import files
from titan.services import memory_files

class MemoryFileTest(testing.ServicesTestCase):

  def setUp(self):
    super(MemoryFileTest, self).setUp()
    services = (
        'titan.services.memory_files',
    )
    self.EnableServices(services)

  def testMemoryFile(self):
    local_files_store = memory_files._GetRequestLocalFilesStore()

    # First request.
    os.environ['REQUEST_ID_HASH'] = 'aaaa'

    # Verify getting uncached and non-existent files.
    self.assertIsNone(files.Get('/foo'))
    self.assertEqual({}, files.Get(['/bar']))

    files.Write('/foo', 'bar', meta={'color': 'blue'})
    self.assertFalse('/foo' in local_files_store)
    self.assertEqual('bar', files.Get('/foo').content)
    self.assertEqual('blue', files.Get('/foo').color)
    self.assertTrue('/foo' in local_files_store)
    self.assertEqual('bar', files.Get(['/fake1', '/foo']).values()[0].content)

    # Writes should clear the cache for that path.
    files.Write('/foo', meta={'color': 'red'})
    self.assertFalse('/foo' in local_files_store)

    # Make sure that a memcache RPC isn't made when getting the file
    # the second time.
    files.Exists('/foo')
    files.Exists('/qux')  # file doesn't exist, but populate globals cache.
    self.stubs.Set(memcache, 'get', lambda x: self.fail('!'))
    self.stubs.Set(memcache, 'get_multi', lambda x: self.fail('!'))
    self.assertEqual('bar', files.Get('/foo').content)
    self.assertEqual('red', files.Get('/foo').color)
    self.assertEqual('bar', files.Get(['/foo']).values()[0].content)
    # Non-existent files (touched above) should also not need to make RPCs:
    self.assertIsNone(files.Get('/qux'))
    self.assertEqual({}, files.Get(['/qux']))
    self.stubs.UnsetAll()

    # Verify that filling the cache past its eviction limit works correctly.
    paths = ['/foo%s' % i for i in range(memory_files.DEFAULT_MRU_SIZE + 10)]
    files.Touch(paths)
    files.Get(paths)
    self.assertEqual(1, len(files.Get(['/fake2', paths[-1]])))
    self.assertEqual(memory_files.DEFAULT_MRU_SIZE, len(local_files_store))

    # Regression test: make sure files.Exists returns booleans.
    self.assertEqual(False, files.Exists('/fake3'))

if __name__ == '__main__':
  basetest.main()
