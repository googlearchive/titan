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

from tests import testing

import os
import re
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
    config = {
        'path_regexes': [re.compile(r'^/special/.*')],
    }
    self.EnableServices(services)
    self.SetServiceConfig(memory_files.SERVICE_NAME, config)

  def testMemoryFile(self):
    global_file_ents = memory_files._global_file_ents
    namespace = memory_files.DEFAULT_MEMCACHE_NAMESPACE

    # Files not matching any regexes are not cached globally in-memory.
    files.Write('/foo', 'foo')
    self.assertTrue('/foo' not in global_file_ents)

    # On any read where the memcache version counter has been evicted, the local
    # globals cache cannot be populated. Have to wait until a second Write().
    files.Write('/special/foo', 'foo')
    memcache.flush_all()
    del os.environ['MEMORY_FILES_VERSION']
    files.Read('/special/foo')
    self.assertFalse('/special/foo' in global_file_ents)
    self.assertEqual(None, os.environ.get('MEMORY_FILES_VERSION'))
    self.assertEqual(None, memcache.get('version',
                                        namespace=namespace))

    # On any read (after a Write has populated the memcache version counter),
    # the globals cache is usable and should be populated.
    files.Write('/special/foo', 'foo')
    files.Read('/special/foo')
    self.assertTrue('/special/foo' in global_file_ents)
    self.assertEqual(1, int(os.environ.get('MEMORY_FILES_VERSION')))
    self.assertEqual(1, memcache.get('version',
                                     namespace=namespace))
    # Reset to test Get() as well.
    memory_files._EvictGlobalEntities(['/special/foo'])
    files.Get('/special/foo')
    self.assertTrue('/special/foo' in global_file_ents)

    # On write operations, the memcache version number is increased and the
    # local cache evicted.
    memory_files._EvictGlobalEntities(['/special/foo'])
    memcache.flush_all()
    files.Write('/special/foo', 'foo')
    self.assertFalse('/special/foo' in global_file_ents)
    self.assertEqual(1, memcache.get('version', namespace=namespace))
    self.assertEqual(1, int(os.environ.get('MEMORY_FILES_VERSION')))

    files.Read('/special/foo')
    self.assertEqual(1, memcache.get('version', namespace=namespace))
    self.assertEqual(1, int(os.environ.get('MEMORY_FILES_VERSION')))
    files.Touch('/special/foo', 'foo')
    self.assertEqual(2, memcache.get('version', namespace=namespace))
    self.assertEqual(2, int(os.environ.get('MEMORY_FILES_VERSION')))
    self.assertFalse('/special/foo' in global_file_ents)

    files.Read('/special/foo')
    self.assertEqual(2, memcache.get('version', namespace=namespace))
    self.assertEqual(2, int(os.environ.get('MEMORY_FILES_VERSION')))
    files.Delete('/special/foo')
    self.assertEqual(3, memcache.get('version', namespace=namespace))
    self.assertEqual(3, int(os.environ.get('MEMORY_FILES_VERSION')))
    self.assertFalse('/special/foo' in global_file_ents)

    # Writing a non-special file shouldn't change the version number.
    files.Read('/foo')
    files.Touch('/foo')
    self.assertEqual(3, memcache.get('version', namespace=namespace))
    self.assertEqual(3, int(os.environ.get('MEMORY_FILES_VERSION')))
    self.assertFalse('/foo' in global_file_ents)

if __name__ == '__main__':
  basetest.main()
