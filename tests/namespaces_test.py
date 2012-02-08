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

from tests import testing

from google.appengine.api import namespace_manager
from titan.common.lib.google.apputils import basetest
from titan.files import files

class NamespacesTest(testing.ServicesTestCase):

  def setUp(self):
    super(NamespacesTest, self).setUp()
    services = (
        'titan.services.namespaces',
    )
    self.EnableServices(services)

  def testWrite(self):
    original_namespace = namespace_manager.get_namespace()
    files.Write('/tmp/foo.txt', 'foobar', namespace='tmp')

    # Verify that /tmp/foo.txt doesn't exist in non-tmp namespace.
    self.assertFalse(files.Exists('/tmp/foo.txt'))

    # Verify reads.
    fp = files.Get('/tmp/foo.txt', namespace='tmp')
    self.assertEqual('foobar', fp.content)

    # Verify that writing to the non-tmp path doesn't affect the tmp path.
    files.Write('/tmp/foo.txt', 'hello world')
    self.assertEqual('hello world', files.Get('/tmp/foo.txt').content)
    fp = files.Get('/tmp/foo.txt', namespace='tmp')
    self.assertEqual('foobar', fp.content)

    # Verify current namespace.
    self.assertEqual(original_namespace, namespace_manager.get_namespace())

  def testTouch(self):
    original_namespace = namespace_manager.get_namespace()
    files.Touch('/tmp/touch.txt', namespace='tmp')

    # Verify tmp file exists.
    self.assertTrue(files.Exists('/tmp/touch.txt', namespace='tmp'))

    # Verify non-tmp file does not exist.
    self.assertFalse(files.Exists('/tmp/touch.txt'))

    # Verify current namespace.
    self.assertEqual(original_namespace, namespace_manager.get_namespace())

  def testDelete(self):
    original_namespace = namespace_manager.get_namespace()

    files.Touch('/tmp/touch.txt')
    files.Touch('/tmp/touch.txt', namespace='tmp')

    self.assertTrue(files.Exists('/tmp/touch.txt'))
    self.assertTrue(files.Exists('/tmp/touch.txt', namespace='tmp'))

    # Verify deleting tmp doesn't affect non-tmp namespace.
    files.Delete('/tmp/touch.txt', namespace='tmp')
    self.assertTrue(files.Exists('/tmp/touch.txt'))
    self.assertFalse(files.Exists('/tmp/touch.txt', namespace='tmp'))

    # Verify current namespace.
    self.assertEqual(original_namespace, namespace_manager.get_namespace())

  def testListDir(self):
    original_namespace = namespace_manager.get_namespace()
    files.Touch('/tmp/touch.txt', namespace='tmp')

    # Verify list dir for tmp and non-tmp namespaces.
    self.assertSequenceEqual([['tmp'], []], files.ListDir('/', namespace='tmp'))
    dirs, file_ents = files.ListDir('/tmp', namespace='tmp')
    self.assertEqual(0, len(dirs))
    self.assertEqual(1, len(file_ents))
    self.assertEqual('/tmp/touch.txt', file_ents[0].path)

    self.assertSequenceEqual([[], []], files.ListDir('/'))

    # Verify current namespace.
    self.assertEqual(original_namespace, namespace_manager.get_namespace())

  def testListFiles(self):
    original_namespace = namespace_manager.get_namespace()
    files.Touch('/tmp/touch.txt', namespace='tmp')

    # Verify list dir for tmp and non-tmp namespaces.
    self.assertSequenceEqual([['tmp'], []], files.ListDir('/', namespace='tmp'))
    tmp_files = files.ListFiles('/tmp', namespace='tmp')
    self.assertEqual(1, len(tmp_files))

    nontmp_files = files.ListFiles('/tmp')
    self.assertEqual(0, len(nontmp_files))

    # Verify current namespace.
    self.assertEqual(original_namespace, namespace_manager.get_namespace())

if __name__ == '__main__':
  basetest.main()
