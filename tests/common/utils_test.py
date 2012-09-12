#!/usr/bin/env python
# Copyright 2012 Google Inc. All Rights Reserved.
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

"""Tests for utils.py."""

from tests.common import testing

from google.appengine.ext import blobstore
from titan.common.lib.google.apputils import app
from titan.common.lib.google.apputils import basetest
from titan.common import utils

class UtilsTestCase(testing.BaseTestCase):

  def testGetCommonDirPath(self):
    paths = ['/foo/bar/baz/test.html', '/foo/bar/test.html']
    self.assertEqual('/foo/bar', utils.GetCommonDirPath(paths))
    paths = ['/foo/bar/baz/test.html', '/z/test.html']
    self.assertEqual('/', utils.GetCommonDirPath(paths))
    paths = ['/foo/bar/baz/test.html', '/footest/bar.html']
    self.assertEqual('/', utils.GetCommonDirPath(paths))

  def testSplitPath(self):
    # No containing paths of '/'.
    self.assertEqual([], utils.SplitPath('/'))
    # / contains /file
    self.assertEqual(['/'], utils.SplitPath('/file'))
    # / and /foo contain /foo/bar
    self.assertEqual(['/', '/foo'], utils.SplitPath('/foo/bar'))

    expected = ['/', '/path', '/path/to', '/path/to/some']
    self.assertEqual(expected, utils.SplitPath('/path/to/some/file.txt'))
    self.assertEqual(expected, utils.SplitPath('/path/to/some/file'))

  def testMakeDestinationPathsMap(self):
    source_paths = [
        '/foo',
        '/a/foo',
        '/a/b/foo',
        '/c/foo',
    ]
    expected = {
        '/foo': '/x/foo',
        '/a/foo': '/x/a/foo',
        '/a/b/foo': '/x/a/b/foo',
        '/c/foo': '/x/c/foo',
    }
    destination_map = utils.MakeDestinationPathsMap(source_paths, '/x')
    self.assertEqual(expected, destination_map)

    # Test strip_prefix.
    source_paths = [
        '/a/foo',
        '/a/b/foo',
    ]
    expected = {
        '/a/foo': '/x/foo',
        '/a/b/foo': '/x/b/foo',
    }
    destination_map = utils.MakeDestinationPathsMap(
        source_paths, destination_dir_path='/x', strip_prefix='/a')
    self.assertEqual(expected, destination_map)
    # With trailing slashes should be equivalent.
    destination_map = utils.MakeDestinationPathsMap(
        source_paths, destination_dir_path='/x/', strip_prefix='/a/')
    self.assertEqual(expected, destination_map)

    # Error handling.
    self.assertRaises(ValueError, utils.MakeDestinationPathsMap, '/a/b', '/x')
    self.assertRaises(
        ValueError,
        utils.MakeDestinationPathsMap, ['/a/b'], '/x/', strip_prefix='/fake')

  def testComposeMethodKwargs(self):

    class Parent(object):

      def Method(self, foo, bar=None):
        return '%s-%s' % (foo, bar)

    class Child(Parent):

      @utils.ComposeMethodKwargs
      def Method(self, **kwargs):
        baz_result = '-%s' % kwargs.pop('baz', False)  # Consume "baz" arg.
        return super(Child, self).Method(**kwargs) + baz_result

    class GrandChild(Child):

      @utils.ComposeMethodKwargs
      def Method(self, **kwargs):
        return super(GrandChild, self).Method(**kwargs)

    child = Child()
    self.assertEqual('1-None-False', child.Method(1))
    child = GrandChild()
    self.assertEqual('1-None-False', child.Method(1))
    self.assertEqual('1-True-False', child.Method(1, True))
    self.assertEqual('1-True-False', child.Method(1, bar=True))
    self.assertEqual('1-True-False', child.Method(foo=1, bar=True))
    self.assertEqual('1-True-True', child.Method(foo=1, bar=True, baz=True))
    self.assertEqual('1-True-False', child.Method(bar=True, foo=1))

  def testRunWithBackoff(self):
    # Returning None forces exponential backoff to occur, and within 5 seconds
    # the callable should be called ~3 times.
    results = utils.RunWithBackoff(func=lambda: None, runtime=5)
    # Weakly verify it fits in some arbitrary range:
    self.assertGreater(len(results), 1)
    self.assertLess(len(results), 6)

    # Test stop_on_success.
    results = utils.RunWithBackoff(func=lambda: True, stop_on_success=True)
    self.assertEqual([True], results)

  def testChunkGenerator(self):
    nums = range(0, 2501)

    # Verify default chunk size.
    new_nums = []
    for i, some_nums in enumerate(utils.ChunkGenerator(nums)):
      new_nums += some_nums
    # Should have processed in 3 chunks (0, 1, 2):
    self.assertEqual(2, i)
    self.assertListEqual(new_nums, nums)

    # Verify chunk size bigger than input.
    new_nums = []
    for i, some_nums in enumerate(utils.ChunkGenerator(nums, chunk_size=5000)):
      new_nums += some_nums
    # Should have processed in 1 chunk:
    self.assertEqual(0, i)
    self.assertListEqual(new_nums, nums)

  def testWriteToBlobstore(self):
    # There are stronger tests for the behavior of this in files_test.
    old_blob_key = utils.WriteToBlobstore('Blobstore!')
    self.assertTrue(old_blob_key)

    old_blobinfo = blobstore.BlobInfo.get(old_blob_key)
    new_blob_key = utils.WriteToBlobstore('Blobstore!',
                                          old_blobinfo=old_blobinfo)
    self.assertEqual(new_blob_key, old_blob_key)
    self.stubs.SmartUnsetAll()

def main(unused_argv):
  basetest.main()

if __name__ == '__main__':
  app.run()
