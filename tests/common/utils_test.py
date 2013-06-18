#!/usr/bin/env python
# -*- coding: utf-8 -*-
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

import datetime
import json
from google.appengine.ext import blobstore
from titan.common.lib.google.apputils import app
from titan.common.lib.google.apputils import basetest
from titan.common import utils

class UtilsTestCase(testing.BaseTestCase):

  def testValidateNamespace(self):
    self.assertIsNone(utils.validate_namespace('a'))
    self.assertIsNone(utils.validate_namespace('a-a'))
    self.assertIsNone(utils.validate_namespace('a123'))
    self.assertIsNone(utils.validate_namespace('123'))
    self.assertIsNone(utils.validate_namespace('1.2'))
    self.assertIsNone(utils.validate_namespace('a' * 99))
    self.assertIsNone(utils.validate_namespace(None))
    invalid_namespaces = [
        '',
        'a ',
        '/',
        '/a',
        'a/a',
        'a/',
        u'∆',
        'a\na',
        'a' * 250,
        ['a'],
    ]
    for namespace in invalid_namespaces:
      try:
        utils.validate_namespace(namespace)
      except ValueError:
        pass
      else:
        self.fail(
            'Invalid namespace should have failed: {!r}'.format(namespace))

  def testSplitPath(self):
    # No containing paths of '/'.
    self.assertEqual([], utils.split_path('/'))
    # / contains /file
    self.assertEqual(['/'], utils.split_path('/file'))
    # / and /foo contain /foo/bar
    self.assertEqual(['/', '/foo'], utils.split_path('/foo/bar'))

    expected = ['/', '/path', '/path/to', '/path/to/some']
    self.assertEqual(expected, utils.split_path('/path/to/some/file.txt'))
    self.assertEqual(expected, utils.split_path('/path/to/some/file'))

    expected = []

  def testGetCommonDirPath(self):
    paths = ['/foo/bar/baz/test.html', '/foo/bar/test.html']
    self.assertEqual('/foo/bar', utils.get_common_dir_path(paths))
    paths = ['/foo/bar/baz/test.html', '/z/test.html']
    self.assertEqual('/', utils.get_common_dir_path(paths))
    paths = ['/foo/bar/baz/test.html', '/footest/bar.html']
    self.assertEqual('/', utils.get_common_dir_path(paths))

  def testSplitSegments(self):
    self.assertEqual(['a'], utils.split_segments('a'))
    self.assertEqual(['a', 'a/b'], utils.split_segments('a/b'))
    self.assertEqual(['a', 'a/b', 'a/b/c'], utils.split_segments('a/b/c'))

    self.assertEqual(['a'], utils.split_segments('a', sep='.'))
    self.assertEqual(['a', 'a.b'], utils.split_segments('a.b', sep='.'))
    self.assertEqual(['a', 'a.b', 'a.b.c'],
                     utils.split_segments('a.b.c', sep='.'))

  def testSafeJoin(self):
    self.assertEqual('foo/bar', utils.safe_join('foo', 'bar'))
    self.assertEqual('foo/bar/baz/', utils.safe_join('foo', 'bar', 'baz/'))
    self.assertEqual('foo.html', utils.safe_join('', 'foo.html'))
    self.assertEqual('/foo/bar', utils.safe_join('/foo', 'bar'))
    self.assertEqual('/foo/bar', utils.safe_join('/foo/', 'bar'))
    self.assertEqual('/foo/bar/baz.html',
                     utils.safe_join('/foo', 'bar', 'baz.html'))
    self.assertEqual('/foo/bar////baz.html',
                     utils.safe_join('/foo', 'bar////', 'baz.html'))

    self.assertRaises(ValueError, utils.safe_join, '/foo', '/bar')
    self.assertRaises(ValueError, utils.safe_join, 'foo', '/bar', 'baz')
    self.assertRaises(ValueError, utils.safe_join, 'foo', 'bar', '/baz')

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
    destination_map = utils.make_destination_paths_map(source_paths, '/x')
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
    destination_map = utils.make_destination_paths_map(
        source_paths, destination_dir_path='/x', strip_prefix='/a')
    self.assertEqual(expected, destination_map)
    # With trailing slashes should be equivalent.
    destination_map = utils.make_destination_paths_map(
        source_paths, destination_dir_path='/x/', strip_prefix='/a/')
    self.assertEqual(expected, destination_map)

    # Error handling.
    self.assertRaises(ValueError, utils.make_destination_paths_map, '/a/b', '/x')
    self.assertRaises(
        ValueError,
        utils.make_destination_paths_map, ['/a/b'], '/x/', strip_prefix='/fake')

  def testComposeMethodKwargs(self):

    class Parent(object):

      def Method(self, foo, bar=None):
        return '%s-%s' % (foo, bar)

    class Child(Parent):

      @utils.compose_method_kwargs
      def Method(self, **kwargs):
        baz_result = '-%s' % kwargs.pop('baz', False)  # Consume "baz" arg.
        return super(Child, self).Method(**kwargs) + baz_result

    class GrandChild(Child):

      @utils.compose_method_kwargs
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

  def testCustomJsonEncoder(self):
    # Test serializing datetimes.
    original = {'test': datetime.datetime(2013, 04, 02, 10, 11, 12, 123)}
    self.assertEqual(
        '{"test": 1364922672.000123}',
        json.dumps(original, cls=utils.CustomJsonEncoder))

    # Test serializing objects with a "serialize" method.
    class Foo(object):

      def serialize(self):
        return 'foo'

    self.assertEqual('"foo"', json.dumps(Foo(), cls=utils.CustomJsonEncoder))

  def testRunWithBackoff(self):
    # Returning None forces exponential backoff to occur, and within 5 seconds
    # the callable should be called ~3 times.
    results = utils.run_with_backoff(func=lambda: None, runtime=5)
    # Weakly verify it fits in some arbitrary range:
    self.assertGreater(len(results), 1)
    self.assertLess(len(results), 6)

    # Test stop_on_success.
    results = utils.run_with_backoff(func=lambda: True, stop_on_success=True)
    self.assertEqual([True], results)

  def testChunkGenerator(self):
    nums = range(0, 2501)

    # Verify default chunk size.
    new_nums = []
    for i, some_nums in enumerate(utils.chunk_generator(nums)):
      new_nums += some_nums
    # Should have processed in 3 chunks (0, 1, 2):
    self.assertEqual(2, i)
    self.assertListEqual(new_nums, nums)

    # Verify chunk size bigger than input.
    new_nums = []
    for i, some_nums in enumerate(utils.chunk_generator(nums, chunk_size=5000)):
      new_nums += some_nums
    # Should have processed in 1 chunk:
    self.assertEqual(0, i)
    self.assertListEqual(new_nums, nums)

  def testWriteToBlobstore(self):
    # There are stronger tests for the behavior of this in files_test.
    old_blob_key = utils.write_to_blobstore('Blobstore!')
    self.assertTrue(old_blob_key)

    old_blobinfo = blobstore.BlobInfo.get(old_blob_key)
    new_blob_key = utils.write_to_blobstore('Blobstore!',
                                            old_blobinfo=old_blobinfo)
    self.assertEqual(new_blob_key, old_blob_key)
    self.stubs.SmartUnsetAll()

  def testGenerateShardNames(self):
    self.assertEqual([], utils.generate_shard_names(0))
    self.assertEqual(['ab'], utils.generate_shard_names(1))
    self.assertEqual(['abc', 'acb'], utils.generate_shard_names(2))
    self.assertEqual(['abc', 'acb', 'bac'], utils.generate_shard_names(3))

    # Verify all the shards are unique.
    tags = self.assertEqual(60, len(set(utils.generate_shard_names(60))))

    # Weakly verify that larger sharding works.
    tags = utils.generate_shard_names(10000)
    self.assertEqual(10000, len(tags))
    self.assertEqual(8, len(tags[0]))

  def testHumanizeDuration(self):
    self.assertEqual('10s', utils.humanize_duration(10))

  def testHumanizeTimeDelta(self):
    self.assertEqual('1d', utils.humanize_time_delta(
        datetime.timedelta(days=1)))

def main(unused_argv):
  basetest.main()

if __name__ == '__main__':
  app.run()
