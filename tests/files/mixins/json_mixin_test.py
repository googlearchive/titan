#!/usr/bin/env python
# Copyright 2013 Google Inc. All Rights Reserved.
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

"""Tests for json_mixin.py."""

from tests.common import testing

import json
from titan.common.lib.google.apputils import basetest
from titan import files
from titan.files.mixins import json_mixin

LARGE_FILE_CONTENT = 'a' * (1 << 21)  # 2 MiB.

class JsonMixinTestCase(testing.BaseTestCase):

  def setUp(self):
    super(JsonMixinTestCase, self).setUp()
    files.register_file_mixins([json_mixin.JsonMixin])

  def testJsonMixin(self):
    titan_file = files.File('/foo/file.json')

    # Verify that invalid JSON raises an error when written.
    self.assertRaises(json_mixin.BadJsonError, titan_file.write, '')
    self.assertRaises(json_mixin.BadJsonError, titan_file.write, '{a:1}')

    # Verify Django templates are not validated as JSON.
    titan_file.write('{% some django tag %}')

    # Verify valid JSON writes successfully.
    titan_file.write(json.dumps({'a': True}))

    # Verify behavior of json property.
    titan_file.write('{"a":"b"}')
    self.assertEqual({'a': 'b'}, titan_file.json)
    titan_file.json['c'] = 'd'
    self.assertEqual({'a': 'b', 'c': 'd'}, titan_file.json)
    titan_file.save()

    # Get json out of the saved file.
    titan_file = files.File('/foo/file.json')
    self.assertEqual({'a': 'b', 'c': 'd'}, titan_file.json)

    # Verify invalid JSON raises an error when accessed using json.
    titan_file = files.File('/foo/file.json')
    titan_file.write('{% some django tag %}')
    self.assertRaises(json_mixin.BadJsonError, lambda: titan_file.json)

    # Verify JSON is not checked for blobs (out of necessity, not correctness).
    titan_file = files.File('/foo/some-blob').write(LARGE_FILE_CONTENT)
    files.File('/foo/file.json').write(blob=titan_file.blob.key())

    # Verify ability to set json on a new file.
    titan_file = files.File('/foo/new_file.json')
    titan_file.json = None
    self.assertIsNone(titan_file.json)
    titan_file.save()
    titan_file.json = {}
    self.assertEqual({}, titan_file.json)
    titan_file.save()
    titan_file.json = {'foo': 'bar'}
    titan_file.save()
    titan_file = files.File('/foo/new_file.json')
    self.assertEqual({'foo': 'bar'}, titan_file.json)

    # Error handling.
    self.assertRaises(
        files.BadFileError, lambda: files.File('/fake.json').json)

  def testApplyPatch(self):
    titan_file = files.File('/foo/file.json')
    titan_file.json = {
        'foo': ['bar', 'baz']
    }
    titan_file.apply_patch([{
        'op': 'add',
        'path': '/foo/1',
        'value': 'qux'
    }])
    expected = {
        'foo': ['bar', 'qux', 'baz']
    }
    self.assertEqual(expected, titan_file.json)

class AbstractDataPersistenceTestCase(testing.BaseTestCase):

  def setUp(self):
    super(AbstractDataPersistenceTestCase, self).setUp()
    files.register_file_mixins([json_mixin.JsonMixin])

  def testAbstractDataPersistence(self):
    # Test abstract class.
    self.assertRaises(
        NotImplementedError, json_mixin.AbstractDataPersistence().serialize)

    # Create an arbitrary "widget" persistent data object.
    widget = Widget('my-widget')
    self.assertFalse(widget.exists)
    self.assertRaises(files.BadFileError, widget.serialize)
    self.assertRaises(files.BadFileError, widget.__getitem__, 'foo')
    widget['bar'] = False
    widget.update({'foo': False})
    self.assertEqual({'bar': False, 'foo': False}, widget.serialize())
    widget.save()
    self.assertEqual({'bar': False, 'foo': False}, widget.serialize())
    del widget['bar']
    self.assertEqual({'foo': False}, widget.serialize())
    widget.save()
    self.assertEqual({'foo': False}, widget.serialize())
    self.assertTrue(widget.exists)
    self.assertRaises(KeyError, lambda: widget['fake'])

    # Test get().
    self.assertIsNone(widget.get('fake'))
    self.assertFalse(widget.get('fake', False))

    # Test __setitem__ and __getitem__.
    widget['foo'] = True
    self.assertTrue(widget['foo'])
    self.assertTrue(widget.get('foo', False))
    self.assertEqual({'foo': True}, widget.serialize())
    self.assertRaises(KeyError, lambda: widget['bar'])

    # Test update().
    widget.update({'bar': True})
    self.assertTrue(widget['foo'])
    self.assertTrue(widget['bar'])
    widget.save()

    # Recreate object and test.
    widget = Widget('my-widget')
    self.assertEqual({'foo': True, 'bar': True}, widget.serialize())

class Widget(json_mixin.AbstractDataPersistence):

  def __init__(self, key):
    self.key = key
    self.__data_file = None

  @property
  def _data_file(self):
    if self.__data_file is None:
      self.__data_file = files.File('/{}.json'.format(self.key))
    return self.__data_file

if __name__ == '__main__':
  basetest.main()
