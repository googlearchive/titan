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

    # Verify behavior of json_content property.
    titan_file.write('{"a":"b"}')
    self.assertEqual({'a': 'b'}, titan_file.json_content)
    titan_file.json_content['c'] = 'd'
    self.assertEqual({'a': 'b', 'c': 'd'}, titan_file.json_content)
    titan_file.save()

    # Get json_content out of the saved file.
    titan_file = files.File('/foo/file.json')
    self.assertEqual({'a': 'b', 'c': 'd'}, titan_file.json_content)

    # Verify invalid JSON raises an error when accessed using json_content.
    titan_file = files.File('/foo/file.json')
    titan_file.write('{% some django tag %}')
    self.assertRaises(json_mixin.BadJsonError, lambda: titan_file.json_content)

    # Verify ability to set json_content on a new file.
    titan_file = files.File('/foo/new_file.json')
    titan_file.json_content = None
    titan_file.save()
    titan_file.json_content = {'foo': 'bar'}
    titan_file.save()
    titan_file = files.File('/foo/new_file.json')
    self.assertEqual({'foo': 'bar'}, titan_file.json_content)

  def testApplyPatch(self):
    titan_file = files.File('/foo/file.json')
    titan_file.json_content = {
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
    self.assertEqual(expected, titan_file.json_content)

if __name__ == '__main__':
  basetest.main()
