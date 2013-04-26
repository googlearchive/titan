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

"""Tests for handlers.py."""

from tests.common import testing

import json
import webapp2
import webtest
from titan.common.lib.google.apputils import basetest
from titan import files
from titan.common import handlers

class HandlersTest(testing.BaseTestCase):

  def testBaseHandler(self):
    self.app = webtest.TestApp(test_application)
    response = self.app.get('/test')
    self.assertEqual(json.dumps({'message': 'hello'}), response.body)

class TestHandler(handlers.BaseHandler):

  def get(self):
    self.write_json_response({'message': 'hello'})

test_application = webapp2.WSGIApplication((
    ('/test', TestHandler),
), debug=False)

if __name__ == '__main__':
  basetest.main()
