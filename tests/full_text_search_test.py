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

"""Tests for full_text_search.py"""

from tests import testing

import datetime
from titan.common.lib.google.apputils import basetest
from google.appengine.api import apiproxy_stub_map
from google.appengine.api.search import simple_search_stub
from titan.files import files
from titan.services import full_text_search

class FullTextSearchTest(testing.ServicesTestCase):

  def setUp(self):
    super(FullTextSearchTest, self).setUp()
    services = (
        'titan.services.full_text_search',
    )
    self.EnableServices(services)
    apiproxy_stub_map.apiproxy.RegisterStub(
        'search',
        simple_search_stub.SearchServiceStub())

  def testFullTextSearch(self):
    files.Touch('/foo')
    files.Write('/bar', 'bar', mime_type='text/plain')
    files.Copy('/bar', '/foo/bar')
    files.Delete('/bar')

    # Run the deferred tasks.
    self._RunDeferredTasks(full_text_search.SERVICE_NAME)

    # path:/foo
    result = full_text_search.SearchRequest('path:/foo')
    self.assertTrue(result)
    # path:/bar
    result = full_text_search.SearchRequest('path:/bar')
    self.assertFalse(result)
    # is:exists
    result = full_text_search.SearchRequest('is:exists')
    self.assertTrue('/foo' in result)
    self.assertTrue('/foo/bar' in result)
    self.assertFalse('/bar' in result)
    # name:foo
    result = full_text_search.SearchRequest('name:foo')
    self.assertEqual(['/foo'], result)
    # mime_type:text/plain
    result = full_text_search.SearchRequest('mime_type:text/plain')
    self.assertEqual(['/foo/bar'], result)
    # content:bar
    result = full_text_search.SearchRequest('content:bar')
    self.assertEqual(['/foo/bar'], result)
    # paths:/foo
    result = full_text_search.SearchRequest('paths:/foo')
    self.assertEqual(['/foo/bar'], result)
    # created:2011-11-28
    today = datetime.date.today()
    result = full_text_search.SearchRequest('created:' + str(today))
    self.assertTrue('/foo/bar' in result)
    # modified:2011-11-28
    today = datetime.date.today()
    result = full_text_search.SearchRequest('modified:' + str(today))
    self.assertTrue('/foo/bar' in result)
    # created_by:titanuser@example.com
    result = full_text_search.SearchRequest('created_by:titanuser@example.com')
    self.assertTrue('/foo/bar' in result)
    # modified_by:titanuser@example.com
    result = full_text_search.SearchRequest('modified_by:titanuser@example.com')
    self.assertTrue('/foo/bar' in result)

if __name__ == '__main__':
  basetest.main()
