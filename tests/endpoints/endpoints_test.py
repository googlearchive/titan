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

"""Tests for endpoints.py."""

from tests.common.lib import endpointstest

import webtest
from protorpc import message_types
from titan.common.lib.google.apputils import basetest
from titan import endpoints

class EndpointsTest(endpointstest.EndpointsTestCase):

  def CreateWsgiApplication(self):
    """Returns the wsgi application for the service endpoint testing."""
    return endpoints.EndpointsApplication([GoodService], restricted=False)

  def testEndpointsApplication(self):
    # You do not want to copy this test; it is specifically for the internal
    # behavior of the endpoints application.
    #
    # Verify that the object correctly delegates all behavior except middleware.
    app = webtest.TestApp(self._endpoints_application)
    internal_discovery_url = '/_ah/spi/BackendService.getApiConfigs'
    response = app.post(
        internal_discovery_url, '{}', content_type='application/json',
        headers={'X-Appengine-Peer': 'apiserving'})
    self.assertEqual(200, response.status_int)

    # Verify error behavior copied from apiserving.api_server.
    self.assertRaises(
        TypeError, endpoints.EndpointsApplication, protocols='foo')

  def testServices(self):
    # Verify that the service method returns the correct message type
    # (defaults to VoidMessage)
    service = self.GetServiceStub(GoodService)
    self.assertEquals(message_types.VoidMessage(), service.foo())

# This also happens to verify that the decorator delegation works.
@endpoints.api(name='titan', version='v1')
class GoodService(endpoints.Service):

  @endpoints.method(name='foo.bar', path='foo/asdf')
  def foo(self, unused_request):
    return message_types.VoidMessage()

if __name__ == '__main__':
  basetest.main()
