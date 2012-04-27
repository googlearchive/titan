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

import cStringIO
import urllib2
import mox
from titan.common.lib.google.apputils import basetest
from titan.common import titan_rpc

class TitanRpcTest(basetest.TestCase):

  def setUp(self):
    self.mox = mox.Mox()
    host = 'www.example.com'
    self.client = titan_rpc.TitanClient(
        host=host, auth_function=titan_rpc.AuthFunc,
        user_agent='TitanClient/1.0', source='-')

  def tearDown(self):
    self.mox.UnsetStubs()
    self.mox.ResetAll()

  def testCopy(self):
    # Verify that attributes aren't shared across copied instances.
    new_client = self.client.Copy()
    self.client.extra_headers['X-Foo'] = 'Bar'
    new_client.extra_headers['X-Foo'] = 'Qux'
    self.assertNotEqual(self.client.extra_headers, new_client.extra_headers)

  def testUrlFetch(self):
    self.mox.StubOutWithMock(self.client, 'Send')
    # /api/foo => return "foobar".
    self.client.Send('/api/foo', payload=None).AndReturn('foobar')

    # /api/bar => return 404 status.
    error = urllib2.HTTPError('', 404, '', '', cStringIO.StringIO())
    self.client.Send('/api/bar', payload=None).AndRaise(error)
    self.mox.ReplayAll()

    resp = self.client.UrlFetch('/api/foo')
    self.assertEqual(200, resp.status_code)
    self.assertEqual('foobar', resp.content)

    resp = self.client.UrlFetch('/api/bar')
    self.assertEqual(404, resp.status_code)

    self.mox.VerifyAll()

if __name__ == '__main__':
  basetest.main()
