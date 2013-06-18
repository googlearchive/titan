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

"""Tests for wsgi.py."""

from tests.common import testing

import webtest

from titan.common.lib.google.apputils import basetest
from titan import wsgi

class WSGITest(testing.BaseTestCase):

  def testRoutesAndWSGIApplication(self):
    routes = (
        ('/simple', GoodHandler),
        wsgi.Route(r'/closed', GoodHandler, login=True),
        wsgi.Route(r'/admin', GoodHandler, admin=True),
        wsgi.Route(
            r'/secure', GoodHandler, login=True, secure=True, schemes=['http']),
        wsgi.Route(r'/error', BadHandler),
        wsgi.Route(r'/.*', GoodHandler),
    )

    application = wsgi.WSGIApplication(routes)
    self.app = webtest.TestApp(application)

    # Verify simple tuple routes are passed through and handled by webapp2.
    response = self.app.get('/simple')
    self.assertEqual(200, response.status_int)

    # Verify login required.
    self.Logout()
    response = self.app.get('/closed')
    self.assertIn('google.com/accounts/Login?', response.headers['Location'])
    self.assertEqual(302, response.status_int)
    self.login()
    response = self.app.get('/closed')
    self.assertEqual(200, response.status_int)

    # Verify admin required.
    self.Logout()
    response = self.app.get('/admin')
    self.assertIn('google.com/accounts/Login?', response.headers['Location'])
    self.assertEqual(302, response.status_int)
    self.login()
    response = self.app.get('/admin', expect_errors=True)
    self.assertEqual(403, response.status_int)
    self.login(is_admin=True)
    response = self.app.get('/admin')
    self.assertEqual(200, response.status_int)

    # Verify SSL redirect (should come first, before auth redirect)
    # and make sure that schemes=['http'] is overridden by secure=True.
    self.Logout()
    response = self.app.get('http://testbed.example.com/secure?query')
    self.assertEqual(301, response.status_int)
    location = response.headers['Location']
    self.assertEqual('https://testbed.example.com/secure?query', location)
    response = self.app.get(location)
    self.assertEqual(302, response.status_int)

    # Verify error handling,
    response = self.app.get('/error', expect_errors=True)
    self.assertEqual(400, response.status_int)

class GoodHandler(wsgi.RequestHandler):

  def get(self):
    pass

class BadHandler(wsgi.RequestHandler):

  def get(self):
    self.abort(400)

if __name__ == '__main__':
  basetest.main()
