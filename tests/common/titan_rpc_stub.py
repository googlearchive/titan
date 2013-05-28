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

"""Stub of titan_rpc for use in unit tests.

This module provides a TitanClientStub as replacement for titan_rpc.TitanClient.
The stub pipes all requests through the real handlers (using webtest) and
ultimately hits the shared unit testing stubs for datastore/memcache/etc.

See TestableRemoteFileFactory in files/files_client_test.py for example usage.
"""

import cStringIO
import mimetools
import urllib2

import webapp2
import webtest

from titan.common import titan_rpc

class TitanClientStub(titan_rpc.TitanClient):

  def __init__(self, *args, **kwargs):
    self._wsgi_apps = kwargs.pop('stub_wsgi_apps')
    super(TitanClientStub, self).__init__(*args, **kwargs)

  def validate_client_auth(self):
    # Disable authentication.
    pass

  def _GetOpener(self):  # Must be non-PEP 8 style name.
    opener = super(TitanClientStub, self)._GetOpener()
    opener.add_handler(HijackAllRequestsHandler(self._wsgi_apps))
    return opener

class HijackAllRequestsHandler(urllib2.BaseHandler):
  """A urllib2 handler which pipes all requests through webtest."""

  def __init__(self, wsgi_applications):
    # BaseHandler has no __init__ and is an old-style class,
    # so don't use super() here.
    self._wsgi_apps = wsgi_applications

  def _get_test_app_from_url_path(self, url_path):
    test_request = webapp2.Request.blank(url_path)
    for wsgi_app in self._wsgi_apps:
      if wsgi_app.router.match(test_request):
        return webtest.TestApp(wsgi_app)
    raise ValueError(
        "url_path %r doesn't match any of the registered wsgi applications."
        % url_path)

  def default_open(self, req):
    """urllib2.BaseHandler method called before any protocol methods.

    If this method returns a non-None response, no other opener methods
    will be called. We use this to hijack the opener entirely and short-circuit
    the response, but this also hides any behavior which might be present
    in other opener handlers added in appengine_rpc.

    Args:
      req: A urllib2 request object.
    Raises:
      urllib2.HTTPError: When a non 2XX or 3XX status code is returned.
    Returns:
      A urllib2.addinfourl object.
    """
    method = req.get_method()
    url_path = req.get_selector()
    headers = req.headers
    payload = req.get_data() if req.has_data() else None
    test_app = self._get_test_app_from_url_path(url_path)

    # 1. urllib2 request/response --> webtest request/response.
    if method == 'GET':
      response = test_app.get(
          url_path, headers=headers, expect_errors=True)
    elif method == 'POST':
      response = test_app.post(
          url_path, payload, headers=headers, expect_errors=True)
    elif method == 'PUT':
      response = test_app.put(
          url_path, payload, headers=headers, expect_errors=True)
    elif method == 'DELETE':
      response = test_app.delete(
          url_path, headers=headers, expect_errors=True)
    else:
      raise NotImplementedError('Method not implemented: %s' % method)

    fp = cStringIO.StringIO(response.body)
    status_code = response.status_int

    # 2. webtest errors --> urllib2 errors.
    if status_code < 200 or status_code > 299:
      # Handle all non-2XX status codes.
      if status_code >= 300 and status_code < 400:
        # Ignore 3XX status codes.
        pass
      else:
        # Raise errors for 4XX-5XX status codes.
        raise urllib2.HTTPError(url_path, status_code, response.status, {}, fp)

    # 3. webtest response --> urllib2 response.

    # Transform a nice header datastructure back into a big RFC 2965 string,
    # wrap that in a file pointer, and then stick it in mimetools.Message to be
    # reparsed later by addinfourl. :|
    urllib2_headers = ['%s: %s' % (k, v) for k, v in response.headers.items()]
    urllib2_headers = cStringIO.StringIO('\n'.join(urllib2_headers))
    urllib2_headers = mimetools.Message(urllib2_headers)

    urllib2_response = urllib2.addinfourl(
        fp, urllib2_headers, url_path, status_code)
    urllib2_response.msg = response.status

    return urllib2_response
