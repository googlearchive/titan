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

"""Base test case classes for App Engine webapp handlers.

Usage:
  class YourTestCase(webapp_testing.WebAppTestCase):

    def testGetHandler(self):
      response = self.Get(handlers.SomeHandlerClass, params={'query': 'param'})
      # ... assert response correctness ...

    def testPostHandler(self):
      response = self.Post(handlers.SomeHandlerClass, params={'query': 'param'})
      # Or, to send data which is already urlencoded:
      response = self.Post(handlers.SomeHandlerClass, payload='query=param')
      # ... assert response correctness ...
"""

import cStringIO
import urllib
from google.appengine.ext import webapp
from titan.common.lib.google.apputils import basetest

class WebAppTestCase(basetest.TestCase):
  """Base test class for a AppEngine webapp tests."""

  @staticmethod
  def GetDefaultEnvironment():
    """Returns a default request environment.

    Override this if you need to define more-specific environment variables.

    Returns:
      A dict of default environment variables.
    """
    env = {
        'CONTENT_LENGTH': '',
        'SERVER_PORT': '8080',
        'CURRENT_VERSION_ID': '1.1',
        'SERVER_SOFTWARE': 'Development/1.0',
        'SCRIPT_NAME': '',
        'REQUEST_METHOD': 'GET',
        'HTTP_HOST': 'localhost:8080',
        'PATH_INFO': '/dir/subdir/myhandler',
        'SERVER_PROTOCOL': 'HTTP/1.0',
        'QUERY_STRING': 'foo=bar&foo=baz&foo2=123',
        'USER_ID': '123',
        'USER_EMAIL': 'test@example.com',
        'HTTP_USER_AGENT': 'FakeUserAgent/1.0',
        'SERVER_NAME': 'localhost',
        'REMOTE_ADDR': '127.0.0.1',
        'GATEWAY_INTERFACE': 'CGI/1.1',
        'HTTP_ACCEPT_LANGUAGE': 'en',
        'APPLICATION_ID': 'dev~testapp',
        'CONTENT_TYPE': 'application/x-www-form-urlencoded',
        'PATH_TRANSLATED': '/tmp/fake-file.py',
        'wsgi.url_scheme': 'http',
    }
    return env

  @staticmethod
  def CreateRequestHandler(handler_factory=None, env=None):
    """Creates an initialized request handler for use in testing.

    Args:
      handler_factory: A callable to construct an instance of the right
        RequestHandler type.  Often this is just the name of the class.
      env: A dictionary storing a CGI environment to pass to
        webapp.Request().  If None, this defaults to
        GetDefaultEnvironment().

    Returns:
      The initialized RequestHandler.  The Request and Response objects
      it will use are available as handler.request and handler.response.
    """
    if not handler_factory:
      handler_factory = webapp.RequestHandler
    if env is None:
      env = WebAppTestCase.GetDefaultEnvironment()
    request = webapp.Request(env)
    response = webapp.Response()
    handler = handler_factory()
    handler.initialize(request, response)
    return handler

  def Get(self, handler_class, params=None, *args, **kwargs):
    """Makes a GET request to a handler and returns the response object.

    Args:
      handler_class: A webapp handler class which defines a get() method.
      params: A dictionary or iterable of two-tuples to be urlencoded.
    Returns:
      A response object processed by the handler's get() method.
    """
    environ = self.GetDefaultEnvironment()
    environ['REQUEST_METHOD'] = 'GET'
    if params:
      environ['QUERY_STRING'] = urllib.urlencode(params)

    handler = self.CreateRequestHandler(
        handler_factory=handler_class, env=environ)
    try:
      handler.get(*args, **kwargs)
    except Exception, e:
      handler.handle_exception(e, False)
    return handler.response

  def Post(self, handler_class, payload=None, params=None, extra_headers=None,
           *args, **kwargs):
    """Makes a POST request on a handler and returns the response object.

    Args:
      handler_class: A webapp handler class which defines a post() method.
      payload: A urlencoded data payload.
      params: If provided, will be urlencoded and sent as the payload.
          This is a convenience arg to allow passing a dictionary of query args.
      extra_headers: A mapping of headers like 'Content-Type' to string values.
    Returns:
      A response object processed by the handler's post() method.
    """
    environ = self.GetDefaultEnvironment()
    environ['REQUEST_METHOD'] = 'POST'
    if payload is None and params is None:
      payload = ''
    if payload is not None or params:
      if params:
        payload = urllib.urlencode(params)
      environ['wsgi.input'] = cStringIO.StringIO(payload)
      environ['CONTENT_LENGTH'] = len(payload)
    if extra_headers:
      for key, value in extra_headers.iteritems():
        environ[key.upper().replace('-', '_')] = str(value)

    handler = self.CreateRequestHandler(
        handler_factory=handler_class, env=environ)
    try:
      handler.post(*args, **kwargs)
    except Exception, e:
      handler.handle_exception(e, False)
    return handler.response

