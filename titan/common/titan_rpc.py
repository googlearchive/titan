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

"""Additional utility methods for appengine_rpc.HttpRpcServer.

This module is meant to be used as a base for Titan RPC clients, but can also be
used directly to make authenticated requests to an App Engine app.

Sample usage:
  client = titan_rpc.TitanClient(
      host=host, auth_function=auth_function, user_agent=user_agent,
      source=source)
  resp = client.UrlFetch('/_titan/read?path=/foo/bar.txt')
  print resp.content
"""

import copy
import getpass
import sys
import urllib2
from google.appengine.tools import appengine_rpc

USER_AGENT = 'TitanRpcClient/1.0'
SOURCE = '-'

class Error(Exception):
  pass

class AuthenticationError(Error):
  pass

class RpcError(Error):
  pass

def AuthFunc():
  """Default auth func."""
  email = ''
  password = ''
  if sys.stdin.isatty():
    email = raw_input('Email: ')
    password = getpass.getpass('Password: ')
  return email, password

class TitanClient(appengine_rpc.HttpRpcServer):
  """RPC class to make authenticated requests to an App Engine app.

  NOTE: This class isn't thread-safe; avoid using the same instance of this
  object in threaded situations. A Copy() method is provided for convenience to
  make copies of instances that can be used in threads.
  """

  def __init__(self, *args, **kwargs):
    super(TitanClient, self).__init__(*args, **kwargs)
    self.method = None
    self.orig_headers = self.extra_headers.copy()

  def Copy(self):
    """Copies an instance of self."""
    obj = copy.copy(self)
    # The copy.copy() method constructs a new object and copies references into
    # it. As a result, we need to create shallow copies of self.extra_headers
    # and self.orig_headers so that the copied object doesn't retain references
    # to the original dicts.
    obj.extra_headers = self.extra_headers.copy()
    obj.orig_headers = self.orig_headers.copy()
    return obj

  def UrlFetch(self, url, method='GET', payload=None, headers=None, **kwargs):
    """Fetches a URL path and returns a Response object.

    Args:
      url: URL path (along with query params) to request.
      method: HTTP method.
      payload: POST body.
      headers: Dict of headers to send with the request.
    Returns:
      A Response object.
    """
    # Set self.method to the HTTP method so that we can override urllib2's
    # "get_method" method with something other than GET or POST. As a result of
    # this "hack", this class/method isn't thread-safe; avoid using the same
    # instance of this object in threaded situations.
    self.method = method.upper()
    if self.method in ['PATCH', 'POST', 'PUT']:
      payload = payload or ''
    else:
      payload = None

    # This is an attribute of AbstractRpcServer, used in self._CreateRequest.
    self.extra_headers = self.orig_headers.copy()
    if headers:
      self.extra_headers.update(headers)

    try:
      # content_type must unfortunately be given to the base class here,
      # not in a header.
      content_type = self.extra_headers.pop(
          'Content-Type', 'application/x-www-form-urlencoded')
      content = self.Send(url, payload=payload, content_type=content_type,
                          **kwargs)
      # NOTE: The status code might not actually be 200 (any 2xx status code
      # might be returned, but appengine_rpc doesn't exactly provide an
      # easy way to get this information.
      status_code = 200
    except urllib2.HTTPError, e:
      content = e.read()
      status_code = e.code

    # Convert any unicode strings to byte strings using utf-8 encoding.
    if isinstance(content, unicode):
      content = content.encode('utf-8')

    resp = Response(content=content, status_code=status_code)
    return resp

  def ValidateClientAuth(self):
    """Test the stored credentials, may raise AuthenticationError."""
    try:
      if self._HostIsDevAppServer():
        self._DevAppServerAuthenticate()
        self.orig_headers.update(self.extra_headers)
        return
      self._Authenticate()
    except appengine_rpc.ClientLoginError, e:
      error = ('Error %d: %s %s' %
               (e.code, e.reason, e.info or e.msg or '')).strip()
      raise AuthenticationError('Invalid username or password. (%s)' % error)

  def _HostIsDevAppServer(self):
    """Make a single GET / request to see if the server is a dev_appserver."""
    # This exists because appserver_rpc doesn't nicely expose auth error paths.
    try:
      response = urllib2.urlopen('%s://%s/' % (self.scheme, self.host))
      server_header = response.headers.get('server', '')
    except urllib2.URLError, e:
      if not hasattr(e, 'headers'):
        raise
      server_header = e.headers.get('server', '')
    if server_header.startswith('Development'):
      return True
    return False

  def _CreateRequest(self, url, data=None):
    """Overrides the base method to allow different HTTP methods to be used."""
    # pylint: disable=protected-access
    request = super(TitanClient, self)._CreateRequest(url, data=data)
    method = 'POST' if data else 'GET'
    if self.method:
      method = self.method
    request.get_method = lambda: method
    return request

class Response(object):
  """An urlfetch response.

  Attributes:
    content: The body of the response.
    status_code: HTTP status code.
    headers: The HTTP headers.
  """

  def __init__(self, content='', status_code=200, headers=None):
    self.content = content
    self.status_code = status_code
    self.headers = headers or {}

class AbstractRemoteFactory(object):
  """Abstract factory for creating Remote* objects."""

  def __init__(self, host, auth_function=AuthFunc, user_agent=USER_AGENT,
               source=SOURCE, secure=True, **kwargs):
    self.host = host
    self.auth_function = auth_function
    self.user_agent = user_agent
    self.source = source
    self.secure = secure
    self.kwargs = kwargs
    self._titan_client = None

  @property
  def titan_client(self):
    if not self._titan_client:
      self._titan_client = self._GetTitanClient(
          host=self.host,
          auth_function=self.auth_function,
          user_agent=self.user_agent,
          source=self.source,
          secure=self.secure,
          **self.kwargs)
    return self._titan_client

  def _GetTitanClient(self, **kwargs):
    return TitanClient(**kwargs)

  def ValidateClientAuth(self):
    self.titan_client.ValidateClientAuth()
