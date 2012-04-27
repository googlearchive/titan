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

  def UrlFetch(self, url, method='GET', payload=None, headers=None):
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
      content = self.Send(url, payload=payload)
      # NOTE: The status code might not actually be 200 (any 2xx status code
      # might be returned, but urllib2's urlopen method doesn't exactly provide
      # an easy way to get this information.
      status_code = 200
    except urllib2.HTTPError, e:
      content = e.read()
      status_code = e.code

    # Convert any unicode strings to byte strings using utf-8 encoding.
    if isinstance(content, unicode):
      content = content.encode('utf-8')

    resp = Response(content=content, status_code=status_code)
    return resp

  def _CreateRequest(self, url, data=None):
    """Overrides the base method to allow different HTTP methods to be used."""
    request = super(TitanClient, self)._CreateRequest(url, data=data)
    if self.method:
      method = self.method
      self.method = None
    else:
      method = 'POST' if data else 'GET'
    request.get_method = lambda: method
    return request

class Response(object):
  """An urlfetch response.

  Attributes:
    content: The body of the response.
    status_code: HTTP status code.
  """

  def __init__(self, content='', status_code=200, headers=None):
    self.content = content
    self.status_code = status_code
    self.headers = headers or {}
