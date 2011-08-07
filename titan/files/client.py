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

"""Client to connect to a Titan service.

Usage:
  host = 'example.appspot.com'
  auth_func = lambda: ('username@example.com', getpass.getpass())
  titan_api = client.TitanClient(host, auth_func,
                                 user_agent='TitanClient/1.0', source='-')
  titan_api.Write('/path/to/file.txt', 'foobar')
"""

try:
  import json as simplejson
except ImportError:
  import simplejson
import datetime
import urllib
import urllib2
from google.appengine.tools import appengine_rpc

class AuthenticationError(Exception):
  pass

class BadFileError(Exception):
  pass

class TitanClient(appengine_rpc.HttpRpcServer):
  """Class that performs Titan file operations over RPC to a Titan service."""

  def Exists(self, path):
    """Returns True if the path exists, False otherwise."""
    return simplejson.loads(self._Get('/_titan/exists', {'path': path}))

  def Get(self, paths, full=False):
    """Gets a serialized version of one or more files.

    Args:
      paths: Absolute filename or iterable of absolute filenames.
      full: Whether or not to include this object's content. Potentially
          expensive if the content is large and particularly if the content is
          stored in blobstore.
    Returns:
      A dictionary (or list of dictionaries) of the File properties.
    """
    is_multiple = hasattr(paths, '__iter__')
    if not is_multiple:
      paths = [paths]
    params = [('path', path) for path in paths]
    if full:
      params += [('full', full)]

    try:
      data = simplejson.loads(self._Get('/_titan/get', params))
    except urllib2.HTTPError, e:
      if e.code == 404:
        raise BadFileError(e)
      raise
    for file_obj in data:
      file_obj['modified'] = datetime.datetime.fromtimestamp(
          file_obj['modified'])
      file_obj['created'] = datetime.datetime.fromtimestamp(
          file_obj['created'])

    return data if is_multiple else data[0]

  def Read(self, path):
    """Returns the contents of a file."""
    try:
      return self._Get('/_titan/read', {'path': path})
    except urllib2.HTTPError, e:
      if e.code == 404:
        raise BadFileError(e)
      raise

  def Write(self, path, content=None, blobs=None, mime_type=None, meta=None):
    """Writes contents to a file.

    Args:
      path: The path of the file.
      content: A byte string representing the contents of the file.
      blobs: If content is not provided, a list of BlobKey strings
          comprising the file.
      meta: A dict of meta key-value pairs.
      mime_type: The MIME type of the file. If not provided, then the MIME type
          will be guessed based on the filename.
    """
    params = [('path', path)]
    if content is not None:
      params.append(('content', content))
    if blobs is not None:
      if not hasattr(blobs, '__iter__'):
        # Prevent single string arguments.
        raise ValueError('blobs argument must be an iterable.')
      for blob_key in blobs:
        params.append(('blobs', str(blob_key)))
    if meta is not None:
      params.append(('meta', simplejson.dumps(meta)))
    if mime_type is not None:
      params.append(('mime_type', mime_type))
    try:
      self._Post('/_titan/write', params)
    except urllib2.HTTPError, e:
      if e.code == 404:
        raise BadFileError(e)
      raise

  def Delete(self, path):
    """Deletes a file."""
    try:
      result = self._Post('/_titan/delete', {'path': path})
    except urllib2.HTTPError, e:
      if e.code == 404:
        raise BadFileError(e)
      raise

  def Touch(self, path):
    """Touches a file."""
    self._Post('/_titan/touch', {'path': path})

  def ListFiles(self, path, recursive=False):
    """Lists files in a directory."""
    params = {'path': path}
    if recursive:
      params['recursive'] = 1
    return simplejson.loads(self._Get('/_titan/listfiles', params))

  def ListDir(self, path):
    """Lists directory strings and files in a directory."""
    params = {'path': path}
    result = simplejson.loads(self._Get('/_titan/listdir', params))
    return (result['dirs'], result['files'])

  def DirExists(self, path):
    """Check the existence of a directory."""
    params = {'path': path}
    return simplejson.loads(self._Get('/_titan/direxists', params))

  def ValidateClientAuth(self):
    """Test the stored credentials, may raise AuthenticationError."""
    try:
      if self._HostIsDevAppServer():
        return
      credentials = self.auth_function()
      self._GetAuthToken(credentials[0], credentials[1])
    except appengine_rpc.ClientLoginError, e:
      error = ('Error %d: %s %s' %
               (e.code, e.reason, e.info or e.msg or '')).strip()
      raise AuthenticationError('Invalid username or password. (%s)' % error)

  def _Get(self, url_path, params=None):
    """Makes a GET request to the API service and returns the response body."""
    if params is None:
      params = {}
    url = '%s?%s' % (url_path, urllib.urlencode(params))
    return self.Send(url, payload=None)

  def _Post(self, url_path, params):
    """Makes a POST request to the API service and returns the response body."""
    return self.Send(url_path, payload=urllib.urlencode(params),
                     content_type='application/x-www-form-urlencoded')

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
