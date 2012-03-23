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
  import json
except ImportError:
  import simplejson as json
import datetime
import httplib
import urllib
import urllib2
import urlparse
from poster import encode
from poster import streaminghttp
from google.appengine.tools import appengine_rpc

class Error(Exception):
  pass

class AuthenticationError(Error):
  pass

class BadFileError(Error):
  pass

class TitanClient(appengine_rpc.HttpRpcServer):
  """Class that performs Titan file operations over RPC to a Titan service."""

  def Exists(self, path, **kwargs):
    """Returns True if the path exists, False otherwise."""
    params = {'path': path}
    params.update(kwargs)
    return json.loads(self._Get('/_titan/exists', params))

  def Get(self, paths, full=False, **kwargs):
    """Gets a serialized version of one or more files.

    Args:
      paths: Absolute filename or iterable of absolute filenames.
      full: Whether or not to include this object's content. Potentially
          expensive if the content is large and particularly if the content is
          stored in blobstore.
      **kwargs: Additional keyword args to be encoded in the request params.
    Returns:
      None: If given single path which didn't exist.
      Serialized File dictionary: If given a single path which did exist.
      Dict: When given multiple paths, returns a dict of paths --> serialized
          File objects. Non-existent file paths are not included in the result.
    """
    is_multiple = hasattr(paths, '__iter__')
    if not is_multiple:
      paths = [paths]
    params = [('path', path) for path in paths]
    if full:
      params += [('full', full)]
    params += kwargs.items()

    data = json.loads(self._Get('/_titan/get', params))
    if not is_multiple and not data:
      # Single file requested doesn't exist.
      return

    for file_obj in data.values():
      file_obj['modified'] = datetime.datetime.fromtimestamp(
          file_obj['modified'])
      file_obj['created'] = datetime.datetime.fromtimestamp(
          file_obj['created'])

    return data if is_multiple else data.values()[0]

  def Read(self, path):
    """Returns the contents of a file."""
    try:
      return self._Get('/_titan/read', {'path': path})
    except urllib2.HTTPError, e:
      if e.code == 404:
        raise BadFileError(e)
      raise

  def Write(self, path, content=None, blob=None, fp=None,
            mime_type=None, meta=None, **kwargs):
    """Writes contents to a file.

    Args:
      path: The path of the file.
      content: A byte string representing the contents of the file.
      blob: If content is not provided, a BlobKey pointing to the file.
      fp: If no content and no blob, pass an already-open file pointer.
          This file will be uploaded as multipart data directly to the
          blobstore and this File's blob keys will be automatically populated.
          This arg must be used instead of content for files over ~10 MB.
      meta: A dict of meta key-value pairs.
      mime_type: The MIME type of the file. If not provided, then the MIME type
          will be guessed based on the filename.
      **kwargs: Additional keyword args to be encoded in the request params.
    """
    params = [('path', path)]
    if content is not None:
      params.append(('content', content))
    if blob is not None:
      params.append(('blob', blob))
    if meta is not None:
      params.append(('meta', json.dumps(meta)))
    if mime_type is not None:
      params.append(('mime_type', mime_type))
    params += kwargs.items()

    try:
      if fp:
        # If given a file pointer, create a multipart encoded request.
        write_blob_url = self._Get('/_titan/newblob')
        params.append(('file', fp))
        content_generator, headers = encode.multipart_encode(params)
        # Make custom opener to support multipart POST requests.
        opener = urllib2.build_opener()
        opener.add_handler(streaminghttp.StreamingHTTPSHandler())
        # Upload directly to the blobstore URL, avoiding authentication.
        request = urllib2.Request(write_blob_url, data=content_generator,
                                  headers=headers)
        response = opener.open(request)
        # Pull the blobkey out of the query params and fall through
        # to the call to /_titan/write.
        url = response.geturl()
        response_params = urlparse.parse_qs(urlparse.urlparse(url).query)

        # Verify that "blob" was not passed in.
        assert not blob
        params.append(('blob', response_params['blob'][0]))
      self._Post('/_titan/write', params=params)
    except urllib2.HTTPError, e:
      if e.code == 404:
        raise BadFileError(e)
      raise

  def Copy(self, source_path, destination_path, **kwargs):
    """Copies a file.

    Args:
      source_path: The source path to copy.
      destination_path: The destination (a full file path).
      **kwargs: Extra keyword args to pass to the handler.
    Raises:
      BadFileError: If the source path does not exist.
    """
    params = {
        'source_path': source_path,
        'destination_path': destination_path,
    }
    params.update(kwargs)
    try:
      self._Post('/_titan/copy', params)
    except urllib2.HTTPError, e:
      if e.code == 404:
        raise BadFileError(e)
      raise

  def Delete(self, path, **kwargs):
    """Deletes a file."""
    try:
      params = {'path': path}
      params.update(kwargs)
      self._Post('/_titan/delete', params)
    except urllib2.HTTPError, e:
      if e.code == 404:
        raise BadFileError(e)
      raise

  def Touch(self, path, **kwargs):
    """Touches a file."""
    params = {'path': path}
    params.update(kwargs)
    self._Post('/_titan/touch', params)

  def ListFiles(self, path, recursive=False, **kwargs):
    """Lists files in a directory."""
    params = {'path': path}
    params.update(kwargs)
    if recursive:
      params['recursive'] = 1
    return json.loads(self._Get('/_titan/listfiles', params))

  def ListDir(self, path, **kwargs):
    """Lists directory strings and files in a directory."""
    params = {'path': path}
    params.update(kwargs)
    result = json.loads(self._Get('/_titan/listdir', params))
    return (result['dirs'], result['files'])

  def DirExists(self, path, **kwargs):
    """Check the existence of a directory."""
    params = {'path': path}
    params.update(kwargs)
    return json.loads(self._Get('/_titan/direxists', params))

  def ValidateClientAuth(self):
    """Test the stored credentials, may raise AuthenticationError."""
    try:
      if self._HostIsDevAppServer():
        return
      credentials = self.auth_function()
      self._GetAuthToken(credentials[0], credentials[1])
      # Valid credentials, call _Authenticate to populate self state.
      self._Authenticate()
    except appengine_rpc.ClientLoginError, e:
      error = ('Error %d: %s %s' %
               (e.code, e.reason, e.info or e.msg or '')).strip()
      raise AuthenticationError('Invalid username or password. (%s)' % error)

  def _Get(self, url_path, params=None):
    """Makes a GET request to the API service and returns the response body."""
    if params is None:
      params = {}
    url = '%s?%s' % (url_path, urllib.urlencode(params)) if params else url_path
    return super(TitanClient, self).Send(url, payload=None)

  def _Post(self, url_path, params=None, payload=None, content_type=None,
            extra_headers=None):
    """Makes a POST request to the API service and returns the response body.

    Args:
      url_path: The URL string to POST to.
      params: A list or dictionary of urllib request params.
      payload: If no params, an already-encoded payload string.
      content_type: Content-Type of the request.
      extra_headers: A dictionary of extra headers to put in the request.
    Returns:
      The response body of the post.
    """
    payload = payload or urllib.urlencode(params)
    if not content_type:
      content_type = 'application/x-www-form-urlencoded'
    if extra_headers:
      # This is an attribute of AbstractRpcServer, used in self._CreateRequest.
      self.extra_headers = extra_headers
    return super(TitanClient, self).Send(url_path, payload=payload,
                                         content_type=content_type)

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
