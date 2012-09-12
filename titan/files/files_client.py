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

"""App Engine RPC client for Titan Files."""

import collections
import datetime
import json
import urllib
import urllib2
import urlparse
from poster import encode
from poster import streaminghttp
from titan.common import titan_rpc
from titan.common import utils

FILE_API_PATH_BASE = '/_titan/file'
FILES_API_PATH_BASE = '/_titan/files'
FILE_READ_API = '/read'
FILE_NEWBLOB_API = '/newblob'

class Error(Exception):
  pass

class BadRemoteFileError(Error):
  pass

class BadRemoteFilesError(Error):
  pass

class RemoteFileFactory(titan_rpc.AbstractRemoteFactory):
  """Factory for creating RemoteFile objects."""

  def MakeRemoteFile(self, *args, **kwargs):
    """Should be used to create all RemoteFile objects."""
    kwargs['_titan_client'] = self.titan_client
    return RemoteFile(*args, **kwargs)

  def MakeRemoteFiles(self, *args, **kwargs):
    """Should be used to create all RemoteFiles objects."""
    kwargs['_titan_client'] = self.titan_client
    return RemoteFiles(*args, **kwargs)

class RemoteFile(object):
  """A remote imitation of files.File."""

  def __init__(self, path, **kwargs):
    utils.ValidateFilePath(path)
    self._path = path
    self._titan_client = kwargs.pop('_titan_client')
    self._file_data = None
    self._real_path = None
    self._meta = None
    self._file_kwargs = kwargs.copy()

  @property
  def file_data(self):
    if not self._file_data:
      params = {'path': self.path}
      if self._file_kwargs:
        params['file_params'] = json.dumps(self._file_kwargs)
      url = '%s?%s' % (FILE_API_PATH_BASE, urllib.urlencode(params))
      response = self._titan_client.UrlFetch(url)
      self._VerifyResponse(response)
      self._file_data = json.loads(response.content)
    return self._file_data

  @property
  def name(self):
    return self.file_data['name']

  @property
  def path(self):
    return self._path

  @property
  def real_path(self):
    return self.file_data.get('real_path') or self._path

  @property
  def paths(self):
    return self.file_data['paths']

  @property
  def mime_type(self):
    return self.file_data['mime_type']

  @property
  def created(self):
    return datetime.datetime.fromtimestamp(self.file_data['created'])

  @property
  def modified(self):
    return datetime.datetime.fromtimestamp(self.file_data['modified'])

  @property
  def size(self):
    return self.file_data['size']

  @property
  def content(self):
    url = '%s%s?%s' % (FILE_API_PATH_BASE, FILE_READ_API,
                       urllib.urlencode({'path': self.path}))
    response = self._titan_client.UrlFetch(url)
    self._VerifyResponse(response)
    # TODO(user): encoding?
    return response.content

  @property
  def exists(self):
    try:
      return bool(self.file_data)
    except BadRemoteFileError:
      return False

  @property
  def blob(self):
    # Different API: returns string key instead of BlobKey object.
    return self.file_data['name']

  @property
  def created_by(self):
    # Different API: return email address instead of User object.
    return self.file_data['created_by']

  @property
  def modified_by(self):
    return self.file_data['modified_by']

  @property
  def md5_hash(self):
    return self.file_data['md5_hash']

  @property
  def meta(self):
    """File meta data."""
    if not self._meta:
      self._meta = utils.DictAsObject(self.file_data['meta'])
    return self._meta

  def Write(self, content=None, blob=None, fp=None, mime_type=None, meta=None,
            **kwargs):
    """Writes contents to a file.

    Args:
      content: A byte string representing the contents of the file.
      blob: If content is not provided, a BlobKey pointing to the file.
      fp: If no content and no blob, pass an already-open file pointer.
          This file will be uploaded as multipart data directly to the
          blobstore and this File's blob keys will be automatically populated.
          This arg must be used instead of content for files over ~10 MB.
      mime_type: The MIME type of the file. If not provided, then the MIME type
          will be guessed based on the filename.
      meta: A dict of meta key-value pairs.
      **kwargs: Additional keyword args to be encoded in the request params.
    """
    self._file_data = None

    params = [('path', self.path)]
    if content is not None:
      params.append(('content', content))
    if blob is not None:
      params.append(('blob', blob))
    if meta is not None:
      params.append(('meta', json.dumps(meta)))
    if mime_type is not None:
      params.append(('mime_type', mime_type))

    # Extra keyword arguments.
    if self._file_kwargs:
      params.append(('file_params', json.dumps(self._file_kwargs)))
    if kwargs:
      params.append(('method_params', json.dumps(kwargs)))

    try:
      if fp:
        # If given a file pointer, create a multipart encoded request.
        path = FILE_API_PATH_BASE + FILE_NEWBLOB_API
        write_blob_url = self._titan_client.UrlFetch(path).content
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
      path_param = {'path': self.path}
      url = '%s?%s' % (FILE_API_PATH_BASE, urllib.urlencode(path_param))
      payload = urllib.urlencode(params)
      response = self._titan_client.UrlFetch(url, method='POST',
                                             payload=payload)
      self._VerifyResponse(response)
    except urllib2.HTTPError, e:
      if e.code == 404:
        raise BadRemoteFileError('File does not exist: %s' % self.path)
      raise

  def Delete(self):
    self._file_data = None
    try:
      params = {'path': self.path}
      url = '%s?%s' % (FILE_API_PATH_BASE, urllib.urlencode(params))
      payload = urllib.urlencode(params)
      response = self._titan_client.UrlFetch(url, method='DELETE',
                                             payload=payload)
      self._VerifyResponse(response)
    except urllib2.HTTPError, e:
      if e.code == 404:
        raise BadRemoteFileError('File does not exist: %s' % self.path)
      raise

  def _VerifyResponse(self, response):
    if response.status_code == 404:
      raise BadRemoteFileError('File does not exist: %s' % self.path)
    elif not 200 <= response.status_code <= 299:
      raise titan_rpc.RpcError(response.content)

class RemoteFiles(collections.Mapping):
  """A remote imitation of files.Files."""

  def __init__(self, paths=None, files=None, **kwargs):
    self._titan_client = kwargs.pop('_titan_client')
    if paths is not None and files is not None:
      raise TypeError('Exactly one of "paths" or "files" args must be given.')
    self._titan_files = {}
    if paths and not hasattr(paths, '__iter__'):
      raise ValueError('"paths" must be an iterable.')
    if files and not hasattr(files, '__iter__'):
      raise ValueError('"files" must be an iterable.')
    if paths is not None:
      for path in paths:
        self._titan_files[path] = RemoteFile(path=path,
                                             _titan_client=self._titan_client)
    else:
      for titan_file in files:
        self._titan_files[titan_file.path] = titan_file

  def __delitem__(self, path):
    del self._titan_files[path]

  def __getitem__(self, path):
    return self._titan_files[path]

  def __setitem__(self, path, titan_file):
    assert isinstance(titan_file, RemoteFile)
    self._titan_files[path] = titan_file

  def __contains__(self, other):
    path = getattr(other, 'path', other)
    return path in self._titan_files

  def __iter__(self):
    for key in self._titan_files:
      yield key

  def __len__(self):
    return len(self._titan_files)

  def __eq__(self, other):
    if not isinstance(other, RemoteFiles) or len(self) != len(other):
      return False
    for titan_file in other.itervalues():
      if titan_file not in self:
        return False
    return True

  def __repr__(self):
    return '<RemoteFiles %r>' % self.keys()

  def clear(self):
    self._titan_files = {}

  def List(self, dir_path, recursive=False, depth=None):
    """Method to populate the current RemoteFiles mapping for the given dir.

    This method knowingly diverges from the API as it doesn't return a
    RemoteFiles object and instead overwrites the current instance. This is due
    to auth token restrictions.

    Args:
      dir_path: Absolute directory path.
      recursive: Whether to list files recursively.
      depth: If recursive, a positive integer to limit the recusion depth.
          1 is one folder deep, 2 is two folders deep, etc.

    """
    params = [('dir_path', dir_path), ('ids_only', 'true')]
    if recursive:
      params.append(('recursive', 'true'))
    if depth is not None:
      params.append(('depth', depth))

    url = '%s?%s' % (FILES_API_PATH_BASE, urllib.urlencode(params))

    response = self._titan_client.UrlFetch(url)
    self._VerifyResponse(response)
    data = json.loads(response.content)

    if self._titan_files:
      self._titan_files = {}

    for path in data['paths']:
      self._titan_files[path] = RemoteFile(path=path,
                                           _titan_client=self._titan_client)
    return self

  def Delete(self):
    # TODO(user): implement batch operation. For now, the naive way:
    for remote_file in self.itervalues():
      remote_file.Delete()
    # Empty the container:
    self.clear()
    return self

  def _VerifyResponse(self, response):
    if response.status_code == 404:
      raise BadRemoteFilesError('Directory does not exist')
    elif not 200 <= response.status_code <= 299:
      raise titan_rpc.RpcError(response.content)

