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

"""App Engine handlers for managing Titan files."""

try:
  import appengine_config
except ImportError:
  pass

try:
  import json
except ImportError:
  import simplejson as json
import logging
import time
import urllib
from google.appengine.api import blobstore
from google.appengine.ext import webapp
from google.appengine.ext.webapp import blobstore_handlers
from google.appengine.ext.webapp import util
from titan.common import hooks
from titan.common import utils
from titan.files import files

class BaseHandler(webapp.RequestHandler):
  """Base handler for Titan API handlers."""

  def WriteJsonResponse(self, data, **kwargs):
    """Data to serialize. Accepts keyword args to pass to the serializer."""
    self.response.headers['Content-Type'] = 'application/json'
    json_data = json.dumps(data, cls=utils.CustomJsonEncoder, **kwargs)
    self.response.out.write(json_data)

class ExistsHandler(BaseHandler):
  """Handler to check whether a file exists."""

  def get(self):
    path = self.request.get('path')
    # Get and validate extra parameters exposed by service layers.
    valid_params = hooks.GetValidParams(
        hook_name='http-file-exists', request_params=self.request.params)
    self.WriteJsonResponse(files.Exists(path, **valid_params))

class GetHandler(BaseHandler):
  """Handler to return a serialized file representation."""

  def get(self):
    paths = self.request.get_all('path')
    full = bool(self.request.get('full'))
    # Get and validate extra parameters exposed by service layers.
    valid_params = hooks.GetValidParams(
        hook_name='http-file-get', request_params=self.request.params)
    self.WriteJsonResponse(files.Get(paths, **valid_params), full=full)

class ReadHandler(blobstore_handlers.BlobstoreDownloadHandler):
  """Handler to return contents of a file."""

  def get(self):
    path = self.request.get('path')
    file_obj = files.Get(path)
    if not file_obj:
      self.error(404)
      return
    self.response.headers['Content-Type'] = str(file_obj.mime_type)
    self.response.headers['Content-Disposition'] = (
        'inline; filename=%s' % file_obj.name.encode('ascii', 'replace'))

    if file_obj.blob:
      blob_key = file_obj.blob
      self.send_blob(blob_key, content_type=str(file_obj.mime_type))
    else:
      self.response.out.write(file_obj.content)

class WriteHandler(BaseHandler):
  """Handler to write to a file."""

  def post(self):
    path = self.request.get('path')
    # Must use str_POST here to preserve the original encoding of the string.
    content = self.request.str_POST.get('content')
    blob = self.request.get('blob', None)

    if blob is not None:
      # Convert any string keys to BlobKey instances.
      if isinstance(blob, basestring):
        blob = blobstore.BlobKey(blob)

    meta = self.request.get('meta', None)
    if meta:
      meta = json.loads(meta)
    mime_type = self.request.get('mime_type', None)

    # Get and validate extra parameters exposed by service layers.
    valid_params = hooks.GetValidParams(
        hook_name='http-file-write', request_params=self.request.params)
    try:
      files.Write(path, content, blob=blob, mime_type=mime_type,
                  meta=meta, **valid_params)
    except files.BadFileError:
      self.error(404)

class NewBlobHandler(BaseHandler):
  """Handler to get a blob upload URL."""

  def get(self):
    upload_url = blobstore.create_upload_url('/_titan/finalizeblob')
    self.response.out.write(upload_url)

class FinalizeBlobHandler(blobstore_handlers.BlobstoreUploadHandler):
  """Handler to finalize a blob, returning the blobkey."""

  def get(self):
    # This is a noop, just here as an unauthenticated final endpoint for the
    # internal-to-blobstore redirect below.
    pass

  def post(self):
    uploads = self.get_uploads('file')
    blob = str(uploads[0].key())
    # Magic: BlobstoreUploadHandlers must return redirects, so we pass the
    # blobkey back as a query param. The client should followup with a call
    # to Write() and include the blobkey.
    params = urllib.urlencode({'blob': blob})
    self.redirect('/_titan/finalizeblob?%s' % params)

class DeleteHandler(BaseHandler):
  """Handler to delete a file."""

  def post(self):
    paths = self.request.get_all('path')
    # Get and validate extra parameters exposed by service layers.
    valid_params = hooks.GetValidParams(
        hook_name='http-file-delete', request_params=self.request.params)
    try:
      files.Delete(paths, **valid_params)
    except files.BadFileError:
      self.error(404)

class TouchHandler(BaseHandler):
  """Handler to touch a file."""

  def post(self):
    paths = self.request.get_all('path')
    # Get and validate extra parameters exposed by service layers.
    valid_params = hooks.GetValidParams(
        hook_name='http-file-touch', request_params=self.request.params)
    files.Touch(paths, **valid_params)

class CopyHandler(BaseHandler):
  """Handler to copy files from one location to another."""

  def post(self):
    """Copies a file to a new destination.

    Params:
      source_path: The path of the file to copy.
      destination_path: The new destination (a full file path).
    """
    path = self.request.get('source_path')
    dest = self.request.get('destination_path')
    # Get and validate extra parameters exposed by service layers.
    valid_params = hooks.GetValidParams(
        hook_name='http-file-copy', request_params=self.request.params)
    try:
      files.Copy(path, dest, **valid_params)
    except files.BadFileError:
      self.error(404)

class ListFilesHandler(BaseHandler):
  """Handler to list files in a directory."""

  def get(self):
    path = self.request.get('path')
    recursive = bool(self.request.get('recursive'))
    # Get and validate extra parameters exposed by service layers.
    valid_params = hooks.GetValidParams(
        hook_name='http-list-files', request_params=self.request.params)
    file_objs = files.ListFiles(path, recursive=recursive, **valid_params)
    return self.WriteJsonResponse(file_objs)

class ListDirHandler(BaseHandler):
  """Handler to list directories and files in a directory."""

  def get(self):
    path = self.request.get('path')
    # Get and validate extra parameters exposed by service layers.
    valid_params = hooks.GetValidParams(
        hook_name='http-list-dir', request_params=self.request.params)
    dirs, file_objs = files.ListDir(path, **valid_params)
    return self.WriteJsonResponse({'dirs': dirs, 'files': file_objs})

class DirExistsHandler(BaseHandler):
  """Handler to check the existence of a directory."""

  def get(self):
    path = self.request.get('path')
    # Get and validate extra parameters exposed by service layers.
    valid_params = hooks.GetValidParams(
        hook_name='http-dir-exists', request_params=self.request.params)
    return self.WriteJsonResponse(files.DirExists(path, **valid_params))

class FileHandler(BaseHandler):
  """RESTful file handler."""

  def get(self):
    """GET handler."""
    path = self.request.get('path')
    full = bool(self.request.get('full'))

    try:
      file_kwargs, _ = _GetExtraParams(self.request.GET)
    except ValueError:
      self.error(400)
      return

    titan_file = files.File(path, **file_kwargs)
    if not titan_file.exists:
      self.error(404)
      return
    # TODO(user): when full=True, this may fail for files with byte-string
    # content.
    self.WriteJsonResponse(titan_file, full=full)

  def post(self):
    """POST handler."""
    path = self.request.get('path')
    # Must use str_POST here to preserve the original encoding of the string.
    content = self.request.str_POST.get('content')
    blob = self.request.get('blob', None)

    try:
      file_kwargs, method_kwargs = _GetExtraParams(self.request.POST)
    except ValueError:
      self.error(400)
      return

    if blob is not None:
      # Convert any string keys to BlobKey instances.
      if isinstance(blob, basestring):
        blob = blobstore.BlobKey(blob)

    meta = self.request.get('meta', None)
    if meta:
      meta = json.loads(meta)
    mime_type = self.request.get('mime_type', None)

    try:
      files.File(path, **file_kwargs).Write(
          content, blob=blob, mime_type=mime_type, meta=meta, **method_kwargs)
      self.response.set_status(201)
      # Headers must be byte-strings, not unicode strings.
      # Since this is a URI, follow RFC 3986 and encode it using UTF-8.
      location_header = ('/_titan/file/?path=%s' % path).encode('utf-8')
      self.response.headers['Location'] = location_header
    except files.BadFileError:
      self.error(404)
    except (TypeError, ValueError):
      self.error(400)
      logging.exception('Bad request:')

  def delete(self):
    """DELETE handler."""
    path = self.request.get('path')
    try:
      file_kwargs, method_kwargs = _GetExtraParams(self.request.POST)
    except ValueError:
      self.error(400)
      return
    try:
      files.File(path, **file_kwargs).Delete(**method_kwargs)
    except files.BadFileError:
      self.error(404)

class FileReadHandler(blobstore_handlers.BlobstoreDownloadHandler):
  """Handler to return contents of a file."""

  def get(self):
    """GET handler."""
    path = self.request.get('path')
    try:
      file_kwargs, _ = _GetExtraParams(self.request.POST)
    except ValueError:
      self.error(400)
      return
    titan_file = files.File(path, **file_kwargs)
    if not titan_file.exists:
      self.error(404)
      return
    self.response.headers['Content-Type'] = str(titan_file.mime_type)
    self.response.headers['Content-Disposition'] = (
        'inline; filename=%s' % titan_file.name.encode('ascii', 'replace'))

    if titan_file.blob:
      blob_key = titan_file.blob
      self.send_blob(blob_key, content_type=str(titan_file.mime_type))
    else:
      self.response.out.write(titan_file.content)

def _GetExtraParams(request_params):
  """Returns a two-tuple of (file_kwargs, method_kwargs)."""
  # Extra keyword arguments for instantiation and method call.
  file_params = request_params.get('file_params')
  file_kwargs = json.loads(file_params) if file_params else {}
  method_params = request_params.get('method_params')
  method_kwargs = json.loads(method_params) if method_params else {}

  # Quick sanity check, verify that no kwargs are internal arguments,
  # for security purposes.
  for key in file_kwargs.keys() + method_kwargs.keys():
    if key.startswith('_'):
      logging.error('Invalid param: %r', key)
      raise ValueError('Invalid param: %r' % key)
  return file_kwargs, method_kwargs

URL_MAP = (
    ('/_titan/file', FileHandler),
    ('/_titan/file/read', FileReadHandler),
    ('/_titan/file/newblob', NewBlobHandler),
    ('/_titan/file/finalizeblob', FinalizeBlobHandler),

    # Deprecated API:
    ('/_titan/exists', ExistsHandler),
    ('/_titan/get', GetHandler),
    ('/_titan/read', ReadHandler),
    ('/_titan/write', WriteHandler),
    ('/_titan/newblob', NewBlobHandler),
    ('/_titan/finalizeblob', FinalizeBlobHandler),
    ('/_titan/delete', DeleteHandler),
    ('/_titan/touch', TouchHandler),
    ('/_titan/listfiles', ListFilesHandler),
    ('/_titan/listdir', ListDirHandler),
    ('/_titan/direxists', DirExistsHandler),
    ('/_titan/copy', CopyHandler),
)
application = webapp.WSGIApplication(URL_MAP, debug=False)

def main():
  util.run_wsgi_app(application)

if __name__ == '__main__':
  main()
