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

import json
import logging
import time
import urllib

import webapp2
from google.appengine.api import blobstore
from google.appengine.ext.webapp import blobstore_handlers

from titan import files
from titan.common import utils
from titan.files import dirs

_ENABLE_EXCEPTION_LOGGING = True

class BaseHandler(webapp2.RequestHandler):
  """Base handler for Titan API handlers."""

  def WriteJsonResponse(self, data, **kwargs):
    """Data to serialize. Accepts keyword args to pass to the serializer."""
    self.response.headers['Content-Type'] = 'application/json'
    json_data = json.dumps(data, cls=utils.CustomJsonEncoder, **kwargs)
    self.response.out.write(json_data)

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

    try:
      titan_file = files.File(path, **file_kwargs)
    except (TypeError, ValueError):
      self.error(400)
      _MaybeLogException('Bad request:')
      return
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
      _MaybeLogException('Bad request:')

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
    except (TypeError, ValueError):
      self.error(400)
      _MaybeLogException('Bad request:')

class FilesHandler(BaseHandler):
  """Handler to return files or a list of files."""

  def get(self):
    """GET handler."""
    paths = self.request.get_all('path', [])
    dir_path = self.request.get('dir_path', None)

    if dir_path and paths or not dir_path and not paths:
      self.error(400)
      self.response.out.write('Exactly one of "path" or "dir_path" '
                              'parameters must be given.')
      return

    if paths:
      try:
        titan_files = files.Files(paths=paths)
        # Get rid of non-existent files so they are not serialized.
        titan_files.Load()
      except (TypeError, ValueError):
        self.error(400)
        _MaybeLogException('Bad request:')
        return
    elif dir_path:
      recursive = self.request.get('recursive', 'false')
      recursive = False if recursive == 'false' else True
      ids_only = self.request.get('ids_only', 'false')
      ids_only = False if ids_only == 'false' else True
      depth = self.request.get('depth', None)
      if depth:
        try:
          depth = int(depth)
        except ValueError:
          self.error(400)
          self.response.out.write('Invalid depth parameter')
      try:
        titan_files = files.OrderedFiles.List(dir_path=dir_path,
                                              recursive=recursive,
                                              depth=depth)
        if ids_only:
          result = {'paths': titan_files.keys()}
          self.WriteJsonResponse(result)
          return
      except ValueError:
        self.error(400)
        _MaybeLogException('Invalid parameter')
        return

    self.WriteJsonResponse(titan_files)

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
    try:
      titan_file = files.File(path, **file_kwargs)
    except (TypeError, ValueError):
      self.error(400)
      _MaybeLogException('Bad request:')
      return
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

class FileNewBlobHandler(BaseHandler):
  """Handler to get a blob upload URL."""

  def get(self):
    upload_url = blobstore.create_upload_url('/_titan/file/finalizeblob')
    self.response.out.write(upload_url)

class FileFinalizeBlobHandler(blobstore_handlers.BlobstoreUploadHandler):
  """Handler to finalize a blob, returning the blobkey."""

  # Be careful here; this handler does NOT require authentication.

  def get(self):
    # This is a noop, just here as an unauthenticated final endpoint for the
    # internal-to-blobstore redirect below.
    pass

  def post(self):
    uploads = self.get_uploads('file')
    if not uploads:
      self.error(400)
      return
    blob = str(uploads[0].key())
    # Magic: BlobstoreUploadHandlers must return redirects, so we pass the
    # blobkey back as a query param. The client should followup with a call
    # to Write() and include the blobkey.
    params = urllib.urlencode({'blob': blob})
    self.redirect('/_titan/file/finalizeblob?%s' % params)

class DirsProcessDataHandler(BaseHandler):
  """Dirs handler."""

  def get(self):
    """GET handler; must be GET because it is run from a cron job."""
    runtime = self.request.get('runtime', dirs.DEFAULT_CRON_RUNTIME_SECONDS)
    dir_task_consumer = dirs.DirTaskConsumer()
    results = dir_task_consumer.ProcessWindowsWithBackoff(runtime=int(runtime))
    self.WriteJsonResponse(results)

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

def _MaybeLogException(message):
  if _ENABLE_EXCEPTION_LOGGING:
    logging.exception(message)

ROUTES = (
    ('/_titan/file', FileHandler),
    ('/_titan/files', FilesHandler),
    ('/_titan/file/read', FileReadHandler),
    ('/_titan/file/newblob', FileNewBlobHandler),
    ('/_titan/file/finalizeblob', FileFinalizeBlobHandler),

    ('/_titan/dirs/processdata', DirsProcessDataHandler),
)
application = webapp2.WSGIApplication(ROUTES, debug=False)
