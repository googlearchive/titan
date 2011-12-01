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
  import json as simplejson
except ImportError:
  import simplejson
import time
import urllib
from google.appengine.api import blobstore
from google.appengine.ext import webapp
from google.appengine.ext.webapp import blobstore_handlers
from google.appengine.ext.webapp import util
from titan.files import files

class BaseHandler(webapp.RequestHandler):
  """Base handler for Titan API handlers."""

  def WriteJsonResponse(self, data, **kwargs):
    """Data to serialize. Accepts keyword args to pass to the serializer."""
    self.response.headers['Content-Type'] = 'application/json'
    json_data = simplejson.dumps(data, cls=CustomFileSerializer, **kwargs)
    self.response.out.write(json_data)

class ExistsHandler(BaseHandler):
  """Handler to check whether a file exists."""

  def get(self):
    path = self.request.get('path')
    self.WriteJsonResponse(files.Exists(path))

class GetHandler(BaseHandler):
  """Handler to return a serialized file representation."""

  def get(self):
    paths = self.request.get_all('path')
    full = bool(self.request.get('full'))
    self.WriteJsonResponse(files.Get(paths), full=full)

class ReadHandler(blobstore_handlers.BlobstoreDownloadHandler):
  """Handler to return contents of a file."""

  def get(self):
    path = self.request.get('path')
    file_obj = files.Get(path)
    if not file_obj:
      self.error(404)
      return
    self.response.headers['Content-Type'] = str(file_obj.mime_type)

    if file_obj.blobs:
      blob_key = file_obj.blobs[0]  # For now, only support a single blob key.
      self.send_blob(blob_key, content_type=str(file_obj.mime_type))
    else:
      self.response.out.write(file_obj.content)

class WriteHandler(blobstore_handlers.BlobstoreUploadHandler):
  """Handler to write to a file."""

  def post(self):
    path = self.request.get('path')
    # Must use str_POST here to preserve the original encoding of the string.
    content = self.request.str_POST.get('content')
    blobs = self.request.get('blobs', None)

    uploads = self.get_uploads('file')
    if uploads:
      # If a multipart POST request is made, the file contents have already been
      # uploaded to blobstore. We can associate the blob keys automatically.
      blobs = [uploads[0].key()]
    elif blobs is not None:
      blobs = self.request.get_all('blobs')
      # Convert any string keys to BlobKey instances.
      blobs = [blobstore.BlobKey(key) if isinstance(key, basestring) else key
               for key in blobs]

    meta = self.request.get('meta', None)
    if meta:
      meta = simplejson.loads(meta)
    mime_type = self.request.get('mime_type', None)
    try:
      files.Write(path, content=content, blobs=blobs, mime_type=mime_type,
                  meta=meta)
      if uploads:
        # BlobstoreUploadHandlers must return redirects.
        self.redirect('/_titan/get?%s' % urllib.urlencode({'path': path}))
    except files.BadFileError:
      self.error(404)

class NewBlobHandler(BaseHandler):
  """Handler to get a blob upload URL."""

  def get(self):
    upload_url = blobstore.create_upload_url('/_titan/write')
    self.response.out.write(upload_url)

class DeleteHandler(BaseHandler):
  """Handler to delete a file."""

  def post(self):
    paths = self.request.get_all('path')
    try:
      files.Delete(paths)
    except files.BadFileError:
      self.error(404)

class TouchHandler(BaseHandler):
  """Handler to touch a file."""

  def post(self):
    paths = self.request.get_all('path')
    files.Touch(paths)

class ListFilesHandler(BaseHandler):
  """Handler to list files in a directory."""

  def get(self):
    path = self.request.get('path')
    recursive = bool(self.request.get('recursive'))
    return self.WriteJsonResponse(files.ListFiles(path, recursive=recursive))

class ListDirHandler(BaseHandler):
  """Handler to list directories and files in a directory."""

  def get(self):
    path = self.request.get('path')
    dirs, file_objs = files.ListDir(path)
    return self.WriteJsonResponse({'dirs': dirs, 'files': file_objs})

class DirExistsHandler(BaseHandler):
  """Handler to check the existence of a directory."""

  def get(self):
    path = self.request.get('path')
    return self.WriteJsonResponse(files.DirExists(path))

class CustomFileSerializer(simplejson.JSONEncoder):
  """A custom serializer for simplejson to support File objects."""

  def __init__(self, full=False, *args, **kwargs):
    self.full = full
    super(CustomFileSerializer, self).__init__(*args, **kwargs)

  def default(self, obj):
    # Objects with custom Serialize() function.
    if hasattr(obj, 'Serialize'):
      return obj.Serialize(full=self.full)

    # Datetime objects => Unix timestamp.
    if hasattr(obj, 'timetuple'):
      # NOTE: timetuple() drops microseconds, so add it back in.
      return time.mktime(obj.timetuple()) + 1e-6 * obj.microsecond

    raise TypeError(repr(obj) + ' is not JSON serializable.')

URL_MAP = (
    ('/_titan/exists', ExistsHandler),
    ('/_titan/get', GetHandler),
    ('/_titan/read', ReadHandler),
    ('/_titan/write', WriteHandler),
    ('/_titan/newblob', NewBlobHandler),
    ('/_titan/delete', DeleteHandler),
    ('/_titan/touch', TouchHandler),
    ('/_titan/listfiles', ListFilesHandler),
    ('/_titan/listdir', ListDirHandler),
    ('/_titan/direxists', DirExistsHandler),
)
application = webapp.WSGIApplication(URL_MAP, debug=False)

def main():
  util.run_wsgi_app(application)

if __name__ == '__main__':
  main()
