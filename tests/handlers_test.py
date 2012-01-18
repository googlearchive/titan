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

"""Tests for Titan handlers."""

from tests import testing

try:
  import json as simplejson
except ImportError:
  import simplejson
import time
import urllib
from google.appengine.api import blobstore
from titan.common.lib.google.apputils import basetest
from tests import webapp_testing
from titan.files import files
from titan.files import handlers

# Content which will be stored in blobstore.
LARGE_FILE_CONTENT = 'a' * (files.MAX_CONTENT_SIZE + 1)

class HandlersTest(testing.BaseTestCase, webapp_testing.WebAppTestCase):

  def setUp(self):
    super(HandlersTest, self).setUp()
    self.valid_path = '/path/to/some/file.txt'
    self.error_path = '/path/to/some/error.txt'

  def tearDown(self):
    super(HandlersTest, self).tearDown()
    self.valid_path = '/path/to/some/file.txt'
    self.error_path = '/path/to/some/error.txt'

  def testExistsHandler(self):
    # Verify that GET requests return a JSON boolean value.
    self.assertFalse(files.Exists(self.valid_path))
    response = self.Get(handlers.ExistsHandler, params={
        'path': self.valid_path,
    })
    self.assertEqual(200, response.status)
    self.assertEqual('application/json', response.headers['Content-Type'])
    self.assertEqual('false', response.out.getvalue())

    files.Touch(self.valid_path)
    self.assertTrue(files.Exists(self.valid_path))
    response = self.Get(handlers.ExistsHandler, params={
        'path': self.valid_path,
    })
    self.assertEqual('true', response.out.getvalue())

    # Verify that errors return a 500 response status.
    self.mox.StubOutWithMock(files, 'Exists')
    files.Exists(self.error_path).AndRaise(Exception)
    self.mox.ReplayAll()
    response = self.Get(handlers.ExistsHandler, params={
        'path': self.error_path,
    })
    self.assertEqual(500, response.status)
    self.mox.VerifyAll()

  def testGetHandler(self):
    # Verify GET requests return a JSON-serialized representation of the file.
    self.assertFalse(files.Exists(self.valid_path))
    key = files.Write(self.valid_path, 'foobar')
    actual_file = files._File.get(key)
    to_timestamp = lambda x: time.mktime(x.timetuple()) + 1e-6 * x.microsecond
    expected_file_obj = {
        'name': 'file.txt',
        'path': self.valid_path,
        'paths': ['/', '/path', '/path/to', '/path/to/some'],
        'mime_type': 'text/plain',
        'created': to_timestamp(actual_file.created),
        'modified': to_timestamp(actual_file.modified),
        'content': 'foobar',
        'blobs': [],
        'exists': True,
        'created_by': 'titanuser@example.com',
        'modified_by': 'titanuser@example.com',
    }
    expected_result = {
        self.valid_path: expected_file_obj,
    }
    response = self.Get(handlers.GetHandler, params={
        'path': self.valid_path,
        'full': True,
    })
    self.assertEqual(200, response.status)
    self.assertEqual('application/json', response.headers['Content-Type'])
    self.assertSameElements(expected_result,
                            simplejson.loads(response.out.getvalue()))
    response = self.Get(handlers.GetHandler, params={
        'path': self.error_path,
    })
    self.assertEqual(200, response.status)
    self.assertEqual({}, simplejson.loads(response.out.getvalue()))

  def testReadHandler(self):
    # Verify that GET requests return the file contents.
    self.assertFalse(files.Exists(self.valid_path))
    file_content = 'foobar'
    files.Write(self.valid_path, file_content)
    response = self.Get(handlers.ReadHandler, params={
        'path': self.valid_path,
    })
    self.assertEqual(200, response.status)
    self.assertEqual('text/plain', response.headers['Content-Type'])
    self.assertEqual(file_content, response.out.getvalue())

    # Verify blob handling.
    files.Write('/foo', LARGE_FILE_CONTENT, mime_type='custom/mimetype')
    response = self.Get(handlers.ReadHandler, params={
        'path': '/foo',
    })
    self.assertEqual(200, response.status)
    self.assertEqual('custom/mimetype', response.headers['Content-Type'])
    self.assertEqual('inline; filename=foo',
                     response.headers['Content-Disposition'])
    self.assertTrue('X-AppEngine-BlobKey' in response.headers)
    self.assertEqual('', response.out.getvalue())

    # Verify that requests with BadFileError return a 404 status.
    response = self.Get(handlers.ReadHandler, params={
        'path': self.error_path,
    })
    self.assertEqual(404, response.status)

  def testWriteHandler(self):
    # Verify successful POST request.
    self.assertFalse(files.Exists(self.valid_path))
    file_content = 'foobar'
    payload = urllib.urlencode({
        'path': self.valid_path,
        'content': file_content,
    })
    response = self.Post(handlers.WriteHandler, payload=payload)
    self.assertEqual(200, response.status)
    self.assertEqual(file_content, files.Get(self.valid_path).content)

    # Verify setting blobs.
    blob_key = 'ablobkey'
    payload = urllib.urlencode({
        'path': self.valid_path,
        'blobs': blob_key,
    })
    response = self.Post(handlers.WriteHandler, payload=payload)
    self.assertEqual(200, response.status)
    self.assertEqual(blob_key, str(files.Get(self.valid_path).blobs[0]))

    # Verify that meta strings are decoded properly.
    files.Delete(self.valid_path)
    self.assertFalse(files.Exists(self.valid_path))
    payload = urllib.urlencode({
        'path': self.valid_path,
        'content': file_content,
        'meta': '{"foo": "bar"}',
    })
    response = self.Post(handlers.WriteHandler, payload=payload)
    self.assertEqual(200, response.status)
    file_obj = files.Get(self.valid_path)
    self.assertEqual('bar', file_obj.foo)

    # Verify that MIME types can be read properly.
    files.Delete(self.valid_path)
    self.assertFalse(files.Exists(self.valid_path))
    payload = urllib.urlencode({
        'path': self.valid_path,
        'content': file_content,
        'mime_type': 'xyz',
    })
    response = self.Post(handlers.WriteHandler, payload=payload)
    self.assertEqual(200, response.status)
    file_obj = files.Get(self.valid_path)
    self.assertEqual('xyz', file_obj.mime_type)

    # Verify that requests with BadFileError return a 404 status.
    payload = urllib.urlencode({
        'path': self.error_path,
        'mime_type': 'test/mimetype',
    })
    response = self.Post(handlers.WriteHandler, payload=payload)
    self.assertEqual(404, response.status)

  def testWriteBlob(self):
    # Verify getting a new blob upload URL.
    response = self.Get(handlers.NewBlobHandler, params={
        'path': '/foo/some-blob',
    })
    self.assertEqual(200, response.status)
    upload_url = response.out.getvalue()
    self.assertIn('http://testbed.example.com:80/_ah/upload/', upload_url)

    # Blob uploads must return redirect responses. In our case, the handler
    # so also always include a special header:
    self.mox.StubOutWithMock(handlers.FinalizeBlobHandler, 'get_uploads')
    mock_blob_info = self.mox.CreateMockAnything()
    mock_blob_info.key().AndReturn(blobstore.BlobKey('fake-blob-key'))
    handlers.FinalizeBlobHandler.get_uploads('file').AndReturn([mock_blob_info])

    self.mox.ReplayAll()
    response = self.Post(handlers.FinalizeBlobHandler, params={
        'path': self.valid_path,
    })
    self.assertEqual(302, response.status)
    self.assertIn('fake-blob-key', response.headers['Location'])
    self.mox.VerifyAll()

  def testDeleteHandler(self):
    files.Touch(self.valid_path)
    self.assertTrue(files.Exists(self.valid_path))
    payload = urllib.urlencode({
        'path': self.valid_path,
    })
    response = self.Post(handlers.DeleteHandler, payload=payload)
    self.assertEqual(200, response.status)
    self.assertFalse(files.Exists(self.valid_path))

    # Verify that requests with BadFileError return a 404 status.
    self.mox.StubOutWithMock(files, 'Delete')
    files.Delete([self.error_path]).AndRaise(files.BadFileError)
    self.mox.ReplayAll()
    payload = urllib.urlencode({
        'path': self.error_path,
    })
    response = self.Post(handlers.DeleteHandler, payload=payload)
    self.assertEqual(404, response.status)
    self.mox.VerifyAll()

  def testTouchHandler(self):
    # Verify that POST requests return a 200 status.
    self.assertFalse(files.Exists(self.valid_path))
    payload = urllib.urlencode({
        'path': self.valid_path,
    })
    response = self.Post(handlers.TouchHandler, payload=payload)
    self.assertEqual(200, response.status)
    self.assertTrue(files.Exists(self.valid_path))

    # Verify that errors return a 500 response status.
    self.mox.StubOutWithMock(files, 'Touch')
    files.Touch([self.error_path]).AndRaise(KeyError)
    self.mox.ReplayAll()
    payload = urllib.urlencode({
        'path': self.error_path,
    })
    response = self.Post(handlers.TouchHandler, payload=payload)
    self.assertEqual(500, response.status)
    self.mox.VerifyAll()

  def testListFiles(self):
    # Verify GET requests return 200 with a list of file paths.
    files.Touch('/foo/bar.txt')
    files.Touch('/foo/bar/qux.txt')
    expected = ['/foo/bar.txt', '/foo/bar/qux.txt']
    response = self.Get(handlers.ListFilesHandler, params={
        'path': '/',
        'recursive': 'hellyes',
    })
    self.assertEqual(200, response.status)
    file_objs = simplejson.loads(response.out.getvalue())
    self.assertSameElements(
        expected, [file_obj['path'] for file_obj in file_objs])

  def testListDir(self):
    # Verify GET requests return 200 with a list of file paths.
    files.Touch('/foo/bar.txt')
    files.Touch('/foo/bar/baz/qux')
    expected_files = ['/foo/bar.txt']
    response = self.Get(handlers.ListDirHandler, params={
        'path': '/foo',
    })
    self.assertEqual(200, response.status)
    result = simplejson.loads(response.out.getvalue())
    dirs, file_objs = result['dirs'], result['files']
    self.assertSameElements(['bar'], dirs)
    self.assertSameElements(
        expected_files, [file_obj['path'] for file_obj in file_objs])

  def testDirExists(self):
    # Verify GET requests return 200 with a true or false.
    files.Touch('/foo/bar/baz/qux')
    response = self.Get(handlers.DirExistsHandler, params={'path': '/foo/bar'})
    self.assertEqual(200, response.status)
    self.assertTrue(simplejson.loads(response.out.getvalue()))
    response = self.Get(handlers.DirExistsHandler, params={'path': '/fake'})
    self.assertEqual(200, response.status)
    self.assertFalse(simplejson.loads(response.out.getvalue()))

if __name__ == '__main__':
  basetest.main()
