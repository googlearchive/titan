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

from tests.common import testing

import hashlib
import json
import time
import webtest
from google.appengine.api import blobstore
from titan.common.lib.google.apputils import basetest
from titan import files
from titan.files import handlers
from titan.common import utils

# Content which will be stored in blobstore.
LARGE_FILE_CONTENT = 'a' * (files.MAX_CONTENT_SIZE + 1)

class HandlersTest(testing.BaseTestCase):

  def setUp(self):
    super(HandlersTest, self).setUp()
    self.app = webtest.TestApp(handlers.application)

  def testFileHandlerGet(self):
    # Verify GET requests return a JSON-serialized representation of the file.
    actual_file = files.File('/foo/bar').Write('foobar')
    to_timestamp = lambda x: time.mktime(x.timetuple()) + 1e-6 * x.microsecond
    expected_data = {
        u'name': u'bar',
        u'path': u'/foo/bar',
        u'paths': [u'/', u'/foo'],
        u'real_path': u'/foo/bar',
        u'mime_type': u'application/octet-stream',
        u'created': to_timestamp(actual_file.created),
        u'modified': to_timestamp(actual_file.modified),
        u'content': u'foobar',
        u'blob': None,
        u'created_by': u'titanuser@example.com',
        u'modified_by': u'titanuser@example.com',
        u'meta': {},
        u'md5_hash': hashlib.md5('foobar').hexdigest(),
        u'size': len('foobar'),
    }
    response = self.app.get('/_titan/file', {'path': '/foo/bar', 'full': True})
    self.assertEqual(200, response.status_int)
    self.assertEqual('application/json', response.headers['Content-Type'])
    self.assertEqual(expected_data, json.loads(response.body))

    response = self.app.get('/_titan/file', {'path': '/fake', 'full': True},
                            expect_errors=True)
    self.assertEqual(404, response.status_int)
    self.assertEqual('', response.body)

    response = self.app.get('/_titan/file', {'path': 'bad-path'},
                            expect_errors=True)
    self.assertEqual(400, response.status_int)
    self.assertEqual('', response.body)

  def testDirsProcessDataHandler(self):
    response = self.app.get('/_titan/dirs/processdata?runtime=1')
    self.assertEqual(200, response.status_int)
    self.assertIn(json.dumps({}), response.body)

  def testFileHandlerPost(self):
    params = {
        'content': 'foobar',
        'path': '/foo/bar',
        'meta': json.dumps({'color': 'blue'}),
        'mime_type': 'fake/mimetype',
    }
    response = self.app.post('/_titan/file', params)
    self.assertEqual(201, response.status_int)
    self.assertIn('/_titan/file/?path=/foo/bar', response.headers['Location'])
    self.assertEqual('', response.body)
    titan_file = files.File('/foo/bar')
    self.assertEqual('foobar', titan_file.content)
    self.assertEqual('fake/mimetype', titan_file.mime_type)
    self.assertEqual('blue', titan_file.meta.color)

    # Verify blob handling.
    params = {
        'path': '/foo/bar',
        'blob': 'some-blob-key',
    }
    response = self.app.post('/_titan/file', params)
    self.assertEqual(201, response.status_int)
    self.assertEqual('some-blob-key', str(files.File('/foo/bar')._file.blob))

    # Non-existent file.
    params = {
        'path': '/fake',
        'mime_type': 'new/mimetype',
    }
    response = self.app.post('/_titan/file', params, expect_errors=True)
    self.assertEqual(404, response.status_int)

    # Verify that bad requests get a 400.
    params = {
        'path': '/foo/bar',
        'content': 'content',
        'blob': 'some-blob-key',
    }
    response = self.app.post('/_titan/file', params, expect_errors=True)
    self.assertEqual(400, response.status_int)

    # Verify that bad requests get a 400.
    params = {
        'path': '/foo/bar',
        'content': 'content',
        'file_params': json.dumps({'_invalid_internal_arg': True}),
    }
    response = self.app.post('/_titan/file', params, expect_errors=True)
    self.assertEqual(400, response.status_int)

  def testFilesHandlerPaths(self):
    first_file = files.File('/foo/bar').Write('foobar')
    second_file = files.File('/abc/baz').Write('abcbaz')

    paths = [
        ('path', '/abc/baz'),
        ('path', '/foo/bar'),
        ('path', '/fake'),
    ]
    response = self.app.get('/_titan/files', paths)

    expected_titan_files = files.Files(files=[first_file, second_file])
    expected_data = json.loads(self._DumpJson(expected_titan_files))
    self.assertEqual(200, response.status_int)
    self.assertEqual(expected_data, json.loads(response.body))

    # Invalid arguments.
    response = self.app.get('/_titan/files', {'path': '/foo/bar',
                                              'dir_path': '/foo'},
                            expect_errors=True)
    self.assertEqual(400, response.status_int)

    # Invalid path.
    response = self.app.get('/_titan/files', {'path': 'bad-path'},
                            expect_errors=True)
    self.assertEqual(400, response.status_int)

  def testFilesHandlerDir(self):
    files.File('/abc/def/ghi').Write('abc')
    files.File('/abc/456/10/22/34').Write('abc123')
    files.File('/abc/123').Write('abcdef')

    response = self.app.get('/_titan/files', {'dir_path': '/abc/def/'})
    expected_titan_files = files.Files.List(dir_path='/abc/def/')

    self.assertEqual(200, response.status_int)
    expected_data = json.loads(self._DumpJson(expected_titan_files))
    self.assertEqual(expected_data, json.loads(response.body))

    response = self.app.get('/_titan/files', {'dir_path': '/abc',
                                              'recursive': 'true'})
    expected_titan_files = files.Files.List(dir_path='/abc/', recursive=True)

    self.assertEqual(200, response.status_int)
    expected_data = json.loads(self._DumpJson(expected_titan_files))
    self.assertEqual(expected_data, json.loads(response.body))

    response = self.app.get('/_titan/files', {'dir_path': '/abc/', 'depth': '2',
                                              'recursive': 'True'})

    expected_titan_files = files.Files.List(dir_path='/abc/', depth=2,
                                            recursive=True)

    self.assertEqual(200, response.status_int)
    expected_data = json.loads(self._DumpJson(expected_titan_files))
    self.assertEqual(expected_data, json.loads(response.body))

    response = self.app.get('/_titan/files', {'dir_path': '/no/file/here'})

    self.assertEqual(200, response.status_int)
    self.assertEqual({}, json.loads(response.body))

    params = {'dir_path': '/abc', 'recursive': 'true', 'ids_only': 'true'}
    response = self.app.get('/_titan/files', params)

    expected_paths = ['/abc/123', '/abc/456/10/22/34', '/abc/def/ghi']
    expected_paths = {'paths': expected_paths}

    self.assertEqual(200, response.status_int)
    self.assertEqual(expected_paths, json.loads(response.body))

  def testFileReadHandler(self):
    files.File('/foo/bar').Write('foobar')
    response = self.app.get('/_titan/file/read', {'path': '/foo/bar'})
    self.assertEqual(200, response.status_int)
    self.assertEqual('foobar', response.body)
    self.assertEqual('application/octet-stream',
                     response.headers['Content-Type'])

    # Verify blob handling.
    files.File('/foo/bar').Write(LARGE_FILE_CONTENT,
                                 mime_type='custom/mimetype')
    response = self.app.get('/_titan/file/read', {'path': '/foo/bar'})
    self.assertEqual(200, response.status_int)
    self.assertEqual('custom/mimetype', response.headers['Content-Type'])
    self.assertEqual('inline; filename=bar',
                     response.headers['Content-Disposition'])
    self.assertTrue('X-AppEngine-BlobKey' in response.headers)
    self.assertEqual('', response.body)

    # User-customized MIME type.
    params = {'path': '/foo/bar', 'mime_type': 'foo/mimetype'}
    response = self.app.get('/_titan/file/read', params)
    self.assertEqual(200, response.status_int)
    self.assertEqual('foo/mimetype', response.headers['Content-Type'])

    # Non-existent file.
    response = self.app.get('/_titan/file/read', {'path': '/fake'},
                            expect_errors=True)
    self.assertEqual(404, response.status_int)

    # Invalid path.
    response = self.app.get('/_titan/file/read', {'path': 'bad-path'},
                            expect_errors=True)
    self.assertEqual(400, response.status_int)

  def testFileHandlerDelete(self):
    files.File('/foo/bar').Write('foobar')
    response = self.app.delete('/_titan/file?path=/foo/bar')
    self.assertEqual(200, response.status_int)
    self.assertFalse(files.File('/foo/bar').exists)

    # Verify that requests with BadFileError return 404.
    response = self.app.delete('/_titan/file?path=/fake', expect_errors=True)
    self.assertEqual(404, response.status_int)

  def testWriteBlob(self):
    # Verify getting a new blob upload URL.
    response = self.app.get('/_titan/file/newblob', {'path': '/foo/bar'})
    self.assertEqual(200, response.status_int)
    self.assertIn('http://testbed.example.com:80/_ah/upload/', response.body)

    # Blob uploads must return redirect responses. In our case, the handler
    # so also always include a special header:
    self.mox.StubOutWithMock(handlers.FileFinalizeBlobHandler, 'get_uploads')
    mock_blob_info = self.mox.CreateMockAnything()
    mock_blob_info.key().AndReturn(blobstore.BlobKey('fake-blob-key'))
    handlers.FileFinalizeBlobHandler.get_uploads('file').AndReturn(
        [mock_blob_info])

    self.mox.ReplayAll()
    response = self.app.post('/_titan/file/finalizeblob')
    self.assertEqual(302, response.status_int)
    self.assertIn('fake-blob-key', response.headers['Location'])
    self.mox.VerifyAll()

  def _DumpJson(self, obj):
    return json.dumps(obj, cls=utils.CustomJsonEncoder)

if __name__ == '__main__':
  basetest.main()
