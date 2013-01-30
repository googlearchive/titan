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

"""Tests for files_client.py."""

from tests.common import testing

import os
from titan.common.lib.google.apputils import basetest
from tests.common import titan_rpc_stub
from titan import files
from titan.files import files_client
from titan.files import handlers

class TestableRemoteFileFactory(files_client.RemoteFileFactory):
  """A RemoteFileFactory with a stubbed-out TitanClient."""

  def _GetTitanClient(self, **kwargs):
    kwargs['stub_wsgi_apps'] = [handlers.application]
    return titan_rpc_stub.TitanClientStub(**kwargs)

class FilesClientTestCase(testing.BaseTestCase):

  def setUp(self):
    super(FilesClientTestCase, self).setUp()
    self.remote_file_factory = TestableRemoteFileFactory(
        host=os.environ['HTTP_HOST'])

  def testRemoteFile(self):
    remote_file = self.remote_file_factory.MakeRemoteFile('/a/foo')
    actual_file = files.File('/a/foo')

    # Error handling for non-existent files.
    self.assertRaises(files_client.BadRemoteFileError, lambda: remote_file.name)
    self.assertEqual(actual_file.exists, remote_file.exists)

    actual_file.Write('foo!', meta={'flag': False})

    # Test properties.
    self.assertEqual(actual_file.name, remote_file.name)
    self.assertEqual(actual_file.path, remote_file.path)
    self.assertEqual(actual_file.real_path, remote_file.real_path)
    self.assertEqual(actual_file.paths, remote_file.paths)
    self.assertEqual(actual_file.mime_type, remote_file.mime_type)
    self.assertEqual(actual_file.created, remote_file.created)
    self.assertEqual(actual_file.modified, remote_file.modified)
    self.assertEqual(actual_file.size, remote_file.size)
    self.assertEqual(actual_file.content, remote_file.content)
    self.assertEqual(actual_file.exists, remote_file.exists)
    # Different remote API (string of blob_key rather than BlobKey):
    self.assertEqual(actual_file.Serialize()['blob'], remote_file.blob)
    # Different remote API (string of email addresses rather than User objects).
    self.assertEqual(
        actual_file.Serialize()['created_by'], remote_file.created_by)
    self.assertEqual(
        actual_file.Serialize()['modified_by'], remote_file.modified_by)
    self.assertEqual(actual_file.md5_hash, remote_file.md5_hash)
    self.assertEqual(actual_file.meta, remote_file.meta)

    # Test Write().
    remote_file.Write(content='bar')
    actual_file = files.File('/a/foo')
    self.assertEqual('bar', remote_file.content)
    self.assertEqual('bar', actual_file.content)

    # Test Delete().
    remote_file.Delete()
    actual_file = files.File('/a/foo')
    self.assertRaises(files_client.BadRemoteFileError, lambda: remote_file.name)
    self.assertFalse(actual_file.exists)
    self.assertFalse(remote_file.exists)

    # TODO(user): add tests for large files and fp argument.

  def testRemoteFiles(self):
    actual_file = files.File('/a/foo')
    actual_file.Write('foo!', meta={'flag': False})
    remote_file = self.remote_file_factory.MakeRemoteFile('/a/foo')

    remote_files = self.remote_file_factory.MakeRemoteFiles()
    self.assertEqual([], remote_files.keys())
    remote_files = self.remote_file_factory.MakeRemoteFiles([])
    self.assertEqual([], remote_files.keys())
    remote_files = self.remote_file_factory.MakeRemoteFiles(['/a/foo'])
    self.assertEqual(['/a/foo'], remote_files.keys())
    remote_files = self.remote_file_factory.MakeRemoteFiles(files=[remote_file])
    self.assertEqual(['/a/foo'], remote_files.keys())

    # Test mapping properties.
    remote_files['/a/bar'] = self.remote_file_factory.MakeRemoteFile('/a/bar')
    self.assertSameElements(['/a/foo', '/a/bar'], remote_files.keys())
    del remote_files['/a/bar']
    self.assertEqual(['/a/foo'], remote_files.keys())
    self.assertIn('/a/foo', remote_files)
    self.assertEqual(1, len(remote_files))
    remote_files.clear()
    self.assertEqual(0, len(remote_files))

    # Test List().
    # Different remote API: List is not a class method due to authentication.
    remote_files = self.remote_file_factory.MakeRemoteFiles()
    remote_files.List('/', recursive=True)
    self.assertEqual(['/a/foo'], remote_files.keys())

    # Test Delete().
    remote_files.Delete()
    actual_file = files.File('/a/foo')
    self.assertFalse(actual_file.exists)

if __name__ == '__main__':
  basetest.main()
