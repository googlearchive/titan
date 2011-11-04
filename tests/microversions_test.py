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

"""Tests for microversions.py."""

from tests import testing

from google.appengine.api import users
from titan.common.lib.google.apputils import basetest
from titan.files import files
from titan.services import microversions
from titan.services import versions

# Content larger than the task invocation RPC size limit.
LARGE_FILE_CONTENT = 'a' * (1 << 21)  # 2 MiB

class MicroversionsTest(testing.ServicesTestCase):

  def setUp(self):
    super(MicroversionsTest, self).setUp()
    services = (
        'titan.services.microversions',
        'titan.services.versions',
    )
    self.EnableServices(services)
    self.vcs = versions.VersionControlService()

  def testHooks(self):
    # Exists() should check root tree files, not _FilePointers.
    self.assertFalse(files.Exists('/foo'))
    files.Touch('/foo', disabled_services=True)
    self.assertTrue(files.Exists('/foo'))

    # Get() should pull from the root tree.
    files.Write('/foo', 'foo', disabled_services=True)
    self.assertEqual('foo', files.Get('/foo').content)
    files.Delete('/foo', 'foo', disabled_services=True)
    self.assertEqual(None, files.Get('/foo'))

    # Write(), Touch(), and Delete() should all modify root tree files
    # and defer a versioning task which commits a single-file changeset.
    files.Write('/foo', 'foo')
    self.assertEqual(1, len(self.taskqueue_stub.get_filtered_tasks()))
    self.assertEqual('foo', files.File('/foo').content)
    files.Touch('/foo')
    self.assertEqual(2, len(self.taskqueue_stub.get_filtered_tasks()))
    self.assertEqual('foo', files.File('/foo').content)
    files.Delete('/foo')
    self.assertEqual(3, len(self.taskqueue_stub.get_filtered_tasks()))
    self.assertEqual(None, files._File.get_by_key_name('/foo'))

    # Verify large RPC deferred task handling.
    files.Write('/foo', LARGE_FILE_CONTENT)
    # TODO(user): right now, these are dropped. When handled later,
    # there will be 4 items in the queue, not 3.
    self.assertEqual(3, len(self.taskqueue_stub.get_filtered_tasks()))

  def testCommitMicroversion(self):
    created_by = users.User('test@example.com')

    # Write.
    final_changeset = microversions._CommitMicroversion(
        created_by=created_by, write=True, path='/foo', content='foo')
    self.assertEqual(2, final_changeset.num)
    file_obj = files.Get('/foo', changeset=final_changeset.linked_changeset)
    self.assertEqual('foo', file_obj.content)

    # Verify the final changeset's created_by.
    self.assertEqual('test@example.com', str(final_changeset.created_by))

    # Write with an existing root file (which should be copied to the version).
    files.Write('/foo', 'new foo', disabled_services=True)
    final_changeset = microversions._CommitMicroversion(
        created_by=created_by, write=True, path='/foo', meta={'color': 'blue'})
    self.assertEqual(4, final_changeset.num)
    file_obj = files.Get('/foo', changeset=final_changeset.linked_changeset)
    self.assertEqual('new foo', file_obj.content)
    self.assertEqual('blue', file_obj.color)

    # Touch.
    final_changeset = microversions._CommitMicroversion(
        created_by=created_by, touch=True, paths='/foo')
    self.assertEqual(6, final_changeset.num)
    file_obj = files.Get('/foo', changeset=final_changeset.linked_changeset)
    self.assertEqual('new foo', file_obj.content)

    # Delete.
    final_changeset = microversions._CommitMicroversion(
        created_by=created_by, delete=True, paths='/foo')
    self.assertEqual(8, final_changeset.num)
    file_obj = files.Get('/foo', changeset=final_changeset.linked_changeset)
    self.assertEqual('', file_obj.content)

    # Check file versions.
    file_versions = self.vcs.GetFileVersions('/foo')
    self.assertEqual(8, file_versions[0].changeset.num)
    self.assertEqual(6, file_versions[1].changeset.num)
    self.assertEqual(4, file_versions[2].changeset.num)
    self.assertEqual(2, file_versions[3].changeset.num)
    self.assertEqual(versions.FILE_DELETED, file_versions[0].status)
    self.assertEqual(versions.FILE_EDITED, file_versions[1].status)
    self.assertEqual(versions.FILE_EDITED, file_versions[2].status)
    self.assertEqual(versions.FILE_CREATED, file_versions[3].status)

    # Touch multi.
    final_changeset = microversions._CommitMicroversion(
        created_by=created_by, touch=True, paths=['/foo', '/bar'])
    self.assertEqual(10, final_changeset.num)
    file_objs = files.Get(['/foo', '/bar'],
                          changeset=final_changeset.linked_changeset)
    self.assertEqual(2, len(file_objs))

    # Delete multi.
    final_changeset = microversions._CommitMicroversion(
        created_by=created_by, delete=True, paths=['/foo', '/bar'])
    self.assertEqual(12, final_changeset.num)
    file_objs = files.Get(['/foo', '/bar'],
                          changeset=final_changeset.linked_changeset)
    self.assertEqual(versions.FILE_DELETED, file_objs['/foo'].status)
    self.assertEqual(versions.FILE_DELETED, file_objs['/bar'].status)

    # Error handling.
    file_obj = files.File('/foo')
    final_changeset = self.assertRaises(
        TypeError, microversions._CommitMicroversion,
        created_by=created_by, write=True, path=file_obj, content='new foo')

if __name__ == '__main__':
  basetest.main()
