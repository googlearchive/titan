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

"""Tests for microversions.py."""

from tests.common import testing

from google.appengine.api import files as blobstore_files
from google.appengine.datastore import datastore_stub_util

from titan.common.lib.google.apputils import basetest
from titan import files
from titan.files.mixins import microversions
from titan.files.mixins import versions
from titan import users

# Content larger than the task invocation RPC size limit.
LARGE_FILE_CONTENT = 'a' * (1 << 21)  # 2 MiB

# The class used during the initial action at the root path.
class MicroversioningMixin(microversions.MicroversioningMixin, files.File):

  @classmethod
  def ShouldApplyMixin(cls, **kwargs):
    if kwargs.get('_no_mixins'):
      return False
    return microversions.MicroversioningMixin.ShouldApplyMixin(**kwargs)

# The class used during the deferred task action.
class FileVersioningMixin(versions.FileVersioningMixin, files.File):

  @classmethod
  def ShouldApplyMixin(cls, **kwargs):
    if kwargs.get('_no_mixins'):
      return False
    return versions.FileVersioningMixin.ShouldApplyMixin(**kwargs)

class MicroversionsTest(testing.BaseTestCase):

  def setUp(self):
    super(MicroversionsTest, self).setUp()
    self.vcs = versions.VersionControlService()
    files.RegisterFileMixins([MicroversioningMixin, FileVersioningMixin])

  def testRootTreeHandling(self):
    # Actions should check root tree files, not versioning _FilePointers.
    self.assertFalse(files.File('/foo').exists)
    files.File('/foo', _no_mixins=True).Write('')
    self.assertTrue(files.File('/foo').exists)
    files.File('/foo', _no_mixins=True).Delete()
    self.assertFalse(files.File('/foo').exists)

    # Write(), and Delete() should all modify root tree files
    # and defer a versioning task which commits a single-file changeset.
    files.File('/foo').Write('foo')
    self.assertEqual(1, len(self.taskqueue_stub.get_filtered_tasks()))
    self.assertEqual('foo', files.File('/foo', _no_mixins=True).content)
    files.File('/foo').Delete()
    self.assertEqual(2, len(self.taskqueue_stub.get_filtered_tasks()))
    self.assertFalse(files.File('/foo', _no_mixins=True).exists)

    # Verify large RPC deferred task handling.
    files.File('/foo').Write(LARGE_FILE_CONTENT)
    self.assertEqual(3, len(self.taskqueue_stub.get_filtered_tasks()))
    self.assertEqual(
        LARGE_FILE_CONTENT, files.File('/foo', _no_mixins=True).content)

  def testContentAndBlobsHandling(self):
    files.File('/foo').Write('foo')
    files.File('/foo').Delete()
    # This will immediately go to blobstore, then the deferred task will
    # have a "blob" argument:
    files.File('/foo').Write(LARGE_FILE_CONTENT)

    self.RunDeferredTasks(microversions.TASKQUEUE_NAME)

    # After tasks actually run, verify correct content was saved over time
    # to the versioned paths:
    file_versions = self.vcs.GetFileVersions('/foo')

    # In reverse-chronological order:
    titan_file = files.File(
        '/foo', changeset=file_versions[0].content_changeset)
    self.assertEqual(LARGE_FILE_CONTENT, titan_file.content)
    self.assertEqual('titanuser@example.com', str(titan_file.created_by))
    self.assertEqual('titanuser@example.com', str(titan_file.modified_by))
    # Blackbox test: created_by and modified_by might be coming from the
    # backwards-compatibility code in versions. Verify they are actually
    # stored correctly.
    self.assertEqual('titanuser@example.com', str(titan_file._file.created_by))
    self.assertEqual('titanuser@example.com', str(titan_file._file.modified_by))

    titan_file = files.File(
        '/foo', changeset=file_versions[1].content_changeset)
    self.assertEqual('', titan_file.content)

    titan_file = files.File(
        '/foo', changeset=file_versions[2].content_changeset)
    self.assertEqual('foo', titan_file.content)

    self.assertEqual(versions.FILE_CREATED, file_versions[0].status)
    self.assertEqual(versions.FILE_DELETED, file_versions[1].status)
    self.assertEqual(versions.FILE_CREATED, file_versions[2].status)

  def testCommitMicroversion(self):
    user = users.TitanUser('test@example.com')

    # Write.
    final_changeset = microversions._CommitMicroversion(
        file_kwargs={'path': '/foo'}, method_kwargs={'content': 'foo'},
        user=user, action=microversions._Actions.WRITE)
    self.assertEqual(2, final_changeset.num)
    titan_file = files.File('/foo', changeset=final_changeset.linked_changeset)
    self.assertEqual('foo', titan_file.content)

    # Verify the final changeset's created_by.
    self.assertEqual('test@example.com', str(final_changeset.created_by))

    # Write with an existing root file (which should be copied to the version).
    files.File('/foo', _no_mixins=True).Write('new foo')
    final_changeset = microversions._CommitMicroversion(
        file_kwargs={'path': '/foo'}, method_kwargs={'meta': {'color': 'blue'}},
        user=user, action=microversions._Actions.WRITE)
    self.assertEqual(4, final_changeset.num)
    titan_file = files.File('/foo', changeset=final_changeset.linked_changeset)
    self.assertEqual('new foo', titan_file.content)
    self.assertEqual('blue', titan_file.meta.color)

    # Delete. In the real code path, the delete of the root file will often
    # complete before the task is started, so we delete /foo to verify that
    # deletes don't rely on presence of the root file.
    files.File('/foo', _no_mixins=True).Delete()
    final_changeset = microversions._CommitMicroversion(
        file_kwargs={'path': '/foo'}, method_kwargs={},
        user=user, action=microversions._Actions.DELETE)
    self.assertEqual(6, final_changeset.num)
    titan_file = files.File('/foo', changeset=final_changeset.linked_changeset)
    self.assertEqual('', titan_file.content)

    # Check file versions.
    file_versions = self.vcs.GetFileVersions('/foo')
    self.assertEqual(6, file_versions[0].changeset.num)
    self.assertEqual(4, file_versions[1].changeset.num)
    self.assertEqual(2, file_versions[2].changeset.num)
    self.assertEqual(versions.FILE_DELETED, file_versions[0].status)
    self.assertEqual(versions.FILE_EDITED, file_versions[1].status)
    self.assertEqual(versions.FILE_CREATED, file_versions[2].status)

  def testStronglyConsistentCommits(self):
    user = users.TitanUser('test@example.com')

    # Microversions uses FinalizeAssociatedPaths so the Commit() path should use
    # the always strongly-consistent GetFiles(), rather than a query. Verify
    # this behavior by simulating a never-consistent HR datastore.
    policy = datastore_stub_util.PseudoRandomHRConsistencyPolicy(probability=0)
    self.testbed.init_datastore_v3_stub(consistency_policy=policy)

    final_changeset = microversions._CommitMicroversion(
        file_kwargs={'path': '/foo'}, method_kwargs={'content': 'foo'},
        user=user, action=microversions._Actions.WRITE)
    self.assertEqual(2, final_changeset.num)
    titan_file = files.File('/foo', changeset=final_changeset.linked_changeset)
    self.assertEqual('foo', titan_file.content)

  def testKeepOldBlobs(self):
    # Create a blob and blob_reader for testing.
    filename = blobstore_files.blobstore.create(
        mime_type='application/octet-stream')
    with blobstore_files.open(filename, 'a') as fp:
      fp.write('Blobstore!')
    blobstore_files.finalize(filename)
    blob_key = blobstore_files.blobstore.get_blob_key(filename)

    # Verify that the blob is not deleted when microversioned content resizes.
    files.File('/foo').Write(blob=blob_key)
    self.RunDeferredTasks(microversions.TASKQUEUE_NAME)
    titan_file = files.File('/foo')
    self.assertTrue(titan_file.blob)
    self.assertEqual('Blobstore!', titan_file.content)
    self.RunDeferredTasks(microversions.TASKQUEUE_NAME)
    # Resize as smaller (shouldn't delete the old blob).
    files.File('/foo').Write('foo')
    files.File('/foo').Write(blob=blob_key)  # Resize back to large size.
    # Delete file (shouldn't delete the old blob).
    files.File('/foo').Delete()
    self.RunDeferredTasks(microversions.TASKQUEUE_NAME)

    file_versions = self.vcs.GetFileVersions('/foo')

    # Deleted file (blob should be None).
    changeset = file_versions[0].changeset.linked_changeset
    titan_file = files.File('/foo', changeset=changeset)
    self.assertIsNone(titan_file.blob)

    # Created file (blob key and blob content should still exist).
    changeset = file_versions[-1].changeset.linked_changeset
    titan_file = files.File('/foo', changeset=changeset)
    self.assertTrue(titan_file.blob)
    self.assertEqual('Blobstore!', titan_file.content)

if __name__ == '__main__':
  basetest.main()
