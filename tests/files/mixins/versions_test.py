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

"""Tests for versions.py."""

from tests.common import testing

import datetime
from google.appengine.api import users
from titan.common.lib.google.apputils import basetest
from titan.files import files
from titan.files.mixins import versions

CHANGESET_NEW = versions.CHANGESET_NEW
CHANGESET_SUBMITTED = versions.CHANGESET_SUBMITTED
CHANGESET_DELETED = versions.CHANGESET_DELETED
CHANGESET_DELETED_BY_SUBMIT = versions.CHANGESET_DELETED_BY_SUBMIT
FILE_CREATED = versions.FILE_CREATED
FILE_EDITED = versions.FILE_EDITED
FILE_DELETED = versions.FILE_DELETED

class VersionedFile(versions.FileVersioningMixin, files.File):
  pass

class VersionsTest(testing.BaseTestCase):

  def setUp(self):
    super(VersionsTest, self).setUp()
    self.vcs = versions.VersionControlService()
    files.RegisterFileFactory(lambda *args, **kwargs: VersionedFile)

  def tearDown(self):
    files.UnregisterFileFactory()
    super(VersionsTest, self).tearDown()

  def InitTestData(self):
    for _ in range(1, 10):
      self.vcs.NewStagingChangeset()

    # Changeset 11 (was changeset 10 before commit):
    changeset = self.vcs.NewStagingChangeset()
    files.File('/foo', changeset=changeset).Write('foo')
    files.File('/bar', changeset=changeset).Write('bar')
    files.File('/qux', changeset=changeset).Write('qux')
    changeset.FinalizeAssociatedFiles()
    self.vcs.Commit(changeset)
    # For testing, move the submitted datetime to 31 days ago.
    changeset_ent = versions.Changeset(11).changeset_ent
    created = datetime.datetime.now() - datetime.timedelta(days=31)
    changeset_ent.created = created
    changeset_ent.put()

    # Changset 13:
    changeset = self.vcs.NewStagingChangeset()
    files.File('/foo', changeset=changeset).Write('foo2')  # edit
    files.File('/bar', changeset=changeset).Write(delete=True)  # delete
    files.File('/baz', changeset=changeset).Write('baz')  # create
    files.File('/qux', changeset=changeset).Write('qux2')  # edit
    changeset.FinalizeAssociatedFiles()
    self.vcs.Commit(changeset)
    self.assertEqual(13, self.vcs.GetLastSubmittedChangeset().num)

    # Changset 15:
    changeset = self.vcs.NewStagingChangeset()
    files.File('/foo', changeset=changeset).Write(delete=True)  # delete
    files.File('/bar', changeset=changeset).Write(delete=True)  # delete
    files.File('/baz', changeset=changeset).Write('baz2')  # edit
    changeset.FinalizeAssociatedFiles()
    self.vcs.Commit(changeset)

    # Changset 17:
    changeset = self.vcs.NewStagingChangeset()
    files.File('/foo', changeset=changeset).Write('foo3')  # re-create
    changeset.FinalizeAssociatedFiles()
    self.vcs.Commit(changeset)

  def testMixin(self):
    # NOTE: Look here first. If this test fails, other tests are likely broken.
    changeset = self.vcs.NewStagingChangeset()
    meta = {'color': 'blue', 'flag': False}
    files.File('/foo', changeset=changeset).Write('foo-versioned')
    files.File('/bar', changeset=changeset).Write('bar-versioned', meta=meta)

    # Exists().
    self.assertFalse(files.File('/foo').exists)
    self.assertTrue(files.File('/foo', changeset=changeset).exists)
    self.assertFalse(files.File('/fake', changeset=changeset).exists)

    # Init with an uncommitted file path:
    self.assertFalse(files.File('/foo').exists)
    # Init with an uncommitted file within a changeset:
    titan_file = files.File('/foo', changeset=changeset)
    self.assertEqual('/foo', titan_file.path)
    self.assertEqual('/_titan/ver/1/foo', titan_file.versioned_path)
    expected_foo = files.File('/foo', changeset=changeset)
    expected_bar = files.File('/bar', changeset=changeset)
    actual_titan_files = files.Files(files=[
        expected_foo,
        expected_bar,
        files.File('/fake', changeset=changeset),
    ])
    actual_titan_files.Load()
    expected_titan_files = files.Files(files=[
        expected_foo,
        expected_bar,
    ])
    self.assertEqual(expected_titan_files, actual_titan_files)

    # Write().
    titan_file = files.File('/foo', changeset=changeset)
    titan_file.Write('foo', meta={'color': 'blue'})
    self.assertEqual('/_titan/ver/1/foo', titan_file.versioned_path)

    # Writing with delete=True marks a file to be deleted on commit.
    titan_file = files.File('/foo', changeset=changeset).Write(delete=True)
    self.assertEqual('/_titan/ver/1/foo', titan_file.versioned_path)
    self.assertEqual(FILE_DELETED, titan_file.meta.status)

    # Delete (which is actually a revert).
    files.File('/foo', changeset=changeset).Delete()
    self.assertFalse(files.File('/foo').exists)
    self.assertFalse(files.File('/foo', changeset=changeset).exists)

    # Commit the changeset (/bar is the only remaining file).
    changeset.FinalizeAssociatedFiles()
    self.vcs.Commit(changeset)

    # Exists() with a committed file path.
    self.assertFalse(files.File('/foo').exists)
    self.assertTrue(files.File('/bar').exists)

    # Writing an already-existing file in a new changeset should
    # copy the existing file's content and attributes.
    changeset = self.vcs.NewStagingChangeset()
    titan_file = files.File('/bar', changeset=changeset)
    # Before Write, the file has not been copied:
    self.assertRaises(files.BadFileError, lambda: titan_file.content)
    # Should copy from last-committed file:
    titan_file.Write(meta={'color': 'red'})

    # Test original object:
    self.assertEqual('bar-versioned', titan_file.content)
    self.assertEqual('red', titan_file.meta.color)
    self.assertEqual(False, titan_file.meta.flag)  # untouched meta property.
    # Test re-inited object:
    titan_file = files.File('/bar', changeset=changeset)
    self.assertEqual('bar-versioned', titan_file.content)
    self.assertEqual('red', titan_file.meta.color)
    self.assertEqual(False, titan_file.meta.flag)  # untouched meta property.

  def testNewStagingChangeset(self):
    changeset = self.vcs.NewStagingChangeset()

    # Verify the auto_current_user_add property.
    self.assertEqual('titanuser@example.com', str(changeset.created_by))

    # First changeset ever created. Should be #1 and have status of 'new'.
    self.assertEqual(changeset.num, 1)
    old_datetime = changeset.created
    self.assertTrue(isinstance(old_datetime, datetime.datetime))
    self.assertEqual(CHANGESET_NEW, changeset.status)

    changeset = self.vcs.NewStagingChangeset()
    self.assertEqual(changeset.num, 2)

  def testCommit(self):
    test_user = users.User('test@example.com')
    changeset = self.vcs.NewStagingChangeset(created_by=test_user)

    # Shouldn't be able to submit changesets with no changed files:
    self.assertRaises(versions.CommitError, self.vcs.Commit, changeset,
                      force=True)

    # Verify that the auto_current_user_add property is overwritten.
    self.assertEqual('test@example.com', str(changeset.created_by))

    # Before a changeset is committed, its associated files must be finalized
    # to indicate that the object's files can be trusted for strong consistency.
    files.File('/foo', changeset=changeset).Write('')
    self.assertRaises(versions.ChangesetError, self.vcs.Commit, changeset)
    changeset.FinalizeAssociatedFiles()
    final_changeset = self.vcs.Commit(changeset)
    # When a changeset is committed, a new changeset is created (so that
    # changes are always sequential) with a created time. The old changeset
    # is marked as deleted by submit.
    staged_changeset = versions.Changeset(1)
    self.assertEqual(CHANGESET_DELETED_BY_SUBMIT, staged_changeset.status)
    self.assertEqual(CHANGESET_SUBMITTED, final_changeset.status)
    # Also, the changesets are linked to each other:
    self.assertEqual(1, final_changeset.linked_changeset_num)
    self.assertEqual(2, staged_changeset.linked_changeset_num)
    self.assertEqual(versions.Changeset(1),
                     final_changeset.linked_changeset)
    self.assertEqual(versions.Changeset(2),
                     staged_changeset.linked_changeset)
    # Verify base_path properties also:
    self.assertEqual('/_titan/ver/2', final_changeset.base_path)
    self.assertEqual('/_titan/ver/1',
                     final_changeset.linked_changeset_base_path)

    # Verify that the auto_current_user_add property is overwritten in the
    # final_changeset because it was overwritten in the staged_changeset.
    self.assertEqual('test@example.com', str(final_changeset.created_by))

    # After Commit(), files in a changeset cannot be modified.
    titan_file = files.File('/foo', changeset=changeset)
    self.assertRaises(versions.ChangesetError, titan_file.Write, '')
    self.assertRaises(versions.ChangesetError, titan_file.Delete)

  def testGetChangeset(self):
    self.assertRaises(versions.ChangesetError,
                      self.vcs.GetLastSubmittedChangeset)
    self.InitTestData()

    # Creating a new changeset should not affect GetLastSubmittedChangeset().
    self.assertEqual(17, self.vcs.GetLastSubmittedChangeset().num)
    self.vcs.NewStagingChangeset()
    self.assertEqual(17, self.vcs.GetLastSubmittedChangeset().num)

    # List files changed by a staged changeset and a final changeset.
    # NOTE: the status checks here are merely for testing purposes.
    # VersionedFile objects should never be trusted for canonical version info.
    titan_files = versions.Changeset(12).ListFiles()
    self.assertEqual(titan_files['/foo'].meta.status, FILE_EDITED)
    self.assertEqual(titan_files['/bar'].meta.status, FILE_DELETED)
    self.assertEqual(titan_files['/baz'].meta.status, FILE_EDITED)
    self.assertEqual(titan_files['/qux'].meta.status, FILE_EDITED)
    titan_files = versions.Changeset(13).ListFiles()
    self.assertEqual(titan_files['/foo'].meta.status, FILE_EDITED)
    self.assertEqual(titan_files['/bar'].meta.status, FILE_DELETED)
    self.assertEqual(titan_files['/baz'].meta.status, FILE_EDITED)
    self.assertEqual(titan_files['/qux'].meta.status, FILE_EDITED)

    # Test Serialize().
    changeset = versions.Changeset(12)
    expected_data = {
        'num': 12,
        'created': changeset.created,
        'status': versions.CHANGESET_DELETED_BY_SUBMIT,
        'base_path': '/_titan/ver/12',
        'linked_changeset_base_path': '/_titan/ver/13',
        'linked_changeset_num': 13,
        'created_by': 'titanuser@example.com',
    }
    self.assertEqual(expected_data, changeset.Serialize())

  def testGetFileVersions(self):
    self.InitTestData()

    # Verify limit argument.
    file_versions = self.vcs.GetFileVersions('/foo', limit=1)
    self.assertEqual(1, len(file_versions))
    file_versions = self.vcs.GetFileVersions('/foo', limit=100)
    self.assertEqual(4, len(file_versions))

    # List all versions of a file, backwards!
    file_versions = self.vcs.GetFileVersions('/foo')
    self.assertEqual(17, file_versions[0].changeset.num)
    self.assertEqual(15, file_versions[1].changeset.num)
    self.assertEqual(13, file_versions[2].changeset.num)
    self.assertEqual(11, file_versions[3].changeset.num)
    self.assertEqual(FILE_CREATED, file_versions[0].status)
    self.assertEqual(FILE_DELETED, file_versions[1].status)
    self.assertEqual(FILE_EDITED, file_versions[2].status)
    self.assertEqual(FILE_CREATED, file_versions[3].status)

    expected = {
        'status': u'created',
        'path': u'/foo',
        'changeset_num': 17,
        'linked_changeset_num': 16,
        'changeset_created_by': 'titanuser@example.com',
        'created': file_versions[0].created,
        # Important: this path uses the staging changeset number (not the
        # final changeset number) since the content is not moved on commit.
        'versioned_path': u'/_titan/ver/16/foo'
    }
    self.assertDictEqual(expected, file_versions[0].Serialize())

if __name__ == '__main__':
  basetest.main()
