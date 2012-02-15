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

"""Tests for versions.py."""

from tests import testing

import datetime
from google.appengine.api import users
from titan.common.lib.google.apputils import basetest
from titan.files import files
from titan.services import versions

CHANGESET_NEW = versions.CHANGESET_NEW
CHANGESET_SUBMITTED = versions.CHANGESET_SUBMITTED
CHANGESET_DELETED = versions.CHANGESET_DELETED
CHANGESET_DELETED_BY_SUBMIT = versions.CHANGESET_DELETED_BY_SUBMIT
FILE_CREATED = versions.FILE_CREATED
FILE_EDITED = versions.FILE_EDITED
FILE_DELETED = versions.FILE_DELETED

class VersionsTest(testing.ServicesTestCase):

  def setUp(self):
    services = (
        'titan.services.versions',
    )
    self.EnableServices(services)
    self.vcs = versions.VersionControlService()
    super(VersionsTest, self).setUp()

  def InitTestData(self):
    for _ in range(1, 10):
      self.vcs.NewStagingChangeset()

    # Changeset 11 (was changeset 10 before commit):
    changeset = self.vcs.NewStagingChangeset()
    files.Write('/foo', 'foo', changeset=changeset)
    files.Write('/bar', 'bar', changeset=changeset)
    files.Write('/qux', 'qux', changeset=changeset)
    changeset.FinalizeAssociatedPaths()
    self.vcs.Commit(changeset)
    # For testing, move the submitted datetime to 31 days ago.
    changeset_ent = versions.Changeset(11).changeset_ent
    created = datetime.datetime.now() - datetime.timedelta(days=31)
    changeset_ent.created = created
    changeset_ent.put()

    # Changset 13:
    changeset = self.vcs.NewStagingChangeset()
    files.Write('/foo', 'foo2', changeset=changeset)  # edit
    files.Write('/bar', delete=True, changeset=changeset)  # delete
    files.Write('/baz', 'baz', changeset=changeset)  # create
    files.Write('/qux', 'qux2', changeset=changeset)  # edit
    changeset.FinalizeAssociatedPaths()
    self.vcs.Commit(changeset)

    # Changset 15:
    changeset = self.vcs.NewStagingChangeset()
    files.Write('/foo', delete=True, changeset=changeset)  # delete
    files.Write('/bar', delete=True, changeset=changeset)  # delete
    files.Write('/baz', 'baz2', changeset=changeset)  # edit
    changeset.FinalizeAssociatedPaths()
    self.vcs.Commit(changeset)

    # Changset 17:
    changeset = self.vcs.NewStagingChangeset()
    files.Write('/foo', 'foo3', changeset=changeset)  # re-create
    changeset.FinalizeAssociatedPaths()
    self.vcs.Commit(changeset)

  def testHooks(self):
    # NOTE: Look here first. If this test fails, other tests are likely broken.
    changeset = self.vcs.NewStagingChangeset()
    files.Write('/foo', 'foo-versioned', changeset=changeset)
    files.Write('/bar', 'bar-versioned', meta={'color': 'blue'},
                changeset=changeset)

    # Exists.
    self.assertFalse(files.Exists('/foo'))
    self.assertTrue(files.Exists('/foo', changeset=changeset))
    self.assertFalse(files.Exists('/fake', changeset=changeset))

    # Get() with an uncommitted file path:
    self.assertEqual(None, files.Get('/foo'))
    # Get() with an uncommitted file within a changeset:
    file_obj = files.Get('/foo', changeset=changeset)
    self.assertEqual('/foo', file_obj.path)
    self.assertEqual('/_titan/ver/1/foo', file_obj.versioned_path)
    expected_file_objs = {
        '/foo': versions.VersionedFile(files.File('/_titan/ver/1/foo')),
        '/bar': versions.VersionedFile(files.File('/_titan/ver/1/bar')),
    }
    self.assertDictEqual(expected_file_objs, files.Get(
        ['/foo', '/bar', '/fake'], changeset=changeset))

    # Write().
    self.assertRaises(TypeError, files.Write, '/foo')
    file_obj = files.Write('/foo', 'foo', meta={'color': 'blue'},
                           changeset=changeset)
    self.assertEqual('/_titan/ver/1/foo', file_obj.versioned_path)

    # Writing with delete=True marks a file to be deleted on commit.
    file_obj = files.Write('/foo', changeset=changeset, delete=True)
    self.assertEqual('/_titan/ver/1/foo', file_obj.versioned_path)
    self.assertEqual(FILE_DELETED, file_obj.status)

    # Touch with single and multiple paths.
    file_obj = files.Touch('/foo', changeset=changeset)
    self.assertEqual('/_titan/ver/1/foo', file_obj.versioned_path)
    file_objs = files.Touch(['/foo', '/bar'], changeset=changeset)
    self.assertEqual(['/_titan/ver/1/foo', '/_titan/ver/1/bar'],
                     [file_obj.versioned_path for file_obj in file_objs])

    # Delete (which is actually a revert).
    self.assertRaises(TypeError, files.Delete, '/foo')
    files.Delete('/foo', changeset=changeset)
    self.assertFalse(files.Exists('/foo'))
    self.assertFalse(files.Exists('/foo', changeset=changeset))

    changeset.FinalizeAssociatedPaths()
    self.vcs.Commit(changeset)

    # Exists() with a committed file path.
    self.assertFalse(files.Exists('/foo'))
    self.assertTrue(files.Exists('/bar'))

    # Get() with uncommitted and committed file paths.
    self.assertEqual(None, files.Get('/foo'))
    self.assertEqual('/bar', files.Get('/bar').path)
    self.assertEqual(['/bar'], files.Get(['/bar']).keys())

    # Writing or touching an already-existing file in a new changeset should
    # persist the existing file's content and attributes.
    changeset = self.vcs.NewStagingChangeset()
    files.Touch('/bar', changeset=changeset)
    file_obj = files.Get('/bar', changeset=changeset)
    self.assertEqual('bar-versioned', file_obj.content)
    self.assertEqual('blue', file_obj.color)
    files.Write('/bar', changeset=changeset, meta={'color': 'red'})
    file_obj = files.Get('/bar', changeset=changeset)
    self.assertEqual('red', file_obj.color)

    # Content shouldn't change after delete and re-create via Touch.
    files.Delete('/bar', changeset=changeset)
    files.Touch('/bar', changeset=changeset)
    file_obj = files.Get('/bar', changeset=changeset)
    self.assertEqual(FILE_EDITED, file_obj.status)
    self.assertEqual('bar-versioned', file_obj.content)

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

    # Before a changeset is committed, its associated paths must be finalized
    # to indicate that the object's paths can be trusted for strong consistency.
    files.Touch('/foo', changeset=changeset)
    self.assertRaises(versions.ChangesetError, self.vcs.Commit, changeset)
    self.assertListEqual(['/foo'], changeset._associated_paths)
    # Test _VerifyRootPaths.
    self.assertRaises(ValueError, changeset.AssociatePaths, '/_titan/ver/123/a')
    changeset.FinalizeAssociatedPaths()
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
    self.assertRaises(versions.ChangesetError, files.Write, '/foo', '',
                      changeset=changeset)
    self.assertRaises(versions.ChangesetError, files.Delete, '/foo',
                      changeset=changeset)
    self.assertRaises(versions.ChangesetError, files.Touch, '/foo',
                      changeset=changeset)

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
    file_versions = versions.Changeset(12).ListFiles()
    self.assertEqual(file_versions['/foo'].status, FILE_EDITED)
    self.assertEqual(file_versions['/bar'].status, FILE_DELETED)
    self.assertEqual(file_versions['/baz'].status, FILE_EDITED)
    self.assertEqual(file_versions['/qux'].status, FILE_EDITED)
    file_versions = versions.Changeset(13).ListFiles()
    self.assertEqual(file_versions['/foo'].status, FILE_EDITED)
    self.assertEqual(file_versions['/bar'].status, FILE_DELETED)
    self.assertEqual(file_versions['/baz'].status, FILE_EDITED)
    self.assertEqual(file_versions['/qux'].status, FILE_EDITED)

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

  def testGenerateDiff(self):
    self.InitTestData()
    file_versions = self.vcs.GetFileVersions('/foo')

    # 'foo' --> 'foo3'
    expected_diff = [(-1, ''), (0, 'foo'), (1, '3')]
    actual_diff = self.vcs.GenerateDiff(file_versions[3], file_versions[0],
                                        semantic_cleanup=True, diff_lines=True,
                                        edit_cost=0)
    self.assertEqual(expected_diff, actual_diff)

    # Deleted file --> 'foo3'
    expected_diff = [(1, 'foo3')]
    actual_diff = self.vcs.GenerateDiff(file_versions[1], file_versions[0],
                                        edit_cost=2)
    self.assertEqual(expected_diff, actual_diff)

if __name__ == '__main__':
  basetest.main()
