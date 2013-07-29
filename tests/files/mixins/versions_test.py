#!/usr/bin/env python
# -*- coding: utf-8 -*-
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
from google.appengine.datastore import datastore_stub_util
from titan.common.lib.google.apputils import basetest
from titan.files import files
from titan.files.mixins import versions
from titan import users

CHANGESET_STAGING = versions.ChangesetStatus.staging
CHANGESET_SUBMITTED = versions.ChangesetStatus.submitted
CHANGESET_DELETED = versions.ChangesetStatus.deleted
CHANGESET_DELETED_BY_SUBMIT = versions.ChangesetStatus.deleted_by_submit
FILE_CREATED = versions.FileStatus.created
FILE_EDITED = versions.FileStatus.edited
FILE_DELETED = versions.FileStatus.deleted

class VersionedFile(versions.FileVersioningMixin, files.File):
  pass

class VersionsTest(testing.BaseTestCase):

  def setUp(self):
    super(VersionsTest, self).setUp()
    self.vcs = versions.VersionControlService()
    files.register_file_factory(lambda *args, **kwargs: VersionedFile)

  def tearDown(self):
    files.unregister_file_factory()
    super(VersionsTest, self).tearDown()

  def make_testdata(self):
    for _ in range(1, 10):
      self.vcs.new_staging_changeset()

    # Changeset 11 (was changeset 10 before commit):
    changeset = self.vcs.new_staging_changeset()
    self.assertIsNone(changeset.base_changeset)
    files.File('/foo', changeset=changeset).write('foo')
    files.File('/bar', changeset=changeset).write('bar')
    files.File('/qux', changeset=changeset).write('qux')
    changeset.finalize_associated_files()
    self.vcs.commit(changeset)
    # For testing, move the submitted datetime to 31 days ago.
    changeset_ent = versions.Changeset(11).changeset_ent
    created = datetime.datetime.now() - datetime.timedelta(days=31)
    changeset_ent.created = created
    changeset_ent.put()

    # Changset 13:
    changeset = self.vcs.new_staging_changeset()
    self.assertEqual(11, changeset.base_changeset.num)
    files.File('/foo', changeset=changeset).write('foo2')  # edit
    files.File('/bar', changeset=changeset).delete()  # delete
    files.File('/baz', changeset=changeset).write('baz')  # create
    files.File('/qux', changeset=changeset).write('qux2')  # edit
    changeset.finalize_associated_files()
    self.vcs.commit(changeset)
    self.assertEqual(13, self.vcs.get_last_submitted_changeset().num)
    self.assertIsNone(self.vcs.get_last_submitted_changeset().namespace)

    # Changset 15:
    changeset = self.vcs.new_staging_changeset()
    self.assertEqual(13, changeset.base_changeset.num)
    files.File('/foo', changeset=changeset).delete()  # delete
    files.File('/bar', changeset=changeset).delete()  # delete
    files.File('/baz', changeset=changeset).write('baz2')  # edit
    changeset.finalize_associated_files()
    self.vcs.commit(changeset)

    # Changset 17:
    changeset = self.vcs.new_staging_changeset()
    modified_by = users.TitanUser('differentuser@example.com')
    files.File('/foo', changeset=changeset).write(
        'foo3', modified_by=modified_by)  # re-create
    changeset.finalize_associated_files()
    self.vcs.commit(changeset)

  def testMixin(self):
    # NOTE: Look here first. If this test fails, other tests are likely broken.
    changeset = self.vcs.new_staging_changeset()
    meta = {'color': 'blue', 'flag': False}
    files.File('/foo', changeset=changeset).write('foo-versioned')
    files.File('/bar', changeset=changeset).write('bar-versioned', meta=meta)

    # exists().
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
    actual_titan_files.load()
    expected_titan_files = files.Files(files=[
        expected_foo,
        expected_bar,
    ])
    self.assertEqual(expected_titan_files, actual_titan_files)

    # write().
    titan_file = files.File('/foo', changeset=changeset)
    titan_file.write('foo', meta={'color': 'blue'})
    self.assertEqual('/_titan/ver/1/foo', titan_file.versioned_path)

    # Delete (really "mark for deletion").
    titan_file = files.File('/foo', changeset=changeset).delete()
    self.assertEqual('/_titan/ver/1/foo', titan_file.versioned_path)
    # Just for testing, set make sure the status is correct.
    titan_file._allow_deleted_files = True
    self.assertEqual(FILE_DELETED, titan_file.meta.status)
    # Files marked for delete technically exist at the versioned path,
    # but through the file interface they should pretend to not exist:
    self.assertFalse(files.File('/foo', changeset=changeset).exists)
    self.assertRaises(files.BadFileError,
                      lambda: files.File('/foo', changeset=changeset).content)

    # Revert a file marked for deletion.
    changeset.revert_file(files.File('/foo', changeset=changeset))
    self.assertFalse(files.File('/foo').exists)
    self.assertFalse(files.File('/foo', changeset=changeset).exists)

    # Commit the changeset (/bar is the only remaining file).
    changeset.finalize_associated_files()
    self.vcs.commit(changeset)

    # Check exists() with a committed file path.
    self.assertFalse(files.File('/foo').exists)
    self.assertTrue(files.File('/bar').exists)

    # Writing an already-existing file in a new changeset should
    # copy the existing file's content and attributes.
    changeset = self.vcs.new_staging_changeset()
    titan_file = files.File('/bar', changeset=changeset)
    # Should copy from last-committed file:
    titan_file.write(meta={'color': 'red'})

    # Test original object:
    self.assertEqual('bar-versioned', titan_file.content)
    self.assertEqual('red', titan_file.meta.color)
    self.assertEqual(False, titan_file.meta.flag)  # untouched meta property.
    # Test re-inited object:
    titan_file = files.File('/bar', changeset=changeset)
    self.assertEqual('bar-versioned', titan_file.content)
    self.assertEqual('red', titan_file.meta.color)
    self.assertEqual(False, titan_file.meta.flag)  # untouched meta property.

  def testEmptyRootFile(self):
    # Regression test: make sure that content='' is correctly handled when
    # copying root file attributes.
    changeset = self.vcs.new_staging_changeset()
    files.File('/foo', changeset=changeset).write(u'∆∆∆')
    changeset.finalize_associated_files()
    self.vcs.commit(changeset)
    changeset = self.vcs.new_staging_changeset()
    files.File('/foo', changeset=changeset).write('', encoding='utf-8')
    changeset.finalize_associated_files()
    self.vcs.commit(changeset)
    self.assertEqual('', files.File('/foo').content)

  def testNewStagingChangeset(self):
    changeset = self.vcs.new_staging_changeset()

    # Verify the auto_current_user_add property.
    self.assertEqual('titanuser@example.com', str(changeset.created_by))

    # First changeset ever created. Should be #1 and have status of 'new'.
    self.assertEqual(changeset.num, 1)
    old_datetime = changeset.created
    self.assertTrue(isinstance(old_datetime, datetime.datetime))
    self.assertEqual(CHANGESET_STAGING, changeset.status)

    changeset = self.vcs.new_staging_changeset()
    self.assertEqual(changeset.num, 2)

    # Test Changeset __eq__ and __ne__.
    self.assertEqual(versions.Changeset(2), versions.Changeset(2))
    self.assertNotEqual(versions.Changeset(2), versions.Changeset(3))
    self.assertNotEqual(
        versions.Changeset(2, namespace='aaa'), versions.Changeset(2))

  def testCommit(self):
    test_user = users.TitanUser('test@example.com')
    changeset = self.vcs.new_staging_changeset(created_by=test_user)

    # Shouldn't be able to submit changesets with no changed files:
    self.assertRaises(versions.CommitError, self.vcs.commit, changeset,
                      force=True)

    # Verify that the auto_current_user_add property is overwritten.
    self.assertEqual('test@example.com', str(changeset.created_by))

    # Before a changeset is committed, its associated files must be finalized
    # to indicate that the object's files can be trusted for strong consistency.
    files.File('/foo', changeset=changeset).write('')
    self.assertRaises(versions.ChangesetError, self.vcs.commit, changeset)
    changeset.finalize_associated_files()
    final_changeset = self.vcs.commit(changeset)

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

    # After commit(), files in a changeset cannot be modified.
    titan_file = files.File('/foo', changeset=changeset)
    self.assertRaises(versions.ChangesetError, titan_file.write, '')
    self.assertRaises(versions.ChangesetError, titan_file.delete)

  def testCommitManyFiles(self):
    # Regression test for "operating on too many entity groups in a
    # single transaction" error. This usually happens through the HTTP API
    # when lazy file objects are created from the manifest and then end up
    # being evaluated one-by-one inside the commit code path.
    test_user = users.TitanUser('test@example.com')
    changeset = self.vcs.new_staging_changeset(created_by=test_user)
    for i in range(100):
      files.File('/foo%s' % i, changeset=changeset).write(str(i))
    changeset.finalize_associated_files()
    # Recreate the changeset from scratch, and reassociate lazy file objects.
    changeset = versions.Changeset(changeset.num)
    for i in range(100):
      changeset.associate_file(files.File('/foo%s' % i, changeset=changeset))
    changeset.finalize_associated_files()
    self.vcs.commit(changeset)

    # Test again, but test the list_files code path with force=True.
    changeset = self.vcs.new_staging_changeset(created_by=test_user)
    for i in range(100):
      files.File('/foo%s' % i, changeset=changeset).write(str(i))
    changeset.finalize_associated_files()
    # Recreate the changeset from scratch, and reassociate lazy file objects.
    changeset = versions.Changeset(changeset.num)
    self.vcs.commit(changeset, force=True)

  def testGetChangeset(self):
    self.assertIsNone(self.vcs.get_last_submitted_changeset())
    self.make_testdata()

    # Creating a new changeset should not affect get_last_submitted_changeset().
    self.assertEqual(17, self.vcs.get_last_submitted_changeset().num)
    self.vcs.new_staging_changeset()
    self.assertEqual(17, self.vcs.get_last_submitted_changeset().num)

    # List files changed in a deleted-by-submit changeset and a final changeset.
    # NOTE: the status checks here are merely for testing purposes.
    # VersionedFile objects should never be trusted for canonical version info.
    self.assertRaises(
        versions.ChangesetError,
        lambda: versions.Changeset(12).list_files('/'))
    self.assertRaises(
        versions.ChangesetError, versions.Changeset(12).get_files)
    titan_files = versions.Changeset(13).list_files('/')
    self.assertEqual(titan_files['/foo'].meta.status, FILE_EDITED)
    self.assertEqual(titan_files['/bar'].meta.status, FILE_DELETED)
    self.assertEqual(titan_files['/baz'].meta.status, FILE_EDITED)
    self.assertEqual(titan_files['/qux'].meta.status, FILE_EDITED)

    # Test serialize().
    changeset = versions.Changeset(12)
    expected_data = {
        'num': 12,
        'namespace': changeset.namespace,
        'base_changeset_num': changeset.base_changeset_num,
        'created': changeset.created,
        'status': CHANGESET_DELETED_BY_SUBMIT,
        'base_path': '/_titan/ver/12',
        'linked_changeset_base_path': '/_titan/ver/13',
        'linked_changeset_num': 13,
        'created_by': 'titanuser@example.com',
    }
    self.assertEqual(expected_data, changeset.serialize())

  def testListFiles(self):
    changeset = self.vcs.new_staging_changeset()
    # Test list files within first staging changeset.
    self.assertEqual(
        [], changeset.list_files('/').keys())
    self.assertEqual(
        [], changeset.list_files('/', include_manifested=True).keys())
    files.File('/foo', changeset=changeset).write('foo')
    files.File('/a/foo', changeset=changeset).write('foo')
    files.File('/a/bar', changeset=changeset).write('bar')
    changeset.finalize_associated_files()
    self.vcs.commit(changeset)  # Changeset 2.

    changeset = self.vcs.new_staging_changeset()
    files.File('/foo', changeset=changeset).delete()
    changeset.finalize_associated_files()
    self.vcs.commit(changeset)  # Changeset 4.

    titan_files = versions.Changeset(2).list_files(dir_path='/')
    self.assertEqual(['/foo'], titan_files.keys())
    titan_files = versions.Changeset(2).list_files(dir_path='/', recursive=True)
    self.assertEqual(['/a/bar', '/a/foo', '/foo'], titan_files.keys())
    titan_files = versions.Changeset(2).list_files(dir_path='/a/')
    self.assertEqual(['/a/bar', '/a/foo'], titan_files.keys())
    # Test limit and offset.
    titan_files = versions.Changeset(2).list_files(
        dir_path='/', recursive=True, limit=1, offset=1)
    self.assertEqual(['/a/foo'], titan_files.keys())

    # Test included_deleted.
    titan_files = versions.Changeset(4).list_files(dir_path='/')
    self.assertEqual(['/foo'], titan_files.keys())
    self.assertEqual(
        versions.FileStatus.deleted, titan_files.values()[0].meta.status)
    titan_files = versions.Changeset(4).list_files(
        dir_path='/', include_deleted=False)
    self.assertEqual([], titan_files.keys())

    # Test include_manifested on a finalized changeset.
    titan_files = versions.Changeset(4).list_files(
        '/', include_deleted=False, include_manifested=True)
    self.assertEqual([], titan_files.keys())
    titan_files = versions.Changeset(4).list_files('/', include_manifested=True)
    self.assertEqual(['/foo'], titan_files.keys())
    titan_files = versions.Changeset(4).list_files(
        '/', recursive=True, include_manifested=True)
    self.assertEqual(['/a/bar', '/a/foo', '/foo'], titan_files.keys())
    titan_files = versions.Changeset(4).list_files(
        '/a/', recursive=True, include_manifested=True)
    self.assertEqual(['/a/bar', '/a/foo'], titan_files.keys())
    titan_files = versions.Changeset(4).list_files(
        '/b', recursive=True, include_manifested=True)
    self.assertEqual([], titan_files.keys())

    # Test include_manifested on a staging changeset.
    changeset = self.vcs.new_staging_changeset()  # Changeset 5 (staging).
    files.File('/a/qux', changeset=changeset).write('qux')
    titan_files = versions.Changeset(5).list_files(
        '/', include_deleted=False, include_manifested=True)
    self.assertEqual([], titan_files.keys())
    titan_files = versions.Changeset(5).list_files(
        '/', recursive=True, include_manifested=True)
    self.assertEqual(['/a/bar', '/a/foo', '/a/qux'], titan_files.keys())
    titan_files = versions.Changeset(5).list_files(
        '/a/', recursive=True, include_manifested=True)
    self.assertEqual(['/a/bar', '/a/foo', '/a/qux'], titan_files.keys())
    titan_files = versions.Changeset(5).list_files(
        '/b', recursive=True, include_manifested=True)
    self.assertEqual([], titan_files.keys())
    # Verify reading a manifested and a non-manifested file.
    titan_files = versions.Changeset(5).list_files(
        '/', recursive=True, include_manifested=True)
    self.assertEqual('foo', titan_files['/a/foo'].content)
    self.assertEqual('qux', titan_files['/a/qux'].content)

    # Error handling.
    self.assertRaises(
        ValueError,
        versions.Changeset(2).list_files,
        dir_path='/', include_manifested=True, depth=1, filters=[], order=[])
    self.assertRaises(
        ValueError, versions.Changeset(2).list_files,
        '/', include_manifested=True, limit=10, offset=1)

  def testPrivateListManifestedPaths(self):
    changeset = self.vcs.new_staging_changeset()
    files.File('/foo', changeset=changeset).write('')
    files.File('/a/foo', changeset=changeset).write('')
    files.File('/a/bar', changeset=changeset).write('')
    files.File('/a/a/foo', changeset=changeset).write('')
    files.File('/b/foo', changeset=changeset).write('')
    changeset.finalize_associated_files()
    changeset = self.vcs.commit(changeset)  # Changeset 2.

    paths_to_changeset_num = versions._list_manifested_paths(
        changeset=changeset, dir_path='/')
    self.assertSameElements(['/foo'], paths_to_changeset_num.keys())

    paths_to_changeset_num = versions._list_manifested_paths(
        changeset=changeset, dir_path='/', recursive=True)
    self.assertSameElements(
        ['/foo', '/a/foo', '/a/bar', '/a/a/foo', '/b/foo'],
        paths_to_changeset_num.keys())

    paths_to_changeset_num = versions._list_manifested_paths(
        changeset=changeset, dir_path='/a')
    self.assertSameElements(['/a/foo', '/a/bar'], paths_to_changeset_num.keys())

    paths_to_changeset_num = versions._list_manifested_paths(
        changeset=changeset, dir_path='/b', recursive=True)
    self.assertSameElements(['/b/foo'], paths_to_changeset_num.keys())

  def testListDirectories(self):
    changeset = self.vcs.new_staging_changeset()
    files.File('/foo', changeset=changeset).write('')
    self.assertEqual([], changeset.list_directories('/').keys())
    files.File('/a/foo', changeset=changeset).write('')
    files.File('/a/a/foo', changeset=changeset).write('')
    self.assertEqual(['/a'], changeset.list_directories('/').keys())
    self.assertEqual([], changeset.list_directories('/fake/').keys())
    changeset = self.vcs.commit(changeset, force=True)
    self.assertEqual(['/a'], changeset.list_directories('/').keys())
    self.assertEqual([], changeset.list_directories('/fake/').keys())

    changeset = self.vcs.new_staging_changeset()
    # Before writes:
    self.assertEqual([], changeset.list_directories('/').keys())
    self.assertEqual(
        ['/a'], changeset.list_directories('/', include_manifested=True).keys())
    self.assertEqual(
        ['/a/a'],
        changeset.list_directories('/a', include_manifested=True).keys())

    files.File('/a/foo', changeset=changeset).write('')
    files.File('/a/a/a/foo', changeset=changeset).write('')
    files.File('/b/foo', changeset=changeset).write('')
    # Before commit:
    self.assertEqual(['/a', '/b'], changeset.list_directories('/').keys())
    self.assertEqual(['/a/a'], changeset.list_directories('/a').keys())
    self.assertEqual(['/a/a/a'], changeset.list_directories('/a/a').keys())
    changeset = self.vcs.commit(changeset, force=True)
    # After commit:
    self.assertEqual(['/a', '/b'], changeset.list_directories('/').keys())
    self.assertEqual(['/a/a'], changeset.list_directories('/a').keys())
    self.assertEqual(['/a/a/a'], changeset.list_directories('/a/a').keys())

    # No namespace mixing:
    changeset = self.vcs.new_staging_changeset(namespace='aaa')
    self.assertEqual([], changeset.list_directories('/').keys())
    files.File('/foo', changeset=changeset, namespace='aaa').write('')
    changeset = self.vcs.commit(changeset, force=True)
    self.assertEqual([], changeset.list_directories('/').keys())

  def testGetFileVersions(self):
    self.make_testdata()

    # Verify limit argument.
    file_versions = self.vcs.get_file_versions('/foo', limit=1)
    self.assertEqual(1, len(file_versions))
    file_versions = self.vcs.get_file_versions('/foo', limit=100)
    self.assertEqual(4, len(file_versions))

    # List all versions of a file, backwards!
    file_versions = self.vcs.get_file_versions('/foo')
    self.assertEqual(17, file_versions[0].changeset.num)
    self.assertEqual(16, file_versions[0].content_changeset.num)
    self.assertEqual(15, file_versions[1].changeset.num)
    self.assertEqual(14, file_versions[1].content_changeset.num)
    self.assertEqual(13, file_versions[2].changeset.num)
    self.assertEqual(12, file_versions[2].content_changeset.num)
    self.assertEqual(11, file_versions[3].changeset.num)
    self.assertEqual(10, file_versions[3].content_changeset.num)
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
        'created_by': 'differentuser@example.com',
        'created': file_versions[0].created,
        # Important: this path uses the staging changeset number (not the
        # final changeset number) since the content is not moved on commit.
        'versioned_path': u'/_titan/ver/16/foo'
    }
    self.assertDictEqual(expected, file_versions[0].serialize())

  def testNamespaceState(self):
    self.make_namespaced_testdata()

    # Verify the state of the filesystem in the default namespace.
    changeset = versions.Changeset(2)
    self.assertEqual('foo', files.File('/foo', changeset=changeset).content)
    # Files from other namespaces should not exist.
    self.assertFalse(files.File('/bar', changeset=changeset).exists)
    self.assertFalse(files.File('/qux', changeset=changeset).exists)
    changeset = versions.Changeset(4)
    # Verify the current state of the filesystem in the default namespace.
    # Force use of dynamic _FilePointer by not passing the changeset arg.
    self.assertEqual('newfoo', files.File('/foo').content)
    self.assertEqual('bar', files.File('/bar').content)

    # Verify the state of the filesystem in the 'aaa' namespace at changeset 2.
    changeset = versions.Changeset(2, namespace='aaa')
    titan_file = files.File('/foo', changeset=changeset, namespace='aaa')
    self.assertEqual('foo-aaa', titan_file.content)
    titan_file = files.File('/bar', changeset=changeset, namespace='aaa')
    self.assertEqual('bar-aaa', titan_file.content)
    # Files from other namespaces should not exist.
    titan_file = files.File('/qux', changeset=changeset, namespace='aaa')
    self.assertFalse(titan_file.exists)

    # Verify the state of the filesystem in the 'aaa' namespace at changeset 4.
    changeset = versions.Changeset(4, namespace='aaa')
    titan_file = files.File('/foo', changeset=changeset, namespace='aaa')
    self.assertEqual('newfoo-aaa', titan_file.content)
    titan_file = files.File('/bar', changeset=changeset, namespace='aaa')
    self.assertFalse(titan_file.exists)

    # Verify the current state of the filesystem in the 'aaa' namespace.
    # Force use of dynamic _FilePointer by not passing the changeset arg.
    self.assertEqual('newfoo-aaa', files.File('/foo', namespace='aaa').content)
    self.assertFalse(files.File('/bar', namespace='aaa').exists)

    # Verify the state of the filesystem in the 'bbb' namespace at changeset 2.
    changeset = versions.Changeset(2, namespace='bbb')
    titan_file = files.File('/foo', changeset=changeset, namespace='bbb')
    self.assertEqual('foo-bbb', titan_file.content)
    titan_file = files.File('/qux', changeset=changeset, namespace='bbb')
    self.assertEqual('qux-bbb', titan_file.content)
    # Files from other namespaces should not exist.
    titan_file = files.File('/bar', changeset=changeset, namespace='bbb')
    self.assertFalse(titan_file.exists)

  def testManifestedViewsAndRebase(self):
    # None of the following behaviors, including new_staging_changeset's setting
    # of base_changeset, should rely on eventually-consistent queries.
    # Verify this behavior by simulating a never-eventually-consistent HRD.
    policy = datastore_stub_util.PseudoRandomHRConsistencyPolicy(probability=0)
    self.datastore_stub.SetConsistencyPolicy(policy)

    self.make_namespaced_testdata()

    # Staging changeset 5.
    changeset = self.vcs.new_staging_changeset()
    self.assertEqual(5, changeset.num)
    self.assertEqual(4, changeset.base_changeset.num)
    files.File('/bar', changeset=changeset).delete()
    files.File('/qux', changeset=changeset).write('qux')

    # Changeset 6 and 7.
    changeset = self.vcs.new_staging_changeset()
    files.File('/foo', changeset=changeset).write('NEWfoo')
    changeset.finalize_associated_files()
    final_changeset = self.vcs.commit(changeset)
    self.assertEqual(7, final_changeset.num)

    # Changeset 8 and 9.
    changeset = self.vcs.new_staging_changeset()
    for i in range(3200):
      files.File('/foo{}'.format(i), changeset=changeset).write('')
    changeset.finalize_associated_files()
    self.vcs.commit(changeset)

    # MANIFESTED FILESYSTEM VIEWS.
    # - Files read through a 'new' or 'submitted' changeset: first pull
    #   from the changeset, then fallback to the base_changeset's manifest.
    #   The manifested filesystem should by overlaid by the changeset's files.
    # - Files read through a 'deleted' or 'deleted-by-submit' changeset:
    #   always pull from the base_changeset's manifest, ignore the changeset
    #   since its changes will manifest when viewed at it's corresponding
    #   submitted changeset.
    # - File modifications must go through 'new' changesets.
    changeset = versions.Changeset(1)  # 'deleted-by-submit'
    self.assertFalse(files.File('/foo', changeset=changeset).exists)
    self.assertFalse(files.File('/bar', changeset=changeset).exists)
    self.assertIsNone(changeset._num_manifest_shards)

    changeset = versions.Changeset(2)  # 'submitted'
    self.assertEqual('foo', files.File('/foo', changeset=changeset).content)
    self.assertFalse(files.File('/bar', changeset=changeset).exists)
    self.assertEqual(1, changeset._num_manifest_shards)

    changeset = versions.Changeset(3)  # 'deleted-by-submit'
    self.assertEqual('foo', files.File('/foo', changeset=changeset).content)
    self.assertFalse(files.File('/bar', changeset=changeset).exists)
    self.assertIsNone(changeset._num_manifest_shards)

    changeset = versions.Changeset(4)  # 'submitted'
    self.assertEqual('newfoo', files.File('/foo', changeset=changeset).content)
    self.assertEqual('bar', files.File('/bar', changeset=changeset).content)
    self.assertFalse(files.File('/qux', changeset=changeset).exists)
    self.assertEqual(1, changeset._num_manifest_shards)

    changeset = versions.Changeset(5)  # 'staging'
    self.assertEqual('newfoo', files.File('/foo', changeset=changeset).content)
    self.assertEqual('qux', files.File('/qux', changeset=changeset).content)
    self.assertFalse(files.File('/bar', changeset=changeset).exists)
    self.assertIsNone(changeset._num_manifest_shards)

    changeset = versions.Changeset(6)  # 'deleted-by-submit'
    self.assertEqual('newfoo', files.File('/foo', changeset=changeset).content)
    self.assertEqual('bar', files.File('/bar', changeset=changeset).content)
    self.assertIsNone(changeset._num_manifest_shards)

    changeset = versions.Changeset(7)  # 'submitted'
    self.assertEqual('NEWfoo', files.File('/foo', changeset=changeset).content)
    self.assertEqual('bar', files.File('/bar', changeset=changeset).content)
    self.assertEqual(1, changeset._num_manifest_shards)

    changeset = versions.Changeset(9)  # 'submitted'
    self.assertEqual(4, changeset._num_manifest_shards)
    # Verify that hash is evenly distributing the paths over the shards by
    # verifying the shards are not nearly full to 1000 paths.
    manifest_shard = changeset._get_manifest_shard_ent('/foo0')
    self.assertLess(900, len(manifest_shard.paths_to_changeset_num))

    # Rebase the staging changeset and verify the new manifest files.
    changeset = versions.Changeset(5)  # 'staging'
    changeset.rebase(versions.Changeset(7))
    self.assertEqual('NEWfoo', files.File('/foo', changeset=changeset).content)
    self.assertFalse(files.File('/bar', changeset=changeset).exists)
    self.assertEqual('qux', files.File('/qux', changeset=changeset).content)

    # Cannot rebase to anything but submitted changesets.
    self.assertRaises(
        versions.ChangesetRebaseError, changeset.rebase, versions.Changeset(5))
    self.assertRaises(
        versions.ChangesetRebaseError, changeset.rebase, versions.Changeset(6))
    # Namespaces must match.
    self.assertRaises(
        versions.NamespaceMismatchError, changeset.rebase,
        versions.Changeset(7, namespace='aaa'))

  def make_namespaced_testdata(self):
    meta = {'color': 'blue'}

    # Filesystem views at final changesets (the whole filesystem, not diffs):
    #
    # DEFAULT NAMESPACE.
    #   Changeset 2:
    #     /foo: 'foo'
    #   Changeset 4:
    #     /foo: 'newfoo'
    #     /bar: 'bar'
    #
    # NAMESPACE 'aaa'.
    #   Changeset 2:
    #     /foo: 'foo-aaa'
    #     /bar: 'bar-aaa' meta:{'color':'blue'}
    #   Changeset 4:
    #     /foo: 'newfoo-aaa'
    #
    # NAMESPACE 'bbb'.
    #   Changeset 2:
    #     /foo: 'foo-bbb'
    #     /qux: 'qux-bbb'

    # Changeset 2, default namespace (was changeset 1 before commit).
    changeset = self.vcs.new_staging_changeset()
    self.assertEqual(1, changeset.num)
    self.assertIsNone(changeset.base_changeset)
    self.assertIsNone(changeset.namespace)
    files.File('/foo', changeset=changeset).write('foo')
    changeset.finalize_associated_files()
    final_changeset = self.vcs.commit(changeset)
    self.assertEqual(2, final_changeset.num)
    self.assertIsNone(final_changeset.namespace)

    # Changeset 4, default namespace.
    changeset = self.vcs.new_staging_changeset()
    self.assertEqual(3, changeset.num)
    self.assertEqual(2, changeset.base_changeset.num)
    files.File('/foo', changeset=changeset).write('newfoo')
    files.File('/bar', changeset=changeset).write('bar')
    changeset.finalize_associated_files()
    final_changeset = self.vcs.commit(changeset)
    self.assertEqual(4, final_changeset.num)
    self.assertIsNone(final_changeset.namespace)

    # Changeset 2, 'aaa' namespace.
    changeset = self.vcs.new_staging_changeset(namespace='aaa')
    self.assertEqual(1, changeset.num)
    self.assertIsNone(changeset.base_changeset)
    self.assertEqual('aaa', changeset.namespace)
    files.File('/foo', changeset=changeset, namespace='aaa').write('foo-aaa')
    titan_file = files.File('/bar', changeset=changeset, namespace='aaa').write(
        'bar-aaa', meta=meta)
    self.assertEqual('blue', titan_file.meta.color)
    changeset.finalize_associated_files()
    final_changeset = self.vcs.commit(changeset)
    self.assertEqual(2, final_changeset.num)
    self.assertEqual('aaa', final_changeset.namespace)

    # Changeset 2, 'bbb' namespace (some filenames overlap 'aaa' files).
    changeset = self.vcs.new_staging_changeset(namespace='bbb')
    self.assertEqual(1, changeset.num)
    files.File('/foo', changeset=changeset, namespace='bbb').write('foo-bbb')
    files.File('/qux', changeset=changeset, namespace='bbb').write('qux-bbb')
    changeset.finalize_associated_files()
    final_changeset = self.vcs.commit(changeset)
    self.assertEqual(2, final_changeset.num)
    self.assertEqual('bbb', final_changeset.namespace)
    self.assertEqual(
        'bbb', self.vcs.get_last_submitted_changeset(namespace='bbb').namespace)

    # Changeset 4, 'aaa' namespace.
    changeset = self.vcs.new_staging_changeset(namespace='aaa')
    self.assertEqual(3, changeset.num)
    self.assertEqual(2, changeset.base_changeset.num)
    files.File('/foo', changeset=changeset, namespace='aaa').write('newfoo-aaa')
    titan_file = files.File(
        '/bar', changeset=changeset, namespace='aaa').delete()
    # While we're here, verify that a marked-for-delete file (whose content and
    # meta have been nullified) is restored correctly if undeleted.
    titan_file = files.File(
        '/bar', changeset=changeset, namespace='aaa', _allow_deleted_files=True)
    self.assertRaises(AttributeError, lambda: titan_file.meta.color)
    # This should restore the file's existence, but the metadata should NOT
    # be present since it's a brand new file after deletion.
    titan_file.write('bar-restored-aaa')
    self.assertEqual('bar-restored-aaa', titan_file.content)
    self.assertRaises(AttributeError, lambda: titan_file.meta.color)
    # However, if the file is reverted THEN written, metadata should restore.
    changeset.revert_file(titan_file)
    titan_file.write(meta={'foo': 'foo'})
    self.assertEqual('bar-aaa', titan_file.content)
    self.assertEqual('blue', titan_file.meta.color)
    # Delete it again, since that's the state we actually want.
    files.File('/bar', changeset=changeset, namespace='aaa').delete()
    changeset.finalize_associated_files()
    final_changeset = self.vcs.commit(changeset)
    self.assertEqual(4, final_changeset.num)
    self.assertEqual('aaa', final_changeset.namespace)

    # Cannot revert a file in a submitted changeset.
    changeset = versions.Changeset(4, namespace='aaa')
    titan_file = files.File('/foo', changeset=changeset, namespace='aaa')
    self.assertRaises(
        versions.ChangesetError, lambda: changeset.revert_file(titan_file))

    # File namespaces must match their associated changeset namespace.
    self.assertRaises(
        versions.NamespaceMismatchError,
        lambda: files.File('/foo', changeset=changeset))
    self.assertRaises(
        versions.NamespaceMismatchError,
        lambda: files.File('/foo', changeset=changeset, namespace='bbb'))

if __name__ == '__main__':
  basetest.main()
