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

"""Tests for dirs.py."""

from tests.common import testing

import datetime
import time
from titan.common.lib.google.apputils import basetest
from titan import files
from titan.files import dirs

PATH_WRITE_ACTION = dirs.ModifiedPath.WRITE
PATH_DELETE_ACTION = dirs.ModifiedPath.DELETE

class DirManagerTest(testing.BaseTestCase):

  def tearDown(self):
    files.unregister_file_factory()
    super(DirManagerTest, self).tearDown()

  def testEndToEnd(self):
    files.register_file_mixins([dirs.DirManagerMixin])

    files.File('/a/b/foo').write('')
    files.File('/a/b/bar').write('')
    files.File('/a/d/foo').write('')

    # List root dir.
    self.assertEqual(dirs.Dirs(['/a']), dirs.Dirs.list('/'))
    # List /a.
    self.assertEqual(dirs.Dirs(['/a/b', '/a/d']), dirs.Dirs.list('/a/'))
    # List /a/b/.
    self.assertEqual(dirs.Dirs([]), dirs.Dirs.list('/a/b/'))
    # List /fake/dir.
    self.assertEqual(dirs.Dirs([]), dirs.Dirs.list('/fake/dir'))

    # Test deleting directories.
    self.assertTrue(dirs.Dir('/a/d').exists)
    files.File('/a/d/foo').delete()
    self.assertFalse(dirs.Dir('/a/d').exists)
    # List /a.
    self.assertEqual(dirs.Dirs(['/a/b']), dirs.Dirs.list('/a/'))
    self.assertEqual(dirs.Dirs(['/a']), dirs.Dirs.list('/'))
    # Delete the remaining files and list again.
    files.File('/a/b/foo').delete()
    files.File('/a/b/bar').delete()
    self.assertEqual(dirs.Dirs([]), dirs.Dirs.list('/'))

    # Verify behavior of set_meta and meta.
    self.assertRaises(
        dirs.InvalidDirectoryError,
        lambda: dirs.Dir('/a/b').set_meta({'flag': True}))
    self.assertRaises(
        dirs.InvalidMetaError,
        lambda: dirs.Dir('/a/b').set_meta({'name': 'foo'}))

    files.File('/a/b/foo').write('')
    # Also, weakly test execution path of process_windows_with_backoff.
    dir_task_consumer = dirs.DirTaskConsumer()
    dir_task_consumer.process_windows_with_backoff(runtime=2)
    titan_dir = dirs.Dir('/a/b')
    self.assertRaises(AttributeError, lambda: titan_dir.meta.flag)

    titan_dir.set_meta(meta={'flag': True})
    titan_dir = dirs.Dir('/a/b')
    self.assertTrue(titan_dir.meta.flag)

    # Verify properties.
    self.assertEqual('b', titan_dir.name)
    self.assertEqual('/a/b', titan_dir.path)

  def testModifiedPath(self):
    # Test serialize().
    now = datetime.datetime.now()
    expected = {
        'path': '/foo',
        'modified': now,
        'action': dirs._STATUS_AVAILABLE,
    }
    self.assertEqual(expected, dirs.ModifiedPath(**expected).serialize())

  def testComputeAffectedDirs(self):
    dir_service = dirs.DirService()

    # /a/b/foo is written.
    modified_path = dirs.ModifiedPath(
        '/a/b/foo', modified=0, action=PATH_WRITE_ACTION)
    affected_dirs = dir_service.compute_affected_dirs([modified_path])
    expected_affected_dirs = {
        'dirs_with_adds': set(['/a', '/a/b']),
        'dirs_with_deletes': set(),
    }
    self.assertEqual(expected_affected_dirs, affected_dirs)

    # /a/b/foo is deleted.
    modified_path = dirs.ModifiedPath(
        '/a/b/foo', modified=0, action=PATH_DELETE_ACTION)
    affected_dirs = dir_service.compute_affected_dirs([modified_path])
    expected_affected_dirs = {
        'dirs_with_adds': set(),
        'dirs_with_deletes': set(['/a', '/a/b']),
    }
    self.assertEqual(expected_affected_dirs, affected_dirs)

    # /a/b/foo is added, then deleted -- dirs should exist in only one list.
    added_path = dirs.ModifiedPath(
        '/a/b/foo', modified=123123.1, action=PATH_WRITE_ACTION)
    deleted_path = dirs.ModifiedPath(
        '/a/b/foo', modified=123123.2, action=PATH_DELETE_ACTION)
    affected_dirs = dir_service.compute_affected_dirs([added_path, deleted_path])
    expected_affected_dirs = {
        'dirs_with_adds': set(),
        'dirs_with_deletes': set(['/a', '/a/b']),
    }
    self.assertEqual(expected_affected_dirs, affected_dirs)

    # Test different file paths -- dirs should exist in both lists.
    added_path = dirs.ModifiedPath(
        '/a/b/foo', modified=123123.1, action=PATH_WRITE_ACTION)
    deleted_path = dirs.ModifiedPath(
        '/a/b/c/d/bar', modified=123123.2, action=PATH_DELETE_ACTION)
    affected_dirs = dir_service.compute_affected_dirs([added_path, deleted_path])
    expected_affected_dirs = {
        'dirs_with_adds': set(['/a', '/a/b']),
        'dirs_with_deletes': set(['/a', '/a/b', '/a/b/c', '/a/b/c/d']),
    }
    self.assertEqual(expected_affected_dirs, affected_dirs)

    # Test chronological ordering, even with out-of-order arguments.
    path1 = dirs.ModifiedPath(
        '/a/b/foo', modified=123123.0, action=PATH_DELETE_ACTION)
    path2 = dirs.ModifiedPath(
        '/a/b/foo', modified=123123.2, action=PATH_WRITE_ACTION)
    path3 = dirs.ModifiedPath(
        '/a/b/foo', modified=123123.1, action=PATH_DELETE_ACTION)
    affected_dirs = dir_service.compute_affected_dirs([path1, path2, path3])
    expected_affected_dirs = {
        'dirs_with_adds': set(['/a', '/a/b']),
        'dirs_with_deletes': set(),
    }
    self.assertEqual(expected_affected_dirs, affected_dirs)

  def testInitializeDirsFromCurrentState(self):
    # Don't register the factory before doing this (so the dirs aren't created):
    files.File('/a/b/foo').write('')
    files.File('/a/b/bar').write('')
    files.File('/a/d/foo').write('').delete()

    # Force the initializer to run twice by making batch size < len(files).
    # This does not test the respawning code path.
    self.stubs.SmartSet(dirs, 'INITIALIZER_BATCH_SIZE', 2)

    self.assertFalse(dirs.Dir('/a').exists)
    dirs.init_dirs_from_current_state()
    self.RunDeferredTasks('default')
    self.assertTrue(dirs.Dir('/a').exists)
    self.assertTrue(dirs.Dir('/a/b').exists)
    self.assertFalse(dirs.Dir('/a/d').exists)

if __name__ == '__main__':
  basetest.main()
