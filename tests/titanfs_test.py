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

"""Tests for titanfs.py."""

from tests import testing

import os
import mox
import gflags as flags
from titan.common.lib.google.apputils import basetest
from tests import client_test
from titan.files import files
from titan.utils import titanfs

FLAGS = flags.FLAGS

class TitanFilesystemCommandsTest(testing.BaseTestCase):

  def setUp(self):
    super(TitanFilesystemCommandsTest, self).setUp()
    self.commands = titanfs.TitanFilesystemCommands()
    self.commands.flags = {
        'host': 'localhost',
        'port': 80,
        'secure': False,
        'sync_dir': [FLAGS.test_tmpdir],
        'target_path': '/',
        'username': 'titanuser@example.com',
    }
    self.titan_client_stub = client_test.TitanClientStub(
        'testserver', lambda: ('testuser', 'testpass'), 'useragent', 'source')
    self.stubs.SmartSet(self.commands, 'titan_rpc_client',
                        self.titan_client_stub)

  def testExcludedFilenamesRegex(self):
    regex = titanfs.EXCLUDED_FILENAMES_REGEX
    self.assertTrue(regex.match('foo~'))
    self.assertFalse(regex.match('foo1234567890)(*&^%$#@ !'))

    self.assertTrue(regex.match('.foo.swp'))
    self.assertTrue(regex.match('.foo.swx'))
    self.assertTrue(regex.match('.foo.swpx'))
    self.assertFalse(regex.match('.swp'))

    self.assertTrue(regex.match('4913'))
    self.assertTrue(regex.match('4999'))
    self.assertFalse(regex.match('4912'))

    self.assertTrue(regex.match('.#foo'))
    self.assertFalse(regex.match('.foo'))

  def testLocalFileEventHandlerHelpers(self):
    handler = titanfs.LocalFileEventHandler(
        base_dir='/local/', target_path='/remote', titan_rpc_client=None)

    event_mock = mox.MockObject(titanfs.pyinotify.ProcessEvent)
    event_mock.name = 'filename'
    event_mock.path = '/local/foo/bar'
    event_mock.pathname = '/local/foo/bar/filename'

    # Test _GetRemoteTarget.
    target = handler._GetRemoteTarget(event_mock)
    self.assertEqual('/remote/foo/bar/filename', target)

    # Test _GetFilenameAndTarget.
    filename, target = handler._GetFilenameAndTarget(event_mock)
    self.assertEqual('/local/foo/bar/filename', filename)
    self.assertEqual('/remote/foo/bar/filename', target)

  def testStart(self):
    # Verify code execution, but the processing logic is unit tested below.
    notifier_mock = self.mox.CreateMockAnything()
    notifier_mock.loop()

    self.mox.StubOutWithMock(titanfs.pyinotify, 'Notifier')
    notifier = titanfs.pyinotify.Notifier(mox.IgnoreArg(), mox.IgnoreArg())
    notifier.AndReturn(notifier_mock)

    self.mox.ReplayAll()
    self.commands.Start()
    self.mox.VerifyAll()

  def testProcessing(self):
    handler = titanfs.LocalFileEventHandler(
        base_dir=FLAGS.test_tmpdir, target_path='/',
        titan_rpc_client=self.titan_client_stub)
    event_mock = mox.MockObject(titanfs.pyinotify.ProcessEvent)
    event_mock.dir = False
    event_mock.name = 'foo'
    event_mock.path = FLAGS.test_tmpdir
    event_mock.pathname = os.path.join(FLAGS.test_tmpdir, 'foo')

    with open(event_mock.pathname, 'w') as fp:
      fp.write('Test')

    # Test process_IN_CREATE.
    handler.process_IN_CREATE(event_mock)
    self.assertEqual('Test', files.Read('/foo'))

    # Test process_IN_MODIFY.
    with open(event_mock.pathname, 'w') as fp:
      fp.write('New content')
    handler.process_IN_MODIFY(event_mock)
    self.assertEqual('New content', files.Read('/foo'))

    # Test process_IN_DELETE.
    handler.process_IN_DELETE(event_mock)
    self.assertFalse(files.Exists('/foo'))

    # Test process_IN_MOVED_FROM.
    files.Touch('/foo')
    handler.process_IN_MOVED_FROM(event_mock)
    self.assertFalse(files.Exists('/foo'))

    # Test process_IN_MOVED_TO.
    handler.process_IN_MOVED_TO(event_mock)
    self.assertTrue(files.Exists('/foo'))

    # Test process_UNMOUNT (no logic, just execution).
    self.assertEqual(None, handler.process_UNMOUNT(event_mock))

if __name__ == '__main__':
  basetest.main()
