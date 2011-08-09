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

"""Tests for utils/titan_client.py."""

import getpass
import inspect
import os
import random
import shutil
import sys
import threading
import mox
import gflags as flags
from titan.common.lib.google.apputils import basetest
from tests import testing
from tests import client_test
from titan.files import files
from titan.utils import titan_client

FLAGS = flags.FLAGS
TEST_TMPDIR = os.path.join(FLAGS.test_tmpdir, 'testdata')

class TitanCommandsTest(testing.BaseTestCase):

  def setUp(self):
    super(TitanCommandsTest, self).setUp()
    self.commands = titan_client.TitanCommands()
    self.commands.flags = {
        'host': 'localhost',
        'port': 80,
        'username': 'titanuser',
        'num_threads': 1,
        'secure': False,
    }
    self.flags = self.commands.flags.copy()
    titan_client_stub = client_test.TitanClientStub(
        'testserver', lambda: ('testuser', 'testpass'), 'useragent', 'source')
    self.stubs.SmartSet(self.commands, 'titan_rpc_client', titan_client_stub)
    if not os.path.exists(TEST_TMPDIR):
      # Make some temporary testing directories.
      os.mkdir(TEST_TMPDIR)
      os.mkdir(os.path.join(TEST_TMPDIR, 'single'))
      os.mkdir(os.path.join(TEST_TMPDIR, 'multiple'))
      os.mkdir(os.path.join(TEST_TMPDIR, 'dir'))

  def tearDown(self):
    super(TitanCommandsTest, self).tearDown()
    if os.path.exists(TEST_TMPDIR):
      shutil.rmtree(TEST_TMPDIR)

  def testHost(self):
    """Verifies the host property."""
    self.commands.flags = {
        'host': 'foo.google.com',
        'port': 8080,
    }
    self.assertEqual('foo.google.com:8080', self.commands.host)

    self.commands.flags = {
        'host': 'foobar.google.com',
        'port': 80,
    }
    self.assertEqual('foobar.google.com', self.commands.host)

  def testHelp(self):
    """Verifies the help command."""
    self.commands.args = []
    self.commands.flags = {}

    # Verify that all the public commands are printed.
    helpdoc = self.commands.Help()
    self.assertIn('download:', helpdoc)
    self.assertIn('help:', helpdoc)
    self.assertIn('upload:', helpdoc)

    # Verify command-specific helpdoc.
    self.commands.args = ['upload']
    self.assertEqual(inspect.getdoc(self.commands.Upload),
                     self.commands.Help())

    # Verify CommandValueError is raised for unknown command.
    self.commands.args = ['unknown_command']
    self.assertRaises(titan_client.CommandValueError, self.commands.Help)

  def testUpload(self):
    """Verifies upload of files."""
    # Initialize test files.
    file1 = os.path.join(TEST_TMPDIR, 'foo.txt')
    with open(file1, 'w') as fp:
      fp.write('foo')

    file2 = os.path.join(TEST_TMPDIR, 'bar.txt')
    with open(file2, 'w') as fp:
      fp.write('bar')

    # Verify checks for required flags.
    self.commands.args = []
    self.commands.flags = {}
    self.assertRaises(titan_client.CommandValueError, self.commands.Upload)

    # Verify no files to upload raises an exception.
    self.commands.args = []
    self.commands.flags = {'target_path': '/'}
    self.assertRaises(titan_client.CommandValueError, self.commands.Upload)

    # Verify single file upload.
    args = [file1]
    self.flags['target_path'] = '/tests/single'
    self.assertFalse(files.Exists('/tests/single/foo.txt'))
    self.commands.RunCommand('upload', args=args, flags=self.flags)
    self.assertTrue(files.Exists('/tests/single/foo.txt'))
    self.assertEqual('foo', files.Read('/tests/single/foo.txt'))

    # Verify multiple file upload.
    args = [file1, file2]
    self.flags['target_path'] = '/tests/multiple'
    self.assertFalse(files.Exists('/tests/multiple/foo.txt'))
    self.assertFalse(files.Exists('/tests/multiple/bar.txt'))
    self.commands.RunCommand('upload', args=args, flags=self.flags)
    self.assertTrue(files.Exists('/tests/multiple/foo.txt'))
    self.assertTrue(files.Exists('/tests/multiple/bar.txt'))

    # Verify directory without --recursive flag raises exception.
    args = [TEST_TMPDIR]
    self.flags['target_path'] = '/tests/dir'
    self.commands.args = args
    self.commands.flags = self.flags
    self.assertRaises(titan_client.CommandValueError, self.commands.Upload)

    # Verify directory upload (recursive).
    self.flags['recursive'] = True
    self.assertFalse(files.Exists('/tests/dir/testdata/foo.txt'))
    self.assertFalse(files.Exists('/tests/dir/testdata/bar.txt'))
    self.commands.RunCommand('upload', args=args, flags=self.flags)
    self.assertTrue(files.Exists('/tests/dir/testdata/foo.txt'))
    self.assertTrue(files.Exists('/tests/dir/testdata/bar.txt'))

  def testUploadTty(self):
    """Verifies upload confirmation flow from a TTY terminal."""
    # Initialize test files.
    test_file_path = os.path.join(TEST_TMPDIR, 'foo.txt')
    with open(test_file_path, 'w') as fp:
      fp.write('foo')

    # Set up mocks.
    mock_stdin = self.mox.CreateMockAnything()
    self.stubs.SmartSet(sys, 'stdin', mock_stdin)
    self.mox.StubOutWithMock(self.commands, '_ConfirmAction')
    self.mox.StubOutWithMock(getpass, 'getpass')

    # Set up expectations.
    mock_stdin.isatty().MultipleTimes().AndReturn(True)
    getpass.getpass().AndReturn('testpassword')
    # First try, user replies 'no'.
    self.commands._ConfirmAction(mox.IsA(str)).AndReturn(False)
    # Second try, user replies 'yes'.
    self.commands._ConfirmAction(mox.IsA(str)).AndReturn(True)
    self.mox.ReplayAll()

    remote_path = '/tests/upload/foo.txt'
    args = [test_file_path]
    self.flags['target_path'] = '/tests/upload'
    self.commands.args = args
    self.commands.flags = self.flags

    # Verify exception is raised when user replies 'no' to confirmation.
    self.assertFalse(files.Exists(remote_path))
    self.assertRaises(titan_client.CommandValueError, self.commands.Upload)
    self.assertFalse(files.Exists(remote_path))

    # Verify upload when user replies 'yes' to confirmation.
    self.commands.Upload()
    self.assertTrue(files.Exists(remote_path))

    # Verify mock conditions were met.
    self.mox.VerifyAll()

  def testDownload(self):
    """Verifies download of files."""
    files.Write('/foo/bar.txt', 'hello world')
    files.Write('/foo/baz.txt', 'foo bar')
    self.assertTrue(files.Exists('/foo/bar.txt'))

    # Verify no files to download raises and exception.
    self.flags['file_path'] = []
    self.flags['dir_path'] = []
    args = ['.']
    self.commands.args = args
    self.commands.flags = self.flags
    self.assertRaises(titan_client.CommandValueError, self.commands.Download)

    # Verify invalid target paths raises an exception.
    bad_target_dir = os.path.join(TEST_TMPDIR, 'afile.txt')
    open(bad_target_dir, 'w').close()
    self.assertTrue(os.path.isfile(bad_target_dir))
    self.flags['file_path'] = []
    self.flags['dir_path'] = ['/']
    args = [bad_target_dir]
    self.commands.args = args
    self.commands.flags = self.flags
    self.assertRaises(titan_client.CommandValueError, self.commands.Download)

    # Verify download of non-existent files raise an error.
    bad_file_path = '/foo/bar/baz.txt'
    self.assertFalse(files.Exists(bad_file_path))
    self.flags['file_path'] = [bad_file_path]
    self.flags['dir_path'] = []
    self.commands.args = ['.']
    self.commands.flags = self.flags
    self.assertRaises(titan_client.CommandValueError, self.commands.Download)

    # Verify download of non-existent dirs raise an error.
    bad_dir_path = '/foo/bar/baz/'
    self.assertFalse(files.DirExists(bad_dir_path))
    self.flags['file_path'] = []
    self.flags['dir_path'] = [bad_dir_path]
    self.assertRaises(titan_client.CommandValueError, self.commands.Download)

    # Verify single file download.
    target_dir = os.path.join(TEST_TMPDIR, 'single')
    self.flags['file_path'] = ['/foo/bar.txt']
    self.flags['dir_path'] = []
    args = [target_dir]
    local_path = os.path.join(target_dir, 'bar.txt')
    self.assertFalse(os.path.exists(local_path))
    self.commands.RunCommand('download', args=args, flags=self.flags)
    self.assertTrue(os.path.exists(local_path))
    self.assertEqual('hello world', open(local_path).read())

    # Verify multiple file download.
    target_dir = os.path.join(TEST_TMPDIR, 'multiple')
    self.flags['file_path'] = ['/foo/bar.txt', '/foo/baz.txt']
    self.flags['dir_path'] = []
    args = [target_dir]
    path1 = os.path.join(target_dir, 'bar.txt')
    path2 = os.path.join(target_dir, 'baz.txt')
    self.assertFalse(os.path.exists(path1))
    self.assertFalse(os.path.exists(path2))
    self.commands.RunCommand('download', args=args, flags=self.flags)
    self.assertTrue(os.path.exists(path1))
    self.assertTrue(os.path.exists(path2))

    # Verify directory download.
    target_dir = os.path.join(TEST_TMPDIR, 'dir')
    self.flags['file_path'] = []
    self.flags['dir_path'] = ['/foo/']
    args = [target_dir]
    path1 = os.path.join(target_dir, 'foo', 'bar.txt')
    path2 = os.path.join(target_dir, 'foo', 'baz.txt')
    self.assertFalse(os.path.exists(path1))
    self.assertFalse(os.path.exists(path2))
    self.commands.RunCommand('download', args=args, flags=self.flags)
    self.assertTrue(os.path.exists(path1))
    self.assertTrue(os.path.exists(path2))

  def testDownloadTty(self):
    """Verifies download confirmation flow from a TTY terminal."""
    # Initialize files.
    files.Touch('/foo/bar.txt')
    self.assertTrue(files.Exists('/foo/bar.txt'))

    # Set up mocks.
    mock_stdin = self.mox.CreateMockAnything()
    self.stubs.SmartSet(sys, 'stdin', mock_stdin)
    self.mox.StubOutWithMock(self.commands, '_ConfirmAction')
    self.mox.StubOutWithMock(getpass, 'getpass')

    # Set up expectations.
    getpass.getpass().AndReturn('testpassword')
    mock_stdin.isatty().MultipleTimes().AndReturn(True)
    # First try, user replies 'no'.
    self.commands._ConfirmAction(mox.IsA(str)).AndReturn(False)
    self.commands._ConfirmAction(mox.IsA(str)).AndReturn(True)
    self.mox.ReplayAll()

    target_path = os.path.join(TEST_TMPDIR, 'bar.txt')
    args = [TEST_TMPDIR]
    self.flags['file_path'] = ['/foo/bar.txt']
    self.flags['dir_path'] = []
    self.commands.args = args
    self.commands.flags = self.flags

    # Verify exception is raised when user replies 'no' to confirmation.
    self.assertFalse(os.path.exists(target_path))
    self.assertRaises(titan_client.CommandValueError, self.commands.Download)
    self.assertFalse(os.path.exists(target_path))

    # Verify upload when user replies 'yes' to confirmation.
    self.commands.Download()
    self.assertTrue(os.path.exists(target_path))

    # Verify mock conditions were met.
    self.mox.VerifyAll()

class ArgumentParserTest(testing.MockableTestCase):

  def setUp(self):
    super(ArgumentParserTest, self).setUp()
    self.parser = titan_client.ArgumentParser()

  def testParseArgs(self):
    """Verify args and flags are parsed from command-line args."""
    mock_args = ['titan_client.py', 'arg1', '--flag1=value1', 'arg2',
                 '--boolflag', '-s', 'shortflag']
    self.stubs.SmartSet(sys, 'argv', mock_args)
    self.parser.AddOption('--flag1')
    self.parser.AddOption('--boolflag', action='store_true')
    self.parser.AddOption('-s', '--short')
    args, options = self.parser.ParseArgs()
    self.assertSequenceEqual(['arg1', 'arg2'], args)
    self.assertDictEqual({
        'flag1': 'value1',
        'boolflag': True,
        'short': 'shortflag',
    }, options)

class TitanTest(testing.MockableTestCase):

  def testMakeDirs(self):
    """Verify that MakeDirs works correctly, including thread-safety."""
    # Test single-threaded creation of dirs
    dir_path = os.path.join(TEST_TMPDIR, 'temp', 'path')
    titan_client.MakeDirs(dir_path)
    self.assertTrue(os.path.isdir(dir_path), 'Not a directory: %s' % dir_path)

    # Test multi-threaded creation of dirs.
    # Race condition check: if many threads are racing to create a deep
    # directory, they may attempt to mkdir the same directory on the way down,
    # raising an OSError and not letting one thread finish.

    # Need a deep directory root, make: scratch_dir/0/1/2/3/4/5/6/7/8/9
    deep_dir_root = os.path.join(TEST_TMPDIR, *[str(i) for i in range(10)])
    all_final_dirs = []

    def MakeDeepDir():
      # Put a random directory at the end, so we can verify that each
      # full path to each unique directory was created.
      final_deep_dir = os.path.join(deep_dir_root, str(random.randint(1, 100)))
      all_final_dirs.append(final_deep_dir)
      titan_client.MakeDirs(final_deep_dir)

    # Start 10 threads racing to creating a unique dir with the common sub path.
    threads = [threading.Thread(target=MakeDeepDir) for _ in range(10)]
    for thread in threads:
      thread.start()
    for thread in threads:
      thread.join()

    # Verify that each unique final dir was created.
    created_dirs = [path for path in all_final_dirs if os.path.isdir(path)]
    # If this fails, then MakeDirs may not be thread-safe anymore.
    self.assertSameElements(all_final_dirs, created_dirs)

  def testGetDefaultParser(self):
    """Verify GetDefaultParser() returns a ArgumentParser object."""
    parser = titan_client.GetDefaultParser()
    self.assertTrue(isinstance(parser, titan_client.ArgumentParser))

  def testRunCommandLine(self):
    """Verifies args are parsed and the registered function is called."""
    mock_args = ['titan_client.py', 'foobar', 'arg1', 'arg2', '--flag1', 'val1']
    self.stubs.SmartSet(sys, 'argv', mock_args)
    commands = titan_client.TitanCommands()
    parser = titan_client.GetDefaultParser()
    parser.AddOption('--flag1')

    # Register a mock function for command 'foobar' and verify that it ran.
    mock_obj = self.mox.CreateMockAnything()
    mock_obj.Foo().AndReturn('success!')
    self.mox.ReplayAll()

    commands.RegisterCommand('foobar', mock_obj.Foo)
    results = titan_client.RunCommandLine(commands, parser)
    self.assertEqual('success!', results)
    self.mox.VerifyAll()

if __name__ == '__main__':
  # Cleanup previous test temp directories.
  if os.path.exists(TEST_TMPDIR):
    shutil.rmtree(TEST_TMPDIR)
  basetest.main()
