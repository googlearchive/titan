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

from tests import testing

import time
import mox
from titan.common.lib.google.apputils import basetest
from titan.files import files

class ExpirationsTest(testing.ServicesTestCase):

  def setUp(self):
    super(ExpirationsTest, self).setUp()
    self.mox = mox.Mox()
    services = (
        'titan.services.expirations',
    )
    self.EnableServices(services)

  def tearDown(self):
    super(ExpirationsTest, self).tearDown()
    self.mox.UnsetStubs()
    self.mox.ResetAll()

  def testWriteAndGet(self):
    self.mox.StubOutWithMock(time, 'time')
    time.time().AndReturn(1000)
    time.time().AndReturn(1001)
    time.time().AndReturn(1002)
    self.mox.ReplayAll()

    files.Write('/foo/bar.txt', 'hello!', expires=1)  # time=1000

    # First attempt, hasn't expired yet.
    titan_file = files.Get('/foo/bar.txt')  # time=1001
    self.assertEqual(1001, titan_file.expires)

    # Second attempt, expired.
    self.assertIsNone(files.Get('/foo/bar.txt'))  # time=1002

    self.mox.VerifyAll()

  def testMultipleGet(self):
    self.mox.StubOutWithMock(time, 'time')
    time.time().AndReturn(1000)
    time.time().AndReturn(1001)
    time.time().AndReturn(1002)
    self.mox.ReplayAll()

    files.Touch('/foo/bar.txt')
    files.Touch('/foo/baz.txt', expires=1)  # time=1000

    self.assertSequenceEqual(
        ['/foo/bar.txt', '/foo/baz.txt'],  # time=1002
        files.Get(['/foo/bar.txt', '/foo/baz.txt']).keys())  # time=1001

    self.assertSequenceEqual(
        ['/foo/bar.txt'],
        files.Get(['/foo/bar.txt', '/foo/baz.txt']).keys())  # time=1002

    self.mox.VerifyAll()

  def testTouch(self):
    self.mox.StubOutWithMock(time, 'time')
    time.time().AndReturn(1000)
    time.time().AndReturn(1001)
    time.time().AndReturn(1002)
    self.mox.ReplayAll()

    files.Touch('/foo/bar.txt', expires=1)  # time=1000

    # First attempt, hasn't expired yet.
    titan_file = files.Get('/foo/bar.txt')  # time=1001
    self.assertEqual(1001, titan_file.expires)

    # Second attempt, expired.
    self.assertIsNone(files.Get('/foo/bar.txt'))  # time=1002

    self.mox.VerifyAll()

  def testListFiles(self):
    self.mox.StubOutWithMock(time, 'time')
    time.time().AndReturn(1000)
    time.time().AndReturn(1001)
    time.time().AndReturn(1002)
    self.mox.ReplayAll()

    files.Touch('/foo/bar.txt')
    files.Touch('/foo/baz.txt')
    files.Touch('/foo/qux.txt', expires=1)  # time=1000
    expected = ['/foo/bar.txt', '/foo/baz.txt', '/foo/qux.txt']
    self.assertSequenceEqual(
        expected,
        [fp.path for fp in files.ListFiles('/foo')])  # time=1001

    expected = ['/foo/bar.txt', '/foo/baz.txt']
    self.assertSequenceEqual(
        expected,
        [fp.path for fp in files.ListFiles('/foo')])  # time=1002

    self.assertFalse(files.Exists('/foo/qux.txt'))

    self.mox.VerifyAll()

  def testListDir(self):
    self.mox.StubOutWithMock(time, 'time')
    time.time().AndReturn(1000)
    time.time().AndReturn(1001)
    time.time().AndReturn(1002)
    self.mox.ReplayAll()

    files.Touch('/foo/bar/baz.txt')
    files.Touch('/foo/qux.txt', expires=1)  # time=1000
    expected = [['bar'], ['/foo/qux.txt']]
    dirs, file_objs = files.ListDir('/foo')  # time=1001
    file_paths = [fp.path for fp in file_objs]
    self.assertSequenceEqual(expected, [dirs, file_paths])

    expected = [['bar'], []]
    dirs, file_objs = files.ListDir('/foo')  # time=1002
    file_paths = [fp.path for fp in file_objs]
    self.assertSequenceEqual(expected, [dirs, file_paths])

    self.mox.VerifyAll()

if __name__ == '__main__':
  basetest.main()
