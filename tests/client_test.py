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

"""Tests for Titan client."""

from tests import testing

import mox
from google.appengine.tools import appengine_rpc
from titan.common.lib.google.apputils import basetest
from tests import testing
from titan.files import client
from titan.files import files

SERVER = 'testserver'
AUTH_FUNCTION = lambda: ('testuser', 'testpass')
USERAGENT = 'useragent'
SOURCE = 'sourcename'

class ClientTest(testing.BaseTestCase):

  def setUp(self):
    super(ClientTest, self).setUp()
    self.titan = testing.TitanClientStub(
        SERVER, AUTH_FUNCTION, USERAGENT, SOURCE)
    self.file_path = '/foo/bar.html'

  def testExists(self):
    self.assertFalse(self.titan.Exists(self.file_path))
    files.Touch(self.file_path)
    self.assertTrue(self.titan.Exists(self.file_path,
                                      fake_arg_stripped_by_validators=False))

  def testRead(self):
    file_content = 'foobar'
    files.Write(self.file_path, file_content)
    self.assertEqual(file_content, self.titan.Read(self.file_path))

    # Error handling.
    self.assertRaises(client.BadFileError, self.titan.Read, '/fake/file')

  def testWrite(self):
    self.assertFalse(files.Exists(self.file_path))
    file_content = 'foobar'
    self.titan.Write(self.file_path, file_content,
                     fake_arg_stripped_by_validators=False)
    self.assertEqual(file_content, files.Get(self.file_path).content)

    # Verify meta properties are set.
    self.titan.Write(self.file_path, 'foobar v2', meta={'foo': 'bar'})
    file_obj = files.Get(self.file_path)
    self.assertEqual('foobar v2', file_obj.content)
    self.assertEqual('bar', file_obj.foo)

    # Verify mime type can be overridden.
    self.titan.Write(self.file_path, 'foobar v3', mime_type='text/w00t')
    file_obj = files.Get(self.file_path)
    self.assertEqual('foobar v3', file_obj.content)
    self.assertEqual('text/w00t', file_obj.mime_type)

    # Blob handling.
    blob_key = 'ablobkey'
    self.titan.Write(self.file_path, blobs=[blob_key])
    self.assertEqual(blob_key, str(self.titan.Get(self.file_path)['blobs'][0]))

    # Error handling.
    self.assertRaises(client.BadFileError, self.titan.Write, '/fake/file',
                      mime_type='test/mimetype')

  def testDelete(self):
    files.Touch(self.file_path)
    self.assertTrue(files.Exists(self.file_path))
    self.titan.Delete(self.file_path,
                      fake_arg_stripped_by_validators=False)
    self.assertFalse(files.Exists(self.file_path))

    # Error handling.
    self.assertRaises(client.BadFileError, self.titan.Delete, '/fake/file')

  def testTouch(self):
    self.assertFalse(files.Exists(self.file_path))
    self.titan.Touch(self.file_path,
                     fake_arg_stripped_by_validators=False)
    self.assertTrue(files.Exists(self.file_path))

  def testGet(self):
    # Get single.
    files.Touch(self.file_path)
    expected = files.Get(self.file_path).Serialize(full=True)
    file_obj = self.titan.Get(self.file_path, full=True,
                              fake_arg_stripped_by_validators=False)
    self.assertDictEqual(expected, file_obj)

    # Get single non-existent.
    self.assertEqual(None, self.titan.Get('/fake/file'))

    # Get multiple.
    expected = files.Get([self.file_path, '/fake'])
    for key in expected:
      expected[key] = expected[key].Serialize()
    file_objs = self.titan.Get([self.file_path, '/fake'])
    self.assertDictEqual(expected, file_objs)

  def testListFiles(self):
    files.Touch('/foo/bar.txt')
    files.Touch('/foo/bar/qux.txt')
    expected = files.ListFiles('/')
    actual = self.titan.ListFiles('/', fake_arg_stripped_by_validators=False)
    self.assertSameElements(expected, actual)

  def testListDir(self):
    files.Touch('/foo/bar.txt')
    files.Touch('/foo/bar/qux.txt')
    expected = files.ListDir('/')
    actual = self.titan.ListDir('/', fake_arg_stripped_by_validators=False)
    self.assertEqual(expected, actual)

  def testDirExists(self):
    files.Touch('/foo/bar/qux.txt')
    expected = files.DirExists('/foo/bar')
    actual = self.titan.DirExists('/foo/bar',
                                  fake_arg_stripped_by_validators=False)
    self.assertEqual(expected, actual)

  def testValidateClientAuth(self):
    self.mox.StubOutWithMock(self.titan, '_HostIsDevAppServer')
    self.titan._HostIsDevAppServer().AndReturn(False)
    self.mox.StubOutWithMock(self.titan, '_GetAuthToken')
    client_login_error = appengine_rpc.ClientLoginError(
        url='', code=403, msg='', headers={}, args={'Error': 'error'})
    self.titan._GetAuthToken(
        mox.IgnoreArg(), mox.IgnoreArg()).AndRaise(client_login_error)
    self.mox.ReplayAll()
    self.assertRaises(client.AuthenticationError,
                      self.titan.ValidateClientAuth, test=True)
    self.mox.VerifyAll()

  def testHostIsDevAppServer(self):
    response_mock_dev = self.mox.CreateMock(client.urllib2.urlopen)
    response_mock_dev.headers = {'server': 'Development 1.0'}
    response_mock_prod = self.mox.CreateMock(client.urllib2.urlopen)
    response_mock_prod.headers = {}
    self.mox.StubOutWithMock(client.urllib2, 'urlopen')
    client.urllib2.urlopen(mox.IgnoreArg()).AndReturn(response_mock_dev)
    client.urllib2.urlopen(mox.IgnoreArg()).AndReturn(response_mock_prod)
    self.mox.ReplayAll()

    self.assertTrue(self.titan._HostIsDevAppServer())
    self.assertFalse(self.titan._HostIsDevAppServer())
    self.mox.VerifyAll()

if __name__ == '__main__':
  basetest.main()
