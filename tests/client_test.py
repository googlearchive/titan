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

import cStringIO
import urlparse
import mox
from google.appengine.tools import appengine_rpc
from tests import appengine_rpc_test_util
from titan.common.lib.google.apputils import basetest
from tests import webapp_testing
from titan.files import client
from titan.files import files
from titan.files import handlers

SERVER = 'testserver'
AUTH_FUNCTION = lambda: ('testuser', 'testpass')
USERAGENT = 'useragent'
SOURCE = 'sourcename'

class ClientTest(testing.BaseTestCase):

  def setUp(self):
    super(ClientTest, self).setUp()
    self.titan = TitanClientStub(SERVER, AUTH_FUNCTION, USERAGENT, SOURCE)
    self.file_path = '/foo/bar.html'

  def testExists(self):
    self.assertFalse(self.titan.Exists(self.file_path))
    files.Touch(self.file_path)
    self.assertTrue(self.titan.Exists(self.file_path))

  def testRead(self):
    file_content = 'foobar'
    files.Write(self.file_path, file_content)
    self.assertEqual(file_content, self.titan.Read(self.file_path))

    # Error handling.
    self.assertRaises(client.BadFileError, self.titan.Read, '/fake/file')

  def testWrite(self):
    self.assertFalse(files.Exists(self.file_path))
    file_content = 'foobar'
    self.titan.Write(self.file_path, file_content)
    self.assertEqual(file_content, files.Read(self.file_path))

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
    self.titan.Delete(self.file_path)
    self.assertFalse(files.Exists(self.file_path))

    # Error handling.
    self.assertRaises(client.BadFileError, self.titan.Delete, '/fake/file')

  def testTouch(self):
    self.assertFalse(files.Exists(self.file_path))
    self.titan.Touch(self.file_path)
    self.assertTrue(files.Exists(self.file_path))

  def testGet(self):
    # Get single.
    files.Touch(self.file_path)
    expected = files.Get(self.file_path).Serialize(full=True)
    file_obj = self.titan.Get(self.file_path, full=True)
    self.assertDictEqual(expected, file_obj)

    # Get multiple.
    expected = files.Get([self.file_path])
    expected = [file_obj.Serialize() for file_obj in expected]
    file_objs = self.titan.Get([self.file_path])
    self.assertListEqual(expected, file_objs)

    # Error handling.
    self.assertRaises(client.BadFileError, self.titan.Get, '/fake/file')

  def testListFiles(self):
    files.Touch('/foo/bar.txt')
    files.Touch('/foo/bar/qux.txt')
    expected = files.ListFiles('/')
    self.assertSameElements(expected, self.titan.ListFiles('/'))

  def testListDir(self):
    files.Touch('/foo/bar.txt')
    files.Touch('/foo/bar/qux.txt')
    expected = files.ListDir('/')
    self.assertEqual(expected, self.titan.ListDir('/'))

  def testDirExists(self):
    files.Touch('/foo/bar/qux.txt')
    expected = files.DirExists('/foo/bar')
    self.assertEqual(expected, self.titan.DirExists('/foo/bar'))

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

class TitanClientStub(appengine_rpc_test_util.TestRpcServer,
                      client.TitanClient):
  """Mocks out RPC openers for Titan."""

  def __init__(self, *args, **kwargs):
    super(client.TitanClient, self).__init__(*args, **kwargs)
    self.handlers = dict(handlers.URL_MAP)  # {'/path': Handler}
    for url_path in self.handlers:
      self.opener.AddResponse(
          'https://%s%s' % (args[0], url_path), self.HandleRequest)

  def ValidateClientAuth(self, test=False):
    # Mocked out entirely, but testable by calling with test=True.
    if test:
      super(TitanClientStub, self).ValidateClientAuth()

  def HandleRequest(self, request):
    """Handles Titan requests by passing to the appropriate webapp handler.

    Args:
      request: A urllib2.Request object.
    Returns:
      A appengine_rpc_test_util.TestRpcServer.MockResponse object.
    """
    url = urlparse.urlparse(request.get_full_url())
    environ = webapp_testing.WebAppTestCase.GetDefaultEnvironment()
    method = request.get_method()

    if method == 'GET':
      environ['QUERY_STRING'] = url.query
    elif method == 'POST':
      environ['REQUEST_METHOD'] = 'POST'
      post_data = request.get_data()
      environ['wsgi.input'] = cStringIO.StringIO(post_data)
      environ['CONTENT_LENGTH'] = len(post_data)

    handler_class = self.handlers.get(url.path)
    if not handler_class:
      return self.MockResponse('Not found: %s' % url.path, code=404)

    handler = webapp_testing.WebAppTestCase.CreateRequestHandler(
        handler_factory=handler_class, env=environ)
    if method == 'GET':
      handler.get()
    elif method == 'POST':
      handler.post()
    else:
      raise NotImplementedError('%s method is not supported' % method)
    result = self.MockResponse(handler.response.out.getvalue(),
                               code=handler.response.status,
                               headers=handler.response.headers)
    return result

if __name__ == '__main__':
  basetest.main()
