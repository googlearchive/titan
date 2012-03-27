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

"""Tests for path_limiting.py."""

from tests.common import testing

import re
from titan.common.lib.google.apputils import basetest
from titan.common import hooks
from titan.files import files

class MockHookError(Exception):
  pass

class PathLimitingTest(testing.ServicesTestCase):

  def testPathLimitingService(self):

    # Set up a mock hook.
    class MockHook(hooks.Hook):

      def Pre(self, **kwargs):
        raise MockHookError()

    # Enable path limiting.
    services = (
        'titan.services.path_limiting',
    )
    self.EnableServices(services)
    path_limits_config = {
        'services_whitelist': {
            'errors-service': re.compile(r'^/affected/path/.*'),
        },
    }
    self.SetServiceConfig('titan-path-limiting', path_limits_config)

    # Register a service that always raises MockHookError if enabled.
    hooks.RegisterHook('errors-service', 'file-exists', hook_class=MockHook)
    hooks.RegisterHook('errors-service', 'file-get', hook_class=MockHook)
    hooks.RegisterHook('errors-service', 'file-write', hook_class=MockHook)
    hooks.RegisterHook('errors-service', 'file-delete', hook_class=MockHook)
    hooks.RegisterHook('errors-service', 'file-touch', hook_class=MockHook)
    hooks.RegisterHook('errors-service', 'file-copy', hook_class=MockHook)
    hooks.RegisterHook('errors-service', 'copy-dir', hook_class=MockHook)
    hooks.RegisterHook('errors-service', 'list-files', hook_class=MockHook)
    hooks.RegisterHook('errors-service', 'list-dir', hook_class=MockHook)
    hooks.RegisterHook('errors-service', 'dir-exists', hook_class=MockHook)

    # files.Exists().
    files.Exists('/unaffected/path/foo')
    self.assertRaises(MockHookError, files.Exists, '/affected/path/foo')

    # files.Get().
    files.Get('/unaffected/path/foo')
    self.assertRaises(MockHookError, files.Get, '/affected/path/foo')
    self.assertRaises(MockHookError, files.Get, ['/affected/path/foo',
                                                 '/unaffected/path/foo'])

    # files.Write().
    files.Write('/unaffected/path/foo', 'foo')
    self.assertRaises(MockHookError, files.Write, '/affected/path/foo', 'bar')

    # files.Delete().
    files.Delete('/unaffected/path/foo')
    self.assertRaises(MockHookError, files.Delete, '/affected/path/foo')
    self.assertRaises(MockHookError, files.Delete, ['/affected/path/foo',
                                                    '/unaffected/path/foo'])

    # files.Touch().
    files.Touch('/unaffected/path/foo')
    self.assertRaises(MockHookError, files.Touch, '/affected/path/foo')
    self.assertRaises(MockHookError, files.Touch, ['/affected/path/foo',
                                                   '/unaffected/path/foo'])

    # files.Copy().
    files.Copy('/unaffected/path/foo', '/other/unaffected/path/bar')
    self.assertRaises(MockHookError, files.Copy, '/affected/path/foo', '/')
    self.assertRaises(MockHookError, files.Copy, '/foo', '/affected/path/')

    # files.CopyDir().
    files.CopyDir('/unaffected/path/foo', '/other/unaffected/path')
    self.assertRaises(MockHookError, files.CopyDir, '/affected/path/foo',
                      '/unaffected/path/foo')

    # files.ListFiles().
    files.ListFiles('/unaffected/path/foo')
    self.assertRaises(MockHookError, files.ListFiles, '/affected/path/foo')

    # files.ListDir().
    files.ListFiles('/unaffected/path/foo')
    self.assertRaises(MockHookError, files.ListDir, '/affected/path/foo')

    # files.DirExists().
    files.DirExists('/unaffected/path/foo')
    self.assertRaises(MockHookError, files.DirExists, '/affected/path/foo')

if __name__ == '__main__':
  basetest.main()
