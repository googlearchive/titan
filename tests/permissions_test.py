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

"""Tests for permissions.py."""

from titan.common import testing

from titan.common.lib.google.apputils import basetest
from titan.files import files
from titan.services import permissions

TITAN_SERVICES = (
    'titan.services.permissions',
)

class PermissionsTest(testing.ServicesTestCase):

  @testing.DisableCaching
  def testPermissions(self):
    self.EnableServices(__name__)

    # Setting write permissions.
    perms = permissions.Permissions(write_users=['titanuser@example.com'])
    files.Write('/bar', 'Test', permissions=perms)

    # Everyone can still read, only titanuser@example.com (the environment
    # default) can write.
    self.assertEqual('Test', files.Read('/bar'))
    self.assertEqual('Test', files.Read('/bar', user='bob'))
    self.assertRaises(
        permissions.PermissionsError, files.Write, '/bar', 'Test', user='bob')
    self.assertRaises(
        permissions.PermissionsError, files.Touch, '/bar', 'Test', user='bob')
    self.assertRaises(
        permissions.PermissionsError, files.Delete, '/bar', user='bob')
    files.Write('/bar', 'New content')
    self.assertEqual('New content', files.Read('/bar'))

    # Verify users with write can also read, even if not in the read whitelist.
    perms = permissions.Permissions(write_users=['titanuser@example.com'],
                                    read_users=['bob'])
    files.Write('/bar', 'Test', permissions=perms)
    self.assertEqual('Test', files.Read('/bar'))

    # Restrict both read and write permissions.
    perms = permissions.Permissions(write_users=['titanuser@example.com'],
                                    read_users=['titanuser@example.com'])
    files.Write('/bar', 'Test', permissions=perms)
    self.assertRaises(permissions.PermissionsError, files.Read, '/bar',
                      user='bob@example.com')
    self.assertRaises(permissions.PermissionsError, files.Write, '/bar', 'Test',
                      user='bob')
    # Verify Get(), Read(), and Delete() hooks.
    self.assertEqual('Test', files.Get('/bar').content)
    self.assertEqual('Test', files.File('/bar').content)
    self.assertEqual('Test', files.Read('/bar'))
    self.assertEqual(None, files.Delete('/bar'))

    # Allow app-level admins to do anything.
    perms = permissions.Permissions(write_users=['bob@example.com'],
                                    read_users=['bob@example.com'])
    files.Write('/bar', 'Test', permissions=perms)
    self.stubs.Set(permissions.users, 'is_current_user_admin', lambda: True)
    files.Write('/bar', 'New content')
    self.assertEqual('New content', files.Read('/bar'))
    # Reset permissions.
    perms = permissions.Permissions(write_users=None, read_users=None)
    files.Write('/bar', 'Test', permissions=perms)
    self.stubs.UnsetAll()

    # Write without permissions specified. Should have no restrictions and
    # (important) should never make an RPC for the current user.
    self.stubs.Set(permissions.users, 'get_current_user',
                   lambda: self.fail('get_current_user should not be called!'))
    self.stubs.Set(permissions.users, 'is_current_user_admin',
                   lambda: self.fail('is_current_user_admin should not '
                                     'be called!'))
    # With user arg:
    files.Write('/bar', content='Test', user='bob')
    # Without user arg, and test the Touch() code path:
    files.Touch('/bar')
    self.assertEqual('Test', files.Read('/bar'))
    self.stubs.UnsetAll()

if __name__ == '__main__':
  basetest.main()
