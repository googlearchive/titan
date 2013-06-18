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

"""Tests for users.py."""

from tests.common import testing

import os

from google.appengine.ext import ndb
from google.appengine.ext.endpoints import users_id_token
from titan.common.lib.google.apputils import basetest
from titan.users import users

class UsersTest(testing.BaseTestCase):

  def testUserInit(self):
    # Verify initializing a user directly.
    titan_user = users.TitanUser('HELLO@EXAMPLE.com')
    self.assertEqual('hello@example.com', titan_user.email)
    self.assertEqual('<TitanUser: hello@example.com>', repr(titan_user))
    self.assertEqual('hello@example.com', str(titan_user))
    self.assertEqual('example.com', titan_user.organization)
    self.assertRaises(ValueError, lambda: titan_user.user_id)

  def testGetCurrentUserNotLoggedIn(self):
    self.Logout()
    titan_user = users.get_current_user()
    self.assertIsNone(titan_user)

  def testGetCurrentUser(self):
    # Login as normal user.
    self.login('foo@example.com')
    titan_user = users.get_current_user()
    self.assertEqual('foo@example.com', titan_user.email)
    self.assertEqual('<TitanUser: foo@example.com>', repr(titan_user))
    self.assertEqual('foo@example.com', str(titan_user))
    self.assertFalse(titan_user.is_admin)
    self.assertEqual('1', titan_user.user_id)

    # Login as admin.
    self.login('admin@example.com', is_admin=True)
    titan_user = users.get_current_user()
    self.assertEqual('admin@example.com', titan_user.email)
    self.assertTrue(titan_user.is_admin)

  def testGetCurrentUserOAuth(self):
    self.Logout()

    # Login as normal OAuth user.
    self.login('bar@example.com', is_oauth_user=True, scopes=users.OAUTH_SCOPES)
    titan_user = users.get_current_user()
    self.assertEqual('bar@example.com', titan_user.email)
    self.assertFalse(titan_user.is_admin)

    # Pass in None scope.
    titan_user = users.get_current_user(oauth_scopes=None)
    self.assertIsNone(titan_user)

  def testGetCurrentUserOAuthAdmin(self):
    # TODO(user): This needs to be an independent test because the
    # users_stub strangely persists old data even when cleared. Figure that out.

    # Login as OAuth admin.
    self.login('oauthadmin@example.com', is_admin=True, is_oauth_user=True,
               scopes=users.OAUTH_SCOPES)
    titan_user = users.get_current_user()
    self.assertEqual('oauthadmin@example.com', titan_user.email)
    self.assertTrue(titan_user.is_admin)

  def testGetCurrentUserEndpoints(self):
    self.Logout()
    os.environ[users_id_token._ENV_AUTH_EMAIL] = 'test@example.com'
    os.environ[users_id_token._ENV_AUTH_DOMAIN] = ''
    user = users.get_current_user()
    self.assertEqual(user.email, 'test@example.com')
    self.assertIsNone(users._get_current_oauth_user(users.OAUTH_SCOPES))

  def testGetCurrentUserDeferredTask(self):
    self.Logout()
    titan_user = users.get_current_user()
    self.assertIsNone(titan_user)

    # Verify that the X-Titan-User header only works when in a task.
    os.environ['HTTP_X_TITAN_USER'] = 'imposter@example.com'
    titan_user = users.get_current_user()
    self.assertIsNone(titan_user)

    os.environ['HTTP_X_APPENGINE_TASKNAME'] = 'task1'
    os.environ['HTTP_X_TITAN_USER'] = 'foo@example.com'
    titan_user = users.get_current_user()
    self.assertEqual('foo@example.com', titan_user.email)
    self.assertFalse(titan_user.is_admin)

  def testCreateLoginUrl(self):
    login_url = users.create_login_url()
    self.assertIn('www.google.com/accounts/Login', login_url)
    login_url = users.create_login_url('http://www.example.com')
    self.assertIn('www.google.com/accounts/Login', login_url)
    self.assertIn('www.example.com', login_url)

  def testCreateLogoutUrl(self):
    logout_url = users.create_logout_url('http://www.example.com')
    self.assertIn('www.google.com/accounts/Logout', logout_url)
    self.assertIn('www.example.com', logout_url)

  def testTitanUserProperty(self):
    # Verify normal user.
    ent = TestUserModel(id='foo', user=users.TitanUser('foo@example.com'))
    ent.put()
    ent = TestUserModel.get_by_id('foo')
    self.assertEqual(ent.user, users.TitanUser('foo@example.com'))
    # Verify auto_current_user_add.
    self.assertEqual(ent.created, users.TitanUser('titanuser@example.com'))

    # On second put, user is NOT overriden with titanadmin@example.com.
    self.login('titanadmin@example.com', is_admin=True)
    ent = TestUserModel.get_by_id('foo')
    ent.put()
    ent = TestUserModel.get_by_id('foo')
    self.assertEqual(ent.created, users.TitanUser('titanuser@example.com'))
    self.login('titanuser@example.com')

    # Verify user arg overrides auto_current_user_add.
    ent = TestUserModel.get_by_id('foo')
    ent.created = users.TitanUser('foo@example.com')
    ent.put()
    ent = TestUserModel.get_by_id('foo')
    self.assertEqual(ent.created, users.TitanUser('foo@example.com'))

    # Verify auto_current_user.
    ent = TestUserModel(id='foo')
    ent.put()
    ent = TestUserModel.get_by_id('foo')
    self.assertEqual(ent.modified, users.TitanUser('titanuser@example.com'))
    # On second put, user IS overriden with titanadmin@example.com.
    self.login('titanadmin@example.com', is_admin=True)
    ent = TestUserModel.get_by_id('foo')
    ent.put()
    ent = TestUserModel.get_by_id('foo')
    self.assertEqual(ent.modified, users.TitanUser('titanadmin@example.com'))
    self.login('titanuser@example.com')

    # Error handling.
    self.assertRaises(
        ValueError,
        users.TitanUserProperty, repeated=True, auto_current_user=True)
    self.assertRaises(
        ValueError,
        users.TitanUserProperty, repeated=True, auto_current_user_add=True)
    # Error: passing a string instead of a TitanUser object.
    self.assertRaises(ValueError, TestUserModel, user='foo@example.com')

  def testSerialize(self):
    user = users.TitanUser('foo@example.com')
    serialized_user = user.serialize()
    self.assertEqual('foo@example.com', serialized_user['email'])
    self.assertEqual('example.com', serialized_user['organization'])
    self.assertFalse(serialized_user['is_admin'])

class TestUserModel(ndb.Model):
  user = users.TitanUserProperty()
  created = users.TitanUserProperty(auto_current_user_add=True)
  modified = users.TitanUserProperty(auto_current_user=True)

if __name__ == '__main__':
  basetest.main()
