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

"""Titan Users module.

This module is a simple wrapper/abstraction around the App Engine Users API and
the OAuth API. The get_current_user method checks to see if a user is logged in
via the regular App Engine Users API, and if not, then checks the OAuth API.

Usage:
  # Get the current user and print their email address.
  user = users.get_current_user()
  if user:
    print user.email

  # Initialize a user based on email.
  user = users.TitanUser('example@example.com')

  # Get login/logout URLs.
  users.create_login_url(dest_url='/afterlogin')
  users.create_logout_url(dest_url='/afterlogout')
"""

import logging
import os
from google.appengine.api import oauth
from google.appengine.api import users as users_lib
from google.appengine.ext import ndb
from google.appengine.ext.ndb import model
from google.appengine.ext.ndb import utils as ndb_utils

__all__ = [
    # Constants.
    'OAUTH_SCOPES',
    # Classes.
    'TitanUser',
    'TitanUserProperty',
    # Functions.
    'get_current_user',
    'create_login_url',
    'create_logout_url',
]

OAUTH_SCOPES = (
    'https://www.googleapis.com/auth/userinfo.email',
)

class TitanUser(object):
  """Simple wrapper around the App Engine Users API.

  Attributes:
    email: The email address of the user.
    is_admin: Whether the user is an admin on the App Engine app. This
        attribute is only available if instantiated via get_current_user().
    user_id: The ID of the user from App Engine's Users API. This attribute is
        only available if instantiated via get_current_user().
  """

  def __init__(self, email, organization=None, _user=None, _is_admin=False,
               _is_oauth_user=False):
    self._email = email.lower()
    self._organization = organization
    self._user = _user
    self._is_admin = _is_admin
    self._is_oauth_user = _is_oauth_user

  def __repr__(self):
    return '<%s: %s>' % (self.__class__.__name__, self.email)

  def __str__(self):
    return self.email

  def __eq__(self, other):
    if not isinstance(other, TitanUser):
      return False
    return self.email == other.email

  @property
  def is_admin(self):
    return self._is_admin

  @property
  def email(self):
    return self._email

  @property
  def user_id(self):
    if not self._user:
      raise ValueError('Cannot retrieve user ID for users that aren\'t '
                       'the current user.')
    return self._user.user_id()

  @property
  def organization(self):
    return self._organization or self.email.split('@')[1]

  def serialize(self):
    data = {
        'email': self.email,
        'organization': self.organization,
        'is_admin': self.is_admin,
    }
    if self._user:
      data['user_id'] = self.user_id
    return data

class TitanUserProperty(ndb.StringProperty):
  """A property for use in NDB models.

  This property is similar to ndb.UserProperty, but internally uses a string of
  the email address as the canonical unique identifier. This avoids the
  historical problems with the behavior of DB and NDB's UserProperty, which
  sometimes uses email and sometimes uses (email + user_id) as the model key.
  """

  # Much of this is shamelessly copied from ndb.UserProperty, with some changes.

  _attributes = ndb.StringProperty._attributes + [
      '_auto_current_user', '_auto_current_user_add']

  _auto_current_user = False
  _auto_current_user_add = False

  @ndb_utils.positional(ndb.StringProperty._positional)
  def __init__(self, name=None, auto_current_user=False,
               auto_current_user_add=False, **kwds):
    super(TitanUserProperty, self).__init__(name=name, **kwds)
    if self._repeated:
      if auto_current_user:
        raise ValueError('TitanUserProperty could use auto_current_user and be '
                         'repeated, but there would be no point.')
      elif auto_current_user_add:
        raise ValueError('TitanUserProperty could use auto_current_user_add '
                         'and be repeated, but there would be no point.')
    self._auto_current_user = auto_current_user
    self._auto_current_user_add = auto_current_user_add

  def _prepare_for_put(self, entity):
    if (self._auto_current_user or
        (self._auto_current_user_add and not self._has_value(entity))):
      value = get_current_user()
      if value is not None:
        self._store_value(entity, value)

  def _db_get_value(self, v, p):
    # Backwards compatibility: support old users.User objects by grabbing
    # the email address. This will be overridden on the next put.
    #
    # Due to various environment complexities, this behavior is not unit tested.
    # Be careful when modifying it.
    #
    if v.has_uservalue():
      return model._unpack_user(v).email()
    return super(TitanUserProperty, self)._db_get_value(v, p)

  def _validate(self, user):
    if not isinstance(user, TitanUser):
      raise ValueError('Expected a TitanUser, got %s' % repr(user))

  def _to_base_type(self, user):
    return user.email

  def _from_base_type(self, email):
    return TitanUser(email)

def get_current_user(oauth_scopes=OAUTH_SCOPES):
  """Returns the currently logged in TitanUser or None.

  Args:
    oauth_scopes: If provided, the OAuth scopes to use to request the current
        OAuth user via the OAuth API. Set to None to skip OAuth checking.
  Returns:
    An initialized TitanUser or None if no user is logged in.
  """
  user = users_lib.get_current_user()
  if user:
    is_admin = users_lib.is_current_user_admin()
    organization = os.environ.get('USER_ORGANIZATION')
    return TitanUser(user.email(), organization=organization, _user=user,
                     _is_admin=is_admin)

  # If an OAuth scope is provided, request the current OAuth user, if any.
  if oauth_scopes:
    user = _get_current_oauth_user(oauth_scopes)
    if user:
      is_admin = oauth.is_current_user_admin(_format_oauth_scopes(oauth_scopes))
      organization = os.environ.get('USER_ORGANIZATION')
      return TitanUser(user.email(), organization=organization, _user=user,
                       _is_admin=is_admin, _is_oauth_user=True)

  # If the request was made in a deferred task, check the X-Titan-User header.
  if 'HTTP_X_APPENGINE_TASKNAME' in os.environ:
    email = os.environ.get('HTTP_X_TITAN_USER')
    if email:
      return TitanUser(email)

def create_login_url(dest_url=None):
  return users_lib.create_login_url(dest_url)

def create_logout_url(dest_url):
  return users_lib.create_logout_url(dest_url)

def _get_current_oauth_user(oauth_scopes):
  try:
    user = oauth.get_current_user(_format_oauth_scopes(oauth_scopes))
    return user
  except oauth.NotAllowedError:
    # Raised if the requested URL does not permit OAuth authentication.
    # Avoid logging noise.
    pass
  except oauth.InvalidOAuthParametersError as e:
    logging.error(e.__class__.__name__)
  except oauth.OAuthRequestError:
    # Raised on any invalid OAuth request.
    logging.exception('Error with OAuth request.')

def _format_oauth_scopes(oauth_scopes):
  if hasattr(oauth_scopes, '__iter__'):
    oauth_scopes = ' '.join(oauth_scopes)
  return oauth_scopes
