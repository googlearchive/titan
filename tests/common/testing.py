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

"""Base test case classes for App Engine."""

import base64
import os
import random
import string
import mox
from mox import stubout
from google.appengine.api import apiproxy_stub_map
from google.appengine.datastore import datastore_stub_util
from google.appengine.ext import deferred
from google.appengine.ext import testbed
from titan.common.lib.google.apputils import basetest
from google.appengine.api.search import simple_search_stub
from titan.files import dirs
from google.appengine.runtime import request_environment
from google.appengine.runtime import runtime

# Replace start_new_thread with a version where new threads inherit os.environ
# from their creator thread.
runtime.PatchStartNewThread()

class MockableTestCase(basetest.TestCase):
  """Base test case supporting stubs and mox."""

  def setUp(self):
    self.stubs = stubout.StubOutForTesting()
    self.mox = mox.Mox()

  def tearDown(self):
    self.stubs.SmartUnsetAll()
    self.stubs.UnsetAll()
    self.mox.UnsetStubs()
    self.mox.ResetAll()

class BaseTestCase(MockableTestCase):
  """Base test case for tests requiring Datastore, Memcache, or Blobstore."""

  def setUp(self, enable_environ_patch=True):
    """Initializes the App Engine stubs."""
    # Evil os-environ patching which mirrors dev_appserver and production.
    # This patch turns os.environ into a thread-local object, which also happens
    # to support storing more than just strings. This patch must come first.
    self.enable_environ_patch = enable_environ_patch
    if self.enable_environ_patch:
      self._old_os_environ = os.environ.copy()
      request_environment.current_request.Clear()
      request_environment.PatchOsEnviron()
      os.environ.update(self._old_os_environ)

    # Setup and activate the testbed.
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    # Don't use init_all_stubs here because we need to exclude the unused
    # Images API due to conflicts with the PIL library and virtualenv tests.
    # http://stackoverflow.com/questions/2485295
    self.testbed.init_app_identity_stub()
    self.testbed.init_blobstore_stub()
    self.testbed.init_capability_stub()
    self.testbed.init_channel_stub()
    # self.testbed.init_datastore_v3_stub()  # Done below.
    self.testbed.init_files_stub()
    # self.testbed.init_images_stub()  # Intentionally excluded.
    self.testbed.init_logservice_stub()
    self.testbed.init_mail_stub()
    self.testbed.init_memcache_stub()
    # self.testbed.init_taskqueue_stub()  # Done below.
    self.testbed.init_urlfetch_stub()
    self.testbed.init_user_stub()
    self.testbed.init_xmpp_stub()

    # Fake an always strongly-consistent HR datastore.
    policy = datastore_stub_util.PseudoRandomHRConsistencyPolicy(probability=1)
    self.testbed.init_datastore_v3_stub(consistency_policy=policy)

    # All task queues must be specified in common/queue.yaml.
    self.testbed.init_taskqueue_stub(root_path=os.path.dirname(__file__),
                                     _all_queues_valid=True)

    # Register the search stub (until included in init_all_stubs).
    self.search_stub = simple_search_stub.SearchServiceStub()
    apiproxy_stub_map.apiproxy.RegisterStub('search', self.search_stub)

    # Save the taskqueue_stub for use in _RunDeferredTasks.
    self.taskqueue_stub = self.testbed.get_stub(testbed.TASKQUEUE_SERVICE_NAME)

    # Save the users_stub for use in Login/Logout methods.
    self.users_stub = self.testbed.get_stub(testbed.USER_SERVICE_NAME)

    # Each time setUp is called, treat it like a different request.
    request_id_hash = ''.join(random.sample(string.letters + string.digits, 26))

    self.testbed.setup_env(
        app_id='testbed-test',
        user_email='titanuser@example.com',
        user_id='1',
        user_organization='example.com',
        user_is_admin='0',
        overwrite=True,
        http_host='testbed.example.com:80',
        default_version_hostname='testbed.example.com',
        request_id_hash=request_id_hash,
        server_software='Test/1.0 (testbed)',
    )
    super(BaseTestCase, self).setUp()

    # Make this buffer negative so dir tasks are available instantly for lease.
    self.stubs.SmartSet(dirs, 'TASKQUEUE_LEASE_ETA_BUFFER', -86400)

  def tearDown(self):
    if self.enable_environ_patch:
      os.environ = self._old_os_environ
    self.testbed.deactivate()
    super(BaseTestCase, self).tearDown()

  def LogoutUser(self):
    self.testbed.setup_env(
        overwrite=True,
        user_id='',
        user_email='',
    )
    # Log out the OAuth user.
    self.users_stub.SetOAuthUser(email=None)

  def LoginNormalUser(self, email='titanuser@example.com',
                      user_organization='example.com'):
    self.LogoutUser()
    self.testbed.setup_env(
        overwrite=True,
        user_email=email,
        user_id='1',
        user_organization=user_organization,
    )
    os.environ['USER_IS_ADMIN'] = '0'

  def LoginAdminUser(self, email='titanadmin@example.com',
                     user_organization='example.com'):
    self.LogoutUser()
    self.LoginNormalUser(email=email, user_organization=user_organization)
    os.environ['USER_IS_ADMIN'] = '1'

  def LoginNormalOAuthUser(self, email='titanoauthuser@example.com',
                           user_organization='example.com'):
    self.LogoutUser()
    self.users_stub.SetOAuthUser(
        email=email, domain=user_organization, is_admin=False)

  def LoginAdminOAuthUser(self, email='titanoauthadmin@example.com',
                          user_organization='example.com'):
    self.LogoutUser()
    self.users_stub.SetOAuthUser(
        email=email, domain=user_organization, is_admin=True)

  def LoginTaskAdminUser(self):
    # Tasks are run with an anonymous admin user.
    self.LogoutUser()
    os.environ['USER_IS_ADMIN'] = '1'

  def assertEntityEqual(self, ent, other_ent, ignore=None):
    """Assert equality of properties and dynamic properties of two entities.

    Args:
      ent: First entity.
      other_ent: Second entity.
      ignore: A list of strings of properties to ignore in equality checking.
    Raises:
      AssertionError: if either entity is None.
    """
    if not ent or not other_ent:
      raise AssertionError('%r != %r' % (ent, other_ent))
    if not self._EntityEquals(ent, other_ent):
      properties = self._GetDereferencedProperties(ent)
      properties.update(ent._dynamic_properties)
      other_properties = self._GetDereferencedProperties(other_ent)
      other_properties.update(other_ent._dynamic_properties)
      if ignore:
        for item in ignore:
          if item in properties:
            del properties[item]
          if item in other_properties:
            del other_properties[item]
      self.assertDictEqual(properties, other_properties)

  def assertEntitiesEqual(self, entities, other_entities):
    """Assert that two iterables of entities have the same elements."""
    self.assertEqual(len(entities), len(other_entities))
    # Compare properties of all entities in O(n^2) (but usually 0 < n < 10).
    for ent in entities:
      found = False
      for other_ent in other_entities:
        # Shortcut: display debug if we expect the two entities to be equal:
        if ent.key() == other_ent.key():
          self.assertEntityEqual(ent, other_ent)
        if self._EntityEquals(ent, other_ent):
          found = True
      if not found:
        raise AssertionError('%s not found in %s' % (ent, other_entities))

  def assertNdbEntityEqual(self, ent, other_ent, ignore=None):
    """Assert equality of properties and dynamic properties of two ndb entities.

    Args:
      ent: First ndb entity.
      other_ent: Second ndb entity.
      ignore: A list of strings of properties to ignore in equality checking.
    Raises:
      AssertionError: if either entity is None.
    """
    ent_properties = {}
    other_ent_properties = {}
    for key, prop in ent._properties.iteritems():
      if key not in ignore:
        ent_properties[key] = prop._get_value(ent)
    for key, prop in other_ent._properties.iteritems():
      if key not in ignore:
        other_ent_properties[key] = prop._get_value(other_ent)
    self.assertDictEqual(ent_properties, other_ent_properties)

  def assertSameObjects(self, objs, other_objs):
    """Assert that two lists' objects are equal."""
    self.assertEqual(len(objs), len(other_objs),
                     'Not equal!\nFirst: %s\nSecond: %s' % (objs, other_objs))
    for obj in objs:
      if obj not in other_objs:
        raise AssertionError('%s not found in %s' % (obj, other_objs))

  def _EntityEquals(self, ent, other_ent):
    """Compares entities by comparing their properties and keys."""
    props = self._GetDereferencedProperties(ent)
    other_props = self._GetDereferencedProperties(other_ent)
    return (ent.key().name() == other_ent.key().name()
            and props == other_props
            and ent._dynamic_properties == other_ent._dynamic_properties)

  def _GetDereferencedProperties(self, ent):
    """Directly get properties since they don't dereference nicely."""
    # ent.properties() contains lazy-loaded objects which are always equal even
    # if their actual contents are different. Dereference all the references!
    props = {}
    for key in ent.properties():
      props[key] = ent.__dict__['_' + key]
    return props

  def _RunDeferredTasks(self, queue):
    tasks = self.taskqueue_stub.GetTasks(queue)
    for task in tasks:
      deferred.run(base64.b64decode(task['body']))
    self.taskqueue_stub.FlushQueue(queue)

