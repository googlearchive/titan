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

"""Base test classes for Titan modules."""

import os
import mox
from mox import stubout
from google.appengine.ext import testbed
from tests.common import basetest as common_basetest
from titan.files import dirs

class MockableTestCase(common_basetest.AppEngineTestCase):
  """Base test case supporting stubs and mox."""

  def setUp(self):
    super(MockableTestCase, self).setUp()
    self.stubs = stubout.StubOutForTesting()
    self.mox = mox.Mox()

  def tearDown(self):
    super(MockableTestCase, self).tearDown()
    self.stubs.SmartUnsetAll()
    self.stubs.UnsetAll()
    self.mox.UnsetStubs()
    self.mox.ResetAll()

class BaseTestCase(MockableTestCase):
  """Base test case for tests requiring Datastore, Memcache, or Blobstore."""

  def setUp(self):
    super(BaseTestCase, self).setUp()

    # For pull queues to work.
    # All task queues must be specified in common/queue.yaml.
    self.testbed.init_taskqueue_stub(root_path=os.path.dirname(__file__),
                                     _all_queues_valid=True)
    self.taskqueue_stub = self.testbed.get_stub(testbed.TASKQUEUE_SERVICE_NAME)

    # Login a default, non-admin user and set the hostname.
    self.Login('titanuser@example.com')
    self.SetHostname(hostname='testbed.example.com')

    # Make this buffer negative so dir tasks are available instantly for lease.
    self.stubs.SmartSet(dirs, 'TASKQUEUE_LEASE_ETA_BUFFER', -86400)

  def InitTestbed(self):
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
    # self.testbed.init_datastore_v3_stub()  # Done later.
    self.testbed.init_files_stub()
    # self.testbed.init_images_stub()  # Intentionally excluded.
    self.testbed.init_logservice_stub()
    self.testbed.init_mail_stub()
    self.testbed.init_memcache_stub()
    # self.testbed.init_taskqueue_stub()  # Done later.
    self.testbed.init_urlfetch_stub()
    self.testbed.init_user_stub()
    self.testbed.init_xmpp_stub()

  def Login(self, *args, **kwargs):
    """Override to have a default titan user email."""
    email = kwargs.pop('email', None)
    if not email and args:
      email = args[0]
    return super(BaseTestCase, self).Login(
        email or 'titanuser@example.com', *args[1:], **kwargs)

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

