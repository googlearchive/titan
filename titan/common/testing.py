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

import cStringIO
import datetime
import os
import shutil
import sys
import types
import urllib
import mox
from mox import stubout
from google.appengine.api import files as blobstore_files
from google.appengine.api import memcache
from google.appengine.ext import blobstore
from google.appengine.ext import db
from google.appengine.ext import testbed
from google.appengine.api import apiproxy_stub_map
from google.appengine.api.blobstore import blobstore_stub
from google.appengine.api.blobstore import file_blob_storage
from google.appengine.api.files import file_service_stub
from titan.common import hooks
from titan.files import files_cache
import gflags as flags
from titan.common.lib.google.apputils import basetest

def DisableCaching(func):
  """Decorator for disabling the files_cache module for a test case."""

  def Wrapper(self, *args, **kwargs):
    # Stub out each function in files_cache.
    func_return_none = lambda *args, **kwargs: None
    for attr in dir(files_cache):
      if isinstance(getattr(files_cache, attr), types.FunctionType):
        self.stubs.Set(files_cache, attr, func_return_none)

    # Stub special packed return from files_cache.GetFiles.
    self.stubs.Set(files_cache, 'GetFiles',
                   lambda *args, **kwargs: (None, False))
    func(self, *args, **kwargs)

  return Wrapper

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

  def setUp(self):
    # Manually setup blobstore and files stubs (until supported in testbed).
    #
    # Setup base blobstore service stubs.
    self.appid = os.environ['APPLICATION_ID'] = 'testbed-test'
    apiproxy_stub_map.apiproxy = apiproxy_stub_map.APIProxyStubMap()
    storage_directory = os.path.join(flags.FLAGS.test_tmpdir, 'blob_storage')
    if os.access(storage_directory, os.F_OK):
      shutil.rmtree(storage_directory)
    blob_storage = file_blob_storage.FileBlobStorage(
        storage_directory, self.appid)
    self.blobstore_stub = blobstore_stub.BlobstoreServiceStub(blob_storage)
    apiproxy_stub_map.apiproxy.RegisterStub('blobstore', self.blobstore_stub)

    # Setup blobstore files service stubs.
    apiproxy_stub_map.apiproxy.RegisterStub(
        'file', file_service_stub.FileServiceStub(blob_storage))
    file_service_stub._now_function = datetime.datetime.now

    # Must come after the apiproxy stubs above.
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_datastore_v3_stub()
    self.testbed.init_memcache_stub()
    self.testbed.init_taskqueue_stub()
    self.testbed.setup_env(
        app_id='testbed-test',
        user_email='titanuser@example.com',
        user_id='1',
        overwrite=True,
    )
    super(BaseTestCase, self).setUp()

  def tearDown(self):
    self.testbed.deactivate()
    super(BaseTestCase, self).tearDown()

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

class ServicesTestCase(BaseTestCase):
  """Base test class for Titan service tests."""

  def tearDown(self):
    hooks.global_hooks = {}
    hooks.global_services_order = []

  def EnableServices(self, config_module_name):
    """Let tests define a custom set of TITAN_SERVICES."""
    hooks.LoadServices(config_module=sys.modules[config_module_name])
