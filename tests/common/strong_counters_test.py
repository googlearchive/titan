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

"""Tests for strong_counters.py."""

from google.appengine.datastore import datastore_stub_util
from google.appengine.ext import testbed
from titan.common.lib.google.apputils import basetest
from titan.common import strong_counters
from tests.common import basetest as common_basetest

class StrongCountersTest(common_basetest.AppEngineTestCase):

  def InitTestbed(self):
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    policy = datastore_stub_util.PseudoRandomHRConsistencyPolicy(probability=1)
    self.testbed.init_datastore_v3_stub(consistency_policy=policy)
    self.testbed.init_user_stub()

  def testCounters(self):
    self.assertEqual(0, strong_counters.GetCount('counter1'))
    strong_counters.Increment('counter1')
    self.assertEqual(1, strong_counters.GetCount('counter1'))
    for _ in range(0, 100):
      strong_counters.Increment('counter1')
    self.assertEqual(101, strong_counters.GetCount('counter1'))

    # Verify separation between counter names.
    strong_counters.Increment('counter2')
    self.assertEqual(1, strong_counters.GetCount('counter2'))
    self.assertEqual(101, strong_counters.GetCount('counter1'))

if __name__ == '__main__':
  basetest.main()
