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

"""Tests for stats_recorder.py."""

from tests import testing

from titan.common.lib.google.apputils import basetest
from titan.files import files
from titan.stats import stats

class StatsRecorderTest(testing.ServicesTestCase):

  def setUp(self):
    super(StatsRecorderTest, self).setUp()
    services = (
        'titan.services.stats_recorder',
    )
    self.EnableServices(services)

  def testRecordStats(self):
    files.Exists('/foo')
    files.Get('/foo')
    files.Write('/foo', 'bar')
    files.Delete('/foo')
    files.Touch('/foo')
    files.Copy('/foo', '/bar')
    files.CopyDir('/', '/baz')
    files.ListFiles('/foo')
    files.ListDir('/foo')
    files.DirExists('/foo')
    # 2 counters stored per invocation above, +2 extra for how CopyDir
    # lists File objects and then internally invokes files.Get when
    # dereferencing the object.
    self.assertEqual(22, len(stats.GetRequestLocalCounters()))

if __name__ == '__main__':
  basetest.main()
