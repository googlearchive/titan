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

from tests.common import testing

from titan.common.lib.google.apputils import basetest
from titan.files import files
from titan.files.mixins import stats_recorder
from titan.stats import stats

class StatsFile(stats_recorder.StatsRecorderMixin, files.File):
  pass

class StatsRecorderTest(testing.BaseTestCase):

  def setUp(self):
    files.RegisterFileFactory(lambda *args, **kwargs: StatsFile)
    super(StatsRecorderTest, self).setUp()

  def tearDown(self):
    files.UnregisterFileFactory()
    super(StatsRecorderTest, self).tearDown()

  def testRecordStats(self):
    # The following numbers should not be over-analyzed if broken, as long
    # as they stay relatively low. Feel free to update if internals change.

    # +2 counters (files/File/load and files/File/load/latency).
    _ = files.File('/foo').exists
    self.assertEqual(2, len(stats.GetRequestLocalCounters()))

    # +6 (internal of Write() + plus load counters).
    files.File('/foo').Write('bar')
    self.assertEqual(8, len(stats.GetRequestLocalCounters()))

    # +4.
    files.File('/foo').Serialize()
    self.assertEqual(12, len(stats.GetRequestLocalCounters()))

    # +12.
    files.File('/foo').CopyTo(files.File('/bar'))
    self.assertEqual(24, len(stats.GetRequestLocalCounters()))

    # +4.
    files.File('/foo').Delete()
    self.assertEqual(28, len(stats.GetRequestLocalCounters()))

    # TODO(user): Add these when implemented and increment count.
    # files.Files.List('/')
    # files.Dir('/').Copy('/baz')
    # files.Dir('/foo').List()
    # files.Dir('/foo').exists

if __name__ == '__main__':
  basetest.main()
