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

import os
from titan.common.lib.google.apputils import basetest
from titan import activities
from titan import files
from titan.files.mixins import stats_recorder

class StatsRecorderTest(testing.BaseTestCase):

  def setUp(self):
    files.register_file_mixins([stats_recorder.StatsRecorderMixin])
    super(StatsRecorderTest, self).setUp()

  def tearDown(self):
    files.unregister_file_factory()
    super(StatsRecorderTest, self).tearDown()

  def testRecordStats(self):
    # The following numbers should not be over-analyzed if broken, as long
    # as they stay relatively low. Feel free to update if internals change.

    # +2 counters (files/File/load and files/File/load/latency).
    _ = files.File('/foo').exists
    self.assertEqual(2, self.CountLoggers())

    # +6 (internal of write() + plus load counters).
    files.File('/foo').write('bar')
    self.assertEqual(8, self.CountLoggers())

    # +4.
    files.File('/foo').serialize()
    self.assertEqual(12, self.CountLoggers())

    # +12.
    files.File('/foo').copy_to(files.File('/bar'))
    self.assertEqual(24, self.CountLoggers())

    # +4.
    files.File('/foo').delete()
    self.assertEqual(28, self.CountLoggers())

    # TODO(user): Add these when implemented and increment count.
    # files.Files.list('/')
    # files.Dir('/').Copy('/baz')
    # files.Dir('/foo').list()
    # files.Dir('/foo').exists

  def CountLoggers(self):
    return len(self.GetCounters())

  def GetCounters(self):
    counters = []
    loggers = os.environ[activities.ACTIVITIES_ENVIRON_KEY]
    for logger in loggers:
      counters += getattr(logger.activity, 'counters', [])
    return counters

if __name__ == '__main__':
  basetest.main()
