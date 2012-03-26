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

"""Tests for handlers.py."""

from tests.common import testing

import os
import webtest
from titan.common.lib.google.apputils import basetest
from titan.stats import handlers

class StatsHandlersTest(testing.BaseTestCase):

  def setUp(self):
    super(StatsHandlersTest, self).setUp()
    self.app = webtest.TestApp(handlers.application)
    os.environ['DJANGO_SETTINGS_MODULE'] = 'django.conf.global_settings'

  def testCounterDataHandler(self):
    # Weakly test execution path:
    params = {
        'counter_name': 'page/view',
        'start_date': '2015-05-15',
        'end_date': '2015-05-20',
    }
    response = self.app.get('/_titan/stats/counterdata', params)
    self.assertEqual('200 OK', response.status)
    self.assertEqual('{}', response.body)

  def testGraphHandler(self):
    # Weakly test execution path:
    params = {
        'counter_name': 'page/view',
        'start_date': '2015-05-15',
        'end_date': '2015-05-20',
    }
    response = self.app.get('/_titan/stats/graph', params)
    self.assertEqual('200 OK', response.status)
    self.assertIn('AnnotatedTimeLine', response.body)

if __name__ == '__main__':
  basetest.main()
