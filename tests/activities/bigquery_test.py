#!/usr/bin/env python
# Copyright 2013 Google Inc. All Rights Reserved.
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

"""Tests for bigquery.py."""

from tests.common import testing

import mock
from titan.common.lib.google.apputils import basetest
from titan import activities
from titan import pipelines
from titan import users
from titan.activities import bigquery

class BigQueryActivityLoggerTest(basetest.TestCase):

  def setUp(self):
    super(BigQueryActivityLoggerTest, self).setUp()

    user = users.TitanUser('test@example.com')
    self.activity = activities.Activity('key', user=user, meta='meta')
    self.activity_logger = bigquery.BigQueryActivityLogger(
        self.activity, project_id='project_id', dataset_id='dataset_id',
        table_id='table_id')

  def testActivity(self):
    self.assertEquals('project_id', self.activity_logger.project_id)
    self.assertEquals('dataset_id', self.activity_logger.dataset_id)
    self.assertEquals('table_id', self.activity_logger.table_id)

  def testAggregate(self):
    # Ensure the processor is returned.
    self.assertEquals(1, len(self.activity_logger.processors))

class ProcessActivitiesTest(testing.BaseTestCase):

  def setUp(self):
    super(ProcessActivitiesTest, self).setUp()

    user = users.TitanUser('test@example.com')
    self.activity = activities.Activity('key', user=user, meta='meta')
    self.activity_logger = bigquery.BigQueryActivityLogger(
        self.activity, project_id='project_id', dataset_id='dataset_id',
        table_id='table_id')

  @mock.patch.object(bigquery.BigQueryActivityLogger, 'finalize')
  def testProcessActivities(self, _):
    self.activity_logger.store()
    # Weakly test that it runs normally.
    activities.process_activity_loggers()
    self.RunDeferredTasks(queue_name=pipelines.PIPELINES_QUEUE)

if __name__ == '__main__':
  basetest.main()
