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

"""Tests for activities.py."""

from tests.common import testing

from titan.common.lib.google.apputils import basetest
from titan.activities import activities
from titan import files
from titan import users

class ActivityTest(basetest.TestCase):

  def testActivity(self):
    user = users.TitanUser('test@example.com')
    activity = activities.Activity('key', user=user, meta='meta')
    self.assertEquals('key', activity.key)
    self.assertEquals(user, activity.user)
    self.assertEquals('meta', activity.meta)

  def testKeysExplosion(self):
    activity = activities.Activity('keys.go.here')
    self.assertEquals('keys.go.here', activity.key)
    self.assertListEqual(['keys', 'keys.go', 'keys.go.here'], activity.keys)

  def testToMessage(self):
    activity = activities.Activity('keys.go.here', meta={'foo': 'bar'})
    message = activity.to_message()
    self.assertEquals('keys.go.here', message.key)
    self.assertEquals('{"foo": "bar"}', message.meta)

  def testActivityId(self):
    # Ensure an activity id is generated with a user.
    user = users.TitanUser('test@example.com')
    activity = activities.Activity('keys.go.here', user=user)
    self.assertTrue(activity.activity_id)

class ActivityServiceTest(basetest.TestCase):

  def testBadActivityId(self):
    activities_service = activities.ActivitiesService()
    with self.assertRaises(activities.InvalidActivityIdError):
      activities_service.get_activity('/bad/activity/id')

class BaseActivityLoggerTest(basetest.TestCase):

  def setUp(self):
    super(BaseActivityLoggerTest, self).setUp()

    user = users.TitanUser('test@example.com')
    self.activity = activities.Activity('key', user=user, meta='meta')
    self.activity_logger = activities.BaseActivityLogger(self.activity)

  def testBaseActivityLogger(self):
    self.assertEquals(False, self.activity_logger.defer_finalize)

  def testFinalize(self):
    # Base activities should not error so the subclasses work correctly.
    self.activity_logger.finalize()

class BaseProcessorActivityLoggerTest(basetest.TestCase):

  def setUp(self):
    super(BaseProcessorActivityLoggerTest, self).setUp()

    user = users.TitanUser('test@example.com')
    self.activity = activities.Activity('key', user=user, meta='meta')
    self.activity_logger = activities.BaseProcessorActivityLogger(self.activity)

  def testBaseBackendActivity(self):
    # Default to not deferring the finalize.
    self.assertEquals(False, self.activity_logger.defer_finalize)

  def testAggregators(self):
    # Default processors are empty.
    self.assertEquals(0, len(self.activity_logger.processors))

  def testFinalize(self):
    # Base activities should not error so the subclasses work correctly.
    self.activity_logger.finalize()

class FileActivityLoggerTest(basetest.TestCase):

  def setUp(self):
    super(FileActivityLoggerTest, self).setUp()

    user = users.TitanUser('test@example.com')
    self.activity = activities.Activity('key', user=user, meta='meta')
    self.activity_logger = activities.FileActivityLogger(self.activity)

  def testDeferFinalize(self):
    self.assertEquals(True, self.activity_logger.defer_finalize)

  def testIndexedMetaKeys(self):
    self.activity.meta = {'files': ['/tmp/file.txt']}
    self.activity_logger.indexed_meta_keys += ['files']
    self.assertIn('files', self.activity_logger.file_meta)
    self.assertEquals(['/tmp/file.txt'],
                      self.activity_logger.file_meta['files'])

class ProcessActivityLoggersTest(testing.BaseTestCase):

  def testProcessActivityLoggers(self):
    user = users.TitanUser('test@example.com')
    activity = activities.Activity('key', user=user, meta='meta')
    activity_logger = activities.FileActivityLogger(activity)
    activity_logger.store()
    # Ensure that it runs normally.
    activities.process_activity_loggers()
    self.RunDeferredTasks(queue_name=activities.ACTIVITY_QUEUE)
    # Verify file was written.
    self.assertTrue(files.File(activity.activity_id, _internal=True).exists)

  def testLog(self):
    activity = activities.log('titan.test')
    activities.process_activity_loggers()
    self.RunDeferredTasks(queue_name=activities.ACTIVITY_QUEUE)
    # Verify file was written.
    self.assertTrue(files.File(activity.activity_id, _internal=True).exists)

if __name__ == '__main__':
  basetest.main()
