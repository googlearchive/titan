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

"""Tests for handlers.py."""

from tests.common import testing

import json

import webtest

from titan.common.lib.google.apputils import basetest
from titan import channel
from titan import tasks
from titan.tasks import handlers

class TaskManagerHandlerTest(testing.BaseTestCase):

  def setUp(self):
    super(TaskManagerHandlerTest, self).setUp()
    self.app = webtest.TestApp(handlers.application)

  def testTaskManagerHandlerTest(self):
    url = '/_titan/tasks/taskmanager'
    response = self.app.get(url, expect_errors=True)
    self.assertEqual(400, response.status_int)

    url = '/_titan/tasks/taskmanager?key=fake'
    response = self.app.get(url, expect_errors=True)
    self.assertEqual(404, response.status_int)

    task_manager = tasks.TaskManager.new(group='group')
    task_manager.defer_task('task-key0', GoodCallback)
    task_manager.defer_task('task-key1', BadCallback)
    task_manager.finalize()

    url = '/_titan/tasks/taskmanager?key=%s&group=%s' % (
        task_manager.key, task_manager.group)
    response = self.app.get(url)
    self.assertEqual(200, response.status_int)
    response_data = json.loads(response.body)

    self.assertIsNone(response_data['broadcast_channel_key'])
    self.assertIsNone(response_data['description'])
    self.assertIn('key', response_data)
    self.assertEqual('group', response_data['group'])
    self.assertEqual('titanuser@example.com', response_data['created_by'])
    self.assertEqual(0, response_data['num_completed'])
    self.assertEqual(0, response_data['num_failed'])
    self.assertEqual(0, response_data['num_successful'])
    self.assertEqual(2, response_data['num_total'])
    self.assertEqual('running', response_data['status'])
    task_keys = response_data['task_keys']
    self.assertSameElements(['task-key0', 'task-key1'], task_keys)

    self.RunDeferredTasks(runs=1)
    response = self.app.get(url)
    self.assertEqual(200, response.status_int)
    response_data = json.loads(response.body)
    self.assertEqual(1, response_data['num_completed'])
    self.assertEqual(0, response_data['num_failed'])
    self.assertEqual(1, response_data['num_successful'])
    self.assertEqual(2, response_data['num_total'])
    self.assertEqual('running', response_data['status'])

    try:
      self.RunDeferredTasks(runs=1)
    except tasks.TaskError:
      pass  # Expected for BadCallback().

    response = self.app.get(url)
    self.assertEqual(200, response.status_int)
    response_data = json.loads(response.body)
    self.assertEqual(2, response_data['num_completed'])
    self.assertEqual(1, response_data['num_failed'])
    self.assertEqual(1, response_data['num_successful'])
    self.assertEqual(2, response_data['num_total'])
    self.assertEqual('failed', response_data['status'])

  def testTaskManagerSubscribeHandlerTest(self):
    url = '/_titan/tasks/taskmanager/subscribe'
    response = self.app.post(url, expect_errors=True)
    self.assertEqual(400, response.status_int)

    url = '/_titan/tasks/taskmanager/subscribe?key=fake&client_id=foo'
    response = self.app.post(url, expect_errors=True)
    self.assertEqual(404, response.status_int)

    client_id = 'unique-client-id'
    token = channel.create_channel(client_id)
    task_manager = tasks.TaskManager.new(broadcast_channel_key='broadcast')
    url = '/_titan/tasks/taskmanager/subscribe?key=%s&client_id=%s' % (
        task_manager.key, client_id)
    response = self.app.post(url)
    self.assertEqual(200, response.status_int)

    # Must connect each client to the channel before broadcast.
    # This happens automatically by the client scripts in the real world.
    self.channel_stub.connect_channel(token)

    task_manager.defer_task('task-key0', GoodCallback)
    task_manager.defer_task('task-key1', BadCallback)
    task_manager.finalize()

    # Weakly assert that messages were sent to the broadcast channel.
    self.assertTrue(self.channel_stub.pop_first_message(token))

def GoodCallback():
  pass

def BadCallback():
  raise tasks.TaskError('Full of fail!')

if __name__ == '__main__':
  basetest.main()
