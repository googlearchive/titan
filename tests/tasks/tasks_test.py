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

"""Tests for tasks.py."""

from tests.common import testing

import cPickle as pickle
import datetime
import json
from titan import channel
from titan import tasks
from titan.common.lib.google.apputils import basetest

class BadCallbackException(Exception):
  pass

class TasksTestCase(testing.BaseTestCase):

  def testTaskManager(self):
    task_manager = tasks.TaskManager.New(
        group='group',
        description='desc',
        broadcast_channel_key='broadcast')

    self.assertEqual('group', task_manager.group)
    self.assertEqual('desc', task_manager.description)
    self.assertEqual('broadcast', task_manager._broadcast_channel.key)
    self.assertFalse(task_manager._finalized)
    self.assertTrue(task_manager.exists)
    token = channel.CreateChannel('foo')
    task_manager.Subscribe('foo')
    self.channel_stub.connect_channel(token)

    self.assertRaises(
        tasks.TaskManagerNotFinalizedError, lambda: task_manager.status)

    # Get a new task manager from the key, like a new request would.
    task_manager = tasks.TaskManager(key=task_manager.key, group='group')

    # First task fails.
    task_manager.DeferTask('key0', BadCallback)
    self.assertEqual(1, len(self.channel_stub.get_channel_messages(token)))
    # 10 tasks succeed.
    for i in range(1, 11):
      task_manager.DeferTask('key%s' % i, GoodCallback, i)

    # Can't add a task with the same key.
    self.assertRaises(
        tasks.DuplicateTaskError,
        lambda: task_manager.DeferTask('key0', lambda: None))
    task_manager.Finalize()
    # Cannot add tasks after finalization.
    self.assertRaises(
        tasks.TaskManagerFinalizedError,
        lambda: task_manager.DeferTask('foo', lambda: None))

    self.assertTrue(task_manager.exists)
    self.assertTrue(task_manager._finalized)
    self.assertRaises(tasks.InvalidTaskError, task_manager.GetTask, 'fake-key')

    self.assertEqual(11, task_manager.num_total)
    self.assertEqual(0, task_manager.num_completed)
    self.assertEqual(0, task_manager.num_successful)
    self.assertEqual(0, task_manager.num_failed)
    self.assertEqual(0, len(task_manager.successful_tasks))
    self.assertEqual(0, len(task_manager.failed_tasks))
    self.assertEqual(tasks.STATUS_RUNNING, task_manager.status)
    self.assertEqual('titanuser@example.com', task_manager.created_by.email)
    self.assertTrue(isinstance(task_manager.created, datetime.datetime))
    for task in task_manager.tasks:
      self.assertEqual(tasks.STATUS_RUNNING, task.status)
    # Verify number of broadcasted messages.
    self.assertEqual(11, len(self.channel_stub.get_channel_messages(token)))

    # Run three deferred tasks and re-check stats.
    try:
      self.RunDeferredTasks(runs=1)
    except tasks.TaskError:
      pass  # Expected for BadCallback().
    self.RunDeferredTasks(runs=2)

    # Get a new task manager from the key, like a new request would.
    task_manager = tasks.TaskManager(key=task_manager.key, group='group')
    # Ensure that the task_manager can be pickled and unpickled correctly.
    task_manager = pickle.loads(pickle.dumps(task_manager))
    # Cannot add tasks after finalization.
    self.assertRaises(
        tasks.TaskManagerFinalizedError,
        lambda: task_manager.DeferTask('foo', lambda: None))
    self.assertEqual('group', task_manager.group)
    self.assertEqual('desc', task_manager.description)
    self.assertEqual('broadcast', task_manager._broadcast_channel.key)
    self.assertTrue(task_manager._finalized)
    self.assertTrue(task_manager.exists)

    self.assertEqual(11, task_manager.num_total)
    self.assertEqual(3, task_manager.num_completed)
    self.assertEqual(2, task_manager.num_successful)
    self.assertEqual(1, task_manager.num_failed)
    self.assertEqual(2, len(task_manager.successful_tasks))
    self.assertEqual(1, len(task_manager.failed_tasks))
    self.assertEqual(tasks.STATUS_RUNNING, task_manager.status)
    for task in task_manager.successful_tasks:
      self.assertEqual(task.status, tasks.STATUS_SUCCESSFUL)
    for task in task_manager.failed_tasks:
      self.assertEqual(task.status, tasks.STATUS_FAILED)
    # Verify number of broadcasted messages (11 queued + 2 per completed task).
    self.assertEqual(17, len(self.channel_stub.get_channel_messages(token)))

    # Run the rest of the deferred tasks and re-check stats.
    self.RunDeferredTasks()
    task_manager = tasks.TaskManager(key=task_manager.key, group='group')
    self.assertEqual(11, task_manager.num_total)
    self.assertEqual(11, task_manager.num_completed)
    self.assertEqual(10, task_manager.num_successful)
    self.assertEqual(1, task_manager.num_failed)
    self.assertEqual(10, len(task_manager.successful_tasks))
    self.assertEqual(1, len(task_manager.failed_tasks))
    self.assertEqual(tasks.STATUS_FAILED, task_manager.status)
    for task in task_manager.successful_tasks:
      self.assertEqual(task.status, tasks.STATUS_SUCCESSFUL)
    for task in task_manager.failed_tasks:
      self.assertEqual(task.status, tasks.STATUS_FAILED)
    # Verify number of broadcasted messages (11 queued + 2 per completed task).
    self.assertEqual(33, len(self.channel_stub.get_channel_messages(token)))

    failed_task = list(task_manager.failed_tasks)[0]
    self.assertEqual('key0', failed_task.key)
    self.assertEqual(
        'Task was too awesome to succeed.', failed_task.error_message)

    # Verify actual broadcasted messages.
    actual_messages = self.channel_stub.get_channel_messages(token)
    actual_messages = [json.loads(msg) for msg in actual_messages]
    for message in actual_messages:
      # Each channel message contains a JSON-encoded task message like this:
      # {
      #     u'message': u'{"status": "successful", "task_key": "key9",
      #         "task_manager_key": "53b642869769c75db38d384deff529e6"}'
      # }
      message = json.loads(message['message'])
      allowed_statuses = (
          tasks.STATUS_QUEUED,
          tasks.STATUS_RUNNING,
          tasks.STATUS_SUCCESSFUL,
          tasks.STATUS_FAILED,
      )
      self.assertIn(message['status'], allowed_statuses)
      self.assertIn('task_manager_key', message)
      self.assertTrue(message['task_key'].startswith('key'))
      if message['status'] == tasks.STATUS_FAILED:
        self.assertEqual('Task was too awesome to succeed.', message['error'])
      else:
        self.assertNotIn('error', message)

    # Verify group is part of what uniquely identifies a task manager.
    task_manager = tasks.TaskManager(key=task_manager.key, group='fake')
    self.assertFalse(task_manager.exists)
    # And verify that the filename is always the keyname.
    task_manager = tasks.TaskManager(key='key', group='group')
    self.assertEqual(
        '/_titan/tasks/group/key/tasks', task_manager._tasks_dir_path)

    # Error handling.
    self.assertRaises(
        ValueError, lambda: tasks.TaskManager.New(group='Invalid Group'))
    self.assertRaises(
        tasks.InvalidTaskManagerError,
        lambda: tasks.TaskManager('fake-key').DeferTask('foo', lambda: None))
    self.assertFalse(tasks.TaskManager(key='fake').exists)

  def testList(self):
    self.assertEqual([], tasks.TaskManager.List())
    foo_task_manager = tasks.TaskManager.New(group='foo')
    foo_task_manager.DeferTask('-', GoodCallback, 0)
    foo_task_manager.Finalize()

    foo2_task_manager = tasks.TaskManager.New(group='foo')
    foo2_task_manager.DeferTask('-', GoodCallback, 0)
    foo2_task_manager.Finalize()

    bar_task_manager = tasks.TaskManager.New(group='bar')
    bar_task_manager.DeferTask('-', GoodCallback, 0)
    bar_task_manager.Finalize()

    self.assertEqual([], tasks.TaskManager.List())

    # Verify __eq__.
    self.assertEqual(
        foo_task_manager, tasks.TaskManager(foo_task_manager.key, group='foo'))
    self.assertNotEqual(
        bar_task_manager, tasks.TaskManager(foo_task_manager.key, group='foo'))

    # Expect order to be descending chronological by creation time.
    self.assertEqual([], tasks.TaskManager.List())
    self.assertEqual(
        [foo2_task_manager.key, foo_task_manager.key],
        [tm.key for tm in tasks.TaskManager.List(group='foo')])
    self.assertEqual(
        [bar_task_manager.key],
        [tm.key for tm in tasks.TaskManager.List(group='bar')])

# These must be module-level functions for pickling.
def GoodCallback(unused_index):
  pass

def BadCallback():
  raise tasks.TaskError('Task was too awesome to succeed.')

if __name__ == '__main__':
  basetest.main()
