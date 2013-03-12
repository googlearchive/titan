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

"""Handlers for Titan Tasks."""

import webapp2
from titan import tasks
from titan.common import handlers

# SECURITY NOTE: These handlers default to "login: required" in
# titan/tasks/handlers.yaml. That means they are accessible to the world,
# but they rely on the hashed task manager key to be useful.
# Be careful not to introduce security vulnerabilities here.
#
# For users of Titan Tasks, you may want to wrap these handlers in an
# application-specific WSGI router which performs app-level security checks.

class TaskManagerHandler(handlers.BaseHandler):
  """Handlers for TaskManager."""

  def get(self):
    key = self.request.get('key')
    group = self.request.get('group', tasks.DEFAULT_GROUP)
    if not key:
      self.abort(400)
    task_manager = tasks.TaskManager(key=key, group=group)
    if not task_manager.exists:
      self.abort(404)
    self.WriteJsonResponse(task_manager.Serialize(full=True))

class TaskManagerSubscribeHandler(handlers.BaseHandler):
  """Handlers for TaskManager.Subscribe."""

  def post(self):
    key = self.request.get('key')
    group = self.request.get('group', tasks.DEFAULT_GROUP)
    client_id = self.request.get('client_id')
    if not key or not client_id:
      self.abort(400)
    task_manager = tasks.TaskManager(key=key, group=group)
    if not task_manager.exists:
      self.abort(404)
    task_manager.Subscribe(client_id)

ROUTES = (
    ('/_titan/tasks/taskmanager', TaskManagerHandler),
    ('/_titan/tasks/taskmanager/subscribe', TaskManagerSubscribeHandler),
)
application = webapp2.WSGIApplication(ROUTES, debug=False)
