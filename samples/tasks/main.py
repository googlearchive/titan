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

"""Demo app for Titan Tasks."""

import hashlib
import json
import os
import random
import time

import jinja2
import webapp2

from titan import channel
from titan import tasks
from titan import users
from titan.tasks import deferred

class MainHandler(webapp2.RequestHandler):

  def get(self):
    template = jinja_environment.get_template('templates/index.html')

    # This is the low-level App Engine Channel API client_id, and should
    # be unique for each individual JavaScript client.
    # https://developers.google.com/appengine/docs/python/channel/functions
    user = users.get_current_user()
    client_id = hashlib.md5(user.email + os.environ['REQUEST_ID_HASH'])
    client_id = client_id.hexdigest()
    token = channel.create_channel(client_id)
    context = {
        'client_id': client_id,
        'token': token,
    }
    self.response.out.write(template.render(context))

class WidgetsTasksNewHandler(webapp2.RequestHandler):

  def post(self):
    # Create a new broadcast channel for each new widget creation task to avoid
    # collisions between task manager events.
    broadcast_channel_key = 'widgets-channel-%s' % os.environ['REQUEST_ID_HASH']
    task_manager = tasks.TaskManager.new(
        group='create-widgets',
        broadcast_channel_key=broadcast_channel_key)
    self.response.headers['Content-Type'] = 'application/json'
    self.response.out.write(json.dumps(task_manager.serialize(full=False)))

class WidgetsTasksStartHandler(webapp2.RequestHandler):

  def post(self):
    key = self.request.get('key')
    group = self.request.get('group', tasks.DEFAULT_GROUP)
    num_widgets = self.request.get('num_widgets', '')
    if not key or not num_widgets.isdigit():
      self.abort(400)
    task_manager = tasks.TaskManager(key=key, group=group)
    if not task_manager.exists:
      self.abort(404)

    # Defer the expensive tasks using the TaskManager.
    num_widgets = int(num_widgets)
    widget_ids = ['widget-%s' % i for i in range(num_widgets)]

    if len(widget_ids) < 100:
      # If we think that enqueuing the tasks will take < 60s, just do it
      # sychronously with the start request to not delay the UI.
      create_deferred_widgets(task_manager, widget_ids)
    else:
      # Since just enqueuing the tasks may take > 60s, defer a task that
      # enqueues the tasks so we can take up to 10 minutes.
      deferred.defer(create_deferred_widgets, task_manager, widget_ids,
                     _queue='deferrer')

    self.response.headers['Content-Type'] = 'application/json'
    self.response.out.write(json.dumps({'widget_ids': widget_ids}))

def create_deferred_widgets(task_manager, widget_ids):
  for task_key in widget_ids:
    task_manager.DeferTask(task_key, create_widget)
  task_manager.Finalize()

def create_widget():
  # A fake method.
  # Do something expensive, like create a new widget.
  time.sleep(random.uniform(0, 3))

  # Fail 7.6% of the tasks for demo purposes.
  if not random.randint(0, 13):
    raise tasks.TaskError('Check out this custom error message!')

jinja_environment = jinja2.Environment(
    loader=jinja2.FileSystemLoader(os.path.dirname(__file__)), autoescape=True)

application = webapp2.WSGIApplication((
    ('/', MainHandler),
    ('/api/widgets/tasks/new', WidgetsTasksNewHandler),
    ('/api/widgets/tasks/start', WidgetsTasksStartHandler),
), debug=True)
