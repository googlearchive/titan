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

"""Demo app for the Titan Channel API."""

import cgi
import hashlib
import os

import jinja2
import webapp2

from titan import channel
from titan import users

class MainHandler(webapp2.RequestHandler):

  def get(self):
    # This is the low-level App Engine Channel API client_id, and should
    # be unique for each individual JavaScript client.
    # https://developers.google.com/appengine/docs/python/channel/functions
    user = users.get_current_user()
    # Make each client unique based on the current user email and request ID.
    client_id = hashlib.md5(user.email + os.environ['REQUEST_ID_HASH'])
    client_id = client_id.hexdigest()
    token = channel.create_channel(client_id)
    context = {
        'client_id': client_id,
        'broadcast_channel_key': _GetBroadcastChannelKey(),
        'token': token,
    }
    template = jinja_environment.get_template('templates/index.html')
    self.response.out.write(template.render(context))

class JoinRoomHandler(webapp2.RequestHandler):

  def post(self):
    client_id = self.request.get('client_id')
    broadcast_channel = channel.BroadcastChannel(key=_GetBroadcastChannelKey())
    broadcast_channel.subscribe(client_id)

class SendMessageHandler(webapp2.RequestHandler):

  def post(self):
    user = users.get_current_user()
    message = self.request.get('message')
    broadcast_channel = channel.BroadcastChannel(key=_GetBroadcastChannelKey())
    message = '%s: %s' % (user.email, cgi.escape(message, quote=True))
    broadcast_channel.send_message(message)

def _GetBroadcastChannelKey():
  user = users.get_current_user()
  return hashlib.md5(user.email).hexdigest()

jinja_environment = jinja2.Environment(
    loader=jinja2.FileSystemLoader(os.path.dirname(__file__)), autoescape=True)

application = webapp2.WSGIApplication([
    ('/', MainHandler),
    ('/join', JoinRoomHandler),
    ('/sendmessage', SendMessageHandler),
], debug=True)
