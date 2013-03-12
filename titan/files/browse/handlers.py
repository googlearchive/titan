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

"""App Engine handlers for Titan Files browser."""

import os
import jinja2
import webapp2

from titan.common import handlers

jinja_environment = jinja2.Environment(
    loader=jinja2.FileSystemLoader(os.path.dirname(__file__)))

class FilesBrowseHandler(handlers.BaseHandler):
  """Handler for file browsing."""

  def get(self):
    template = jinja_environment.get_template('templates/index.html')
    self.response.out.write(template.render())

ROUTES = (
    ('/_titan/files/browse', FilesBrowseHandler),
)
application = webapp2.WSGIApplication(ROUTES, debug=False)
