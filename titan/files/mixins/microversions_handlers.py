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

"""Handlers for Titan Microversions."""

import webapp2
from titan.common import handlers
from titan.files.mixins import microversions

class ProcessMicroversionsHandler(handlers.BaseHandler):
  """Cron job handlers for microversions."""

  def get(self):
    # Must be a GET handler because this runs in a cron job.
    max_tasks = int(self.request.get('max_tasks', 0))
    if max_tasks:
      self.WriteJsonResponse(microversions.ProcessData(max_tasks=max_tasks))
    else:
      self.WriteJsonResponse(microversions.ProcessDataWithBackoff())

ROUTES = (
    ('/_titan/files/microversions/processdata', ProcessMicroversionsHandler),
)
application = webapp2.WSGIApplication(ROUTES, debug=False)
