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

"""Handlers for stats module."""

try:
  import appengine_config
except ImportError:
  pass

import datetime
import json
import os
import time
from django import template
import webapp2
from titan.stats import stats

TEMPLATES_PATH = os.path.join(os.path.dirname(__file__), 'templates')

class CounterDataHandler(webapp2.RequestHandler):
  """Handler for getting counter data."""

  def get(self):
    """GET request handler.

    Params:
      counter_name: A counter name. Multiple names allowed.
      start_date: An ISO-8601 date string.
      end_date: An ISO-8601 date string.
    Returns:
      JSON response of the aggregate counter data from
      CountersService.GetCounterData().
    """
    params = _ParseRequestParams(self.request)
    counters_service = stats.CountersService()
    aggregate_data = counters_service.GetCounterData(
        counter_names=params['counter_names'],
        start_date=params['start_date'],
        end_date=params['end_date'])
    self.response.headers['Content-Type'] = 'application/json'
    self.response.out.write(aggregate_data)

class GraphHandler(webapp2.RequestHandler):
  """Handler for graphing counter data."""

  def get(self):
    """GET request handler.

    Params:
      counter_name: A counter name. Multiple names allowed.
      start_date: An ISO-8601 date string.
      end_date: An ISO-8601 date string.
    Returns:
      Rendered HTML template with graph of counter data.
    """
    params = _ParseRequestParams(self.request)

    counters_service = stats.CountersService()
    aggregate_data = counters_service.GetCounterData(
        counter_names=params['counter_names'],
        start_date=params['start_date'],
        end_date=params['end_date'])
    # Render template:
    path = os.path.join(TEMPLATES_PATH, 'graph.html')
    tpl = template.Template(open(path).read())
    data = {
        'aggregate_data': aggregate_data,
    }
    context = template.Context(data)
    self.response.out.write(tpl.render(context))

def _ParseRequestParams(request):
  counter_names = request.get_all('counter_name')
  start_date = request.get('start_date')
  end_date = request.get('end_date')
  if start_date:
    parsed_time = time.strptime(start_date, '%Y-%m-%d')
    start_date = datetime.date(
        parsed_time.tm_year, parsed_time.tm_mon, parsed_time.tm_mday)
  if end_date:
    parsed_time = time.strptime(end_date, '%Y-%m-%d')
    end_date = datetime.date(
        parsed_time.tm_year, parsed_time.tm_mon, parsed_time.tm_mday)
  result = {
      'counter_names': counter_names,
      'start_date': start_date,
      'end_date': end_date,
  }
  return result

URL_MAP = (
    ('/_titan/stats/counterdata', CounterDataHandler),
    ('/_titan/stats/graph', GraphHandler),
)
application = webapp2.WSGIApplication(URL_MAP, debug=False)
