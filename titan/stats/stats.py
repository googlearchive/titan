#!/usr/bin/env python
# Copyright 2012 Google Inc. All Rights Reserved.
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

"""Customizable numeric counters for recording time-tracked app statistics.

Usage:
  # Define a function that can be used to create fresh counters for aggregation.
  def make_counters():
    return [stats.Counter('page/view')]

  # Increment a counter for some particular stat, like a page view:
  page_view_counter = stats.Counter('page/view')
  page_view_counter.Increment()

  # Or measure the average latency of any arbitrary code block:
  latency_counter = stats.AverageTimingCounter('widget/render/latency')
  latency_counter.Start()
  # ... code block ...
  latency_counter.Stop()

  # Log the counters using Titan Activities.
  stats.log_counters([latency_counter, page_view_counter],
                     counters_func=make_counters)

  # Process the logged Titan Activities.
  # (this should happen at the absolute end of a request).
  activities.process_activity_loggers()

Internal design and terminology:
  - "Window" or "aggregation window" used below is a unix timestamp rounded to
    some number of seconds. Each window can be thought of as a bucket of time
    used to hold counter data. Each window is also the unit-of-aggregation for
    data, meaning that if the window size is 60 seconds, there will potentially
    be 1440 data points stored permanently per day, per counter (since there
    are 1440 minutes in a day).

  - The counters work in two steps:
    1. Application code creates, increments, and saves counters during a
       request. Request counters are aggregated and sent into a pipeline using
       the Titan Activities logging.
    2. The activities pipeline runs to collect, aggregate, and store any pending
       counter data.
"""

import datetime
import json
import logging
import os
import time
from titan import activities
from titan import files
from titan.common import utils

__all__ = [
    # Constants.
    'DEFAULT_WINDOW_SIZE',
    'STATS_ETA_DELTA',
    'BASE_DIR',
    'DATA_FILENAME',
    # Classes.
    'AbstractBaseCounter',
    'Counter',
    'AverageCounter',
    'AverageCounter',
    'StaticCounter',
    'AverageTimingCounter',
    'CountersService',
    'StatsActivity',
    'StatsActivityLogger',
    # Functions.
    'log_counters',
]

# The bucket size for an aggregation window, in number of seconds.
DEFAULT_WINDOW_SIZE = 60
STATS_ETA_DELTA = 60

BASE_DIR = '/_titan/stats/counters'
DATA_FILENAME = 'data-%ss.json' % DEFAULT_WINDOW_SIZE

class AbstractBaseCounter(object):
  """Base class for all counters."""

  # Aggregate existing counter data (if different) instead of overwriting.
  overwrite = False

  def __init__(self, name):
    self.name = name
    # If this property is changed, the window is not calculated automatically.
    self.timestamp = None
    if ':' in self.name:
      raise ValueError('":" is not allowed in counter name: %s'
                       % name)
    if name.startswith('/') or name.endswith('/'):
      raise ValueError('"/" is not allowed to begin or end counter name: %s'
                       % name)

  def __repr__(self):
    return '<%s %s>' % (self.__class__.__name__, self.name)

  def Aggregate(self, value):
    """Abstract method, must aggregate data together before Finalize."""
    raise NotImplementedError('Subclasses should implement abstract method.')

  def Finalize(self):
    """Abstract method; must be idempotent and return finalized counter data."""
    raise NotImplementedError('Subclasses should implement abstract method.')

class Counter(AbstractBaseCounter):
  """The simplest of counters; providing offsets to a single value."""

  def __init__(self, *args, **kwargs):
    super(Counter, self).__init__(*args, **kwargs)
    self._value = 0

  def __repr__(self):
    return '<Counter %s %s>' % (self.name, self._value)

  def Increment(self):
    """Increment the counter by one."""
    self._value += 1

  def Offset(self, value):
    """Offset the counter by some value."""
    self._value += value

  def Aggregate(self, value):
    self.Offset(value)

  def Finalize(self):
    return self._value

class AverageCounter(Counter):
  """A cumulative moving average counter.

  Each data point will represent the average during the aggregation window;
  averages are not affected by values from previous aggregation windows.
  """

  def __init__(self, *args, **kwargs):
    super(AverageCounter, self).__init__(*args, **kwargs)
    self._weight = 0

  def Increment(self):
    self.Aggregate((1, 1))  # (value, weight)

  def Offset(self, value):
    self.Aggregate((value, 1))  # (value, weight)

  def Aggregate(self, value):
    """Combine another average counter's values into this counter."""
    # Cumulative moving average:
    # (n*weight(n) + m*weight(m)) / (weight(n) + weight(m))
    value, weight = value

    # Ignore empty weight aggregation since it is an empty counter.
    if weight == 0:
      return

    # Numerator:
    self._value *= self._weight
    self._value += value * weight
    # Denominator:
    self._weight += weight
    self._value /= float(self._weight)

  def Finalize(self):
    return (self._value, self._weight)

class AverageTimingCounter(AverageCounter):
  """An AverageCounter with convenience methods for timing code blocks.

  Records data in millisecond integers.

  Usage:
    timing_counter = AverageTimingCounter('page/render/latency')
    timing_counter.Start()
    ...page render logic...
    timing_counter.Stop()
  """

  def __init__(self, *args, **kwargs):
    super(AverageTimingCounter, self).__init__(*args, **kwargs)
    self._start = None

  def Start(self):
    assert self._start is None, 'Counter started again without stopping.'
    self._start = time.time()

  def Stop(self):
    assert self._start is not None, 'Counter stopped without starting.'
    self.Offset(int((time.time() - self._start) * 1000))
    self._start = None

  def Finalize(self):
    assert self._start is None, 'Counter finalized without stopping.'
    value, weight = super(AverageTimingCounter, self).Finalize()
    return (int(value), weight)

class StaticCounter(Counter):
  """Static version of the counter that replaces instead of aggregates."""

  # Ignore existing counters in file and overwrite.
  overwrite = True

  def Aggregate(self, value):
    """Replaces the current value completely rather than offsetting."""
    self._value = value

class CountersService(object):
  """A service class to retrieve permanently stored counter stats."""

  def GetCounterData(self, counter_names, start_date=None, end_date=None):
    """Get a date range of stored counter data.

    Args:
      counter_names: An iterable of counter names.
      start_date: A datetime.date object. Defaults to the current day.
      end_date: A datetime.date object. Defaults to current day.
    Raises:
      ValueError: if end time is greater than start time.
    Returns:
      A dictionary mapping counter_names to a list of counter data. For example:
      {
          'page/view': [(<window>, <value>), (<window>, <value>), ...],
      }
    """
    if end_date and end_date < start_date:
      raise ValueError('End time %s must be greater than start time %s'
                       % (end_date, start_date))
    now = datetime.datetime.now()
    if not start_date:
      start_date = now
    if not end_date:
      end_date = now

    # Convert to datetime objects for the queries:
    start_date = datetime.datetime(
        start_date.year, start_date.month, start_date.day)
    end_date = datetime.datetime(
        end_date.year, end_date.month, end_date.day)

    # Get all files within the range.
    titan_files = files.Files([])
    for counter_name in counter_names:
      filters = [
          files.FileProperty('stats_counter_name') == counter_name,
          files.FileProperty('stats_date') >= start_date,
          files.FileProperty('stats_date') <= end_date,
      ]
      new_titan_files = files.Files.list(BASE_DIR, recursive=True,
                                         filters=filters, _internal=True)
      titan_files.update(new_titan_files)

    final_counter_data = {}
    for titan_file in titan_files.itervalues():
      # Since JSON only represents lists, convert each inner-list back
      # to a two-tuple with the proper types.
      raw_data = json.loads(titan_file.content)
      counter_data = []
      for data in raw_data:
        counter_data.append(tuple(data))
      _, counter_name = _ParseLogPath(titan_file.path)
      if not counter_name in final_counter_data:
        final_counter_data[counter_name] = []
      final_counter_data[counter_name].extend(counter_data)

    # Keep the counter data sorted by window.
    for counter_data in final_counter_data.itervalues():
      counter_data.sort(key=lambda tup: tup[0])
    return final_counter_data

class StatsActivity(object):
  """A stat activity."""

  def __init__(self, counters):
    super(StatsActivity, self).__init__()
    self.counters = counters

class StatsActivityLogger(activities.BaseProcessorActivityLogger):
  """An activity for logging Stat counters."""

  def __init__(self, activity, counters_func, **kwargs):
    super(StatsActivityLogger, self).__init__(activity, **kwargs)

    self.counters_func = counters_func

  @property
  def processors(self):
    """Add the aggregator to the set of processors."""
    processors = super(StatsActivityLogger, self).processors
    if _RequestProcessor not in processors:
      processors[_RequestProcessor] = _RequestProcessor(self.counters_func)
    return processors

  def process(self, processors):
    """Add item to the processors."""
    super(StatsActivityLogger, self).process(processors)
    processors[_RequestProcessor].process(self.activity)

class _BatchProcessor(activities.BaseProcessor):
  """Batch request aggregator for Stat counters."""

  def __init__(self, counters_func):
    super(_BatchProcessor, self).__init__(
        'stats-batch', eta_delta=datetime.timedelta(seconds=STATS_ETA_DELTA))
    self.counters_func = counters_func
    self.window_counters = {}
    self.window_counters_available = {}

  def finalize(self):
    """Store the aggregated stats data."""
    final_aggregate_data = []

    for window, counters in self.window_counters.iteritems():
      aggregate_data = {
          'counters': {},
          'window': window,
      }
      for counter in counters.itervalues():
        if counter.name not in self.window_counters_available[window]:
          # Don't store anything for counters with no data in this window.
          continue
        aggregate_data['counters'][counter.name] = counter
      # Only add to the final aggregates if there were valid counters.
      if aggregate_data['counters']:
        final_aggregate_data.append(aggregate_data)

    # Save the aggregated counters.
    if final_aggregate_data:
      self._save_aggregate_data(final_aggregate_data)

  def process(self, window_counter_data):
    """Aggregate the request stat counters."""
    for data in window_counter_data.itervalues():
      # Make sure that the counters are created for the window.
      window = data['window']
      self._init_counters(window)

      # Aggregate the counter data into each counter object.
      for counter_name, counter_value in data['counters'].iteritems():
        try:
          self.window_counters[window][counter_name].Aggregate(counter_value)
          self.window_counters_available[window].add(counter_name)
        except KeyError:
          logging.error('Counter named "%s" is not configured! Discarding '
                        'counter task data... fix this by adding the counter '
                        'to the objects created in the `counters_func`.',
                        counter_name)

  def _init_counters(self, window):
    self.window_counters[window] = {}
    self.window_counters_available[window] = set()
    counters = self.counters_func()
    for counter in counters:
      if counter.name not in self.window_counters:
        self.window_counters[window][counter.name] = counter

  def _save_aggregate_data(self, final_aggregate_data):
    """Permanently store aggregate data to Titan Files."""

    # Combine all data before writing files to minimize same file writes.
    window_files = {}
    for aggregate_data in final_aggregate_data:
      window = aggregate_data['window']
      window_datetime = datetime.datetime.utcfromtimestamp(window)
      for counter_name, counter in aggregate_data['counters'].iteritems():
        path = _MakeLogPath(window_datetime, counter_name)
        if path not in window_files:
          titan_file = files.File(path, _internal=True)
          content = []
          if titan_file.exists:
            content = json.loads(titan_file.content)
          window_files[path] = {
              'file': titan_file,
              'path': path,
              'content': content,
              'counter_name': counter_name,
              'window_datetime': window_datetime,
          }

        # Add the counter data if it doesn't exist or is different.
        old_content = window_files[path]['content']
        window_files[path]['content'] = []
        window_exists = False
        for old_window, old_value in old_content:
          # If we didn't find the window add it as a new counter.
          if old_window > window and not window_exists:
            window_exists = True
            window_files[path]['content'].append((window, counter.Finalize()))
          # If the data is the same ignore, otherwise add old data to new.
          if old_window == window:
            window_exists = True
            if old_value != counter.Finalize():
              if not counter.overwrite:
                counter.Aggregate(old_value)
              old_value = counter.Finalize()
          window_files[path]['content'].append((old_window, old_value))
        if not window_exists:
          window_files[path]['content'].append((window, counter.Finalize()))

        # Keep the data sorted for update efficiency.
        window_files[path]['content'].sort(key=lambda tup: tup[0])

    # Write the changed window files.
    for file_item in window_files.itervalues():
      # Strip hours/minutes/seconds from date since the datastore can only
      # store datetime objects, but we only need the date itself.
      window_datetime = file_item['window_datetime']
      date = datetime.datetime(
          window_datetime.year, window_datetime.month, window_datetime.day)
      meta = {
          'stats_counter_name': file_item['counter_name'],
          'stats_date': date,
      }
      file_item['file'].write(content=json.dumps(file_item['content']),
                              meta=meta)

class _RequestProcessor(activities.BaseProcessor):
  """End of request aggregator for Stat counters."""

  def __init__(self, counters_func):
    super(_RequestProcessor, self).__init__('stats')

    self.counters_func = counters_func
    self.final_counters = []
    self.names_to_counters = {}
    self.window_counter_data = {}
    self.default_window = _GetWindow(time.time())

  @property
  def batch_processor(self):
    """Return a clean processor for processing batch aggregations."""
    return _BatchProcessor(self.counters_func)

  def process(self, activity):
    """Aggregate the request counters."""
    counters = activity.counters
    if not counters:  # Ignore empty counter activities.
      return

    # Aggregate data from counters of the same name.
    for counter in counters:
      if counter.name not in self.names_to_counters:
        self.names_to_counters[counter.name] = counter
        self.final_counters.append(counter)
      else:
        if counter.timestamp is not None:
          # Counter has a manual timestamp applied; treat this as it's own
          # unique counter and don't aggregate data.
          self.final_counters.append(counter)
          continue
        # Counter name already seen; combine data into the previous counter.
        self.names_to_counters[counter.name].Aggregate(counter.Finalize())

    # Break the counters up into windows of time.
    for counter in self.final_counters:
      # Each counter can potentially belong to a different window, because
      # each timestamp can be overwritten:
      if counter.timestamp is None:
        window = self.default_window
      else:
        window = counter.timestamp
      if window not in self.window_counter_data:
        self.window_counter_data[window] = {'window': window, 'counters': {}}
      self.window_counter_data[window]['counters'][counter.name] = (
          counter.Finalize())

  def serialize(self):
    return self.window_counter_data

def log_counters(counters, counters_func):
  """Logs an stat counter."""
  activity = StatsActivity(counters=counters)
  activity_logger = StatsActivityLogger(
      activity, counters_func=counters_func)
  activity_logger.store()
  # If inside of a task then process now instead of waiting.
  if 'HTTP_X_APPENGINE_TASKNAME' in os.environ:
    activities.process_activity_loggers()
  return activity

def _GetWindow(timestamp, window_size=DEFAULT_WINDOW_SIZE):
  """Get the aggregation window for the given unix time and window size."""
  return int(window_size * round(float(timestamp) / window_size))

def _MakeLogPath(date, counter_name):
  # Make a path like:
  # /_titan/activities/stats/counters/2015/05/15/page/view/data-10s.json
  path = utils.safe_join(
      BASE_DIR, str(date.year), str(date.month), str(date.day),
      counter_name, DATA_FILENAME)
  return path

def _ParseLogPath(path):
  parts = path.split('/')
  date = datetime.date(int(parts[4]), int(parts[5]), int(parts[6]))
  counter_name = '/'.join(parts[7:-1])
  return date, counter_name
