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

"""Tests for stats.py."""

from tests.common import testing

import datetime
import mock
import os
from titan.common.lib.google.apputils import basetest
from titan import activities
from titan.stats import stats

class StatsTestCase(testing.BaseTestCase):

  @mock.patch('titan.stats.stats._get_window')
  def testCounterBuffer(self, internal_window):
    internal_window.return_value = 0
    page_counter = stats.Counter('page/view')
    widget_counter = stats.Counter('widget/render')
    page_counter.increment()
    page_counter.increment()

    # Buffered until save.
    def counters_func():
      return [stats.Counter('page/view'), stats.Counter('widget/render')]

    # Push local stats to a task.
    processors, processor = _log_counters([page_counter], counters_func)
    expected = {
        0: {
            'counters': {
                'page/view': 2,
            },
            'window': 0,
        }
    }
    self.assertEqual(expected, processor.serialize())

    widget_counter.offset(15)
    widget_counter.offset(-5)
    _log_counters([page_counter, widget_counter], counters_func,
                  processors=processors)
    expected = {
        0: {
            'counters': {
                'page/view': 4,  # Increased since it is saved twice.
                'widget/render': 10,
            },
            'window': 0,
        }
    }
    self.assertEqual(expected, processor.serialize())

    # finalize() should be idempotent.
    self.assertEqual(10, widget_counter.finalize())
    self.assertEqual(10, widget_counter.finalize())

  def testErrors(self):
    # Error handling:
    self.assertRaises(ValueError, stats.Counter, 'test:with:colons')
    self.assertRaises(ValueError, stats.Counter, '/test/left/slash')
    self.assertRaises(ValueError, stats.Counter, 'test/right/slash/')

  def testAverageCounter(self):
    # Aggregation within a request.
    counter = stats.AverageCounter('widget/render/latency')
    counter.offset(50)
    self.assertEqual((50.0, 1), counter.finalize())
    counter.offset(100)
    self.assertEqual((75.0, 2), counter.finalize())
    counter.offset(150)
    self.assertEqual((100.0, 3), counter.finalize())
    counter.offset(111)
    self.assertEqual((102.75, 4), counter.finalize())

    # Aggregation between requests.
    counter = stats.AverageCounter('widget/render/latency')
    counter.aggregate((100.0, 3))
    self.assertEqual((100.0, 3), counter.finalize())
    counter.aggregate((200.0, 3))
    self.assertEqual((150.0, 6), counter.finalize())
    counter.aggregate((111, 1))
    # ((100 * 3) + (200 * 3) + 111) / 7 = 144.4285714
    value, weight = counter.finalize()
    self.assertAlmostEqual(144.4285714, value)
    self.assertEqual(7, weight)

    # Aggregation between requests (with empty counter values).
    counter = stats.AverageCounter('widget/render/latency')
    counter.aggregate((0, 0))
    self.assertEqual((0, 0), counter.finalize())

  def testStaticCounter(self):
    # offset within a request.
    counter = stats.StaticCounter('widget/render/views')
    counter.offset(50)
    self.assertEqual(50, counter.finalize())
    counter.offset(100)
    self.assertEqual(150, counter.finalize())

    # Aggregation between requests.
    counter = stats.StaticCounter('widget/render/views')
    counter.aggregate(100)
    self.assertEqual(100, counter.finalize())
    counter.aggregate(200)
    self.assertEqual(200, counter.finalize())

  @mock.patch('time.time')
  def testAverageTimingCounter(self, internal_time):
    counter = stats.AverageTimingCounter('widget/render/latency')
    internal_time.side_effect = [10, 220]
    counter.start()
    counter.stop()
    value, weight = counter.finalize()
    self.assertEqual(210000, value)
    self.assertEqual(1, weight)

  @mock.patch('titan.stats.stats._get_window')
  def testAggregatorAndCountersService(self, internal_window):
    # Setup some data.
    def counters_func():
      return [stats.Counter('page/view'), stats.Counter('widget/render')]
    page_counter = stats.Counter('page/view')
    widget_counter = stats.Counter('widget/render')
    internal_window.return_value = 0

    # Log an initial set into the 3600 window.
    page_counter.offset(10)
    widget_counter.offset(20)
    processors, processor = _log_counters(
        [page_counter, widget_counter], counters_func, timestamp=3600)
    # Save another set of data, an hour later:
    page_counter.increment()
    widget_counter.increment()
    _log_counters([page_counter, widget_counter], counters_func,
                  processors=processors, timestamp=7200)
    # Save another set of data, a day later:
    page_counter.increment()
    widget_counter.increment()
    _log_counters([page_counter, widget_counter], counters_func,
                  processors=processors, timestamp=93600)
    expected = {
        3600: {
            'window': 3600,
            'counters': {
                'page/view': 10,
                'widget/render': 20,
            }
        },
        7200: {
            'window': 7200,
            'counters': {
                'page/view': 11,
                'widget/render': 21,
            }
        },
        93600: {
            'window': 93600,
            'counters': {
                'page/view': 12,
                'widget/render': 22,
            }
        }
    }
    self.assertEqual(expected, processor.serialize())

    # Save again, to make sure that duplicate data is collapsed when saved.
    _log_counters([page_counter, widget_counter], counters_func,
                  processors=processors, timestamp=93600)
    self.assertEqual(expected, processor.serialize())

    # Save the data as if from a batch processor.
    _finalize_processor(processor)

    # Get the data from the CountersService.
    counters_service = stats.CountersService()
    expected = {
        'page/view': [(3600, 10), (7200, 11), (93600, 12)],
        'widget/render': [(3600, 20), (7200, 21), (93600, 22)],
    }
    start_date = datetime.datetime.utcfromtimestamp(0)
    counter_data = counters_service.get_counter_data(
        ['page/view', 'widget/render'], start_date=start_date)
    self.assertEqual(expected, counter_data)

  @mock.patch('titan.stats.stats._get_window')
  def testAggregatorForStaticCounters(self, internal_window):
    # Setup some data.
    def counters_func():
      return [stats.StaticCounter('page/view')]
    counters_service = stats.CountersService()
    start_date = datetime.datetime.utcfromtimestamp(0)
    internal_window.return_value = 0

    # Initial counter with an offset.
    page_counter = stats.StaticCounter('page/view')
    page_counter.offset(10)
    _, processor = _log_counters([page_counter], counters_func)
    expected = {
        0: {
            'window': 0,
            'counters': {
                'page/view': 10,
            }
        },
    }
    self.assertEqual(expected, processor.serialize())

    # Save the data as if from a batch processor.
    _finalize_processor(processor)

    # Get the data from the CountersService.
    counter_data = counters_service.get_counter_data(
        ['page/view'], start_date=start_date)
    expected = {'page/view': [(0, 10)]}
    self.assertEqual(expected, counter_data)

    # New counter, same timeframe.
    page_counter = stats.StaticCounter('page/view')
    page_counter.offset(20)
    _, processor = _log_counters([page_counter], counters_func)
    expected = {
        0: {
            'window': 0,
            'counters': {
                'page/view': 20,
            }
        },
    }
    self.assertEqual(expected, processor.serialize())

    # Save the data as if from a batch processor.
    _finalize_processor(processor)

    # Get the data from the CountersService.
    counter_data = counters_service.get_counter_data(
        ['page/view'], start_date=start_date)

    # The new value should not have aggregated the previous counter data.
    expected = {'page/view': [(0, 20)]}
    self.assertEqual(expected, counter_data)

  @mock.patch('titan.stats.stats._get_window')
  def testAggregatorCounterUnusedCounterInWindow(self, internal_window):
    # Setup some data.
    def counters_func():
      return [stats.Counter('page/view'), stats.Counter('widget/render')]
    page_counter = stats.Counter('page/view')
    widget_counter = stats.Counter('widget/render')
    internal_window.return_value = 0

    # Log an initial set into the 3600 window.
    page_counter.offset(10)
    processors, processor = _log_counters([page_counter], counters_func)

    # Save different counter in a different window:
    widget_counter.increment()
    _log_counters([widget_counter], counters_func,
                  processors=processors, timestamp=3600)
    # Save both counters in a later window:
    page_counter.increment()
    widget_counter.increment()
    _log_counters([page_counter, widget_counter], counters_func,
                  processors=processors, timestamp=7200)
    expected = {
        0: {
            'window': 0,
            'counters': {
                'page/view': 10,
            }
        },
        3600: {
            'window': 3600,
            'counters': {
                'widget/render': 1,
            }
        },
        7200: {
            'window': 7200,
            'counters': {
                'page/view': 11,
                'widget/render': 2,
            }
        },
    }
    self.assertEqual(expected, processor.serialize())

    # Save the data as if from a batch processor.
    batch_processor = processor.batch_processor
    batch_processor.process(processor.serialize())
    batch_processor.finalize()

    # Get the data from the CountersService.
    counters_service = stats.CountersService()
    # The counters should not exist in an empty state between windows.
    expected = {
        'page/view': [(0, 10), (7200, 11)],
        'widget/render': [(3600, 1), (7200, 2)],
    }
    start_date = datetime.datetime.utcfromtimestamp(0)
    counter_data = counters_service.get_counter_data(
        ['page/view', 'widget/render'], start_date=start_date)
    self.assertEqual(expected, counter_data)

  @mock.patch('titan.stats.stats._get_window')
  def testSaveCountersAggregation(self, internal_window):
    internal_window.return_value = 0
    widget_counter1 = stats.Counter('widget/render')
    widget_counter2 = stats.Counter('widget/render')
    latency_counter1 = stats.AverageCounter('widget/render/latency')
    latency_counter2 = stats.AverageCounter('widget/render/latency')
    counters = [widget_counter1, widget_counter2]
    counters += [latency_counter1, latency_counter2]
    def counters_func():
      return [stats.Counter('widget/render'),
              stats.AverageCounter('widget/render/latency')]

    # log_counters should aggregate counters of the same type.
    # This is to make sure that different code paths in a request can
    # independently instantiate counter objects of the same name, and then the
    # intra-request counts will be aggregated together for the task data.
    widget_counter1.increment()
    widget_counter1.increment()
    widget_counter2.increment()
    latency_counter1.offset(50)
    latency_counter2.offset(100)
    _, processor = _log_counters(counters, counters_func)
    expected = {
        0: {
            'window': 0,
            'counters': {
                'widget/render': 3,
                'widget/render/latency': (75.0, 2),
            }
        }
    }
    self.assertEqual(expected, processor.serialize())

  @mock.patch('titan.stats.stats._get_window')
  def testManualCounterTimestamp(self, internal_window):
    def counters_func():
      return [stats.Counter('widget/render')]

    normal_counter = stats.Counter('widget/render')
    normal_counter.offset(20)
    internal_window.return_value = 10000

    old_counter = stats.Counter('widget/render')
    old_counter.offset(10)
    old_counter.timestamp = 3600.0

    oldest_counter = stats.Counter('widget/render')
    oldest_counter.offset(5)
    oldest_counter.timestamp = 0

    counters = [normal_counter, old_counter, oldest_counter]
    _, processor = _log_counters(counters, counters_func)

    expected = {
        0: {
            'window': 0,
            'counters': {
                'widget/render': 5,
            }
        },
        3600: {
            'window': 3600,
            'counters': {
                'widget/render': 10,
            }
        },
        10000: {
            'window': 10000,
            'counters': {
                'widget/render': 20,
            }
        },
    }
    self.assertEqual(expected, processor.serialize())

  def testMakeLogPath(self):
    test_date = datetime.date(2013, 5, 1)

    # Default date format and filename.
    counter = stats.Counter('test/counter')
    self.assertEqual(
        '/_titan/stats/counters/2013/05/01/test/counter/data-60s.json',
        stats._make_log_path(test_date, counter))

    # Custom formatted date.
    counter = stats.Counter('test/counter', date_format='%Y/%m')
    self.assertEqual(
        '/_titan/stats/counters/2013/05/test/counter/data-60s.json',
        stats._make_log_path(test_date, counter))

    # Custom filename.
    counter = stats.Counter('test/counter', data_filename='my/data.json')
    self.assertEqual(
        '/_titan/stats/counters/2013/05/01/test/counter/my/data.json',
        stats._make_log_path(test_date, counter))

class StatsDecoratorTestCase(testing.BaseTestCase):

  @mock.patch('time.time')
  def testAverageTime(self, internal_time):
    """Ensures that the AverageTime decorator runs as expected."""

    @stats.AverageTime('page/render')
    def render_page(num, text_format='{}'):
      return text_format.format(1 + num)

    # Processor should be there after the function definition.
    loggers = _get_loggers_from_environ()
    self.assertEqual(1, len(loggers))
    logger = loggers[0]

    # Get the counter being used for the decorator.
    counter = logger.activity.counters[0]

    # Call once.
    internal_time.side_effect = [10, 60]
    result = render_page(1, text_format='something {}')
    self.assertEqual('something 2', result)  # Method still works.

    # Confirm counter ran.
    self.assertEqual((50000.0, 1), counter.finalize())

    # Call again.
    internal_time.side_effect = [32, 132]
    result = render_page(3, text_format='else {}')
    self.assertEqual('else 4', result)  # Method still works.

    # Confirm the counter ran and averaged.
    self.assertEqual((75000.0, 2), counter.finalize())

    # Still only one logger, even though called multiple times.
    loggers = _get_loggers_from_environ()
    self.assertEqual(1, len(loggers))

  def testCount(self):
    """Ensures that the Count decorator runs as expected."""

    @stats.Count('page/render')
    def render_page(num, text_format='{}'):
      return text_format.format(1 + num)

    # Processor should be there after the function definition.
    loggers = _get_loggers_from_environ()
    self.assertEqual(1, len(loggers))
    logger = loggers[0]

    # Get the counter being used for the decorator.
    counter = logger.activity.counters[0]

    # Call once.
    result = render_page(1, text_format='something {}')
    self.assertEqual('something 2', result)  # Method still works.

    # Confirm counter ran.
    self.assertEqual(1, counter.finalize())

    # Call again.
    result = render_page(3, text_format='else {}')
    self.assertEqual('else 4', result)  # Method still works.

    # Confirm the counter ran.
    self.assertEqual(2, counter.finalize())

    # Still only one logger, even though called multiple times.
    loggers = _get_loggers_from_environ()
    self.assertEqual(1, len(loggers))

def _finalize_processor(processor):
  # Save the data as if from a batch processor.
  batch_processor = processor.batch_processor
  batch_processor.process(processor.serialize())
  batch_processor.finalize()

def _get_loggers_from_environ():
  return os.environ[activities.ACTIVITIES_ENVIRON_KEY]

def _get_processor_from_environ():
  processors = _get_loggers_from_environ().processors
  return _get_processor_from_processors(processors)

def _get_processor_from_processors(processors):
  return [p for p in processors.values()][0]

def _log_counters(counters, counters_func, processors=None,
                  timestamp=None):
  if timestamp is not None:
    for counter in counters:
      counter.timestamp = timestamp
  activity = stats.StatsActivity(counters=counters)
  activity_logger = stats.StatsActivityLogger(
      activity, counters_func=counters_func)
  processors = processors or activity_logger.processors
  activity_logger.process(processors)
  return (processors, _get_processor_from_processors(processors))

if __name__ == '__main__':
  basetest.main()
