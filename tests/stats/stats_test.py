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
from titan.common.lib.google.apputils import basetest
from titan.stats import stats

class StatsTestCase(testing.BaseTestCase):

  @mock.patch('titan.stats.stats._GetWindow')
  def testCounterBuffer(self, internal_window):
    internal_window.return_value = 0
    page_counter = stats.Counter('page/view')
    widget_counter = stats.Counter('widget/render')
    page_counter.Increment()
    page_counter.Increment()

    # Buffered until save.
    def counters_func():
      return [stats.Counter('page/view'), stats.Counter('widget/render')]

    # Push local stats to a task.
    processors = self._log_counters([page_counter], counters_func)
    processor = self._get_processor_from_processors(processors)
    expected = {
        0: {
            'counters': {
                'page/view': 2,
            },
            'window': 0,
        }
    }
    self.assertEqual(expected, processor.serialize())

    widget_counter.Offset(15)
    widget_counter.Offset(-5)
    self._log_counters([page_counter, widget_counter], counters_func,
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

    # Finalize() should be idempotent.
    self.assertEqual(10, widget_counter.Finalize())
    self.assertEqual(10, widget_counter.Finalize())

  def testErrors(self):
    # Error handling:
    self.assertRaises(ValueError, stats.Counter, 'test:with:colons')
    self.assertRaises(ValueError, stats.Counter, '/test/left/slash')
    self.assertRaises(ValueError, stats.Counter, 'test/right/slash/')

  def testAverageCounter(self):
    # Aggregation within a request.
    counter = stats.AverageCounter('widget/render/latency')
    counter.Offset(50)
    self.assertEqual((50.0, 1), counter.Finalize())
    counter.Offset(100)
    self.assertEqual((75.0, 2), counter.Finalize())
    counter.Offset(150)
    self.assertEqual((100.0, 3), counter.Finalize())
    counter.Offset(111)
    self.assertEqual((102.75, 4), counter.Finalize())

    # Aggregation between requests.
    counter = stats.AverageCounter('widget/render/latency')
    counter.Aggregate((100.0, 3))
    self.assertEqual((100.0, 3), counter.Finalize())
    counter.Aggregate((200.0, 3))
    self.assertEqual((150.0, 6), counter.Finalize())
    counter.Aggregate((111, 1))
    # ((100 * 3) + (200 * 3) + 111) / 7 = 144.4285714
    value, weight = counter.Finalize()
    self.assertAlmostEqual(144.4285714, value)
    self.assertEqual(7, weight)

    # Aggregation between requests (with empty counter values).
    counter = stats.AverageCounter('widget/render/latency')
    counter.Aggregate((0, 0))
    self.assertEqual((0, 0), counter.Finalize())

  def testAverageTimingCounter(self):
    counter = stats.AverageTimingCounter('widget/render/latency')
    counter.Start()
    counter.Stop()
    value, weight = counter.Finalize()
    self.assertTrue(isinstance(value, int))
    self.assertEqual(1, weight)

  @mock.patch('titan.stats.stats._GetWindow')
  def testAggregatorAndCountersService(self, internal_window):
    # Setup some data.
    def counters_func():
      return [stats.Counter('page/view'), stats.Counter('widget/render')]
    page_counter = stats.Counter('page/view')
    widget_counter = stats.Counter('widget/render')
    internal_window.return_value = 0

    # Log an initial set into the 3600 window.
    page_counter.Offset(10)
    widget_counter.Offset(20)
    processors = self._log_counters([page_counter, widget_counter],
                                    counters_func, timestamp=3600)
    processor = self._get_processor_from_processors(processors)
    # Save another set of data, an hour later:
    page_counter.Increment()
    widget_counter.Increment()
    self._log_counters([page_counter, widget_counter], counters_func,
                       processors=processors, timestamp=7200)
    # Save another set of data, a day later:
    page_counter.Increment()
    widget_counter.Increment()
    self._log_counters([page_counter, widget_counter], counters_func,
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
    self._log_counters([page_counter, widget_counter], counters_func,
                       processors=processors, timestamp=93600)
    self.assertEqual(expected, processor.serialize())

    # Save the data as if from a batch processor.
    batch_processor = processor.batch_processor
    batch_processor.process(processor.serialize())
    batch_processor.finalize()

    # Get the data from the CountersService.
    counters_service = stats.CountersService()
    expected = {
        'page/view': [(3600, 10), (7200, 11), (93600, 12)],
        'widget/render': [(3600, 20), (7200, 21), (93600, 22)],
    }
    start_date = datetime.datetime.utcfromtimestamp(0)
    counter_data = counters_service.GetCounterData(
        ['page/view', 'widget/render'], start_date=start_date)
    self.assertEqual(expected, counter_data)

  @mock.patch('titan.stats.stats._GetWindow')
  def testAggregatorCounterUnusedCounterInWindow(self, internal_window):
    # Setup some data.
    def counters_func():
      return [stats.Counter('page/view'), stats.Counter('widget/render')]
    page_counter = stats.Counter('page/view')
    widget_counter = stats.Counter('widget/render')
    internal_window.return_value = 0

    # Log an initial set into the 3600 window.
    page_counter.Offset(10)
    processors = self._log_counters([page_counter],
                                    counters_func)
    processor = self._get_processor_from_processors(processors)

    # Save different counter in a different window:
    widget_counter.Increment()
    self._log_counters([widget_counter], counters_func,
                       processors=processors, timestamp=3600)
    # Save both counters in a later window:
    page_counter.Increment()
    widget_counter.Increment()
    self._log_counters([page_counter, widget_counter], counters_func,
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
    counter_data = counters_service.GetCounterData(
        ['page/view', 'widget/render'], start_date=start_date)
    self.assertEqual(expected, counter_data)

  @mock.patch('titan.stats.stats._GetWindow')
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

    # SaveCounters should aggregate counters of the same type.
    # This is to make sure that different code paths in a request can
    # independently instantiate counter objects of the same name, and then the
    # intra-request counts will be aggregated together for the task data.
    widget_counter1.Increment()
    widget_counter1.Increment()
    widget_counter2.Increment()
    latency_counter1.Offset(50)
    latency_counter2.Offset(100)
    processors = self._log_counters(counters, counters_func)
    processor = self._get_processor_from_processors(processors)
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

  @mock.patch('titan.stats.stats._GetWindow')
  def testManualCounterTimestamp(self, internal_window):
    def counters_func():
      return [stats.Counter('widget/render')]

    normal_counter = stats.Counter('widget/render')
    normal_counter.Offset(20)
    internal_window.return_value = 10000

    old_counter = stats.Counter('widget/render')
    old_counter.Offset(10)
    old_counter.timestamp = 3600.0

    oldest_counter = stats.Counter('widget/render')
    oldest_counter.Offset(5)
    oldest_counter.timestamp = 0

    counters = [normal_counter, old_counter, oldest_counter]
    processors = self._log_counters(counters, counters_func)
    processor = self._get_processor_from_processors(processors)

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

  def _log_counters(self, counters, counters_func, processors=None,
                    timestamp=None):
    if timestamp is not None:
      for counter in counters:
        counter.timestamp = timestamp
    activity = stats.StatsActivity(counters=counters)
    activity_logger = stats.StatsActivityLogger(
        activity, counters_func=counters_func)
    processors = processors or activity_logger.processors
    activity_logger.process(processors)
    return processors

  def _get_processor_from_processors(self, processors):
    return [p for p in processors.values()][0]

if __name__ == '__main__':
  basetest.main()
