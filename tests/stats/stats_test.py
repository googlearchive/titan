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
from titan.common.lib.google.apputils import basetest
from titan.stats import stats

class StatsTestCase(testing.BaseTestCase):

  def setUp(self):
    super(StatsTestCase, self).setUp()
    self.stubs.SmartSet(stats, 'TASKQUEUE_LEASE_BUFFER_SECONDS', 0)

  def testCounterBuffer(self):
    page_counter = stats.Counter('page/view')
    widget_counter = stats.Counter('widget/render')
    page_counter.Increment()
    page_counter.Increment()

    # Buffered until save.
    all_counters = [stats.Counter('page/view'), stats.Counter('widget/render')]
    aggregator = stats.Aggregator(all_counters)
    self.assertEqual({}, aggregator.ProcessNextWindow())

    # Push local stats to a task.
    stats.SaveCounters([page_counter], timestamp=0)
    expected = {
        'counters': {
            'page/view': 2,
        },
        'window': 0,
    }
    self.assertEqual(expected, aggregator.ProcessNextWindow())
    widget_counter.Offset(15)
    widget_counter.Offset(-5)
    stats.SaveCounters([page_counter, widget_counter], timestamp=0)
    expected = {
        'counters': {
            'page/view': 4,  # Increased since it is saved twice.
            'widget/render': 10,
        },
        'window': 0,
    }
    all_counters = [stats.Counter('page/view'), stats.Counter('widget/render')]
    aggregator = stats.Aggregator(all_counters)

    # Finalize() should be idempotent.
    self.assertEqual(10, widget_counter.Finalize())
    self.assertEqual(10, widget_counter.Finalize())

    # Save aggregate information to Titan Files.
    aggregator.ProcessNextWindow()

    # The next window should not contain any data.
    all_counters = [stats.Counter('page/view'), stats.Counter('widget/render')]
    aggregator = stats.Aggregator(all_counters)
    self.assertEqual({}, aggregator.ProcessNextWindow())

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

    # Aggregation between requests (from the cron job).
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

  def testAverageTimingCounter(self):
    counter = stats.AverageTimingCounter('widget/render/latency')
    counter.Start()
    counter.Stop()
    value, weight = counter.Finalize()
    self.assertTrue(isinstance(value, int))
    self.assertEqual(1, weight)

  def testAggregatorAndCountersService(self):
    # Setup some data.
    page_counter = stats.Counter('page/view')
    widget_counter = stats.Counter('widget/render')
    page_counter.Offset(10)
    widget_counter.Offset(20)
    stats.SaveCounters([page_counter, widget_counter], timestamp=3600)
    # Save another set of data, an hour later:
    page_counter.Increment()
    widget_counter.Increment()
    stats.SaveCounters([page_counter, widget_counter], timestamp=7200)
    # Save another set of data, a day later:
    page_counter.Increment()
    widget_counter.Increment()
    stats.SaveCounters([page_counter, widget_counter], timestamp=93600)

    expected = {
        'window': 3600,
        'counters': {
            'page/view': 10,
            'widget/render': 20,
        }
    }
    all_counters = [stats.Counter('page/view'), stats.Counter('widget/render')]
    aggregator = stats.Aggregator(all_counters)
    aggregate_data = aggregator.ProcessNextWindow()
    self.assertEqual(expected, aggregate_data)

    expected = {
        'window': 7200,
        'counters': {
            'page/view': 11,
            'widget/render': 21,
        }
    }
    all_counters = [stats.Counter('page/view'), stats.Counter('widget/render')]
    aggregator = stats.Aggregator(all_counters)
    aggregate_data = aggregator.ProcessNextWindow()
    self.assertEqual(expected, aggregate_data)

    expected = {
        'window': 93600,
        'counters': {
            'page/view': 12,
            'widget/render': 22,
        }
    }
    all_counters = [stats.Counter('page/view'), stats.Counter('widget/render')]
    aggregator = stats.Aggregator(all_counters)
    aggregate_data = aggregator.ProcessNextWindow()
    self.assertEqual(expected, aggregate_data)

    # Save again, to make sure that duplicate data is collapsed when saved.
    stats.SaveCounters([page_counter, widget_counter], timestamp=93600)
    all_counters = [stats.Counter('page/view'), stats.Counter('widget/render')]
    aggregator = stats.Aggregator(all_counters)
    aggregate_data = aggregator.ProcessNextWindow()

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

    # Weakly test execution path of ProcessWindowsWithBackoff:
    self.assertTrue(aggregator.ProcessWindowsWithBackoff(0))

  def testSaveCountersAggregation(self):
    widget_counter1 = stats.Counter('widget/render')
    widget_counter2 = stats.Counter('widget/render')
    latency_counter1 = stats.AverageCounter('widget/render/latency')
    latency_counter2 = stats.AverageCounter('widget/render/latency')
    all_counters = [widget_counter1, widget_counter2]
    all_counters += [latency_counter1, latency_counter2]

    # SaveCounters should aggregate counters of the same type.
    # This is to make sure that different code paths in a request can
    # independently instantiate counter objects of the same name, and then the
    # intra-request counts will be aggregated together for the task data.
    widget_counter1.Increment()
    widget_counter1.Increment()
    widget_counter2.Increment()
    latency_counter1.Offset(50)
    latency_counter2.Offset(100)
    actual = stats.SaveCounters(all_counters, timestamp=0)
    expected = {
        0: {
            'window': 0,
            'counters': {
                'widget/render': 3,
                'widget/render/latency': (75.0, 2),
            }
        }
    }
    self.assertEqual(expected, actual)

  def testManualCounterTimestamp(self):
    normal_counter = stats.Counter('widget/render')
    normal_counter.Offset(20)

    old_counter = stats.Counter('widget/render')
    old_counter.Offset(10)
    old_counter.timestamp = 3600.0

    oldest_counter = stats.Counter('widget/render')
    oldest_counter.Offset(5)
    oldest_counter.timestamp = 0

    all_counters = [normal_counter, old_counter, oldest_counter]
    stats.SaveCounters(all_counters, eta=0)

    expected = {
        'window': 0,
        'counters': {
            'widget/render': 5,
        }
    }
    aggregator = stats.Aggregator([stats.Counter('widget/render')])
    aggregate_data = aggregator.ProcessNextWindow()
    self.assertEqual(expected, aggregate_data)

    expected = {
        'window': 3600,
        'counters': {
            'widget/render': 10,
        }
    }
    aggregator = stats.Aggregator([stats.Counter('widget/render')])
    aggregate_data = aggregator.ProcessNextWindow()
    self.assertEqual(expected, aggregate_data)

    expected = {
        'counters': {
            'widget/render': 20,
        }
    }
    aggregator = stats.Aggregator([stats.Counter('widget/render')])
    aggregate_data = aggregator.ProcessNextWindow()
    # normal_counter's timestamp is automatically set to the current time.
    self.assertGreater(aggregate_data['window'], 10000)
    del aggregate_data['window']
    self.assertEqual(expected, aggregate_data)

if __name__ == '__main__':
  basetest.main()
