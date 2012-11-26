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

"""Enable Titan Stats recording of all core file operations."""

import random

from titan.files import files
from titan.stats import stats

class StatsRecorderMixin(files.File):
  """Mixin which uses Titan Stats to record usage and timing stats."""

  def __init__(self, *args, **kwargs):
    self._stats_invocation_counters = {}
    self._stats_latency_counters = {}
    super(StatsRecorderMixin, self).__init__(*args, **kwargs)

  def _StartStatsRecording(self, unique_counter_name):
    counter_name = unique_counter_name.name
    latency_counter_name = '%s/latency' % counter_name
    latency_counter = stats.AverageTimingCounter(latency_counter_name)
    latency_counter.Start()

    unique_id = hash(unique_counter_name)
    self._stats_latency_counters[unique_id] = latency_counter
    self._stats_invocation_counters[unique_id] = stats.Counter(counter_name)

  def _StopStatsRecording(self, unique_counter_name):
    unique_id = hash(unique_counter_name)
    self._stats_latency_counters[unique_id].Stop()
    self._stats_invocation_counters[unique_id].Increment()

    counters = [
        self._stats_invocation_counters[unique_id],
        self._stats_latency_counters[unique_id],
    ]
    stats.StoreRequestLocalCounters(counters)

  @property
  def _file(self):
    if self.is_loaded:
      # Only record stats if not loaded.
      return super(StatsRecorderMixin, self)._file
    unique_counter_name = _UniqueCounterName('files/File/load')
    self._StartStatsRecording(unique_counter_name)
    try:
      return super(StatsRecorderMixin, self)._file
    finally:
      self._StopStatsRecording(unique_counter_name)

  def Write(self, *args, **kwargs):
    unique_counter_name = _UniqueCounterName('files/File/Write')
    self._StartStatsRecording(unique_counter_name)
    try:
      return super(StatsRecorderMixin, self).Write(*args, **kwargs)
    finally:
      self._StopStatsRecording(unique_counter_name)

  def CopyTo(self, *args, **kwargs):
    unique_counter_name = _UniqueCounterName('files/File/CopyTo')
    self._StartStatsRecording(unique_counter_name)
    try:
      return super(StatsRecorderMixin, self).CopyTo(*args, **kwargs)
    finally:
      self._StopStatsRecording(unique_counter_name)

  def Delete(self, *args, **kwargs):
    unique_counter_name = _UniqueCounterName('files/File/Delete')
    self._StartStatsRecording(unique_counter_name)
    try:
      return super(StatsRecorderMixin, self).Delete(*args, **kwargs)
    finally:
      self._StopStatsRecording(unique_counter_name)

  def Serialize(self, *args, **kwargs):
    unique_counter_name = _UniqueCounterName('files/File/Serialize')
    self._StartStatsRecording(unique_counter_name)
    try:
      return super(StatsRecorderMixin, self).Serialize(*args, **kwargs)
    finally:
      self._StopStatsRecording(unique_counter_name)

class _UniqueCounterName(object):
  """A unique counter name container.

  This object's hash is used to prevent overlap of the same counter name
  which may be created multiple times within a code path.
  """

  def __init__(self, name):
    self.random_offset = random.randint(0, 1000000)
    self.name = name

  def __hash__(self):
    return id(self) + self.random_offset

def MakeAllCounters():
  """Make a new list of all counters which can be aggregated and saved."""
  counters = [
      # Invocation counters.
      stats.Counter('files/File/load'),
      stats.Counter('files/File/Write'),
      stats.Counter('files/File/Touch'),
      stats.Counter('files/File/Delete'),
      stats.Counter('files/File/Serialize'),

      # TODO(user): Add these when the new APIs are implemented.
      # stats.Counter('files/Files/List'),
      # stats.Counter('files/File/Copy'),
      # stats.Counter('files/Dir/Copy'),
      # stats.Counter('files/Dir/List'),
      # stats.Counter('files/Dir/exists'),

      # Timing counters.
      stats.AverageTimingCounter('files/File/load/latency'),
      stats.AverageTimingCounter('files/File/Write/latency'),
      stats.AverageTimingCounter('files/File/Touch/latency'),
      stats.AverageTimingCounter('files/File/Delete/latency'),
      stats.AverageTimingCounter('files/File/Serialize/latency'),

      # TODO(user): Add these when the new APIs are implemented.
      # stats.AverageTimingCounter('files/Files/List/latency'),
      # stats.AverageTimingCounter('files/File/Copy/latency'),
      # stats.AverageTimingCounter('files/Dir/Copy/latency'),
      # stats.AverageTimingCounter('files/Dir/List/latency'),
      # stats.AverageTimingCounter('files/Dir/exists/latency'),
  ]
  return counters
