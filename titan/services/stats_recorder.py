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

"""Enable Titan Stats recording of all core file operations.

Documentation:
  http://code.google.com/p/titan-files/wiki/StatsRecorderService
"""

from titan.common import hooks
from titan.stats import stats

SERVICE_NAME = 'titan-stats-recorder'

def RegisterService():
  """Method required for all Titan service plugins."""
  hooks.RegisterHook(SERVICE_NAME, 'file-exists', hook_class=StatsHook,
                     hook_kwargs={'counter_name': 'files/Exists'})
  hooks.RegisterHook(SERVICE_NAME, 'file-get', hook_class=StatsHook,
                     hook_kwargs={'counter_name': 'files/Get'})
  hooks.RegisterHook(SERVICE_NAME, 'file-write', hook_class=StatsHook,
                     hook_kwargs={'counter_name': 'files/Write'})
  hooks.RegisterHook(SERVICE_NAME, 'file-delete', hook_class=StatsHook,
                     hook_kwargs={'counter_name': 'files/Delete'})
  hooks.RegisterHook(SERVICE_NAME, 'file-touch', hook_class=StatsHook,
                     hook_kwargs={'counter_name': 'files/Touch'})
  hooks.RegisterHook(SERVICE_NAME, 'file-copy', hook_class=StatsHook,
                     hook_kwargs={'counter_name': 'files/Copy'})
  hooks.RegisterHook(SERVICE_NAME, 'copy-dir', hook_class=StatsHook,
                     hook_kwargs={'counter_name': 'files/CopyDir'})
  hooks.RegisterHook(SERVICE_NAME, 'list-files', hook_class=StatsHook,
                     hook_kwargs={'counter_name': 'files/ListFiles'})
  hooks.RegisterHook(SERVICE_NAME, 'list-dir', hook_class=StatsHook,
                     hook_kwargs={'counter_name': 'files/ListDir'})
  hooks.RegisterHook(SERVICE_NAME, 'dir-exists', hook_class=StatsHook,
                     hook_kwargs={'counter_name': 'files/DirExists'})

class StatsHook(hooks.Hook):
  """Statistics hook for all core methods."""

  def __init__(self, counter_name, *args, **kwargs):
    super(StatsHook, self).__init__(*args, **kwargs)
    self.counter_name = counter_name
    self.invocation_counter = stats.Counter(self.counter_name)
    self.latency_counter = stats.AverageTimingCounter('%s/latency'
                                                      % self.counter_name)

  def Pre(self, **kwargs):
    self.invocation_counter.Increment()
    self.latency_counter.Start()

  def Post(self, result):
    self._StopAndStoreCounters()
    return result

  def OnError(self, unused_error):
    self._StopAndStoreCounters()

  def _StopAndStoreCounters(self):
    self.latency_counter.Stop()
    counters = [self.invocation_counter, self.latency_counter]
    stats.StoreRequestLocalCounters(counters)

def MakeAllCounters():
  """Make a new list of all counters which can be aggregated and saved."""
  counters = [
      # Invocation counters.
      stats.Counter('files/Exists'),
      stats.Counter('files/Get'),
      stats.Counter('files/Write'),
      stats.Counter('files/Delete'),
      stats.Counter('files/Touch'),
      stats.Counter('files/Copy'),
      stats.Counter('files/CopyDir'),
      stats.Counter('files/ListFiles'),
      stats.Counter('files/ListDir'),
      stats.Counter('files/DirExists'),
      # Timing counters.
      stats.AverageTimingCounter('files/Exists/latency'),
      stats.AverageTimingCounter('files/Get/latency'),
      stats.AverageTimingCounter('files/Write/latency'),
      stats.AverageTimingCounter('files/Delete/latency'),
      stats.AverageTimingCounter('files/Touch/latency'),
      stats.AverageTimingCounter('files/Copy/latency'),
      stats.AverageTimingCounter('files/CopyDir/latency'),
      stats.AverageTimingCounter('files/ListFiles/latency'),
      stats.AverageTimingCounter('files/ListDir/latency'),
      stats.AverageTimingCounter('files/DirExists/latency'),
  ]
  return counters
