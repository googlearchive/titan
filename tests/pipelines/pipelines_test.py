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

"""Tests for pipelines.py."""

from tests.common import testing

import mock
from google.appengine.api import memcache
from google.appengine.ext import deferred
from google.appengine.ext import ndb
from titan.common.lib.google.apputils import basetest
from titan import pipelines

class PushTest(testing.BaseTestCase):

  def testPushBadProcess(self):
    # Weakly test that it executes completely.
    processor = BadProcessorProcess()
    data = {'test': True}
    with self.assertRaises(pipelines.PipelineProcessError):
      pipelines.push('mock_pipeline', processor=processor, data=data)
      self.RunDeferredTasks(queue_name=pipelines.PIPELINES_QUEUE)

  def testPushBadFinalize(self):
    # Weakly test that it executes completely.
    processor = BadProcessorFinalize()
    data = {'test': True}
    with self.assertRaises(pipelines.PipelineProcessError):
      pipelines.push('mock_pipeline', processor=processor, data=data)
      self.RunDeferredTasks(queue_name=pipelines.PIPELINES_QUEUE)

  def testPushGood(self):
    # Weakly test that it executes completely.
    processor = GoodProcessor()
    data = {'test': True}
    pipelines.push('mock_pipeline', processor=processor, data=data)
    self.RunDeferredTasks(queue_name=pipelines.PIPELINES_QUEUE)

class PipelineTest(testing.BaseTestCase):

  @mock.patch.object(deferred, 'defer')
  def testPipeline(self, unused_defer):
    processor = mock.Mock()
    memcache.add(pipelines.PIPELINES_PREFIX + '-name-index', 1)
    pipeline = pipelines.Pipeline('name')
    self.assertEquals(pipelines.PIPELINES_PREFIX + '-name', pipeline.batch_name)
    self.assertEquals(pipelines.PIPELINES_PREFIX + '-name-index',
                      pipeline.index_name)
    self.assertEquals(
        (pipelines.PIPELINES_PREFIX +
         '-name-c4ca4238a0b923820dcc509a6f75849b-lock'),
        pipeline.lock_name)
    self.assertEquals('c4ca4238a0b923820dcc509a6f75849b', pipeline.md5_hash)
    self.assertEquals('pipelines-name-c4ca4238a0b923820dcc509a6f75849b',
                      pipeline.work_index)

  @mock.patch.object(deferred, 'defer')
  def testCloseBatch(self, unused_defer):
    memcache.add(pipelines.PIPELINES_PREFIX + '-name-index', 1)
    pipeline = pipelines.Pipeline('name')
    pipeline._close_batch()
    self.assertLessEqual(2, memcache.get(pipeline.index_name))
    # 2**16 - 100 > lock.
    self.assertGreater(2**16 - 100, memcache.get(pipeline.lock_name))

class PipelineCleanupTest(testing.BaseTestCase):

  @mock.patch.object(pipelines.Pipeline, '_cleanup')
  def testPipelineCleanup(self, internal_cleanup):
    processor = GoodProcessor()
    data = {'test': True}
    # Push twice to test that it cleans up multiple work items.
    pipelines.push('mock_pipeline', processor=processor, data=data)
    pipelines.push('mock_pipeline', processor=processor, data=data)
    self.RunDeferredTasks(queue_name=pipelines.PIPELINES_QUEUE)
    # Check that the cleanup was called with one argument.
    internal_cleanup.assert_called_with(
        [ndb.Key('WorkItem', 1), ndb.Key('WorkItem', 2)])

class BadProcessorProcess(pipelines.BaseProcessor):
  """A sample processor that can be pickled."""

  def process(self, item):
    raise Exception()

  def finalize(self):
    pass

class BadProcessorFinalize(pipelines.BaseProcessor):
  """A sample processor that can be pickled."""

  def process(self, item):
    pass

  def finalize(self):
    raise Exception()

class GoodProcessor(pipelines.BaseProcessor):
  """A sample processor that can be pickled."""

  @property
  def batch_processor(self):
    return GoodProcessor()

  def process(self, item):
    pass

  def finalize(self):
    pass

if __name__ == '__main__':
  basetest.main()
