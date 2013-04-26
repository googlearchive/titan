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

"""Task Pipelines for processing data across requests in a timely fashion.

Provides an 'eventually consistent' pipeline implementation based on
Brett Slatkin's data pipelines design.

Uses a batch processor (BaseProcessor) that processes batched data.

Usage:
  # Create a processor that will process data from the batch.
  class MyProcessor(pipelines.BaseAggregateProcessor):
    def finalize(self):
      # Use the batch aggregated data...
      # Ex: Insert aggregated data to datastore, send email, etc.

  processor = MyProcessor()
  # Push the request data to a batch with the batch processor.
  pipelines.push('batch_name', processor,
                 data=['Data', 'to', 'be', 'processed.'])
  # Profit. The batch processing is tasked and will be processed and finalized.

See also:
  http://youtu.be/zSDC_TU7rtc
"""

import cPickle as pickle
import datetime
import hashlib
import json
import logging
import os
import random
import time
from google.appengine.api import memcache
from google.appengine.api import taskqueue
from google.appengine.ext import ndb
from titan.common import utils
from titan.tasks import deferred

__all__ = [
    # Constants.
    'PIPELINES_BATCH_SIZE',
    'PIPELINES_TASK_DELAY_SECONDS',
    'PIPELINES_ETA_DEFAULT_DELTA',
    'PIPELINES_ETA_BUFFER_CLEANUP',
    'PIPELINES_PREFIX',
    'PIPELINES_QUEUE',
    # Classes.
    'BaseAggregateProcessor',
    'BaseProcessor',
    'Pipeline',
    'PipelineProcessError',
    'WorkItem',
    # Functions.
    'push',
]

# Size of query batches.
PIPELINES_BATCH_SIZE = 2000
PIPELINES_ETA_DEFAULT_DELTA = datetime.timedelta(seconds=60)
# Estimate cleanup after the 10 min task timeout limit with a fan-out buffer.
PIPELINES_ETA_BUFFER_CLEANUP = datetime.timedelta(minutes=12)
PIPELINES_PREFIX = 'pipelines'
PIPELINES_QUEUE = 'titan-pipelines'
# Delay processing for db to hopefully become consistent.
PIPELINES_TASK_DELAY_SECONDS = 3
# Sential value used when testing for a lock on a batch.
# More information in Brett's video above.
_PIPELINES_SENTINAL_VALUE = 2**16
_PIPELINES_SENTINAL_OFFSET_VALUE = 2**15

class Error(Exception):
  pass

class PipelineProcessError(Error):
  def __init__(self, errors):
    super(PipelineProcessError, self).__init__()
    self.errors = errors

class BaseProcessor(object):
  """An base for building pipeline processors."""

  def finalize(self):
    """Finalize the use of the processed data."""
    raise NotImplementedError

  def process(self, data):
    """Process the given data."""
    raise NotImplementedError

class BaseAggregateProcessor(BaseProcessor):
  """An base aggregation pipeline processor."""

  def __init__(self):
    super(BaseAggregateProcessor, self).__init__()

    self.data = []

  def process(self, data):
    """Process the given data."""
    self.data.append(data)

  def serialize(self):
    return self.data

class Pipeline(object):
  """Pipeline for processing through a waiting set of work items."""

  def __init__(self, name, task_delay_seconds=PIPELINES_TASK_DELAY_SECONDS):
    super(Pipeline, self).__init__()

    self.md5_hash = None
    self.work_index = None
    self.lock_name = None
    self.batch_name = '{}-{}'.format(PIPELINES_PREFIX, name)
    self.task_delay_seconds = task_delay_seconds
    self._open_batch()

  def _cleanup(self, work_item_keys):
    """Cleanup the work items after they have been finalized."""
    ndb.delete_multi(work_item_keys)

  def _close_batch(self):
    """Release the lock and increment the index to move to next batch."""
    # Cuttoff ability to add to the index since it is now processing.
    # Increment randomly to lower collision of task names when cache evicted.
    memcache.incr(self.index_name, delta=random.randrange(1, 25))
    # The processing has started, stop using index.
    memcache.decr(self.lock_name, _PIPELINES_SENTINAL_OFFSET_VALUE)

    # Add a task to cleanup any items that were missed from database delay.
    eta = (datetime.datetime.utcfromtimestamp(time.time()) +
           PIPELINES_ETA_BUFFER_CLEANUP)
    try:
      deferred.defer(self._process, _name=self.work_index + '-cleanup',
                     _eta=eta, _queue=PIPELINES_QUEUE)
    except taskqueue.TaskAlreadyExistsError:
      pass  # Expected error to fan-in the tasks.

  def _fan_in(self):
    """Do some cleanup and start the processing."""
    # Stop the ability to add to the batch now that it is processing.
    self._close_batch()

    # Add a delay to allow tail end items to finish "eventual" consistency.
    time.sleep(self.task_delay_seconds)

    return self._process()

  def _open_batch(self):
    """Determine which batch to use for the pipeline."""
    self.index_name = '{}-index'.format(self.batch_name)

    while True:
      # Find the latest batch index.
      index = memcache.get(self.index_name)
      if index is None:
        # Random to lower collision of task names when cache evicted.
        memcache.add(self.index_name, random.randrange(10, 10000))
        index = memcache.get(self.index_name)

      # Create a non-monotonically increasing name for the work index.
      self.md5_hash = hashlib.md5(str(index)).hexdigest()
      self.work_index = '{}-{}'.format(self.batch_name, self.md5_hash)
      self.lock_name = '{}-lock'.format(self.work_index)

      # Determine if the batch is still valid and available.
      writers = memcache.incr(self.lock_name,
                              initial_value=_PIPELINES_SENTINAL_VALUE)
      if writers < _PIPELINES_SENTINAL_VALUE:
        memcache.decr(self.lock_name)
        continue
      return

  def _process(self, cursor=None):
    """Query and process through all pipeline work items."""
    query = WorkItem.query()
    query = query.filter(WorkItem.work_index == self.work_index)

    results, cursor, has_more = query.fetch_page(PIPELINES_BATCH_SIZE,
                                                 start_cursor=cursor)
    if not results:
      return

    # Use fan-out to process in batches using cursor.
    if has_more:
      deferred.defer(self._process, cursor=cursor, _queue=PIPELINES_QUEUE)

    # Process the stored items.
    processor = None
    result_keys = []
    process_errors = []
    for result in results:
      try:
        if processor is None:
          processor = pickle.loads(result.processor)
        processor.process(json.loads(result.data))
        result_keys.append(result.key)
      except Exception as e:
        logging.exception('Error processing data in the batch.')
        process_errors.append(e)

    try:
      processor.finalize()
      # Only remove when they successfully finalize.
      self._cleanup(result_keys)
    except Exception as e:
      logging.exception('Error finalizing data in the batch.')
      process_errors.append(e)

    if process_errors:
      raise PipelineProcessError(process_errors)

    logging.info(
        'Processed {} WorkItems in the {} task'.format(
            len(result_keys), os.environ.get('HTTP_X_APPENGINE_TASKNAME', '')))

  @ndb.toplevel
  def fan_in(self, work_item, eta_delta=PIPELINES_ETA_DEFAULT_DELTA):
    """Creates a fan in task for batch processing the work item."""
    counter = 0
    eta = datetime.datetime.utcfromtimestamp(time.time()) + eta_delta

    # Keep trying to find a valid work index when the taskname is tombstoned.
    while True:
      try:
        try:
          # Use a named queue to perform the fan-in.
          deferred.defer(self._fan_in, _name=self.work_index, _eta=eta,
                         _queue=PIPELINES_QUEUE)
        except taskqueue.TaskAlreadyExistsError:
          pass  # Expected error to fan-in the tasks.
        break  # End the loop since the task was created or fanned-in.
      except taskqueue.TombstonedTaskError:
        # Keep trying until we get to a non-tombstoned task name.
        # Increment randomly to find another non-tomestoned task name.
        memcache.incr(self.index_name, delta=random.randrange(1, 100))
        self._open_batch()  # Reattempt to reopen a new batch.
        counter += 1
        logging.info('Tombstoned error, retrying: {}'.format(counter))

    # Add processor to the Datastore if successfully fanned in.
    work_item.work_index = self.work_index
    work_item.put_async()

class WorkItem(ndb.Model):
  """Pipeline work item to store the work for later."""
  work_index = ndb.StringProperty(required=True)
  created_on = ndb.DateTimeProperty(auto_now_add=True)
  data = ndb.BlobProperty(indexed=False)
  processor = ndb.BlobProperty(indexed=False)

def push(name, processor, data):
  """Specifies a processor to be used in process the batched data."""
  pipeline = Pipeline(name)
  work_item = WorkItem(data=json.dumps(data, cls=utils.CustomJsonEncoder),
                       processor=pickle.dumps(processor))

  # Start the fan-in.
  eta_delta = getattr(processor, 'eta_delta', PIPELINES_ETA_DEFAULT_DELTA)
  pipeline.fan_in(work_item, eta_delta)
