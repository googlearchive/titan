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

"""Titan Activities API for activity logging into BigQuery."""

import datetime
from protorpc.remote import protojson
from titan.activities import activities
from titan.activities import messages

__all__ = [
    # Constants.
    'BIGQUERY_THRESHOLD_SIZE',
    # Classes.
    'BigQueryActivityLogger',
]

BIGQUERY_QUEUE = 'titan-activities-bigquery'
BIGQUERY_ETA_DELTA = 60
BIGQUERY_THRESHOLD_SIZE = 500

class BigQueryActivityLogger(activities.BaseProcessorActivityLogger):
  """An activity for aggregating data for BigQuery logging."""

  def __init__(self, activity, project_id, dataset_id, table_id, **kwargs):
    super(BigQueryActivityLogger, self).__init__(activity, **kwargs)

    self.project_id = project_id
    self.dataset_id = dataset_id
    self.table_id = table_id

  @property
  def processors(self):
    """Add the aggregator to the set of processors."""
    existing = super(BigQueryActivityLogger, self).processors
    if _Processor not in existing:
      existing[_Processor] = _Processor()
    return existing

  @property
  def bigquery_data(self):
    """Control the structure of activity data sent to BigQuery."""
    return self.activity.to_message()

  def process(self, processors):
    """Add item to the processors."""
    super(BigQueryActivityLogger, self).process(processors)
    processors[_Processor].process(self.bigquery_data)

class _BatchProcessor(activities.BaseAggregateProcessor):
  """Aggregator for BigQuery items export."""

  def __init__(self):
    super(_BatchProcessor, self).__init__(
        'bigquery-batch',
        eta_delta=datetime.timedelta(seconds=BIGQUERY_ETA_DELTA))

  def finalize(self):
    """Send the aggregated data to BigQuery."""
    # TODO(user): Send the aggregated data to BigQuery.
    # self.data contains a list of all ActivityMessage objects.

    # Reset the data after sending to BigQuery
    self.data = []

  def process(self, data):
    """Aggregate the data and check threshold."""
    decoded_data = []
    for item in data:
      decoded_data.append(
          protojson.decode_message(messages.ActivityMessage, item))
    self.data += decoded_data

    # Ensure the size of the BigQuery request does not grow too large.
    if len(self.data) > BIGQUERY_THRESHOLD_SIZE:
      self.finalize()

class _Processor(activities.BaseAggregateProcessor):
  """Aggregator for BigQuery items export."""

  def __init__(self):
    super(_Processor, self).__init__('bigquery')

  @property
  def batch_processor(self):
    """Return a clean processor for processing batch aggregations."""
    return _BatchProcessor()

  def process(self, data):
    """Aggregate the data and check threshold."""
    super(_Processor, self).process(protojson.encode_message(data))
