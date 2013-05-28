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

"""Titan Activities API for activity logging."""

import datetime
import hashlib
import json
import os
from protorpc.remote import protojson
from titan import files
from titan import pipelines
from titan import users
from titan.activities import messages
from titan.common import utils
from titan.tasks import deferred

__all__ = [
    # Constants.
    'BASE_DIR',
    'ACTIVITY_DIR',
    'ACTIVITY_QUEUE',
    'ACTIVITIES_ENVIRON_KEY',
    # Classes.
    'Activity',
    'ActivitiesService',
    'BaseActivityLogger',
    'BaseProcessorActivityLogger',
    'BaseProcessor',
    'BaseAggregateProcessor',
    'FileActivityLogger',
    'ActivitiesService',
    # Functions.
    'log',
    'process_activity_loggers',
]

BASE_DIR = '/_titan/activities'
ACTIVITY_DIR = BASE_DIR + '/activities'
ACTIVITY_QUEUE = 'titan-activities'
ACTIVITIES_ENVIRON_KEY = 'TITAN_ACTIVITIES'

class Error(Exception):
  pass

class ActivityNotFoundError(Error):
  pass

class InvalidActivityIdError(Error):
  pass

class Activity(object):
  """An activity."""

  def __init__(self, key, user=None, meta=None):
    super(Activity, self).__init__()

    self.timestamp = datetime.datetime.utcnow()
    self.key = key
    self.user = user
    self.meta = meta
    self._keys = utils.split_segments(self.key, sep='.')

    # Make the activity_id relatively unique.
    hashed = hashlib.md5()
    hashed.update(self.key)
    hashed.update(self.timestamp.isoformat(sep='T'))
    hashed.update(os.environ.get('REQUEST_ID_HASH', ''))
    if self.user:
      hashed.update(self.user.email)
    self.activity_id = utils.safe_join(
        ACTIVITY_DIR, self.key, self.timestamp.strftime('%Y/%m/%d'),
        '{}-{}.json'.format(
            self.timestamp.isoformat(sep='T'), hashed.hexdigest()[:6]))

  @property
  def keys(self):
    return self._keys

  def to_message(self):
    return messages.ActivityMessage(
        activity_id=self.activity_id,
        timestamp=self.timestamp,
        user=self.user.email if self.user else None,
        key=self.key,
        keys=self.keys,
        meta=json.dumps(self.meta))

class BaseActivityLogger(object):
  """An abstract activity for logging."""

  def __init__(self, activity, **unused_kwargs):
    super(BaseActivityLogger, self).__init__()

    self.activity = activity
    self.defer_finalize = False

  def store(self):
    if not ACTIVITIES_ENVIRON_KEY in os.environ:
      # os.environ is replaced by the runtime environment with a request-local
      # object, allowing non-string types to be stored globally and
      # automatically cleaned up at the end of each request.
      os.environ[ACTIVITIES_ENVIRON_KEY] = []
    os.environ[ACTIVITIES_ENVIRON_KEY].append(self)

  def finalize(self):
    """Finalizes the logging of the activity."""
    pass

class BaseProcessorActivityLogger(BaseActivityLogger):
  """An base activity for aggregating multiple activities."""

  @property
  def processors(self):
    return {}

  def process(self, processors):
    """Process the data using singleton processors."""
    pass

class BaseProcessor(pipelines.BaseProcessor):
  """Aggregate processor for aggregating activity data."""

  def __init__(self, batch_name, eta_delta=None):
    super(BaseProcessor, self).__init__()

    # Processor determines what to name the batch it will be processed in.
    self.batch_name = batch_name
    self.eta_delta = eta_delta

class BaseAggregateProcessor(pipelines.BaseAggregateProcessor):
  """Aggregate processor for aggregating activity data."""

  def __init__(self, batch_name, eta_delta=None):
    super(BaseAggregateProcessor, self).__init__()

    # Processor determines what to name the batch it will be processed in.
    self.batch_name = batch_name
    self.eta_delta = eta_delta

  @property
  def batch_processor(self):
    """Return a batch processor for processing batch data."""
    raise NotImplementedError

class FileActivityLogger(BaseActivityLogger):
  """An activity for logging to Titan Files."""

  def __init__(self, activity, **kwargs):
    super(FileActivityLogger, self).__init__(activity, **kwargs)

    self.defer_finalize = True
    self.indexed_meta_keys = []
    # TODO(user): Make the filename more unique.

  @property
  def file_meta(self):
    """Translates into the Titan files meta indexing."""
    meta = {}

    # Allow for the user specified meta keys to be indexed.
    for key in self.indexed_meta_keys:
      if key in self.activity.meta:
        meta[key] = self.activity.meta[key]

    # Renaming since ndb indexing does not work with `key` as a meta property.
    meta['activity_key'] = self.activity.key
    meta['activity_keys'] = self.activity.keys

    return meta

  def finalize(self):
    """Log the activity in Titan Files."""
    titan_file = files.File(self.activity.activity_id, _internal=True)
    titan_file.write(
        content=protojson.encode_message(self.activity.to_message()),
        meta=self.file_meta, created=self.activity.timestamp,
        modified=self.activity.timestamp)

    # Ensure that it gets written first.
    super(FileActivityLogger, self).finalize()

class ActivitiesService(object):
  """A service class to retrieve permanently stored activities."""

  def get_activities(self, keys, user=None, start=None, end=None):
    """Query for a set of stored activities.

    Args:
      keys: A list of keys to filter on.
      user: A user to filter on.
      start: A datetime.datetime object. Defaults to the previous hour.
      end: A datetime.datetime object. Defaults to current day.
    Raises:
      ValueError: if end time is greater than start time.
    Returns:
      An array of matched activities.
    """
    if end and end < start:
      raise ValueError('End time {} must be greater than start time {}'.format(
          end, start))
    now = datetime.datetime.now()
    if not start:
      start = now - datetime.timedelta(hours=1)
    if not end:
      end = now

    # Get all activities within the range.
    titan_files = files.OrderedFiles([])
    for key in keys:
      filters = [
          files.FileProperty('activity_key') == key,
          files.FileProperty('created') >= start,
          files.FileProperty('created') <= end,
      ]
      if user:
        filters.append(files.FileProperty('user') == user.email)
      new_titan_files = files.OrderedFiles.list(BASE_DIR, recursive=True,
                                                filters=filters, _internal=True)
      titan_files.update(new_titan_files)

    final_results = []
    for titan_file in titan_files.itervalues():
      final_results.append(self._loads_activity(titan_file.content))
    return final_results

  def get_activity(self, activity_id):
    """Query for a stored activity.

    Args:
      activity_id: The full file name of the activity.
    Raises:
      InvalidActivityIdError: When path is not prefixed correctly.
      ActivityNotFoundError: When activity is not found in system.
    Returns:
      The activity matching the activity_id.
    """
    if not activity_id or not activity_id.startswith(ACTIVITY_DIR):
      raise InvalidActivityIdError()
    titan_file = files.File(activity_id, _internal=True)
    if not titan_file.exists:
      raise ActivityNotFoundError()
    return self._loads_activity(titan_file.content)

  def _loads_activity(self, content):
    return protojson.decode_message(messages.ActivityMessage, content)

def _finalize_activity_loggers(activities):
  for activity in activities:
    activity.finalize()

def log(key, user=None, meta=None):
  """Logs a file based activity."""
  if user is None:
    user = users.get_current_user()
  activity = Activity(key, user=user, meta=meta)
  activity_logger = FileActivityLogger(activity)
  activity_logger.store()

  # If inside of a task then process now instead of waiting.
  if 'HTTP_X_APPENGINE_TASKNAME' in os.environ:
    process_activity_loggers()

  return activity

def process_activity_loggers():
  """Processes the pending activities from the local request environ."""
  if not ACTIVITIES_ENVIRON_KEY in os.environ:
    return

  processors = {}
  deferred_activity_loggers = []
  for logger in os.environ[ACTIVITIES_ENVIRON_KEY]:
    # Discover all the processors.
    processors.update(getattr(logger, 'processors', {}))

  for logger in os.environ[ACTIVITIES_ENVIRON_KEY]:
    if logger.defer_finalize:
      deferred_activity_loggers.append(logger)

    # Perform processing.
    if hasattr(logger, 'processors'):
      logger.process(processors)

  if deferred_activity_loggers:
    deferred.defer(_finalize_activity_loggers, deferred_activity_loggers,
                   _queue=ACTIVITY_QUEUE)

  for processor in processors.itervalues():
    pipelines.push(
        processor.batch_name, processor.batch_processor,
        data=processor.serialize())
