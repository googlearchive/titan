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

"""Titan Activities API Messages."""

from protorpc import message_types
from protorpc import messages

class ActivityMessage(messages.Message):
  """Message for Activity Response."""
  activity_id = messages.StringField(1)
  timestamp = message_types.DateTimeField(2)
  key = messages.StringField(3)
  keys = messages.StringField(4, repeated=True)
  user = messages.StringField(5)
  meta = messages.StringField(6)

class GetActivityRequest(messages.Message):
  """Message for Activity Request."""
  activity_id = messages.StringField(1)

class GetActivityResponse(messages.Message):
  """Message for Activity Request."""
  activity = messages.MessageField(ActivityMessage, 1)

class GetActivitiesRequest(messages.Message):
  """Message for Activities List Requests."""
  keys = messages.StringField(1, repeated=True)
  user = messages.StringField(2)
  start = message_types.DateTimeField(3)
  end = message_types.DateTimeField(4)

class GetActivitiesResponse(messages.Message):
  """Message for Activities List Responses."""
  activities = messages.MessageField(ActivityMessage, 1, repeated=True)
