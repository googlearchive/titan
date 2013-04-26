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

"""Titan Activities API Endpoints for activities."""

from titan import endpoints
from titan.activities import activities
from titan.activities import messages

@endpoints.api(name='titan.activities', version='v1',
               description='Titan Activities API')
class ActivitiesService(endpoints.Service):
  """Endpoints for the Activities API."""

  @endpoints.method(
      messages.GetActivityRequest, messages.GetActivityResponse,
      name='activities.get', path='activities/{activity_id}')
  def get_activity(self, request):
    """Retrieve a list of matching activities."""
    activities_service = activities.ActivitiesService()
    activity = activities_service.get_activity(request.activity_id)
    return messages.GetActivityResponse(activity.to_message())

  @endpoints.method(
      messages.GetActivitiesRequest, messages.GetActivitiesResponse,
      name='activities.list', path='activities')
  def get_activities(self, request):
    """Retrieve a list of matching activities."""
    activities_service = activities.ActivitiesService()
    activities_results = activities_service.get_activities(
        keys=request.keys, user=request.user,
        start=request.start,
        end=request.end)
    results = []
    for activity in activities_results:
      results.append(activity.to_message())
    return messages.GetActivitiesResponse(activities=results)
