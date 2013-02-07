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

from google.appengine.ext import deferred
from titan.users import users

def Defer(*args, **kwargs):
  """Launches a deferred task on behalf of the user initiating the request.

  If a user is currently logged in, an X-Titan-User header is passed to the
  deferred task, which is recognized by the Titan users module.
  """
  titan_user = users.GetCurrentUser()
  if titan_user:
    headers = kwargs.pop('_headers', {})
    headers['X-Titan-User'] = titan_user.email
    kwargs['_headers'] = headers
  deferred.defer(*args, **kwargs)
