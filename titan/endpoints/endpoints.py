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

"""Titan Endpoints; like App Engine Endpoints but with middleware.

Usage:
  api_services = (
      MyService,  # A subclass of endpoints.Service.
  )
  api_application = endpoints.EndpointsApplication(api_services)
"""

try:
  import appengine_config
except ImportError:
  pass

import protorpc
from protorpc import remote
from google.appengine.ext import endpoints
from google.appengine.ext.endpoints import apiserving

__all__ = [
    # Constants.
    'API_EXPLORER_CLIENT_ID',
    # Functions.
    'api',
    'method',
    # Classes.
    'Service',
    'EndpointsApplication',
    # Exceptions.
    'ServiceException',
    'BadRequestException',
    'ForbiddenException',
    'InternalServerErrorException',
    'NotFoundException',
    'UnauthorizedException',
]

# Constant aliases.
API_EXPLORER_CLIENT_ID = endpoints.API_EXPLORER_CLIENT_ID

# Decorator aliases.
api = endpoints.api
method = endpoints.method

# Exception aliases.
ServiceException = endpoints.ServiceException
BadRequestException = endpoints.BadRequestException
ForbiddenException = endpoints.ForbiddenException
InternalServerErrorException = endpoints.InternalServerErrorException
NotFoundException = endpoints.NotFoundException
UnauthorizedException = endpoints.UnauthorizedException

class Service(remote.Service):
  # For future use.
  pass

class EndpointsApplication(apiserving._ApiServer):

  def __init__(self, api_services, **kwargs):
    # NOTE: We intentionally subclass the protected class (which really should
    # be public) to consistently provide an object-style interface for
    # application creation rather than a module-level factory interface. Because
    # of this, we have to duplicate this tiny amount of logic from
    # endpoints.api_server(), and watch for updates of the factory method.
    #
    if 'protocols' in kwargs:
      raise TypeError(
          "__init__() got an unexpected keyword argument 'protocols'")
    # ---

    # Titan middleware handling.
    if 'middleware' in kwargs:
      for unused_service in api_services:
        # TODO(user): build this.
        pass
      raise NotImplementedError
    super(EndpointsApplication, self).__init__(api_services, **kwargs)
