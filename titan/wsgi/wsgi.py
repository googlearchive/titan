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

"""Titan for App Engine WSGI framework.

Usage:
  from titan import wsgi

  routes = (
      ('/simple', Handler),
      wsgi.Route(r'/admin', AdminHandler, login=True, admin=True, secure=True),
  )
  application = wsgi.WSGIApplication(routes)
"""

try:
  import appengine_config
except ImportError:
  pass

import webapp2
from webob import exc as webob_exceptions
from titan import users

__all__ = [
    # Classes.
    'RequestHandler',
    'Route',
    'WSGIApplication',
]

class RequestHandler(webapp2.RequestHandler):
  # For future expansion.
  pass

class Route(webapp2.Route):
  """Route that maps a URI path to a handler.

  This is a normal webapp2.Route and accepts all base arguments:
  http://webapp-improved.appspot.com/guide/routing.html

  In addition, the following are keyword arguments are supported:

  Args:
    secure: Boolean of whether or not this route requires SSL.
    login: Boolean of whether or not this route requires logged in users,
        either from the Users API or an OAuth2 user. This does not enforce
        any authorization, just authentication.
    admin: Boolean of whether or not this route requires an app admin user.
  """

  def __init__(self, *args, **kwargs):
    self.require_secure = kwargs.pop('secure', False)
    if self.require_secure:
      # Unset schemes so that SSL redirect can be handled and not 404.
      kwargs['schemes'] = None
    self.require_login = kwargs.pop('login', False)
    self.require_admin = kwargs.pop('admin', False)
    super(Route, self).__init__(*args, **kwargs)

class _RouterWithMiddleware(webapp2.Router):
  """Router with support for Titan routes and middleware."""

  def dispatch(self, request, response):
    """Main routing method called by every request."""

    # TODO(user): memoize this and override match, so that we don't
    # have to rematch again in the super default_dispatcher call.
    route, unused_args, unused_kwargs = self.match(request)

    # SSL redirect handling. Must come before auth handling.
    require_secure = getattr(route, 'require_secure', False)
    if require_secure and not request.scheme == 'https':
      redirect_url = 'https://{}{}'.format(
          request.server_name, request.path_qs)
      raise webob_exceptions.HTTPMovedPermanently(location=redirect_url)

    # Maybe redirect to login or raise 403 Forbidden for non-admins.
    require_login = getattr(route, 'require_login', False)
    require_admin = getattr(route, 'require_admin', False)
    if require_login or require_admin:
      user = users.get_current_user(oauth_scopes=users.OAUTH_SCOPES)
      if not user:
        login_url = users.create_login_url(dest_url=request.url)
        raise webob_exceptions.HTTPFound(location=login_url)
      elif require_admin and not user.is_admin:
        raise webob_exceptions.HTTPForbidden

    return super(_RouterWithMiddleware, self).default_dispatcher(
        request, response)

class WSGIApplication(webapp2.WSGIApplication):
  """WSGI application with custom middleware."""

  router_class = _RouterWithMiddleware

  def __init__(self, routes=None, debug=False, config=None, middleware=None):
    # TODO(user): add middleware support.
    self.middleware = middleware
    super(WSGIApplication, self).__init__(routes, debug=debug, config=config)
