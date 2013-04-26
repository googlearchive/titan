#!/usr/bin/env python
"""A base test case for testing App Engine Endpoints services.

This testing utility library simplifies the process for testing App Engine
Endpoints by abstracting some of the menial setup.

Usage:
  class MyTest(endpointstest.EndpointsTestCase):

    def CreateWsgiApplication(self):
      return endpoints.api_server([MyService], restricted=False)

    def testMyTest(self):
      service = self.GetServiceStub(MyService)
      self.assertEquals(message_types.VoidMessage(), service.foo())

  @endpoints.api(name='myapi')
  class MyService(remote.Service):

    @endpoints.method(name='foo')
    def foo(self, request):
      message_types.VoidMessage()
"""

import socket
import threading
from wsgiref import simple_server
from wsgiref import validate
from protorpc import protojson
from protorpc import transport
from titan.common.lib.google.apputils import basetest

class EndpointsTestCase(basetest.TestCase):
  """Base test case for App Engine Endpoints.

  Much of this is based off of webapp_test_util, but updated for accuracy
  and better interoperability.
  """

  _ENDPOINTS_SERVICE_PATH = '/my/service'

  def setUp(self):
    self._endpoints_server = None
    self._endpoints_schema = 'http'
    self._ResetServer()
    super(EndpointsTestCase, self).setUp()

  def tearDown(self):
    self._endpoints_server.Shutdown()
    super(EndpointsTestCase, self).tearDown()

  # This method name is used by webapp_test_util.
  def CreateWsgiApplication(self):
    """Returns the wsgi application for the service endpoint testing."""
    raise NotImplementedError(
        'Test needs to define a CreateWsgiApplication method.')

  def GetServiceStub(self, service, url=None):
    """Retrieve a service stub to use to test the service calls."""
    if not url:
      url = '/_ah/spi/{}'.format(service.__name__)
    service_transport = transport.HttpTransport(
        self._MakeServiceUrl(url), protocol=protojson)
    return service.Stub(service_transport)

  def _ResetServer(self, application=None):
    """Reset web server.

    Shuts down existing server if necessary and starts a new one.

    Args:
      application: Optional WSGI function.  If none provided will use
        tests CreateWsgiApplication method.
    """
    if self._endpoints_server:
      self._endpoints_server.Shutdown()

    self.port = _PickUnusedPort()
    self._endpoints_server, self._endpoints_application = self._StartWebServer(
        self.port, application)

  def _CreateTransport(self, protocol=protojson):
    """Create a new transportation object."""
    return transport.HttpTransport(
        self._MakeServiceUrl(self._ENDPOINTS_SERVICE_PATH), protocol=protocol)

  def _StartWebServer(self, port, application=None):
    """Start web server.

    Args:
      port: Port to start application on.
      application: Optional WSGI function.  If none provided will use
        tests CreateWsgiApplication method.

    Returns:
      A tuple (server, application):
        server: An instance of ServerThread.
        application: Application that web server responds with.
    """
    if not application:
      application = self.CreateWsgiApplication()
    validated_application = validate.validator(application)
    server = simple_server.make_server('localhost', port, validated_application)
    server = ServerThread(server)
    server.start()
    return server, application

  def _MakeServiceUrl(self, path):
    """Make service URL using current schema and port."""
    return '%s://localhost:%d%s' % (self._endpoints_schema, self.port, path)

class ServerThread(threading.Thread):
  """Thread responsible for managing WSGI server."""

  def __init__(self, server, *args, **kwargs):
    """Constructor.

    Args:
      server: The WSGI server that is served by this thread, as per
      threading.Thread base class.
      *args: Args passed through the Thread superclass.
      **kwargs: Args passed through the Thread superclass.
    """
    self._endpoints_server = server
    # This timeout is for the socket when a connection is made.
    self._endpoints_server.socket.settimeout(None)
    # This timeout is for when waiting for a connection.  The allows
    # server.handle_request() to listen for a short time, then timeout,
    # allowing the server to check for shutdown.
    self._endpoints_server.timeout = 0.05
    self.__serving = True

    super(ServerThread, self).__init__(*args, **kwargs)

  def Shutdown(self):
    """Notify server that it must shutdown gracefully."""
    self.__serving = False

  def run(self):
    """Handle incoming requests until shutdown."""
    while self.__serving:
      self._endpoints_server.handle_request()

    self._endpoints_server = None

def _PickUnusedPort():
  """Find an unused port to use in tests.

  Derived from Damon Kohlers example:
      http://code.activestate.com/recipes/531822-pick-unused-port

  Returns:
    An unused port integer.
  """
  temp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  try:
    temp.bind(('localhost', 0))
    port = temp.getsockname()[1]
  finally:
    temp.close()
  return port
