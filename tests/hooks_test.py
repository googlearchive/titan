#!/usr/bin/env python
# Copyright 2011 Google Inc. All Rights Reserved.
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

"""Tests for hooks.py."""

from tests import testing

from titan.common.lib.google.apputils import basetest
from titan.common import hooks

class HooksTest(testing.ServicesTestCase):

  def tearDown(self):
    hooks._global_hooks = {}
    hooks._global_services_order = []

  def testHooks(self):
    bar_foo_hook = self.mox.CreateMockAnything()
    baz_foo_hook = self.mox.CreateMockAnything()

    # Order is important here...it's an onion. The pre hooks are called in the
    # _global_services_order and the post hooks are called in reverse.
    #
    # Call stack for the wrapped Foo method:
    # Baz pre hook --> Bar pre hook --> Foo --> Bar post hook --> Baz post hook
    baz_foo_hook().AndReturn(baz_foo_hook)
    baz_foo_hook.Pre(
        base_arg=False, baz_arg=False, bar_arg=False
    ).AndReturn({'base_arg': True})  # baz modifies the base_arg
    bar_foo_hook().AndReturn(bar_foo_hook)
    bar_foo_hook.Pre(
        base_arg=True, baz_arg=False, bar_arg=False
    ).AndReturn(None)
    # The core titan method will be called here.
    bar_foo_hook.Post(0).AndReturn(1)
    baz_foo_hook.Post(1).AndReturn(2)

    self.mox.ReplayAll()

    # First, the baz and bar services register.
    hooks.RegisterHook('baz', 'foo-hook', hook_class=baz_foo_hook)
    hooks.RegisterHook('bar', 'foo-hook', hook_class=bar_foo_hook)

    # Then, the decorated and wrapped Foo() method is called.
    # "Foo" is a core Titan function and requires "base_arg".
    # The "bar" service requires "bar_arg", similarly for the "baz" service.
    @hooks.ProvideHook('foo-hook')
    def Foo(base_arg):
      self.assertTrue(base_arg)
      return 0

    result = Foo(base_arg=False, bar_arg=False, baz_arg=False)
    self.assertEqual(2, result)

    self.mox.VerifyAll()

  def testTitanMethodResult(self):
    """Verify that TitanMethodResult correctly short circuits hook responses."""
    bar_foo_hook = self.mox.CreateMockAnything()
    baz_foo_hook = self.mox.CreateMockAnything()

    # Call stack for the wrapped Foo method:
    # Baz pre hook --> Bar pre hook --> Foo --> Bar post hook --> Baz post hook
    #
    # First time through, the Baz pre hook returns a TitanMethodResult.
    baz_foo_hook().AndReturn(baz_foo_hook)
    baz_foo_hook.Pre().AndReturn(hooks.TitanMethodResult('first-result'))

    # Second time through, the Bar post hook returns a TitanMethodResult.
    baz_foo_hook().AndReturn(baz_foo_hook)
    baz_foo_hook.Pre().AndReturn(None)
    bar_foo_hook().AndReturn(bar_foo_hook)
    bar_foo_hook.Pre().AndReturn(None)
    # The core titan method will be called here.
    bar_foo_hook.Post(0).AndReturn(hooks.TitanMethodResult('second-result'))

    self.mox.ReplayAll()

    # Then, the decorated and wrapped Foo() method is called twice.
    @hooks.ProvideHook('foo-hook')
    def Foo():
      return 0

    # First, the baz and bar services register.
    hooks.RegisterHook('baz', 'foo-hook', hook_class=baz_foo_hook)
    hooks.RegisterHook('bar', 'foo-hook', hook_class=bar_foo_hook)
    # Also, throw in an arbitrary service and disable it in the calls below
    # to test the disabled_services path.
    hooks.RegisterHook('disabled', 'foo-hook', hook_class=bar_foo_hook)

    self.assertEqual('first-result', Foo(disabled_services=['disabled']))
    self.assertEqual('second-result', Foo(disabled_services=['disabled']))
    self.assertEqual(0, Foo(disabled_services=True))
    self.assertRaises(TypeError, Foo, disabled_services='disabled')

    self.mox.VerifyAll()

  def testComposeArguments(self):

    def Method(foo, bar=None, baz=False):
      pass

    expected = {'foo': 1, 'bar': True, 'baz': True, 'non_core': True}
    core_args, composite_kwargs = hooks.ProvideHook._ComposeArguments(
        Method, 1, True, baz=True, non_core=True)
    self.assertListEqual(['foo', 'bar', 'baz'], core_args)
    self.assertDictEqual(expected, composite_kwargs)

    expected = {'foo': 1, 'bar': None, 'baz': True}
    core_args, composite_kwargs = hooks.ProvideHook._ComposeArguments(
        Method, foo=1, baz=True)
    self.assertListEqual(['foo', 'bar', 'baz'], core_args)
    self.assertDictEqual(expected, composite_kwargs)

    # Error handling:
    self.assertRaises(TypeError, hooks.ProvideHook._ComposeArguments(Method))

  def testLoadServices(self):
    # Verify "does not define a RegisterService() method" error.
    self.assertRaises(AttributeError, self.EnableServices, ('sys',))

    # Basic integration with the versions service just for testing LoadServices.
    services = (
        'titan.services.versions',
    )
    self.EnableServices(services)
    self.assertIn('file-write', hooks._global_hooks)
    self.assertEqual(['versions'], hooks._global_services_order)

  def testOnError(self):

    class FooError(Exception):
      pass

    # Set up a mock hook.
    mock_hook_class = self.mox.CreateMockAnything()
    mock_hook = self.mox.CreateMockAnything()
    mock_hook_class().AndReturn(mock_hook)
    # Raise an error when Pre() hook is called.
    mock_hook.Pre().AndRaise(FooError)
    # Verify that the OnError method is called.
    mock_hook.OnError(testing.mox.IsA(FooError))
    self.mox.ReplayAll()

    hooks.RegisterHook('foo', 'foo-hook', hook_class=mock_hook_class)

    @hooks.ProvideHook('foo-hook')
    def Foo():
      return 0

    self.assertRaises(FooError, Foo)

    self.mox.VerifyAll()

  def testParamValidators(self):

    def ParamValidatorForFoo(request_params):
      return {'service_arg': int(request_params['service_arg'])}

    hooks.RegisterParamValidator(service_name='service', hook_name='http-foo',
                                 validator_func=ParamValidatorForFoo)

    request_params = {'base_arg': 'base', 'service_arg': '123'}
    actual_valid_kwargs = hooks.GetValidParams('http-foo', request_params)
    expected_valid_kwargs = {'service_arg': 123}
    self.assertDictEqual(expected_valid_kwargs, actual_valid_kwargs)

    # Error handling.
    self.assertRaises(KeyError, hooks.GetValidParams, 'http-foo', {})
    self.assertRaises(ValueError,
                      hooks.GetValidParams, 'http-foo', {'service_arg': ''})

if __name__ == '__main__':
  basetest.main()
