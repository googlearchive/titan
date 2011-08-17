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

TITAN_SERVICES = (
    # Module which does not define RegisterService() method:
    'sys',
)

class HooksTest(testing.ServicesTestCase):

  def tearDown(self):
    hooks._global_hooks = {}
    hooks._global_services_order = []

  def testHooks(self):
    bar_foo_hook = self.mox.CreateMockAnything()
    baz_foo_hook = self.mox.CreateMockAnything()

    # "Foo" is a core Titan function and requires "base_arg".
    # The "bar" service requires "bar_arg", similarly for the "baz" service.
    def Foo(base_arg):
      self.assertTrue(base_arg)
      return 0

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
    decorator = hooks.ProvideHook('foo-hook')
    decorated_foo = decorator(Foo)
    result = decorated_foo(base_arg=False, bar_arg=False, baz_arg=False)
    self.assertEqual(2, result)

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
    self.assertRaises(AttributeError, self.EnableServices, __name__)

    # Basic integration with the versions service just for testing LoadServices.
    global TITAN_SERVICES
    TITAN_SERVICES = (
        'titan.services.versions',
    )
    self.EnableServices(__name__)
    self.assertIn('file-write', hooks._global_hooks)
    self.assertEqual(['versions'], hooks._global_services_order)

if __name__ == '__main__':
  basetest.main()
