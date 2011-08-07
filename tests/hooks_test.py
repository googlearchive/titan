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

from titan.common import testing

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
    core_titan = self.mox.CreateMockAnything()
    foo_hook = self.mox.CreateMockAnything()
    another_foo_hook = self.mox.CreateMockAnything()
    # "Foo" is a core Titan function which takes "base_arg".
    # The "bar" service hooks around Foo and requires "extra_arg".
    # The "baz" service hooks around Foo and requires "diff_arg".
    #
    # Return from baz service pre hook (which pops diff_arg):
    first_new_kwargs = {
        'base_arg': True,  # baz service modifies base_arg.
        'extra_arg': False,  # baz service passes on other arguments unmodified.
    }
    # Return from bar service pre hook (which pops extra_arg):
    second_new_kwargs = {
        'base_arg': False,  # bar service modifies base_arg.
    }

    # Order is important here...it's an onion. The pre hooks are called in the
    # _global_services_order and the post hooks are called in reverse.
    another_foo_hook().AndReturn(another_foo_hook)
    another_foo_hook.Pre(
        base_arg=False, diff_arg=False, extra_arg=False
    ).AndReturn(first_new_kwargs)
    foo_hook().AndReturn(foo_hook)
    foo_hook.Pre(
        base_arg=True, extra_arg=False
    ).AndReturn(second_new_kwargs)
    core_titan.Foo(base_arg=False).AndReturn(0)
    foo_hook.Post(0).AndReturn(1)
    another_foo_hook.Post(1).AndReturn(2)

    self.mox.ReplayAll()

    # First, the bar and baz services register.
    hooks.RegisterHook('baz', 'foo-hook', hook_class=another_foo_hook)
    hooks.RegisterHook('bar', 'foo-hook', hook_class=foo_hook)

    # Then, the decorated and wrapped Foo() method is called.
    decorator = hooks.ProvideHook('foo-hook')
    decorated_foo = decorator(core_titan.Foo)
    result = decorated_foo(base_arg=False, extra_arg=False, diff_arg=False)
    self.assertEqual(2, result)

    self.mox.VerifyAll()

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
