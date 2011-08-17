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

"""Hook system to provide services or "plugins" that wrap Titan file operations.

Documentation:
  http://code.google.com/p/titan-files/wiki/Services
"""

import functools
import inspect
import sys

try:
  import appengine_config
except ImportError:
  appengine_config = None

# For now, store registered services in global variables. These vars are
# populated when files.py is imported and runs LoadServices(). Because
# of this, we can guarantee services are registered at the lowest level
# no matter which handler is hit first.
#
# Data structure:
#   _global_hooks = {'<hook name>': <HookContainer object>}
#   _global_services_order = ['<service name>']
#
# The order of the names in _global_services_order is determined by the order
# that they were registered (specified by appengine_config.TITAN_SERVICES).
_global_hooks = {}
_global_services_order = []

def LoadServices(config_module=appengine_config):
  """Load all the services specified by appengine_config.TITAN_SERVICES."""
  if not hasattr(config_module, 'TITAN_SERVICES'):
    # No services are configured.
    return
  for service_module_str in config_module.TITAN_SERVICES:
    # Load each module and call module.RegisterService(). Each service must
    # define RegisterService(), which calls RegisterHook for each service hook.
    __import__(service_module_str)
    service_module = sys.modules[service_module_str]
    if not hasattr(service_module, 'RegisterService'):
      raise AttributeError(
          'Service module "%s" does not define a RegisterService() method. '
          'This method is required for all service modules.'
          % service_module_str)
    service_module.RegisterService()

class ProvideHook(object):
  """Decorator for wrapping a function to execute pre and post hooks."""

  def __init__(self, hook_name):
    self.hook_name = hook_name

  def __call__(self, func):

    @functools.wraps(func)
    def WrappedFunc(*func_args, **func_kwargs):
      core_arg_names, composite_kwargs = self._ComposeArguments(
          func, *func_args, **func_kwargs)
      return self._HandleHookedCall(
          self.hook_name, func, core_arg_names, composite_kwargs)
    return WrappedFunc

  @staticmethod
  def _ComposeArguments(func, *args, **kwargs):
    """Condense all args and kwargs into a single kwargs dictionary."""
    core_arg_names, _, _, defaults = inspect.getargspec(func)
    composite_kwargs = {}

    # Loop through the defaults backwards, associating each to its core arg
    # name. Anything left over is the name of a core method positional arg.
    defaults = defaults or ()
    for i, default in enumerate(defaults[::-1]):
      composite_kwargs[core_arg_names[-(i + 1)]] = default

    # Overlay given positional arguments over their keyword-arg equivalent.
    for i, arg in enumerate(args):
      composite_kwargs[core_arg_names[i]] = arg

    # Overlay given keyword arguments over the defaults.
    composite_kwargs.update(kwargs)
    return core_arg_names, composite_kwargs

  @staticmethod
  def _HandleHookedCall(hook_name, func, core_args, composite_kwargs):
    """Executing pre and post hooks around the given function."""
    # Pull out the services_override var before executing hooks.
    services_override = composite_kwargs.get('services_override')
    if services_override is not None:
      del composite_kwargs['services_override']

    # Get the current hooks if any exist.
    hooks = _global_hooks.get(hook_name)
    if hooks:
      result = hooks.RunWithHooks(
          services_override, func, core_args, composite_kwargs)
    else:
      result = func(**composite_kwargs)
    return result

def RegisterHook(service_name, hook_name, hook_class):
  """Register pre and post callbacks for a given service at a specific hook.

  Args:
    service_name: A unique service name string identifying the plugin.
    hook_name: The hook name provided by the core Titan methods.
    hook_class: A class pointer to a subclass of Hook.
  """
  # Get the hooks object for this service.
  if hook_name not in _global_hooks:
    _global_hooks[hook_name] = HookContainer()
  hooks = _global_hooks[hook_name]

  # Make sure the given service_name is registered the first time the
  # service_name is seen.
  if service_name not in _global_services_order:
    _global_services_order.append(service_name)
  hooks.RegisterHook(service_name=service_name, hook_class=hook_class)

class Hook(object):
  """A base hook object. Subclasses should define Pre and/or Post methods."""
  # For all hooks to inherit, to support future needs.

class HookContainer(object):
  """A container for all callbacks at a specific hook point."""

  def __init__(self):
    self._hook_classes = {}

  def RegisterHook(self, service_name, hook_class):
    self._hook_classes[service_name] = hook_class

  def RunWithHooks(self, services_override, func, core_args, composite_kwargs):
    """Run the given method and arguments with any registered hooks.

    Args:
      services_override: A list of the enabled service names, or None if all
          services are enabled.
      func: The low-level Titan method to call with **composite_kwargs.
      core_args: A list of strings of args accepted by the core method.
      composite_kwargs: A dictionary of all given arguments.
    Returns:
      The result of running func wrapped in the service layers.
    """
    hook_runner = HookRunner(self._hook_classes, services_override)
    return hook_runner.Run(func, core_args, composite_kwargs)

class HookRunner(object):
  """A one-time-use object to run a set of hooks around a core titan method."""

  def __init__(self, hook_classes, services_override=None):
    self._hooks = {}
    self._hook_classes = hook_classes
    self.services_override = services_override

  def Run(self, func, core_args, composite_kwargs):
    """Run pre hooks --> func --> post hooks."""
    # 1. Execute all service pre hooks, returning the final arguments dict.
    new_core_kwargs = self._ExecutePreHooks(core_args, composite_kwargs)

    # 2. Call the lowest-level Titan function using the arguments which have
    # gone through all service layers.
    data = func(**new_core_kwargs)

    # 3. Execute post hooks (in reverse order), possibly changing the results.
    return self._ExecutePostHooks(data)

  def _ExecutePreHooks(self, core_args, composite_kwargs):
    """In order of the global services, execute pre hooks."""
    new_core_kwargs = composite_kwargs.copy()
    for service_name in _global_services_order:
      service_is_enabled = (self.services_override is None
                            or service_name in self.services_override)
      if service_is_enabled and service_name in self._hook_classes:
        # Populate the hooks dictionary, which are also used by the post hooks.
        self._hooks[service_name] = self._hook_classes[service_name]()

        # Only call hooks which define the Pre() handler.
        if not hasattr(self._hooks[service_name], 'Pre'):
          continue

        # Service layers return None, or a dict of which core args to modify.
        args_to_change = self._hooks[service_name].Pre(**new_core_kwargs)
        if args_to_change:
          new_core_kwargs.update(args_to_change)

    # Remove non-core arguments which were consumed by service layers.
    core_kwargs = {}
    for core_arg in core_args:
      core_kwargs[core_arg] = new_core_kwargs[core_arg]
    return core_kwargs

  def _ExecutePostHooks(self, data):
    """In reverse order of the global services, execute post hooks.

    Args:
      data: The result of the lowest-level Titan operation.
    Returns:
      The result data, possibly modified by post hooks.
    """
    for service_name in reversed(_global_services_order):
      service_is_enabled = (self.services_override is None
                            or service_name in self.services_override)
      if service_is_enabled and service_name in self._hooks:
        # Only call hooks which define the Pre() handler.
        if not hasattr(self._hooks[service_name], 'Post'):
          continue

        # Each post hook is given the result of the command that just ran and
        # can modify the result, then must return it.
        data = self._hooks[service_name].Post(data)
    return data
