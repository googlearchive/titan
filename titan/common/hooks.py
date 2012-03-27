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
import logging
import sys
import traceback

# Store registered services in global variables. These vars are
# populated when files.py is imported and appengine_config runs LoadServices().
# Because of this, we can guarantee services are registered at the lowest level
# no matter which handler is hit first.
#
# Data structure:
#   _global_hooks = {'<hook name>': <HookContainer object>}
#   _global_service_configs = {'<service name>': <config object>}
#   _global_services_order = ['<service name>']
#
# The order of the names in _global_services_order is determined by the order
# that they were registered (specified by appengine_config.TITAN_SERVICES).
_global_hooks = {}
_global_service_configs = {}
_global_services_order = []

class ConfigError(KeyError):
  pass

def LoadServices(services):
  """Import and initialize the given service modules strings.

  This should be called at the module level in appengine_config.py.

  Args:
    services: An iterable of module name strings.
  """
  for service_module_str in services:
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

def SetServiceConfig(service_name, config):
  """Save an arbitrary config object for a particular service."""
  _global_service_configs[service_name] = config

def GetServiceConfig(service_name):
  """Returns the config object stored for a service."""
  if not service_name in _global_service_configs:
    raise ConfigError('No configuration provided for service "%s".'
                      % service_name)
  return _global_service_configs[service_name]

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
    # Pull out the disabled_services var before executing hooks. This argument
    # is consumed by the hooks system and shouldn't be passed to core methods.
    disabled_services = composite_kwargs.get('disabled_services')
    if disabled_services is not None:
      if (disabled_services is not True
          and not hasattr(disabled_services, '__iter__')):
        raise TypeError('disabled_services must be an iterable or True. Got: %r'
                        % disabled_services)
      del composite_kwargs['disabled_services']

    # Get the current hooks if any exist.
    hook_container = _global_hooks.get(hook_name)
    if hook_container and disabled_services is not True:
      result = hook_container.RunWithHooks(
          disabled_services, func, core_args, composite_kwargs)
    else:
      result = func(**composite_kwargs)
    return result

def _SetupServiceNameAndHooks(service_name, hook_name):
  """Verify correctly populated hooks object and hook name."""
  # Make sure the hooks object is populated.
  if hook_name not in _global_hooks:
    _global_hooks[hook_name] = HookContainer()

  # Make sure the given service_name is registered the first time the
  # service_name is seen.
  if service_name not in _global_services_order:
    _global_services_order.append(service_name)

def RegisterHook(service_name, hook_name, hook_class, hook_kwargs=None):
  """Register pre and post callbacks for a given service at a specific hook.

  Args:
    service_name: A unique service name string identifying the plugin.
    hook_name: The hook name provided by the core Titan methods.
    hook_class: A class pointer to a subclass of Hook.
    hook_kwargs: Keyword arguments to be passed to the hook constructor.
  """
  _SetupServiceNameAndHooks(service_name, hook_name)
  # Register the given hook class.
  hook_container = _global_hooks[hook_name]
  hook_container.RegisterHook(service_name=service_name, hook_class=hook_class,
                              hook_kwargs=hook_kwargs)

def RegisterParamValidator(service_name, hook_name, validator_func):
  """Register function which validates args to an HTTP handler.

  Args:
    service_name: A unique service name string identifying the plugin.
    hook_name: The hook name provided by the Titan HTTP handler method.
    validator_func: A callable which validates HTTP request params.
       This callable needs to accept one argument, "request_params", which
       will be a WebOb NestedMultiDict of the request parameters.
  """
  _SetupServiceNameAndHooks(service_name, hook_name)
  # Register the given hook class.
  hook_container = _global_hooks[hook_name]
  hook_container.RegisterParamValidator(service_name=service_name,
                                        validator_func=validator_func)

def GetValidParams(hook_name, request_params):
  """For use by Titan handlers; gets a validated list of request params.

  Args:
    hook_name: The hook name provided by the Titan HTTP handler method.
    request_params: A WebOb NestedMultiDict instance.
  Returns:
    A dictionary of valid parameters processed by all registered validators.
  """
  hook_container = _global_hooks.get(hook_name)
  if not hook_container:
    return {}
  return hook_container.RunParamValidators(request_params)

class Hook(object):
  """A base hook object. Subclasses should define Pre and/or Post methods."""
  # For all hooks to inherit, to support future needs.

class TitanMethodResult(object):
  """Wrapper for short circuiting responses from a Hook's Pre() or Post()."""

  def __init__(self, result):
    self.actual_result = result

class HookContainer(object):
  """A container for all callbacks at a specific hook point."""

  def __init__(self):
    self._hook_classes = {}
    self._hook_kwargs = {}
    self._param_validator_funcs = {}

  def RegisterHook(self, service_name, hook_class, hook_kwargs):
    self._hook_classes[service_name] = hook_class
    if hook_kwargs:
      self._hook_kwargs[service_name] = hook_kwargs

  def RegisterParamValidator(self, service_name, validator_func):
    self._param_validator_funcs[service_name] = validator_func

  def RunWithHooks(self, disabled_services, func, core_args, composite_kwargs):
    """Run the given method and arguments with any registered hooks.

    Args:
      disabled_services: A list of the service names to disable, None if all
          services are enabled, True if all services are disabled.
      func: The low-level Titan method to call with **composite_kwargs.
      core_args: A list of strings of args accepted by the core method.
      composite_kwargs: A dictionary of all given arguments.
    Returns:
      The result of running func wrapped in the service layers.
    """
    hook_runner = HookRunner(self._hook_classes, self._hook_kwargs,
                             disabled_services)
    return hook_runner.Run(func, core_args, composite_kwargs)

  def RunParamValidators(self, request_params):
    """Run all the param validators in the global services order.

    Args:
      request_params: A mapping of request parameters from an HTTP request.
    Returns:
      A dictionary of validated request parameters.
    """
    valid_params = {}
    for service_name in _global_services_order:
      validator_func = self._param_validator_funcs.get(service_name)
      if validator_func:
        valid_params.update(validator_func(request_params))
    return valid_params

class HookRunner(object):
  """A one-time-use object to run a set of hooks around a core titan method."""

  def __init__(self, hook_classes, hook_kwargs, disabled_services=None):
    self._hooks = {}
    self._hook_classes = hook_classes
    self._hook_kwargs = hook_kwargs
    self.disabled_services = disabled_services

  def Run(self, func, core_args, composite_kwargs):
    """Run pre hooks --> func --> post hooks."""
    try:
      # 1. Execute all service pre hooks, returning the final arguments dict.
      result_or_kwargs, is_final_result = self._ExecutePreHooks(
          core_args, composite_kwargs)
      if is_final_result:
        # The Pre() hook has short-circuited the response. Stop and return it.
        return result_or_kwargs.actual_result
      kwargs = result_or_kwargs

      # 2. Call the lowest-level Titan function using the arguments which have
      # gone through all service layers.
      data = func(**kwargs)

      # 3. Execute post hooks (in reverse order), possibly changing the results.
      return self._ExecutePostHooks(data)
    except Exception, e:
      self._ExecuteErrorHandlers(e)
      logging.exception('Error while calling %s:', func.__name__)
      raise

  def _ExecutePreHooks(self, core_args, composite_kwargs):
    """In order of the global services, execute pre hooks.

    Args:
      core_args: A list of strings of args accepted by the core method.
      composite_kwargs: A dictionary of all given arguments.
    Returns:
      A two-tuple of (<object>, is_final_result). This can be one of two forms:
      (<dictionary of new core args>, False) or (<final result obj>, True)
    """
    new_core_kwargs = composite_kwargs.copy()
    for service_name in _global_services_order:
      service_is_enabled = (self.disabled_services is None
                            or service_name not in self.disabled_services)
      if service_is_enabled and service_name in self._hook_classes:
        # Populate the hooks dictionary, which are also used by the post hooks.
        self._hooks[service_name] = self._hook_classes[service_name](
            **self._hook_kwargs.get(service_name, {}))

        # Only call hooks which define the Pre() handler.
        if not hasattr(self._hooks[service_name], 'Pre'):
          continue

        # Service layers return None, or a dict of which core args to modify,
        # or a TitanMethodResult object which short circuits the response.
        try:
          args_to_change = self._hooks[service_name].Pre(**new_core_kwargs)
        except TypeError:
          # Likely missing a method argument required by a hook.
          logging.error('Called %s.%s().Pre(**%s)',
                        self._hooks[service_name].__class__.__module__,
                        self._hooks[service_name].__class__.__name__,
                        new_core_kwargs)
          raise

        if args_to_change and isinstance(args_to_change, TitanMethodResult):
          # Short-circuit the response by returning this TitanMethodResult.
          return args_to_change, True
        elif args_to_change:
          # Pre-hooks can modify future disabled_services in their call stack.
          if 'disabled_services' in args_to_change:
            self.disabled_services = self.disabled_services or []
            self.disabled_services += args_to_change['disabled_services']
            del args_to_change['disabled_services']
          new_core_kwargs.update(args_to_change)

    # Remove non-core arguments which were consumed by service layers.
    core_kwargs = {}
    for core_arg in core_args:
      core_kwargs[core_arg] = new_core_kwargs[core_arg]
    return core_kwargs, False

  def _ExecutePostHooks(self, data):
    """In reverse order of the global services, execute post hooks.

    Args:
      data: The result of the lowest-level Titan operation.
    Returns:
      The result data, possibly modified by post hooks.
    """
    for service_name in reversed(_global_services_order):
      service_is_enabled = (self.disabled_services is None
                            or service_name not in self.disabled_services)
      if service_is_enabled and service_name in self._hooks:
        # Only call hooks which define the Post() handler.
        if not hasattr(self._hooks[service_name], 'Post'):
          continue

        # Each post hook is given the result of the command that just ran and
        # can modify the result, then must return it.
        try:
          data = self._hooks[service_name].Post(data)
        except TypeError:
          # Likely missing a method argument required by a hook.
          logging.error('Called %s.%s().Post(%s)',
                        self._hooks[service_name].__class__.__module__,
                        self._hooks[service_name].__class__.__name__,
                        data)
          raise

        # Post hooks can return a TitanMethodResult to short circuit the return.
        if data and isinstance(data, TitanMethodResult):
          return data.actual_result

    return data

  def _ExecuteErrorHandlers(self, error):
    """In order of the global services, execute on error hooks.

    Args:
      error: The Exception object that was raised.
    """
    for service_name in _global_services_order:
      service_is_enabled = (self.disabled_services is None
                            or service_name not in self.disabled_services)
      if service_is_enabled and service_name in self._hooks:
        # Only call hooks which define the OnError() handler.
        if not hasattr(self._hooks[service_name], 'OnError'):
          continue

        try:
          self._hooks[service_name].OnError(error)
        except Exception, e:
          # Since the original error will be re-raised in the hook runner,
          # ignore Exceptions that occur in the OnError methods and simply log
          # the error and traceback.
          logging.error('Exception while executing %s.OnError():\n%s\n%s',
                        service_name, e, traceback.format_exc())
