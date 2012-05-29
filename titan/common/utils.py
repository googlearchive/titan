#!/usr/bin/env python
# Copyright 2012 Google Inc. All Rights Reserved.
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

"""Common utility functions."""

import functools
import inspect
import mimetypes
import os

def GetCommonDirPath(paths):
  """Given an iterable of file paths, returns the top common prefix."""
  common_dir = os.path.commonprefix(paths)
  if common_dir != '/' and common_dir.endswith('/'):
    return common_dir[:-1]
  # If common_dir doesn't end in a slash, we need to pop the last directory off.
  return os.path.split(common_dir)[0]

def ValidateFilePath(path):
  """Validates that a given file path is valid.

  Args:
    path: An absolute filename.
  Raises:
    ValueError: If the path is invalid.
  """
  if not path or not isinstance(path, basestring) or path == '/':
    raise ValueError('Path is invalid: ' + repr(path))
  _ValidateCommonPath(path)

def ValidateDirPath(path):
  """Validates that a given directory path is valid.

  Args:
    path: An absolute directory path.
  Raises:
    ValueError: If the path is invalid.
  """
  if not path or not isinstance(path, basestring):
    raise ValueError('Path is invalid: %r' % path)
  _ValidateCommonPath(path)

def _ValidateCommonPath(path):
  # TODO(user): re-write this to use a regex.
  if not path.startswith('/'):
    raise ValueError('Path must have a leading "/": %s' % path)
  if '//' in path:
    raise ValueError('Double-slashes ("//") are not allowed in '
                     'path: %s' % path)
  if '..' in path:
    raise ValueError('Double-dots ("..") are not allowed in path: %s' % path)
  if '\n' in path or '\r' in path:
    raise ValueError('Line-breaks are not allowed in path: %s' % path)

def GuessMimeType(path):
  return mimetypes.guess_type(path)[0] or 'application/octet-stream'

def SplitPath(path):
  """Makes a list of all containing dirs."""
  # '/path/to/some/file' --> ['/', '/path', '/path/to', '/path/to/some']
  if path == '/':
    return []
  path = os.path.split(path)[0]
  return SplitPath(path) + [path]

def ComposeMethodKwargs(func):
  """Decorator for composing all method arguments to be keyword arguments.

  This decorator is specifically created for subclass instance methods that
  only accept **kwargs; it is not useful to decorate a base class method.

  Usage:
    class Parent(object):
      def Foo(base_arg):
        print base_arg

    class Child(Parent):
      @utils.ComposeMethodKwargs
      def Foo(**kwargs):
        kwargs['base_arg'] = kwargs['base_arg'].lower()
        extra_arg = kwargs.pop('extra_arg', None)  # consumes argument.

    Child().Foo('FOO', extra_arg='child arg')  # prints 'foo'.

  Args:
    func: Decorated function.
  Returns:
    Decorator wrapper function.
  """

  @functools.wraps(func)
  def Wrapper(*args, **kwargs):
    """Wrapper function."""
    func_self = args[0]
    # Find the top-level parent class by method resolution order.
    # We need to read arguments from the super class because func_self is
    # a subclass method which just takes **kwargs, but we need to inspect
    # the core superclass arguments.
    # -1 is "object", -2 is the highest-level parent class.
    parent_class = inspect.getmro(func_self.__class__)[-2]
    func_to_inspect = getattr(parent_class, func.__name__)
    core_arg_names, _, _, defaults = inspect.getargspec(func_to_inspect)
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

    # Delete 'self' from kwargs and pass as first arg directly.
    del composite_kwargs[core_arg_names[0]]
    return func(func_self, **composite_kwargs)

  return Wrapper
