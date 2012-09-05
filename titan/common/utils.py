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

import cStringIO
import errno
import functools
import hashlib
import inspect
import json
import mimetypes
import os
import time
try:
  from google.appengine.api import files as blobstore_files
except ImportError:
  # Allow this module to be imported by scripts which don't depend on functions
  # which use the App Engine libraries.
  pass

DEFAULT_CHUNK_SIZE = 1000

BLOBSTORE_APPEND_CHUNK_SIZE = 1 << 19  # 500 KiB

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
  if path.endswith('/'):
    raise ValueError('Path cannot end with a trailing "/": %s' % path)
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
  if len(path) > 500:
    raise ValueError('Path cannot be longer than 500 characters: %s' % path)

def GuessMimeType(path):
  return mimetypes.guess_type(path)[0] or 'application/octet-stream'

def SplitPath(path):
  """Makes a list of all containing dirs."""
  # '/path/to/some/file' --> ['/', '/path', '/path/to', '/path/to/some']
  if path == '/':
    return []
  path = os.path.split(path)[0]
  return SplitPath(path) + [path]

def ChunkGenerator(iterable, chunk_size=DEFAULT_CHUNK_SIZE):
  """Yield chunks of some iterable number of tasks at a time.

  Usage:
    for items_subset in utils.ChunkGenerator(items):
      # do something with items_subset
  Args:
    iterable: An iterable.
    chunk_size: The size of the yielded data.
  Yields:
    A slice of data from the iterable.
  """
  for i in xrange(0, len(iterable), chunk_size):
    yield iterable[i:i + chunk_size]

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

def MakeDirs(dir_path):
  """A thread-safe version of os.makedirs."""
  if not os.path.exists(dir_path) or not os.path.isdir(dir_path):
    try:
      os.makedirs(dir_path)
    except OSError, e:
      if e.errno == errno.EEXIST:
        # The directory exists (probably from another thread creating it),
        # but we need to keep going all the way until the tail directory.
        MakeDirs(dir_path)
      else:
        raise

def WriteToBlobstore(content, old_blobinfo=None):
  """Write content to blobstore.

  Args:
    content: A byte-string.
    old_blobinfo: An optional BlobInfo object used for comparison. If the
        md5_hash of the old blob and the new content are equal, the
        old blobkey is returned and nothing new is written.
  Returns:
    A BlobKey.
  """
  # Require pre-encoded content.
  assert not isinstance(content, unicode)

  # Blob de-duping: if the blob content is the same as the old file,
  # just use the old blob key to save space and RPCs.
  if (old_blobinfo
      and old_blobinfo.md5_hash == hashlib.md5(content).hexdigest()):
    return old_blobinfo.key()

  # Write new blob.
  filename = blobstore_files.blobstore.create()
  content_file = cStringIO.StringIO(content)
  blobstore_file = blobstore_files.open(filename, 'a')
  # Blobstore writes cannot exceed an RPC size limit, so chunk the writes.
  while True:
    content_chunk = content_file.read(BLOBSTORE_APPEND_CHUNK_SIZE)
    if not content_chunk:
      break
    blobstore_file.write(content_chunk)
  blobstore_file.close()
  blobstore_files.finalize(filename)
  blob_key = blobstore_files.blobstore.get_blob_key(filename)
  return blob_key

def RunWithBackoff(func, runtime=60, min_backoff=1, max_backoff=10,
                   expontential_backoff=True, stop_on_success=False):
  """Long-running function to process multiple windows.

  Does not catch errors.

  Args:
    func: The callable to run. Any null response indicates that the function
          did not process any data and the algorithm should backoff and sleep
          before retrying.
    runtime: The number of seconds to run for (approximate).
    min_backoff: The starting number of seconds to wait after null response.
    max_backoff: Number of seconds limiting the max of expontential backoff.
    expontential_backoff: Whether or not backoff should double.
    stop_on_success: Whether or not to stop on the first successful run of func.
  Returns:
    A list of results from callable().
  """
  results = []
  backoff = min_backoff
  end_time = time.time() + runtime
  while True:
    result = func()
    results.append(result)
    if result:
      backoff = min_backoff
      if stop_on_success:
        return results
    else:
      if time.time() + backoff > end_time:
        # If we're about to sleep past the end times, just quit now.
        break
      time.sleep(backoff)
      if expontential_backoff:
        backoff *= 2
      else:
        backoff += min_backoff
      if backoff > max_backoff:
        backoff = max_backoff
  return results

class CustomJsonEncoder(json.JSONEncoder):
  """A custom JSON encoder to support objects providing a Serialize() method."""

  def __init__(self, *args, **kwargs):
    self.full = kwargs.pop('full', None)
    super(CustomJsonEncoder, self).__init__(*args, **kwargs)

  def default(self, obj):
    """Override of json.JSONEncoder method."""
    # Objects with custom Serialize() function.
    if hasattr(obj, 'Serialize'):
      if self.full is not None:
        return obj.Serialize(full=self.full)
      return obj.Serialize()

    # Datetime objects => Unix timestamp.
    if hasattr(obj, 'timetuple'):
      # NOTE: timetuple() drops microseconds, so add it back in.
      return time.mktime(obj.timetuple()) + 1e-6 * obj.microsecond

    raise TypeError(repr(obj) + ' is not JSON serializable.')

class DictAsObject(object):
  """Lightweight wrapper for exposing a dictionary as object attributes."""

  def __init__(self, data):
    self._data = data

  def __getattr__(self, name):
    try:
      return self._data[name]
    except KeyError:
      raise AttributeError("'%s' object has no attribute '%s'"
                           % (self.__class__.__name__, name))

  def Serialize(self):
    return self._data.copy()
