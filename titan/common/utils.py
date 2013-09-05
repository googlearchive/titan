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
import datetime
import errno
import functools
import hashlib
import inspect
import itertools
try:
  import json
except ImportError:
  import simplejson as json
import math
import mimetypes
import os
import string
import time
try:
  from google.appengine.api import files as blobstore_files
  from google.appengine.api import namespace_manager
except ImportError:
  # Allow this module to be imported by scripts which don't depend on functions
  # which use the App Engine libraries.
  pass

DEFAULT_CHUNK_SIZE = 1000

BLOBSTORE_APPEND_CHUNK_SIZE = 1 << 19  # 500 KiB

def get_common_dir_path(paths):
  """Given an iterable of file paths, returns the top common prefix."""
  common_dir = os.path.commonprefix(paths)
  if common_dir != '/' and common_dir.endswith('/'):
    return common_dir[:-1]
  # If common_dir doesn't end in a slash, we need to pop the last directory off.
  return os.path.split(common_dir)[0]

def validate_file_path(path):
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
  _validate_common_path(path)

def validate_dir_path(path):
  """Validates that a given directory path is valid.

  Args:
    path: An absolute directory path.
  Raises:
    ValueError: If the path is invalid.
  """
  if not path or not isinstance(path, basestring):
    raise ValueError('Path is invalid: %r' % path)
  _validate_common_path(path)

def _validate_common_path(path):
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

def validate_namespace(namespace):
  if namespace is not None:
    if not namespace:
      raise ValueError('Invalid namespace: {!r}'.format(namespace))
    namespace_manager.validate_namespace(namespace, exception=ValueError)

def guess_mime_type(path):
  return mimetypes.guess_type(path)[0] or 'application/octet-stream'

def split_segments(source, sep='/'):
  """Makes a list of all delimited segments."""
  # 'path/to/file' --> ['path', 'path/to', 'path/to/file']
  segments = []
  current = ''
  for segment in source.split(sep):
    current = sep.join([current, segment]) if current else segment
    segments.append(current)
  return segments

def split_path(path):
  """Makes a list of all containing dirs."""
  # '/path/to/some/file' --> ['/', '/path', '/path/to', '/path/to/some']
  if path == '/':
    return []
  path = os.path.split(path)[0]
  return split_path(path) + [path]

def safe_join(base, *paths):
  """A safe version of os.path.join.

  The os.path.join() method opens a directory traversal vulnerability when a
  user-supplied input begins with a slash. With this method, any intermediary
  path that starts with slash will raise an error.

  Args:
    base: Base directory.
    **paths: List of paths to join.
  Returns:
    pass
  Raises:
    ValueError: If any intermediate path starts with a slash.
  """
  result = base
  for path in paths:
    # Prevent directory traversal attacks by preventing intermediate paths that
    # start with a slash.
    if path.startswith('/'):
      raise ValueError('Intermediate path cannot start with \'/\': %s' % path)

    if result == '' or result.endswith('/'):
      result += path
    else:
      result += '/' + path
  return result

def make_destination_paths_map(source_paths, destination_dir_path,
                               strip_prefix=None):
  """Create a mapping of source paths to destination paths.

  Args:
    source_paths: An iterable of absolute paths.
    destination_dir_path: A destination directory path.
    strip_prefix: A path prefix to strip from source paths.
  Raises:
    ValueError: On invalid input of source_paths or strip_prefix.
  Returns:
    A mapping of source paths to destination paths.
  """
  # Assume that source_paths and destination_dir_path have already been
  # validated to avoid re-processing (and since they'd fail at lower layers).
  if not hasattr(source_paths, '__iter__'):
    raise ValueError(
        '"source_paths" must be an iterable. Got: %r' % source_paths)

  # Add trailing slash to destination_dir_path and strip_prefix if not present.
  if not destination_dir_path.endswith('/'):
    destination_dir_path += '/'
  if strip_prefix and not strip_prefix.endswith('/'):
    strip_prefix += '/'
  elif not strip_prefix:
    strip_prefix = '/'

  destination_map = {}
  for source_path in source_paths:
    if not source_path.startswith(strip_prefix):
      raise ValueError(
          'Mismatch of source_paths and strip_prefix: could not strip '
          '%r from source path %r.' % (strip_prefix, source_path))
    temp_source_path = source_path[len(strip_prefix):]
    destination_path = destination_dir_path + temp_source_path
    destination_map[source_path] = destination_path
  return destination_map

def chunk_generator(iterable, chunk_size=DEFAULT_CHUNK_SIZE):
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

def compose_method_kwargs(func):
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

def make_dirs(dir_path):
  """A thread-safe version of os.makedirs."""
  if not os.path.exists(dir_path) or not os.path.isdir(dir_path):
    try:
      os.makedirs(dir_path)
    except OSError, e:
      if e.errno == errno.EEXIST:
        # The directory exists (probably from another thread creating it),
        # but we need to keep going all the way until the tail directory.
        make_dirs(dir_path)
      else:
        raise

def write_to_blobstore(content, old_blobinfo=None):
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

  # write new blob.
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

def run_with_backoff(func, runtime=60, min_backoff=1, max_backoff=10,
                   expontential_backoff=True, stop_on_success=False, **kwargs):
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
    result = func(**kwargs)
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

def generate_shard_names(num_shards):
  """Generates strings of of ASCII sequence permutations for naming shards.

  Usage:
    >>> generate_shard_names(3)
    ['abc', 'acb', 'bac']
  Args:
    num_shards: The number of shard names to generate.
  Returns:
    A list of generally monotonically-increasing ASCII sequence strings.
  """
  # Find a number of permutations that is larger than the number of shards
  # requested. The number of ordered permutations is just a factorial.
  permutation_length = None
  for i in range(1, 10):  # Support num_shards from 1 to 362880.
    if math.factorial(i) > num_shards:
      permutation_length = i
      break
  # Generate permutations of ASCII letters, then trim it to num_shards.
  permutable_characters = string.ascii_letters[:permutation_length]
  tags = [''.join(p) for p in itertools.permutations(permutable_characters)]
  return tags[:num_shards]

class CustomJsonEncoder(json.JSONEncoder):
  """A custom JSON encoder to support objects providing a Serialize() method."""

  def __init__(self, *args, **kwargs):
    self.full = kwargs.pop('full', None)
    super(CustomJsonEncoder, self).__init__(*args, **kwargs)

  def default(self, obj):
    """Override of json.JSONEncoder method."""
    # Objects with custom Serialize() function.
    serialize_func = (getattr(obj, 'serialize', None) or
                      getattr(obj, 'Serialize', None))
    if serialize_func:
      if self.full is not None:
        return serialize_func(full=self.full)
      return serialize_func()

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

  def __eq__(self, other):
    serialize_func = getattr(
        self, 'serialize', getattr(self, 'Serialize', None))
    other_serialize_func = getattr(
        other, 'serialize', getattr(other, 'Serialize', None))
    if not serialize_func or not other_serialize_func:
      return False
    return serialize_func() == other_serialize_func()

  def serialize(self):
    return self._data.copy()

def humanize_duration(duration, separator=' '):
  """Formats a nonnegative number of seconds into a human-readable string.

  Args:
    duration: A float duration in seconds.
    separator: A string separator between days, hours, minutes and seconds.

  Returns:
    Formatted string like '5d 12h 30m 45s'.
  """
  try:
    delta = datetime.timedelta(seconds=duration)
  except OverflowError:
    return '>=' + humanize_time_delta(datetime.timedelta.max)
  return humanize_time_delta(delta, separator=separator)

def humanize_time_delta(delta, separator=' '):
  """Format a datetime.timedelta into a human-readable string.

  Args:
    delta: The datetime.timedelta to format.
    separator: A string separator between days, hours, minutes and seconds.

  Returns:
    Formatted string like '5d 12h 30m 45s'.
  """
  parts = []
  seconds = delta.seconds
  if delta.days:
    parts.append('%dd' % delta.days)
  if seconds >= 3600:
    parts.append('%dh' % (seconds // 3600))
    seconds %= 3600
  if seconds >= 60:
    parts.append('%dm' % (seconds // 60))
    seconds %= 60
  seconds += delta.microseconds / 1e6
  if seconds or not parts:
    parts.append('%gs' % seconds)
  return separator.join(parts)
