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

"""Provides JSON validation and convenience methods.

Usage:
  # Sets json directly and saves the file.
  titan_file = files.File('/foo/file.json')
  titan_file.json = {'foo': 'bar'}
  titan_file.save()

  # Applies a JSON Patch (http://tools.ietf.org/html/rfc6902) to json
  # and saves the file.
  titan_file = files.File('/foo/file.json')
  titan_file.json = {'foo': 'bar'}
  titan_file.apply_patch([
      {'op': 'add', 'path': '/baz', 'value': 'qux'},
  ])
  titan_file.save()
"""

import copy
import json
import jsonpatch
from titan import files
from titan.common import utils

class Error(Exception):
  pass

class BadJsonError(ValueError, Error):
  pass

class DataFileNotFoundError(Error):
  pass

class UnsetValue(object):
  pass

class JsonMixin(files.File):
  """Mixin to provide JSON validation and convenience methods."""

  @classmethod
  def should_apply_mixin(cls, **kwargs):
    # Disable mixin for versioned files.
    if kwargs.get('_mixin_state', {}).get('is_versions_enabled'):
      return False
    return kwargs['path'].endswith('.json')

  @utils.compose_method_kwargs
  def __init__(self, **kwargs):
    self._json = UnsetValue
    super(JsonMixin, self).__init__(**kwargs)

  @property
  def json(self):
    if self._json is UnsetValue:
      try:
        self._json = json.loads(self.content)
      except ValueError:
        raise BadJsonError(self.content)
    return self._json

  @json.setter
  def json(self, value):
    self._json = value

  @property
  def has_unsaved_json(self):
    return self._json is not UnsetValue

  @utils.compose_method_kwargs
  def write(self, **kwargs):
    """Write method. See superclass docstring.

    Checks if the json content being written to the file is valid before
    writing it to the file and raises an error if invalid.
    Returns:
      Self-reference.
    """
    # JSON validation won't happen when writing blobs instead of content.
    # Prevent invalid JSON from ever being written to JSON files.
    if not self._is_django_template(kwargs['content'] or ''):
      self._json = self._get_json_or_die(kwargs['content'] or '')
    return super(JsonMixin, self).write(**kwargs)

  def _is_django_template(self, content):
    # Weak test to see if the file is actually a Django template that
    # generates a JSON file when rendered. These types of files are not
    # validated and do not allow the use of the json property.
    return '{%' in content

  def _get_json_or_die(self, content):
    try:
      return json.loads(content)
    except ValueError:
      message = 'Error saving %s. Invalid JSON: %s' % (self.path, repr(content))
      raise BadJsonError(message)

  def save(self, sort_keys=False, indent=2, separators=None, meta=None):
    """Dumps the JSON content to a string and writes it to the file."""
    if separators is None:
      separators = (',', ': ')
    content = json.dumps(self.json, sort_keys=sort_keys,
                         indent=indent, separators=separators)
    return self.write(content, meta=meta)

  def apply_patch(self, patch):
    """Applies a JSON Patch to the JSON content.

    Args:
      patch: JSON Patch.
    Returns:
      JSON content.
    """
    self._json = jsonpatch.apply_patch(self.json, patch)
    return self.json

class AbstractDataPersistence(object):
  """Convenience class for decoupling data objects from persistence layer.

  For first-party data objects that need persistent storage, subclass this
  class rather than files.File.

  The data is always stored as a JSON-serialized dictionary.

  Usage:
    class Widget(json_mixin.AbstractDataPersistence):

      def __init__(self, key):
        self.key = key
        self.__data_file = None

      @property
      def _data_file(self):
        if self.__data_file is None:
          self.__data_file = files.File('/{}.json'.format(self.key))
        return self.__data_file

    # Subclasses are dictionary-like and are stored when save() is called.
    widget = Widget(key='foo')
    widget['foo'] = True
    widget.save()
  """

  @property
  def _data_file(self):
    raise NotImplementedError(
        'Superclass must implement _data_file property. It must return a '
        'files.File object that is memoized per instance.')

  @property
  def exists(self):
    return self._data_file.exists

  def __setitem__(self, key, value):
    # Allow setting values to unsaved files.
    if not self.exists and not self._data_file.has_unsaved_json:
      self._data_file.json = {}

    self._data_file.json[key] = value

  def __getitem__(self, key):
    return self._data_file.json[key]

  def __delitem__(self, key):
    del self._data_file.json[key]

  def update(self, data):
    # Allow setting values to unsaved files.
    if not self.exists and not self._data_file.has_unsaved_json:
      self._data_file.json = {}
    self._data_file.json.update(data)

  def get(self, key, default=None):
    return self._data_file.json.get(key, default)

  def save(self, sort_keys=False, indent=2, separators=None, meta=None):
    """Save the current state of the object to the data file."""
    if not self.exists and not self._data_file.has_unsaved_json:
      # As opposed to the JsonMixin, we can allow non-existent data JSON files
      # to be saved without content because we assume it's an empty dictionary.
      self._data_file.json = {}
    self._data_file.save(
        sort_keys=sort_keys, indent=indent, separators=separators, meta=meta)

  def serialize(self):
    return copy.deepcopy(self._data_file.json)
