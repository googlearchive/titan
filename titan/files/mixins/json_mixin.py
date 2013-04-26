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
  # Sets json_content directly and saves the file.
  titan_file = files.File('/foo/file.json')
  titan_file.json_content = {'foo': 'bar'}
  titan_file.save()

  # Applies a JSON Patch (http://tools.ietf.org/html/rfc6902) to json_content
  # and saves the file.
  titan_file = files.File('/foo/file.json')
  titan_file.json_content = {'foo': 'bar'}
  titan_file.apply_patch([
      {'op': 'add', 'path': '/baz', 'value': 'qux'},
  ])
  titan_file.save()
"""

import json
import jsonpatch
from titan import files
from titan.common import utils

class Error(Exception):
  pass

class BadJsonError(ValueError, Error):
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
    self._json_content = None
    super(JsonMixin, self).__init__(**kwargs)

  @property
  def json_content(self):
    if self._json_content is None:
      try:
        self._json_content = json.loads(self.content) if self.exists else {}
      except ValueError:
        raise BadJsonError(self.content)
    return self._json_content

  @json_content.setter
  def json_content(self, value):
    self._json_content = value

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
      self._json_content = self._get_json_or_die(kwargs['content'] or '')
    return super(JsonMixin, self).write(**kwargs)

  def _is_django_template(self, content):
    # Weak test to see if the file is actually a Django template that
    # generates a JSON file when rendered. These types of files are not
    # validated and do not allow the use of the json_content property.
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
    content = json.dumps(self.json_content, sort_keys=sort_keys,
                         indent=indent, separators=separators)
    return self.write(content, meta=meta)

  def apply_patch(self, patch):
    """Applies a JSON Patch to the JSON content.

    Args:
      patch: JSON Patch.
    Returns:
      JSON content.
    """
    self._json_content = jsonpatch.apply_patch(self.json_content, patch)
    return self.json_content
