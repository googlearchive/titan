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

"""Titan Hook for timed expirations.

NOTE: This service must come last in your TITAN_SERVICES config. Specifically,
it must come after any path-modifying services.

Usage:
  files.Write('/path/to/file.txt', 'hello!', expires=60)  # expire after 60s.
  files.Get('/path/to/file.txt')  # True
  time.sleep(61)
  files.Exists('/path/to/file.txt')  # False
"""

import time
from titan import files
from titan.common import hooks

SERVICE_NAME = 'expirations'

def RegisterService():
  """Registers the hooks into the Titan services system."""
  hooks.RegisterHook(SERVICE_NAME, 'file-exists', hook_class=HookForExists)
  hooks.RegisterHook(SERVICE_NAME, 'file-write',
                     hook_class=HookForWriteAndTouch)
  hooks.RegisterHook(SERVICE_NAME, 'file-touch',
                     hook_class=HookForWriteAndTouch)
  hooks.RegisterHook(SERVICE_NAME, 'file-get', hook_class=HookForGet)
  hooks.RegisterHook(SERVICE_NAME, 'list-files', hook_class=HookForListFiles)
  hooks.RegisterHook(SERVICE_NAME, 'list-dir', hook_class=HookForListDir)

class HookForExists(hooks.Hook):
  """Hook for files.Exists that checks for timed expirations."""

  def Pre(self, path, **unused_kwargs):
    """Overrides the default files.Exists method by using files.Get instead."""
    return hooks.TitanMethodResult(bool(files.Get(path)))

class HookForWriteAndTouch(hooks.Hook):
  """Hook for files.Write that checks for timed expirations."""

  def Pre(self, expires=None, **kwargs):
    """Pre hook for files.Write.

    If an expiration time is provided, then it'll be written to the Titan file
    as a meta attribute.

    Args:
      expires: Relative time to expire in seconds.
    Returns:
      A dict of changed kwargs.
    """
    changed_kwargs = {}
    if expires:
      expires += time.time()

      meta = kwargs.get('meta') or {}
      meta['expires'] = expires
      changed_kwargs['meta'] = meta
    return changed_kwargs

class HookForGet(hooks.Hook):
  """Hook for files.Get that checks for timed expirations."""

  def Post(self, results):
    """Returns the Titan file object if it hasn't expired."""
    files_to_delete = []

    if results:
      if hasattr(results, 'has_key'):
        # Multiple Titan files in return.
        for path, file_obj in results.items():
          if _IsExpired(file_obj):
            files_to_delete.append(file_obj)
            del results[path]
      else:
        # Single Titan file in return.
        if _IsExpired(results):
          files_to_delete.append(results)
          results = None

    files.Delete(files_to_delete, async=True)
    return results

class HookForListFiles(hooks.Hook):
  """Hook for files.ListFiles that checks for timed expirations."""

  def Post(self, file_objs):
    """Iterates through the result set and removes any expired files."""
    expired, unexpired = _CheckExpirations(file_objs)
    files.Delete(expired, async=True)
    return unexpired

class HookForListDir(hooks.Hook):
  """Hook for files.ListDir that checks for timed expirations."""

  def Post(self, results):
    """Iterates through the files in the dir and removes any expired files."""
    dirs, file_objs = results
    expired, unexpired = _CheckExpirations(file_objs)
    files.Delete(expired, async=True)
    return dirs, unexpired

def _CheckExpirations(file_objs):
  """Returns a list of expired and unexpired file objs."""
  expired = []
  unexpired = []
  for file_obj in file_objs:
    if _IsExpired(file_obj):
      expired.append(file_obj)
    else:
      unexpired.append(file_obj)
  return expired, unexpired

def _IsExpired(file_obj):
  try:
    return file_obj.expires < time.time() if file_obj.expires else False
  except AttributeError:
    # Titan file has no "expires" attribute.
    return False
