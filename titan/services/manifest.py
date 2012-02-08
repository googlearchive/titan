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

"""Service which maintains a manifest file, listing a file's available variants.

Example:
  # If we have language-specific variants of a file:
  variant_data = manifest.VariantData(base_path='/foo.html', lang='de')
  files.Write('/de/foo.html', 'German Foo', variant_data=variant_data)

  # And, to retrieve the manifest of available variants:
  manifest.GetManifest('/foo.html')

Documentation:
  http://code.google.com/p/titan-files/wiki/ManifestService
"""

try:
  import json
except ImportError:
  import simplejson as json
from google.appengine.ext import db
from titan.common import hooks
from titan.files import files

SERVICE_NAME = 'manifest'
DEFAULT_MANIFEST_FILE_EXTENSION = '.manifest'

class VariantData(object):
  """Encapsulation of data related to a unique file variant."""

  def __init__(self, base_path, **kwargs):
    files.ValidatePaths(base_path)
    self.base_path = base_path
    for key, value in kwargs.items():
      setattr(self, key, value)

  def Serialize(self):
    data = self.__dict__.copy()
    del data['base_path']
    return data

  def Validate(self):
    """Optional abstract method for customized validation."""
    pass

def RegisterService():
  hooks.RegisterHook(SERVICE_NAME, 'file-write', hook_class=HookForWrite)

class HookForWrite(hooks.Hook):
  """Hook for files.Write()."""

  @staticmethod
  def __UpdateOrCreateManifest(variant_path, manifest_path, data):
    """Update or create the manifest file.

    Args:
      variant_path: The path to the variant, ex: /de/foo.html
      manifest_path: The path to the manfiest, ex: /foo.html.manifest
      data: The dictionary of variant-specific data.
    """
    # Keep this function light--it's run in a transaction.
    manifest_file = files.Get(manifest_path, disabled_services=[SERVICE_NAME])
    new_data = {}
    if manifest_file:
      new_data.update(json.loads(manifest_file.content))
    new_data[variant_path] = data

    new_data = json.dumps(new_data)
    files.Write(manifest_path, new_data, disabled_services=[SERVICE_NAME])

  def Pre(self, variant_data=None, **kwargs):
    """Pre-hook method which updates manifest file.

    Args:
      variant_data: A VariantData instance.
    """
    if not variant_data:
      return
    variant_path = kwargs['path']
    manifest_path = variant_data.base_path + DEFAULT_MANIFEST_FILE_EXTENSION
    variant_data.Validate()
    data = variant_data.Serialize()

    # Update the manifest file. This is run in a transaction to avoid race
    # conditions where a manifest update could lose data when many files are
    # uploaded concurrently and all affect the same manifest.
    db.run_in_transaction(
        self.__UpdateOrCreateManifest,
        variant_path=variant_path, manifest_path=manifest_path, data=data)

def GetManifest(base_path):
  """Get the manifest file for a particular resource path.

  Args:
    base_path: The base path for which to get the manifest. Ex: /foo.html
  Returns:
    A dictionary containing the manifest data; keys are the variant paths.
  """
  manifest_file = files.Get(base_path + DEFAULT_MANIFEST_FILE_EXTENSION)
  return json.loads(manifest_file.content) if manifest_file else None
