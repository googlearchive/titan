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

"""Tests for manifest.py."""

from tests.common import testing

from titan.common.lib.google.apputils import basetest
from titan.files import files
from titan.services import manifest

class ManifestTest(testing.ServicesTestCase):

  def setUp(self):
    super(ManifestTest, self).setUp()
    services = (
        'titan.services.manifest',
    )
    self.EnableServices(services)

  def testWrite(self):
    variant_data = manifest.VariantData(base_path='/foo.html', lang='de')
    files.Write('/de/foo.html', 'German Foo', variant_data=variant_data)
    variant_data = manifest.VariantData(base_path='/foo.html', lang='fr')
    files.Write('/fr/foo.html', 'French Foo', variant_data=variant_data)
    self.assertFalse(files.Exists('/foo.html'))

    manifest_data = manifest.GetManifest('/foo.html')
    expected_manifest = {
        '/de/foo.html': {
            'lang': 'de',
        },
        '/fr/foo.html': {
            'lang': 'fr',
        },
    }
    self.assertDictEqual(expected_manifest, manifest_data)
    self.assertEqual('German Foo', files.Get('/de/foo.html').content)
    self.assertEqual('French Foo', files.Get('/fr/foo.html').content)
    self.assertIsNone(manifest.GetManifest('/fake'))

    # No action taken when variant_data is not given.
    files.Write('/foo.html', 'bar')
    self.assertTrue(files.Exists('/foo.html'))

if __name__ == '__main__':
  basetest.main()
