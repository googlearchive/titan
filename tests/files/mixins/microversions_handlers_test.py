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

"""Tests for microversions_handlers.py."""

from tests.common import testing

import webtest

from titan.common.lib.google.apputils import basetest
from titan import files
from titan.files.mixins import microversions
from titan.files.mixins import microversions_handlers
from titan.files.mixins import versions

class MicroversionsHandlersTest(testing.BaseTestCase):

  def setUp(self):
    super(MicroversionsHandlersTest, self).setUp()
    self.app = webtest.TestApp(microversions_handlers.application)
    files.register_file_mixins(
        [microversions.MicroversioningMixin, versions.FileVersioningMixin])

  def testProcessMicroversionsHandler(self):
    self.stubs.SmartSet(microversions, 'DEFAULT_PROCESSING_TIMEOUT_SECONDS', 1)
    self.stubs.Set(microversions, 'process_data', lambda _: None)
    self.stubs.Set(microversions, 'process_data_with_backoff', lambda: None)

    # Weakly test execution path; the rest is tested by microversions_test.
    response = self.app.get('/_titan/files/microversions/processdata')
    self.assertEqual(200, response.status_int)
    response = self.app.get('/_titan/files/microversions/processdata?times=2')
    self.assertEqual(200, response.status_int)

if __name__ == '__main__':
  basetest.main()
