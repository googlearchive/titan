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

"""Tests for versions.py."""

from tests import testing

from titan.common.lib.google.apputils import basetest
from titan.files import files
from titan.services import versions

TITAN_SERVICES = (
    'titan.services.versions',
)

class VersionsTest(testing.ServicesTestCase):

  def testVersions(self):
    # TODO(user): implement the versions service, but until then
    # verify that the hook wrapping is working correctly.
    self.assertRaises(files.BadFileError, files.Get, '/foo')
    self.EnableServices(__name__)
    self.assertRaises(NotImplementedError, files.Get, '/foo')
    self.assertRaises(NotImplementedError, files.Write, '/foo')

if __name__ == '__main__':
  basetest.main()
