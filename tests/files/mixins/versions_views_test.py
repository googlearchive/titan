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

"""Tests for versions_views.py."""

from tests.common import testing

import json
import os

import mox
import webtest

from titan.common.lib.google.apputils import basetest
from titan.files.mixins import versions_views

class HandlersTest(testing.BaseTestCase):

  def setUp(self):
    super(HandlersTest, self).setUp()
    self.app = webtest.TestApp(versions_views.application)

  def testChangesetHandler(self):
    # Weakly test execution path:
    response = self.app.post('/_titan/files/versions/changeset')
    self.assertEqual(201, response.status_int)
    self.assertIn('num', json.loads(response.body))

  def testChangesetCommitHandler(self):
    mock_vcs = self.mox.CreateMockAnything()
    self.mox.StubOutWithMock(versions_views.versions, 'VersionControlService')
    versions_views.versions.VersionControlService().AndReturn(mock_vcs)
    mock_vcs.Commit(mox.IgnoreArg(), force=True).AndReturn('success')

    self.mox.ReplayAll()
    # Weakly test execution path:
    response = self.app.post(
        '/_titan/files/versions/changeset/commit?changeset=1')
    self.assertEqual(201, response.status_int)
    self.assertEqual('success', json.loads(response.body))
    self.mox.VerifyAll()

if __name__ == '__main__':
  basetest.main()
