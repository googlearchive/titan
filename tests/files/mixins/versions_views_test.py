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
import urllib

import mox
import webtest

from titan.common.lib.google.apputils import basetest
from titan import files
from titan.files.mixins import versions
from titan.files.mixins import versions_views

class VersionedFile(versions.FileVersioningMixin, files.File):
  pass

class HandlersTest(testing.BaseTestCase):

  def setUp(self):
    super(HandlersTest, self).setUp()
    self.app = webtest.TestApp(versions_views.application)
    files.register_file_factory(lambda *args, **kwargs: VersionedFile)

  def testChangesetHandler(self):
    # Weakly test execution path:
    response = self.app.post('/_titan/files/versions/changeset')
    self.assertEqual(201, response.status_int)
    self.assertIn('num', json.loads(response.body))

  def testChangesetCommitHandler(self):
    mock_vcs = self.mox.CreateMockAnything()
    self.mox.StubOutWithMock(
        versions_views.versions, 'VersionControlService')

    # 1st:
    versions_views.versions.VersionControlService().AndReturn(mock_vcs)

    # 2nd:
    versions_views.versions.VersionControlService().AndReturn(mock_vcs)
    mock_vcs.commit(
        mox.IgnoreArg(), force=True, save_manifest=True).AndReturn('success')

    # 3rd:
    versions_views.versions.VersionControlService().AndReturn(mock_vcs)
    mock_vcs.commit(
        mox.IgnoreArg(), force=False, save_manifest=False).AndReturn('success')

    self.mox.ReplayAll()

    # Manifest and force not given.
    url = '/_titan/files/versions/changeset/commit?changeset=1'
    response = self.app.post(url, expect_errors=True)
    self.assertEqual(400, response.status_int)

    # Force eventually consistent commit.
    url = ('/_titan/files/versions/changeset/commit'
           '?changeset=1&force=true&save_manifest=true')
    response = self.app.post(url)
    self.assertEqual(201, response.status_int)
    self.assertEqual('success', json.loads(response.body))

    # Use manifest for strongly consistent commit.
    manifest = ['/foo', '/bar']
    url = ('/_titan/files/versions/changeset/commit'
           '?changeset=1&save_manifest=false')
    params = {'manifest': json.dumps(manifest)}
    response = self.app.post(url, params=params)
    self.assertEqual(201, response.status_int)
    self.assertEqual('success', json.loads(response.body))

    self.mox.VerifyAll()

if __name__ == '__main__':
  basetest.main()
