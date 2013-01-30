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

"""Tests for files_client.py."""

from tests.common import testing

import os
from titan.common.lib.google.apputils import basetest
from tests.common import titan_rpc_stub
from titan import files
from titan.files import handlers
from tests.files import files_client_test
from titan.files.mixins import versions
from titan.files.mixins import versions_client
from titan.files.mixins import versions_views

class VersionedFile(versions.FileVersioningMixin, files.File):
  pass

class TestableRemoteVcsFactory(versions_client.RemoteVcsFactory):
  """A RemoteVcsFactory with a stubbed-out TitanClient."""

  def _GetTitanClient(self, **kwargs):
    kwargs['stub_wsgi_apps'] = [
        versions_views.application,
        handlers.application
    ]
    return titan_rpc_stub.TitanClientStub(**kwargs)

class VersionsClientTestCase(testing.BaseTestCase):

  def setUp(self):
    files.RegisterFileFactory(lambda *args, **kwargs: VersionedFile)
    super(VersionsClientTestCase, self).setUp()
    self.remote_vcs_factory = TestableRemoteVcsFactory(
        host=os.environ['HTTP_HOST'])
    self.remote_file_factory = files_client_test.TestableRemoteFileFactory(
        host=os.environ['HTTP_HOST'])

  def testRemoteVersionControlService(self):
    remote_vcs = self.remote_vcs_factory.MakeRemoteVersionControlService()
    remote_changeset = remote_vcs.NewStagingChangeset()

    # Error handling: writing without a changeset.
    remote_file = self.remote_file_factory.MakeRemoteFile('/a/foo')
    # TODO(user): build more specific remote objects error handling.
    self.assertRaises(Exception, remote_file.Write, 'foo!')

    remote_file = self.remote_file_factory.MakeRemoteFile(
        '/a/foo', changeset=remote_changeset.num)
    remote_file.Write('foo!')
    remote_changeset.AssociateFile(remote_file)

    # Error handling: changeset's files are not finalized.
    self.assertRaises(
        versions_client.RemoteChangesetError,
        remote_vcs.Commit, remote_changeset)
    self.assertRaises(
        versions_client.RemoteChangesetError,
        remote_changeset.GetFiles)

    remote_changeset.FinalizeAssociatedFiles()
    remote_vcs.Commit(remote_changeset)
    self.assertEqual(['/a/foo'], remote_changeset.GetFiles().keys())

    # Verify actual state.
    actual_file = files.File('/a/foo')
    self.assertEqual('foo!', actual_file.content)
    self.assertTrue(actual_file.changeset)

    # Test Commit(force=True).
    remote_changeset = remote_vcs.NewStagingChangeset()
    self.assertRaises(Exception,
                      remote_vcs.Commit, remote_changeset, force=True)
    remote_file = self.remote_file_factory.MakeRemoteFile(
        '/a/foo', changeset=remote_changeset.num)
    remote_file.Write('bar!')
    # Don't call AssociateFile or FinalizeAssociatedFiles.
    remote_vcs.Commit(remote_changeset, force=True)
    actual_file = files.File('/a/foo')
    self.assertEqual('bar!', actual_file.content)
    self.assertTrue(actual_file.changeset)

    # Test MakeRemoteChangeset().
    num = actual_file.changeset.num
    remote_changeset = self.remote_vcs_factory.MakeRemoteChangeset(num)

if __name__ == '__main__':
  basetest.main()
