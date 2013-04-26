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

"""App Engine RPC client for Titan Files versions mixin."""

import json
import urllib
from titan.common import titan_rpc
from titan.files import files_client

# Mixin-specific URL paths:
VERSIONS_CHANGESET_API = '/_titan/files/versions/changeset'
VERSIONS_CHANGESET_COMMIT_API = '/_titan/files/versions/changeset/commit'

class Error(Exception):
  pass

class RemoteChangesetError(Error):
  pass

class RemoteVcsFactory(titan_rpc.AbstractRemoteFactory):
  """Factory for creating RemoteVersionControlService objects."""

  def make_remote_vcs(self, *args, **kwargs):
    """Should be used to create all RemoteVersionControlService objects."""
    kwargs['_titan_client'] = self.titan_client
    return RemoteVersionControlService(*args, **kwargs)

  def make_remote_changeset(self, *args, **kwargs):
    """Should be used to create all RemoteChangeset objects."""
    kwargs['_titan_client'] = self.titan_client
    return RemoteChangeset(*args, **kwargs)

class RemoteVersionControlService(object):
  """A remote imitation of versions.VersionControlService."""

  def __init__(self, **kwargs):
    self._titan_client = kwargs.pop('_titan_client')

  def new_staging_changeset(self):
    """Imitation of versions.VersionControlService.NewStagingChangeset()."""
    response = self._titan_client.UrlFetch(VERSIONS_CHANGESET_API,
                                           method='POST')
    if not response.status_code in (200, 201):
      raise titan_rpc.RpcError(response.content)
    return RemoteChangeset(json.loads(response.content)['num'],
                           _titan_client=self._titan_client)

  def commit(self, staging_changeset, force=False):
    """Imitation of versions.VersionControlService.Commit()."""
    api_path = '%s?changeset=%d' % (VERSIONS_CHANGESET_COMMIT_API,
                                    staging_changeset.num)
    if force:
      api_path += '&force=true'
    payload = ''
    headers = {}
    if not force:
      # Pull manifest from staging_changeset.
      remote_files = staging_changeset.get_files()
      manifest = remote_files.keys()
      payload = urllib.urlencode({'manifest': json.dumps(manifest)})
    response = self._titan_client.UrlFetch(api_path, method='POST',
                                           payload=payload, headers=headers)
    if not 200 <= response.status_code <= 201:
      raise titan_rpc.RpcError(response.content)
    return RemoteChangeset(json.loads(response.content)['num'],
                           _titan_client=self._titan_client)

class RemoteChangeset(object):
  """Remote imitation of versions.Changeset."""

  def __init__(self, num, **kwargs):
    self._titan_client = kwargs.pop('_titan_client')
    self._num = int(num)
    self._associated_files = []
    self._finalized_files = False

  def __repr__(self):
    return '<RemoteChangeset num:%d>' % self.num

  @property
  def num(self):
    return self._num

  def associate_file(self, titan_file):
    """Imitation of versions.Changeset.AssociateFile()."""
    self._associated_files.append(titan_file)
    self._finalized_files = False

  def disassociate_file(self, titan_file):
    """Imitation of versions.Changeset.DisassociateFile()."""
    self._associated_files.remove(titan_file)
    self._finalized_files = False

  def finalize_associated_files(self):
    """Imitation of versions.Changeset.finalize_associated_files()."""
    if not self._associated_files:
      raise RemoteChangesetError(
          'Cannot finalize: no associated remote file objects.')
    self._finalized_files = True

  def get_files(self):
    """Imitation of versions.Changeset.get_files()."""
    if not self._finalized_files:
      raise RemoteChangesetError(
          'Cannot guarantee strong consistency when associated file paths '
          'have not been finalized. Perhaps you want list_files?')
    return files_client.RemoteFiles(files=self._associated_files,
                                    _titan_client=self._titan_client)
