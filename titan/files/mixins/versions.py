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

"""Titan version control system, including atomic commits of groups of files.

Documentation:
  http://googlecloudplatform.github.io/titan/files/versions.html
"""

import hashlib
import logging
import re

from google.appengine.ext import ndb

from titan.common import strong_counters
from titan import files
from titan import users
from titan.common import utils

class ChangesetStatus(object):
  staging = 'new'
  presubmit = 'pre-submit'
  submitted = 'submitted'  # "final changeset".
  deleted = 'deleted'
  deleted_by_submit = 'deleted-by-submit'

class FileStatus(object):
  created = 'created'
  edited = 'edited'
  deleted = 'deleted'

VERSIONS_PATH_BASE_REGEX = re.compile('^/_titan/ver/([0-9]+)')
# For formating "/_titan/ver/123/some/file/path"
VERSIONS_PATH_FORMAT = '/_titan/ver/%d%s'

_CHANGESET_COUNTER_NAME = 'num_changesets'
_MAX_MANIFEST_SHARD_PATHS = 1000

class Error(Exception):
  pass

class ChangesetError(Error, ValueError):
  pass

class InvalidChangesetError(ChangesetError):
  pass

class FileVersionError(Error, ValueError):
  pass

class CommitError(Error, ValueError):
  pass

class InvalidBaseChangesetCommitError(CommitError):
  pass

class NoBaseManifestCommitError(CommitError):
  pass

class NamespaceMismatchError(Error):
  pass

class NoManifestError(ChangesetError):
  pass

class ChangesetRebaseError(ChangesetError):
  pass

class FileVersioningMixin(files.File):
  """Mixin to provide versioned file handling.

  If created without an associated changeset, this object will dynamically
  determine the real file location from it's latest commited changeset.
  """

  @classmethod
  def should_apply_mixin(cls, **kwargs):
    # Enable always, unless microversions is enabled.
    mixin_state = kwargs.get('_mixin_state')
    if mixin_state and mixin_state.get('is_microversions_enabled'):
      return False
    if mixin_state is not None:
      mixin_state['is_versions_enabled'] = True
    return True

  @utils.compose_method_kwargs
  def __init__(self, path, **kwargs):
    # If given, this File represents the file at the given changeset.
    # If not, this File represents the latest committed file version,
    # but it cannot be written or changed (since that must happen with an
    # associated changeset).
    self.changeset = kwargs.get('changeset', None)

    # Internal-only flag to make this file not error if marked for delete.
    # This is used internally when listing files in a changeset, reverting
    # files, and in _copy_file_from_root.
    self._allow_deleted_files = kwargs.pop('_allow_deleted_files', False)

    # Internal-only state flag of whether manifested views are enabled or not.
    # This is used when reading/writing files, and internally in Changeset.
    # NOTE: This makes single File objects even less thread-safe, but there is
    # not yet another mechanism to make real_path be context aware.
    self.__set_enable_manifested_views(
        kwargs.pop('_enable_manifested_views', True))

    super(FileVersioningMixin, self).__init__(path, **kwargs)

    # Support initing with a /_titan/ver path if changeset is not given.
    # If it is, the changeset arg wins.
    versioned_path_match = VERSIONS_PATH_BASE_REGEX.match(path)
    if versioned_path_match and not self.changeset:
      self.changeset = int(versioned_path_match.group(1))
    # Strip /_titan/ver/123 from the path.
    if versioned_path_match:
      self._path = VERSIONS_PATH_BASE_REGEX.sub('', path)

    self._real_path = None
    if self.changeset and isinstance(self.changeset, int):
      # Support integer changeset argument.
      self.changeset = Changeset(self.changeset, namespace=self.namespace)

    if self.changeset and self.namespace != self.changeset.namespace:
      raise NamespaceMismatchError(
          'File namespace "{}" does not match changeset namespace "{}".'.format(
              self.namespace, self.changeset.namespace))

    # Make "changeset" a part of the file's composite key for hashing.
    if self.changeset:
      self._composite_key_elements['changeset'] = str(self.changeset.num)

  def __repr__(self):
    return '<File %s (cs:%r)>' % (self._path, getattr(self, 'changeset', None))

  @property
  def _file(self):
    """Handle dynamic determination of correct file entity."""
    if not self.changeset:
      # No associated changeset. Dynamically pick the file entity based on
      # the latest FilePointers.
      root_file_pointer = _FilePointer.get_root_key(namespace=self.namespace)
      file_pointer = _FilePointer.get_by_id(
          self.path, parent=root_file_pointer, namespace=self.namespace)
      if file_pointer:
        # Associate to the final changeset.
        self.changeset = Changeset(
            file_pointer.changeset_num,
            namespace=self.namespace).linked_changeset
        self._composite_key_elements['changeset'] = str(self.changeset.num)
      else:
        raise files.BadFileError('File does not exist: %s' % self.path)

    # A changeset exists, so real_path will resolve correctly. Fall through to
    # finding the file entity normally.
    file_ent = super(FileVersioningMixin, self)._file

    # For normal file interface interactions, if a file is "marked for delete"
    # in a changeset it should look deleted in this interface.
    if not self._allow_deleted_files and file_ent.status == FileStatus.deleted:
      raise files.BadFileError('File does not exist: %s' % self.path)
    return file_ent

  @property
  def created_by(self):
    created_by = super(FileVersioningMixin, self).created_by
    if not self._file.created_by:
      # Backwards-compatibility: before microversions had user passthrough,
      # files were written within tasks without a user. For these files, we
      # incur an extra RPC and fetch the data from the changelist instead.
      return self.changeset.created_by
    return created_by

  @property
  def modified_by(self):
    modified_by = super(FileVersioningMixin, self).modified_by
    if not self._file.modified_by:
      # Backwards-compatibility: before microversions had user passthrough,
      # files were written within tasks without a user. For these files, we
      # incur an extra RPC and fetch the data from the changelist instead.
      #
      # NOTE: This is correctly "created_by" and not "modified_by".
      return self.changeset.created_by
    return modified_by

  @property
  def real_path(self):
    """Override the storage location of the file to the versioned path."""
    if self._real_path:
      return self._real_path

    # MANIFESTED FILESYSTEM VIEWS.
    # Allow the entire filesystem tree to be viewed at all changesets,
    # not just the delta of file changes at that changeset. Also, provide
    # copy-on-write behavior for staging changesets.
    #
    # Rules:
    # - Files read through a 'new' or 'submitted' changeset: first pull
    #   from the changeset, then fall back to the base_changeset's manifest.
    #   The manifested filesystem should by overlaid by the changeset's files.
    # - Files read through a 'deleted' or 'deleted-by-submit' changeset:
    #   always pull from the base_changeset's manifest, ignore the changeset
    #   since its changes will manifest when viewed at it's corresponding
    #   submitted changeset.
    # - File modifications must go through 'new' changesets.

    _require_file_has_changeset(self)

    if not self._enable_manifested_views:
      # WRITE/DELETE and existence checking through a changeset.
      self._real_path = _make_versioned_path(self._path, self.changeset)
      return self._real_path

    # READ.
    kwargs = self._original_kwargs.copy()
    kwargs.pop('changeset', None)
    # Point to a non-existent changeset by default (mostly to catch
    # implementation errors).
    self._real_path = _make_versioned_path(
        self._path, Changeset(0, namespace=self.namespace))

    if self.changeset.status == ChangesetStatus.staging:
      if self.path in self.changeset:
        self._real_path = _make_versioned_path(self._path, self.changeset)
      elif self.changeset.base_changeset:
        # Read non-changeset file through manifest.
        titan_file = self.changeset.base_changeset.get_file_from_manifest(
            **kwargs)
        if titan_file is not None:
          self._real_path = titan_file.real_path

    elif self.changeset.status == ChangesetStatus.submitted:
      if self.path not in self.changeset.linked_changeset:
        # Read non-changeset file through manifest.
        titan_file = self.changeset.get_file_from_manifest(**kwargs)
        if titan_file is not None:
          self._real_path = titan_file.real_path
      else:
        # Optimization: instead of reading through the manifest (which would
        # still be correct), read directly from the submitted changeset.
        self._real_path = _make_versioned_path(
            self._path, self.changeset.linked_changeset)

    elif self.changeset.status in (ChangesetStatus.deleted_by_submit,
                                   ChangesetStatus.deleted):
      if not self.changeset.base_changeset:
        # Edge case: the first-ever staging changeset will not have a
        # base_changeset, and no files should exist yet. Point to the
        # non-existent "0" changeset.
        self._real_path = _make_versioned_path(
            self._path, Changeset(0, namespace=self.namespace))
        return self._real_path

      # Read all files through the deleted changeset's base_changeset manifest
      # so we don't need to search for the last submitted changeset.
      # This assumes that:
      # - 'deleted-by-submit' changesets MUST have been rebased to head
      #    before submit (even if HEAD is passed the deleted changeset number).
      # - 'deleted' changesets MUST have be rebased to the last submitted
      #   changeset, but only up to the deleted changeset's number.
      titan_file = self.changeset.base_changeset.get_file_from_manifest(
          **kwargs)
      if titan_file is not None:
        self._real_path = titan_file.real_path

    elif self.changeset.status == ChangesetStatus.presubmit:
      # Pre-submit changesets should only read from the current changeset.
      pass
    else:
      raise NotImplementedError(
          'Changeset status: "{}".'.format(self.changeset.status))
    return self._real_path

  @property
  def versioned_path(self):
    return self.real_path

  def __set_enable_manifested_views(self, enabled):
    """Sets whether or not manifested views are enabled."""
    # NOTE: This makes single File objects even less thread-safe, but there is
    # not yet another mechanism to make real_path be context aware.
    self._enable_manifested_views = enabled
    self._real_path = None  # Un-memoize property.

  @utils.compose_method_kwargs
  def write(self, **kwargs):
    """Write method. See superclass docstring."""
    _require_file_has_changeset(self)
    _require_file_has_staging_changeset(self)
    self.__set_enable_manifested_views(False)

    kwargs.pop('_run_mixins_only', False)
    mark_version_for_delete = kwargs.pop('_mark_version_for_delete', False)
    self.changeset.associate_file(self)

    # Update meta data.
    kwargs['meta'] = kwargs.get('meta') or {}
    # This is unfortunately generically named, but we need to reserve this name.
    assert 'status' not in kwargs['meta']

    # Never delete blobs when using versions.
    # This will orphan blobs if a large file is uploaded many times in a
    # changeset without committing, but that's better than losing the data.
    # TODO(user): add a flag to entities signifying if they have been
    # copied or deleted, so that we can notice and delete orphaned blobs.
    kwargs['_delete_old_blob'] = False

    if mark_version_for_delete:
      kwargs['content'] = ''
      kwargs['meta']['status'] = FileStatus.deleted
    else:
      kwargs['meta']['status'] = FileStatus.edited

      # The first time the versioned file is touched in the changeset, we have
      # to branch all content and properties from the base_changeset's file.
      # If the versioned file is marked for delete in the changeset and then
      # un-deleted by being touched, properties are NOT copied since the file
      # is treated as a brand new file, not a copy-on-write.
      if self.path not in self.changeset and self.changeset.base_changeset:

        if self.changeset.base_changeset.has_manifest:
          # If there's a manifest on the base_changeset, use that to determine
          # what the root file should be.
          root_file = self.changeset.base_changeset.get_file_from_manifest(
              self.path, namespace=self.namespace, **kwargs)
        else:
          # If there's not a manifest, there is no way to know what the right
          # root is without searching through historical changesets.
          # For now, just pull from the dynamically-determined root.
          root_file = files.File(self.path, namespace=self.namespace)

        if root_file is not None and root_file.exists:
          # Copy properties that were not changed in the current write request.
          # Don't use root_file.copy_to because it is troublesome.
          kwargs['mime_type'] = kwargs.get('mime_type', root_file.mime_type)
          kwargs['created'] = kwargs.get('created', root_file.created)
          kwargs['created_by'] = kwargs.get(
              'created_by', root_file.created_by)
          kwargs['modified_by'] = kwargs.get(
              'modified_by', root_file.modified_by)
          kwargs['modified'] = kwargs.get('modified', root_file.modified)
          if not kwargs['content'] and not kwargs['blob']:
            # Neither content or blob given, copy content from root_file.
            if root_file.blob:
              kwargs['blob'] = root_file.blob
            else:
              kwargs['content'] = root_file.content
          # Copy meta attributes.
          for key, value in root_file.meta.serialize().iteritems():
            if key not in kwargs['meta']:
              kwargs['meta'][key] = value
    try:
      return super(FileVersioningMixin, self).write(**kwargs)
    finally:
      self.__set_enable_manifested_views(True)

  @utils.compose_method_kwargs
  def delete(self, **kwargs):
    """Mark the file for deletion upon commit.

    To revert a file, use changeset.revert_file().

    Args:
      **kwargs: All composed keyword arguments for this method.
    Raises:
      InvalidChangesetError: If a changeset was not associated to this file.
    Returns:
      Self-reference.
    """
    _require_file_has_changeset(self)

    # From changeset.revert_file().
    if kwargs.pop('_revert', False):
      # Allow files that are marked for deletion to be reverted.
      self._allow_deleted_files = True
      result = super(FileVersioningMixin, self).delete(**kwargs)
      self._allow_deleted_files = False
      return result

    # Mark file for deletion.
    self.__set_enable_manifested_views(False)
    try:
      return self.write(content='', _mark_version_for_delete=True, **kwargs)
    finally:
      self.__set_enable_manifested_views(True)

# ------------------------------------------------------------------------------

class Changeset(object):
  """Unit of consistency over a group of files.

  Attributes:
    num: An integer of the changeset number.
    namespace: The datastore namespace, or None for the default namespace.
    created: datetime.datetime object of when the changeset was created.
    created_by: The User object of who created this changeset.
    status: An integer of one of the CHANGESET_* constants.
    base_path: The path prefix for all files in this changeset,
        for example: '/_titan/ver/123'
    linked_changeset_base_path: Same as base_path, but for the linked changeset.
    exists: If the given changeset exists.
  """

  # TODO(user): make changeset_ent protected.
  def __init__(self, num, namespace=None, changeset_ent=None):
    utils.validate_namespace(namespace)
    self._changeset_ent = changeset_ent
    self._num = int(num)
    self._namespace = namespace
    self._associated_files = set()
    self._finalized_files = False

  def __eq__(self, other):
    """Compare equality of two Changeset objects."""
    return (isinstance(other, self.__class__)
            and self.num == other.num
            and self.namespace == other.namespace)

  def __ne__(self, other):
    return not self == other

  def __contains__(self, path):
    assert isinstance(path, basestring)
    if self._finalized_files:
      return any([path == f.path for f in self._associated_files])
    return files.File(
        path,
        namespace=self.namespace, changeset=self,
        _internal=True, _allow_deleted_files=True,
        _enable_manifested_views=False).exists

  def __repr__(self):
    return '<Changeset %d evaluated: %s>' % (self._num,
                                             bool(self._changeset_ent))

  @property
  def changeset_ent(self):
    """Lazy-load the _Changeset entity."""
    if not self._changeset_ent:
      root_changeset = _Changeset.get_root_key(namespace=self.namespace)
      self._changeset_ent = _Changeset.get_by_id(
          str(self._num), parent=root_changeset, namespace=self.namespace)
      if not self._changeset_ent:
        raise ChangesetError('Changeset %s does not exist.' % self._num)
    return self._changeset_ent

  @property
  def num(self):
    return self._num

  @property
  def namespace(self):
    return self._namespace

  @property
  def created(self):
    return self.changeset_ent.created

  @property
  def created_by(self):
    return self.changeset_ent.created_by

  @property
  def status(self):
    return self.changeset_ent.status

  @property
  def base_path(self):
    return VERSIONS_PATH_FORMAT % (self.num, '')

  @property
  def linked_changeset_base_path(self):
    if self.linked_changeset:
      return VERSIONS_PATH_FORMAT % (self.linked_changeset_num, '')

  @property
  def linked_changeset(self):
    if self.linked_changeset_num:
      return Changeset(num=self.linked_changeset_num, namespace=self.namespace)

  @property
  def linked_changeset_num(self):
    if self.status not in (ChangesetStatus.staging, ChangesetStatus.deleted):
      return int(self.changeset_ent.linked_changeset.id())
    raise ChangesetError(
        'Cannot reference linked_changeset_num for "{}" changesets.'.format(
            self.status))

  @property
  def base_changeset(self):
    if self.base_changeset_num:
      return Changeset(num=self.base_changeset_num, namespace=self.namespace)

  @property
  def base_changeset_num(self):
    if self.status in (
        ChangesetStatus.staging, ChangesetStatus.deleted_by_submit):
      if self.changeset_ent.base_changeset:
        return int(self.changeset_ent.base_changeset.id())
      return
    raise ChangesetError(
        'Cannot reference base_changeset_num for "{}" changesets.'.format(
            self.status))

  @property
  def associated_paths(self):
    return set([f.path for f in self._associated_files])

  @property
  def has_manifest(self):
    if self.status != ChangesetStatus.submitted:
      raise ChangesetError(
          'Cannot reference has_manifest for "{}" changesets.'.format(
              self.status))
    return bool(self.changeset_ent.num_manifest_shards)

  @property
  def _num_manifest_shards(self):
    return self.changeset_ent.num_manifest_shards

  @property
  def exists(self):
    try:
      return bool(self.changeset_ent)
    except ChangesetError:
      return False

  def _get_manifest_shard_ent(self, path):
    """Get the shard entity which may contain the given path."""
    shard_index = _get_manifest_shard_index(path, self._num_manifest_shards)
    shard_id = _make_manifest_shard_id(self, shard_index)
    parent = _ChangesetManifestShard.get_root_key(
        final_changeset_num=self.num, namespace=self.namespace)
    return _ChangesetManifestShard.get_by_id(id=shard_id, parent=parent)

  def get_file_from_manifest(self, path, **kwargs):
    """Gets a file through the manifest.

    Args:
      path: The file path to look into the manifest.
      **kwargs: Other keyword args to pass through to the File object.
    Raises:
      NoManifestError: If the status of the changeset is not 'submitted', or
          if a manifest was not saved at this changeset.
    Returns:
      The file's associated Changeset, or None if the file doesn't exist.
    """
    if self.status != ChangesetStatus.submitted:
      raise NoManifestError(
          'Changeset {:d} with status "{}" does not have a manifest. Only '
          '"submitted" changesets may have manifests.'.format(
              self.num, self.status))
    if not self._num_manifest_shards:
      raise NoManifestError(
          'Changeset {:d} was not committed with a manifest.'.format(
              self.num))

    manifest_shard_ent = self._get_manifest_shard_ent(path=path)
    paths_to_changeset_num = manifest_shard_ent.paths_to_changeset_num
    if path not in paths_to_changeset_num:
      # We know deterministically that the given file did not exist
      # at this changeset.
      return None

    kwargs.pop('changeset', None)
    kwargs.pop('namespace', None)

    changeset = self.__class__(
        num=paths_to_changeset_num[path], namespace=self.namespace)
    titan_file = files.File(
        path, changeset=changeset, namespace=self.namespace, **kwargs)
    return titan_file

  def get_files(self):
    """Gets all files associated with this changeset.

    Guarantees strong consistency, but requires that associated file paths
    have been finalized on this specific Changeset instance.

    Raises:
      ChangesetError: If associated file paths have not been finalized.
    Returns:
      A files.Files object.
    """
    if self.status in (
        ChangesetStatus.deleted_by_submit, ChangesetStatus.deleted):
      raise ChangesetError(
          'Cannot get files from changeset {:d} which is "{}".'.format(
              self.num, self.status))
    if not self._finalized_files:
      raise ChangesetError(
          'Cannot guarantee strong consistency when associated file paths '
          'have not been finalized. Perhaps you want list_files?')

    titan_files = files.Files(
        files=list(self._associated_files), namespace=self.namespace)
    return titan_files

  def list_files(self):
    """Queries and returns a Files object containing this changeset's files.

    This method is always eventually consistent and may not contain recently
    changed files.

    Raises:
      ChangesetError: If the status is 'deleted' or 'deleted-by-submit'.
    Returns:
      A files.Files object.
    """
    if self.status in (
        ChangesetStatus.deleted_by_submit, ChangesetStatus.deleted):
      raise ChangesetError(
          'Cannot list files from changeset {:d} which is "{}".'.format(
              self.num, self.status))
    changeset = self
    if changeset.status == ChangesetStatus.submitted:
      # The files stored for submitted changesets are actually stored under the
      # the staging changeset's number, since they are never moved.
      changeset = changeset.linked_changeset

    versioned_files = files.Files.list(
        changeset.base_path, namespace=self.namespace, recursive=True,
        # Important: use changeset=self, not changeset=changeset, to make sure
        # submitted changesets are viewed correctly and not manifested.
        changeset=self,
        _allow_deleted_files=True)
    # Recreate a Files object to get rid of versioned paths in the keys:
    return files.Files(files=versioned_files.values(), namespace=self.namespace)

  def revert_file(self, titan_file):
    _require_file_has_staging_changeset(titan_file)
    self.disassociate_file(titan_file)
    titan_file.delete(_revert=True)

  def rebase(self, new_base_changeset):
    """Rebase the changeset to a new base_changeset.

    This assumes that all neccessary merges have already happened. Calling this
    is the equivalent of a "choose all mine" merge strategy.

    Args:
      new_base_changeset: The new, already-submitted changeset from which
      the changeset will now be based. The base_changeset number might be
      greater than the current staging changeset's number.
    Raises:
      ChangesetRebaseError: If new_base_changeset is not a submitted changeset.
      NamespaceMismatchError: If the new_base_changeset's namespace differs.
    """
    if self.namespace != new_base_changeset.namespace:
      raise NamespaceMismatchError(
          'Current namespace "{}" does not match new_base_changeset namespace '
          '"{}".'.format(
              self.namespace, new_base_changeset.namespace))
    if new_base_changeset.status != ChangesetStatus.submitted:
      raise ChangesetRebaseError(
          'new_base_changeset must be a submitted changeset. Got: {!r}'.format(
              new_base_changeset))
    self.changeset_ent.base_changeset = new_base_changeset.changeset_ent.key
    self.changeset_ent.put()

  def serialize(self):
    """Serializes changeset data into simple types."""
    data = {
        'num': self.num,
        'namespace': self.namespace,
        'created': self.created,
        'status': self.status,
        'base_path': self.base_path,
        'created_by': str(self.created_by) if self.created_by else None,
    }
    if self.status != ChangesetStatus.staging:
      # Cannot reference linked_changeset_num for "new" changesets.
      data['linked_changeset_num'] = self.linked_changeset_num
      data['linked_changeset_base_path'] = self.linked_changeset_base_path
    if self.status == ChangesetStatus.submitted:
      data['has_manifest'] = self.has_manifest
    else:
      data['base_changeset_num'] = self.base_changeset_num
    return data

  def associate_file(self, titan_file):
    """Associate a file temporally to this changeset object before commit.

    Args:
      titan_file: File object.
    """
    # Internal-only flag to allow files which have been marked for deletion.
    titan_file._allow_deleted_files = True

    self._associated_files.add(titan_file)
    self._finalized_files = False

  def disassociate_file(self, titan_file):
    """Disassociate a file from this changeset object before commit.

    Args:
      titan_file: File object.
    """
    self._associated_files.remove(titan_file)
    self._finalized_files = False

  def finalize_associated_files(self):
    """Indicate that this specific Changeset object was used for all operations.

    This flag is used during commit to indicate if this object can be trusted
    for strong consistency guarantees of which files paths will be committed.
    Only call this method if you are sure that this same Changeset instance was
    passed in for all file operations associated with this changeset.

    Raises:
      ChangesetError: if no files have been associated.
    """
    if not self._associated_files:
      raise ChangesetError('Cannot finalize: no associated file objects.')
    self._finalized_files = True

class _Changeset(ndb.Model):
  """Model representing a changeset.

  All _Changeset entities are in the same entity group, as well as all
  _FileVersion entities. An example entity group relationship might look like:

  _Changeset 0 (root ancestor, non-existent)
  |
  + _Changeset 1 (deleted-by-submit)
  |
  + _Changeset 2 (submitted)
  |  |
  |  + _FileVersion /foo
  |  + _FileVersion /bar
  |
  + _Changeset 3 (staging)
  |
  ...

  Attributes:
    num: Integer of the entity's key.id().
    created: datetime.datetime object of when this entity was created.
    created_by: A users.TitanUser object of the user who created the changeset.
    status: A string status of the changeset.
    linked_changeset: A reference between staging and finalized changesets.
    base_changeset: A reference to the current base for staging changesets.
    num_manifest_shards: The number of shards of the filesystem manifest. Only
        set for final changesets and only if the manifest was saved.
  """
  # NOTE: This model should be kept as lightweight as possible. Anything
  # else added here increases the amount of time that commit() will take,
  # though not as drastically as additions to _FileVersion and _FilePointer.
  num = ndb.IntegerProperty(required=True)
  created = ndb.DateTimeProperty(auto_now_add=True)
  created_by = users.TitanUserProperty()
  status = ndb.StringProperty(choices=[ChangesetStatus.staging,
                                       ChangesetStatus.presubmit,
                                       ChangesetStatus.submitted,
                                       ChangesetStatus.deleted,
                                       ChangesetStatus.deleted_by_submit])
  linked_changeset = ndb.KeyProperty(kind='_Changeset')
  base_changeset = ndb.KeyProperty(kind='_Changeset')
  num_manifest_shards = ndb.IntegerProperty()

  def __repr__(self):
    return ('<_Changeset %d namespace:%r status:%s base_changeset:%r '
            'num_manifest_shards:%r>') % (
                self.num, self.key.namespace(), self.status,
                self.base_changeset, self.num_manifest_shards)

  @staticmethod
  def get_root_key(namespace):
    """Get the root key, the parent of all changeset entities."""
    # All changesets are in the same entity group by being children of the
    # arbitrary, non-existent "0" changeset.
    return ndb.Key(_Changeset, '0', namespace=namespace)

class _ChangesetManifestShard(ndb.Model):
  """Model for one shard of a snapshot of a filesystem manifest at a changeset.

  Attributes:
    key.id(): The key for this model is "<changeset_num>:<shard_num>".
        Example: "3:0" is the first shard for Changeset 3.
    paths_to_changeset_num: A manifest of filesystem paths to the last
        changeset that affected the path.
  """
  # NOTE: This model should be kept as lightweight as possible. Anything
  # else added here increases the amount of time that commit() will take,
  # and decreases the number of files that can be committed at once.
  paths_to_changeset_num = ndb.JsonProperty()

  @staticmethod
  def get_root_key(final_changeset_num, namespace):
    """Get the root key, the parent of each group of manifest shard entities."""
    # These entities are grouped in an entity group if they are part of the same
    # manifest. Use an arbitrary, non-existent key named after the changeset.
    return ndb.Key(
        _ChangesetManifestShard, final_changeset_num, namespace=namespace)

class FileVersion(object):
  """Metadata about a committed file version.

  NOTE: Always trust FileVersions as the canonical source of a file's revision
  history metadata. Don't use the 'status' meta property or other properties of
  File objects as authoritative.

  Attributes:
    path: The committed file path. Example: /foo.html
    namespace: The datastore namespace, or None for the default namespace.
    versioned_path: The path of the versioned file. Ex: /_titan/ver/123/foo.html
    changeset: A final Changeset object.
    changeset_created_by: The TitanUser who created the changeset.
    created_by: The TitanUser who created the file version. This usually is the
        same as changeset_created_by.
    created: datetime.datetime object of when the file version was created.
    status: The edit type of the affected file.
  """

  def __init__(self, path, namespace, changeset, file_version_ent=None):
    self._path = path
    self._namespace = namespace
    self._file_version_ent = file_version_ent
    self._changeset = changeset
    if isinstance(changeset, int):
      self._changeset = Changeset(changeset, namespace=self.namespace)

  @property
  def _file_version(self):
    """Lazy-load the _FileVersion entity."""
    if not self._file_version_ent:
      file_version_id = _FileVersion.make_key_name(self._changeset, self._path)
      self._file_version_ent = _FileVersion.get_by_id(
          file_version_id, parent=self._changeset.changeset_ent.key,
          namespace=self.namespace)
      if not self._file_version_ent:
        raise FileVersionError('No file version of %s at %s.'
                               % (self._path, self._changeset.num))
    return self._file_version_ent

  def __repr__(self):
    return ('<FileVersion path: %s namespace: %s versioned_path: %s '
            'created: %s status: %s>' % (
                self.path, self.namespace, self.versioned_path, self.created,
                self.status))

  @property
  def path(self):
    return self._path

  @property
  def namespace(self):
    return self._namespace

  @property
  def versioned_path(self):
    return VERSIONS_PATH_FORMAT % (self._changeset.linked_changeset_num,
                                   self._path)

  @property
  def changeset(self):
    return self._changeset

  @property
  def content_changeset(self):
    """Convenience property for finding the committed content changeset."""
    if self.changeset.status != ChangesetStatus.submitted:
      raise TypeError(
          'content_changeset can only be accessed from final, committed '
          'changesets. Current changeset: %r' % self)
    return self.changeset.linked_changeset

  @property
  def changeset_created_by(self):
    return self._file_version.changeset_created_by

  @property
  def created_by(self):
    return self._file_version.created_by

  @property
  def created(self):
    return self._file_version.created

  @property
  def status(self):
    return self._file_version.status

  def serialize(self):
    """Serializes a FileVersion into native types."""
    cs_created_by = self.changeset_created_by
    result = {
        'path': self.path,
        'versioned_path': self.versioned_path,
        'created': self.created,
        'status': self.status,
        'changeset_num': self._changeset.num,
        'changeset_created_by': str(cs_created_by) if cs_created_by else None,
        'created_by': str(self.created_by) if self.created_by else None,
        'linked_changeset_num': self.changeset.linked_changeset_num,
    }
    return result

class _FileVersion(ndb.Model):
  """Model representing metadata about a committed file version.

  A _FileVersion entity will only exist for committed file changes.

  Attributes:
    key.id(): '<changeset num>:<path>', such as '123:/foo.html'.
    path: The Titan File path.
    changeset_num: The changeset number in which the file was changed.
    changeset_created_by: A users.TitanUser object of who created the changeset.
    created: datetime.datetime object of when the entity was created.
    status: The edit type of the file at this version.
  """
  # NOTE: This model should be kept as lightweight as possible. Anything
  # else added here increases the amount of time that commit() will take,
  # and decreases the number of files that can be committed at once.
  path = ndb.StringProperty(required=True)
  changeset_num = ndb.IntegerProperty(required=True)
  changeset_created_by = users.TitanUserProperty()
  # In limited cases, the user who created the file version may be different
  # than the changeset user (such as with microversions, where the changeset
  # user is None, but each file version has an overwritten real author).
  created_by = users.TitanUserProperty()
  created = ndb.DateTimeProperty(auto_now_add=True)
  status = ndb.StringProperty(
      required=True,
      choices=[FileStatus.created, FileStatus.edited, FileStatus.deleted])

  def __repr__(self):
    return ('<_FileVersion id:%s path:%s namespace:%s changeset_num:%s'
            'created:%s status:%s>' % (
                self.key.id(), self.path, self.key.namespace(),
                self.changeset_num, self.created, self.status))

  @staticmethod
  def make_key_name(changeset, path):
    return ':'.join([str(changeset.num), path])

class _FilePointer(ndb.Model):
  """Pointer from a root file path to its current file version.

  All _FilePointers are in the same entity group. As such, the entities
  are updated atomically to point a set of files at new versions.

  Attributes:
    key.id(): Root file path string. Example: '/foo.html'
    changeset_num: An integer pointing to the file's latest committed changeset.
        Technically, this is the 'deleted-by-submit' content changeset.
    versioned_path: Versioned file path. Example: '/_titan/ver/1/foo.html'
  """
  # NOTE: This model should be kept as lightweight as possible. Anything
  # else added here increases the amount of time that commit() will take,
  # and decreases the number of files that can be committed at once.
  changeset_num = ndb.IntegerProperty(required=True)

  def __repr__(self):
    return '<_FilePointer path:%s namespace:%s changeset: %s>' % (
        self.key.id(), self.key.namespace(), self.changeset_num)

  @property
  def versioned_path(self):
    return VERSIONS_PATH_FORMAT % (self.changeset_num, self.key.id())

  @staticmethod
  def get_root_key(namespace):
    # The parent of all _FilePointers is a non-existent _FilePointer arbitrarily
    # named '/', since no file path can be a single slash.
    return ndb.Key(_FilePointer, '/', namespace=namespace)

class VersionControlService(object):
  """A service object providing version control methods."""

  def new_staging_changeset(self, created_by=None, namespace=None):
    """Create a new staging changeset with a unique number ID.

    Args:
      created_by: A users.TitanUser object, will default to the current user.
      namespace: The namespace in which to create the new changeset, or None
          to use the default namespace.
    Returns:
      A Changeset.
    """
    return self._new_changeset(
        status=ChangesetStatus.staging,
        created_by=created_by, namespace=namespace)

  def _new_changeset(self, status, created_by, namespace,
                     nested_transaction=False):
    """Create a changeset with the given status."""
    utils.validate_namespace(namespace)

    def transaction():
      """The changeset creation transaction."""
      new_changeset_num = strong_counters.Increment(
          _CHANGESET_COUNTER_NAME, namespace=namespace, nested_transaction=True)

      base_changeset_key = None
      # Set the base_changeset, but only for new staging changesets.
      if status == ChangesetStatus.staging:
        base_changeset = self.get_last_submitted_changeset(namespace=namespace)
        if base_changeset:
          base_changeset_key = base_changeset.changeset_ent.key

      changeset_ent = _Changeset(
          # NDB properties:
          # NDB can support integer keys, but this needs to be a string for
          # support of legacy IDs created when using db.
          id=str(new_changeset_num),
          parent=_Changeset.get_root_key(namespace=namespace),
          namespace=namespace,
          # Model properties:
          num=new_changeset_num,
          status=status,
          base_changeset=base_changeset_key)
      if created_by:
        changeset_ent.created_by = created_by
      else:
        changeset_ent.created_by = users.get_current_user()
      changeset_ent.put()
      staging_changeset = Changeset(
          num=new_changeset_num,
          namespace=namespace,
          # TODO(user): maybe pass _base_changeset_ent for optimization.
          changeset_ent=changeset_ent)
      return staging_changeset

    if nested_transaction:
      return transaction()
    # xg-transaction between the StrongCounter and query over _Changeset when
    # calling get_last_submitted_changeset().
    return ndb.transaction(transaction, xg=True)

  def get_last_submitted_changeset(self, namespace=None):
    """Returns a Changeset object of the last submitted changeset.

    Args:
      namespace: The datastore namespace, or None for the default namespace.
    Raises:
      ChangesetError: If no changesets currently exist.
    Returns:
      A Changeset.
    """
    changeset_root_key = _Changeset.get_root_key(namespace=namespace)
    # Use an ancestor query to maintain strong consistency.
    changeset_query = _Changeset.query(
        ancestor=changeset_root_key, namespace=namespace)
    changeset_query = changeset_query.filter(
        _Changeset.status == ChangesetStatus.submitted)
    changeset_query = changeset_query.order(-_Changeset.num)
    latest_changeset = list(changeset_query.fetch(1))
    if not latest_changeset:
      return None
    return Changeset(num=latest_changeset[0].num, namespace=namespace)

  def get_file_versions(self, path, namespace=None, limit=1000):
    """Get FileVersion objects of the revisions of this file path.

    Args:
      path: An absolute file path.
      namespace: The datastore namespace, or None for the default namespace.
      limit: The limit to the number of objects returned.
    Returns:
      A list of FileVersion objects, ordered from latest to earliest.
    """
    changeset_root_key = _Changeset.get_root_key(namespace=namespace)
    file_version_ents = _FileVersion.query(
        ancestor=changeset_root_key, namespace=namespace)
    file_version_ents = file_version_ents.filter(_FileVersion.path == path)

    # Order in descending chronological order, which will also happen to
    # order by changeset_num.
    file_version_ents = file_version_ents.order(-_FileVersion.created)

    # Encapsulate all the _FileVersion objects in public FileVersion objects.
    file_versions = []
    for file_version_ent in file_version_ents.fetch(limit=limit):
      file_version = FileVersion(
          path=file_version_ent.path,
          namespace=namespace,
          changeset=Changeset(
              file_version_ent.changeset_num, namespace=namespace),
          file_version_ent=file_version_ent)
      file_versions.append(file_version)

    return file_versions

  def _verify_staging_changeset_ready_for_commit(
      self, namespace, staging_changeset, save_manifest):
    if not save_manifest:
      return

    # Fail-fast: save_manifest is True and base_changeset is not up to date.
    last_changeset = self.get_last_submitted_changeset(namespace=namespace)
    if staging_changeset.base_changeset != last_changeset:
      raise InvalidBaseChangesetCommitError(
          'Changeset {:d} with base_changeset {} needs rebase to head before '
          'commit. Last committed changeset: {}.'.format(
              staging_changeset.num,
              getattr(staging_changeset.base_changeset, 'num', None),
              getattr(last_changeset, 'num', None)))

    # Fail-fast: save_manifest is True and no manifest exists on base_changeset.
    if (staging_changeset.base_changeset  # May be None if first changeset.
        and not staging_changeset.base_changeset.has_manifest):
      raise NoBaseManifestCommitError(
          'The base_changeset for changeset {:d} was not originally committed '
          'with a manifest, so the manifest shards cannot be copied.'.format(
              staging_changeset.num))

  def commit(self, staging_changeset, force=False, save_manifest=True):
    """Commit the given changeset.

    Args:
      staging_changeset: A Changeset object with a status of staging.
      force: Commit a changeset even if using an eventually-consistent query.
          This could cause files recently added to the changeset to be missed
          on commit.
      save_manifest: Whether or not to save a manifest of the entire
          filesystem state upon commit. This requires that the associated
          base_changeset was also committed with a snapshot.
    Raises:
      CommitError: If a changeset contains no files or it is already committed.
    Returns:
      The final Changeset object.
    """
    if staging_changeset.status != ChangesetStatus.staging:
      raise CommitError('Cannot commit changeset with status "%s".'
                        % staging_changeset.status)

    try:
      staged_files = staging_changeset.get_files()
    except ChangesetError:
      if not force:
        raise
      # Got force=True, get files with an eventually-consistent query.
      staged_files = staging_changeset.list_files()

    # Preload files so that they are not lazily loaded inside of the commit
    # transaction and count against the xg-transaction limit.
    staged_files.load()

    if not staged_files:
      raise CommitError('Changeset %d contains no file changes.'
                        % staging_changeset.num)

    # Fail if rebase is needed or the base manifest is missing and needed.
    # This is also in the _commit path below to guarantee strong consistency,
    # but is duplicated here as an optimization to fail outside the transaction.
    namespace = staging_changeset.namespace
    self._verify_staging_changeset_ready_for_commit(
        namespace, staging_changeset, save_manifest=save_manifest)

    transaction_func = (
        lambda: self._commit(staging_changeset, staged_files, save_manifest))
    final_changeset = ndb.transaction(transaction_func, xg=True)
    return final_changeset

  def _commit(self, staging_changeset, staged_files, save_manifest):
    """Commit a staged changeset."""
    # Fail if rebase is needed or the base manifest is missing and needed.
    namespace = staging_changeset.namespace
    base_changeset = staging_changeset.base_changeset
    self._verify_staging_changeset_ready_for_commit(
        namespace, staging_changeset, save_manifest=save_manifest)

    final_changeset = self._new_changeset(
        status=ChangesetStatus.presubmit,
        created_by=staging_changeset.created_by,
        namespace=namespace, nested_transaction=True)

    changes = ['%s: %s' % (f.meta.status, f.path)
               for f in staged_files.values()]
    logging.info(
        'Submitting staging changeset %d in namespace %s as final changeset %d '
        'with %d files:\n%s',
        staging_changeset.num, namespace, final_changeset.num,
        len(staged_files), '\n'.join(changes))

    # Copy the manifest data from the base_changeset to the final_changeset.
    # TODO(user): optimize this by pulling the data reading for small
    # manifests out of the commit path.
    new_manifest_shards = []
    if save_manifest:
      new_manifest = {}
      # If this isn't the first-ever committed changeset, copy the old manifest.
      if staging_changeset.base_changeset:
        manifest_shard_keys = _make_manifest_shard_keys(base_changeset)
        manifest_shards = ndb.get_multi(manifest_shard_keys)
        if not all(manifest_shards):
          raise CommitError(
              'Expected complete manifest shards, but got: {!r}'.format(
                  manifest_shards))
        for manifest_shard in manifest_shards:
          new_manifest.update(manifest_shard.paths_to_changeset_num)

      for staged_file in staged_files.itervalues():
        if staged_file.meta.status == FileStatus.deleted:
          # Remove from new manifest if it existed in previous manifests.
          new_manifest.pop(staged_file.path, None)
        else:
          # New file or edited file: point to the current final_changeset.
          new_manifest[staged_file.path] = final_changeset.num

      # Split up the new_manifest into shards by hashing each path and modding
      # it into the right bucket. Hashing the paths provides a relatively even
      # distribution over the available buckets, and also provides a
      # deterministic O(1) way to know which manifest shard a path may exist in.
      num_manifest_shards = len(new_manifest) / _MAX_MANIFEST_SHARD_PATHS + 1
      manifest_buckets = [{}] * num_manifest_shards
      while new_manifest:
        path, changeset_num = new_manifest.popitem()
        index = _get_manifest_shard_index(path, num_manifest_shards)
        manifest_buckets[index][path] = changeset_num

      # Create the new manifest entities.
      parent = _ChangesetManifestShard.get_root_key(
          final_changeset_num=final_changeset.num, namespace=namespace)
      for i, manifest_bucket in enumerate(manifest_buckets):
        shard_ent = _ChangesetManifestShard(
            id=_make_manifest_shard_id(final_changeset, i),
            paths_to_changeset_num=manifest_bucket, parent=parent)
        new_manifest_shards.append(shard_ent)

    # Update status of the staging and final changesets.
    staging_changeset_ent = staging_changeset.changeset_ent
    staging_changeset_ent.status = ChangesetStatus.deleted_by_submit
    staging_changeset_ent.linked_changeset = final_changeset.changeset_ent.key
    final_changeset_ent = final_changeset.changeset_ent
    final_changeset_ent.status = ChangesetStatus.submitted
    final_changeset_ent.linked_changeset = staging_changeset.changeset_ent.key
    if save_manifest:
      final_changeset_ent.num_manifest_shards = num_manifest_shards
    ndb.put_multi([
        staging_changeset_ent,
        final_changeset_ent,
    ])

    # Get a mapping of paths to current _FilePointers (or None).
    file_pointers = {}
    root_file_pointer = _FilePointer.get_root_key(namespace=namespace)
    ordered_paths = staged_files.keys()
    file_pointer_keys = []
    for path in ordered_paths:
      key = ndb.Key(
          _FilePointer, path, parent=root_file_pointer, namespace=namespace)
      file_pointer_keys.append(key)
    file_pointer_ents = ndb.get_multi(file_pointer_keys)
    for i, file_pointer_ent in enumerate(file_pointer_ents):
      file_pointers[ordered_paths[i]] = file_pointer_ent

    new_file_versions = []
    updated_file_pointers = []
    deleted_file_pointers = []
    for path, titan_file in staged_files.iteritems():
      file_pointer = file_pointers[titan_file.path]

      # Update "edited" status to be "created" on commit if file doesn't exist.
      status = titan_file.meta.status
      if titan_file.meta.status == FileStatus.edited and not file_pointer:
        status = FileStatus.created

      # Create a _FileVersion entity containing revision metadata.
      new_file_version = _FileVersion(
          # NDB args:
          id=_FileVersion.make_key_name(final_changeset, titan_file.path),
          namespace=namespace,
          parent=final_changeset.changeset_ent.key,
          # Model args:
          path=titan_file.path,
          changeset_num=final_changeset.num,
          changeset_created_by=final_changeset.created_by,
          # This is correctly modified_by, not created_by. We want to store the
          # user who made this file revision, not the original created_by user.
          created_by=titan_file.modified_by,
          status=status)
      new_file_versions.append(new_file_version)

      # Create or change the _FilePointer for this file.
      if not file_pointer and status != FileStatus.deleted:
        # New file, setup the pointer.
        file_pointer = _FilePointer(id=titan_file.path,
                                    parent=root_file_pointer,
                                    changeset_num=staging_changeset.num,
                                    namespace=namespace)
      elif file_pointer:
        # Important: the file pointer is pointed to the staged changeset number,
        # since a file is not copied on commit from ver/1/file to ver/2/file.
        file_pointer.changeset_num = staging_changeset.num

      # Files versions marked as "deleted" should delete the _FilePointer.
      if status == FileStatus.deleted:
        # Only delete file_pointer if it exists.
        if file_pointer:
          deleted_file_pointers.append(file_pointer)
      else:
        updated_file_pointers.append(file_pointer)

    # For all file changes and updated pointers, do the RPCs.
    if new_file_versions:
      ndb.put_multi(new_file_versions)
    if updated_file_pointers:
      ndb.put_multi(updated_file_pointers)
    if deleted_file_pointers:
      ndb.delete_multi([p.key for p in deleted_file_pointers])
    if new_manifest_shards:
      ndb.put_multi(new_manifest_shards)

    logging.info('Submitted staging changeset %d as final changeset %d.',
                 staging_changeset.num, final_changeset.num)

    return final_changeset

def _get_manifest_shard_index(path, num_manifest_shards):
  path_hash_num = int(hashlib.md5(path).hexdigest(), 16)
  return path_hash_num % num_manifest_shards

def _make_manifest_shard_keys(changeset):
  """Gets a list of ndb.Key objects for all of a changeset's manifest shards."""
  num_shards = changeset._num_manifest_shards
  namespace = changeset.namespace
  ids = [_make_manifest_shard_id(changeset, i) for i in range(num_shards)]
  keys = []
  parent = _ChangesetManifestShard.get_root_key(
      changeset.num, namespace=namespace)
  for shard_id in ids:
    shard_key = ndb.Key(
        _ChangesetManifestShard, shard_id, namespace=namespace, parent=parent)
    keys.append(shard_key)
  return keys

def _make_manifest_shard_id(changeset, shard_index):
  return '{:d}:{:d}'.format(changeset.num, shard_index)

def _make_versioned_path(path, changeset):
  """Return a two-tuple of (versioned paths, is_multiple)."""
  # Make sure we're not accidentally using non-strings,
  # which could create a path like /_titan/ver/123<Some object>
  if not isinstance(path, basestring):
    raise TypeError('path argument must be a string: %r' % path)
  return VERSIONS_PATH_FORMAT % (changeset.num, path)

def _require_file_has_changeset(titan_file):
  if not titan_file.changeset:
    raise InvalidChangesetError(
        'File modification requires an associated changeset.')

def _require_file_has_staging_changeset(titan_file):
  """If changeset is committed, don't allow files to be changed."""
  if titan_file.changeset.status != ChangesetStatus.staging:
    raise ChangesetError('Cannot change files in a "%s" changeset.'
                         % titan_file.changeset.status)

def _verify_root_paths(paths):
  """Make sure all given paths are not versioned paths."""
  is_multiple = hasattr(paths, '__iter__')
  for path in paths if is_multiple else [paths]:
    if VERSIONS_PATH_BASE_REGEX.match(path):
      raise ValueError('Not a root file path: %s' % path)

def _verify_versioned_paths(paths):
  """Make sure all given paths are versioned paths."""
  is_multiple = hasattr(paths, '__iter__')
  for path in paths if is_multiple else [paths]:
    if not VERSIONS_PATH_BASE_REGEX.match(path):
      raise ValueError('Not a versioned file path: %s' % path)
