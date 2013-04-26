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

http://code.google.com/p/titan-files/wiki/Versions
"""

import logging
import re

from google.appengine.ext import ndb

from titan.common import strong_counters
from titan import files
from titan import users
from titan.common import utils

CHANGESET_NEW = 'new'
CHANGESET_PRE_SUBMIT = 'pre-submit'
CHANGESET_SUBMITTED = 'submitted'
CHANGESET_DELETED = 'deleted'
CHANGESET_DELETED_BY_SUBMIT = 'deleted-by-submit'

FILE_CREATED = 'created'
FILE_EDITED = 'edited'
FILE_DELETED = 'deleted'

VERSIONS_PATH_BASE_REGEX = re.compile('^/_titan/ver/([0-9]+)')
# For formating "/_titan/ver/123/some/file/path"
VERSIONS_PATH_FORMAT = '/_titan/ver/%d%s'

_CHANGESET_COUNTER_NAME = 'num_changesets'

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
    # If not, this File represents the lastest committed file version,
    # but it cannot be written or changed (since that must happen with an
    # associated changeset).
    self.changeset = kwargs.get('changeset', None)
    self._disable_root_copy = kwargs.get('_disable_root_copy', False)

    super(FileVersioningMixin, self).__init__(path, **kwargs)

    # Support initing with a /_titan/ver path instead of a changeset number.
    versioned_path_match = VERSIONS_PATH_BASE_REGEX.match(path)
    if versioned_path_match:
      self.changeset = int(versioned_path_match.group(1))
      # Replace the path argument.
      self._path = VERSIONS_PATH_BASE_REGEX.sub('', path)

    self._real_path = None
    if self.changeset and isinstance(self.changeset, int):
      # Support integer changeset argument.
      self.changeset = Changeset(self.changeset)

    # Overrides for created_by and modified_by.
    self._created_by_override = kwargs.get('_created_by_override', None)
    self._modified_by_override = kwargs.get('_modified_by_override', None)

  def __repr__(self):
    return '<File %s (cs:%r)>' % (self._path, getattr(self, 'changeset', None))

  def _get_created_by_user(self):
    if self._created_by_override:
      return self._created_by_override
    return super(FileVersioningMixin, self)._get_created_by_user()

  def _get_modified_by_user(self):
    if self._modified_by_override:
      return self._modified_by_override
    return super(FileVersioningMixin, self)._get_modified_by_user()

  @property
  def _file(self):
    """Handle dynamic determination of correct file entity."""
    if not self.changeset:
      # No associated changeset. Dynamically pick the file entity based on
      # the latest FilePointers.
      root_file_pointer = _FilePointer.get_root_key()
      file_pointer = _FilePointer.get_by_id(self.path, parent=root_file_pointer)
      if file_pointer:
        # Associate to the committed changeset.
        self.changeset = Changeset(file_pointer.changeset_num)
      else:
        raise files.BadFileError('File does not exist: %s' % self._path)

    # A changeset exists, so real_path will resolve correctly. Fall through to
    # finding the file entity normally.
    return super(FileVersioningMixin, self)._file

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
    # Avoid using other properties of self.changeset here, to avoid making
    # an RPC for the Changeset entity.
    if not self.changeset:
      raise InvalidChangesetError(
          'File modification requires an associated changeset.')
    if not self._real_path:
      self._real_path = _make_versioned_path(self._path, self.changeset)
    return self._real_path

  @property
  def versioned_path(self):
    return self.real_path

  @utils.compose_method_kwargs
  def write(self, **kwargs):
    """Write method. See superclass docstring."""
    if not self.changeset:
      raise InvalidChangesetError(
          'File modification requires an associated changeset.')

    kwargs.pop('_run_mixins_only', False)
    delete = kwargs.pop('delete', False)
    _verify_is_new_changeset(self.changeset)
    self.changeset.AssociateFile(self)

    # Update meta data.
    kwargs['meta'] = kwargs.get('meta') or {}
    if delete:
      kwargs['content'] = ''
      kwargs['meta']['status'] = FILE_DELETED
      # This will orphan blobs if a large file is uploaded many times in a
      # changeset without committing, but that's better than losing the data.
      # TODO(user): add a flag to entities signifying if they have been
      # copied or deleted, so that we can notice and delete orphaned blobs.
      kwargs['_delete_old_blob'] = False
    else:
      # The first time the versioned file is created (or un-deleted), we have
      # to branch all content and properties from the current root file version.
      if not self._disable_root_copy:
        copy_kwargs = self._original_kwargs.copy()
        del copy_kwargs['changeset']
        del copy_kwargs['path']
        _copy_file_from_root(self.path, self.changeset, **copy_kwargs)
      kwargs['meta']['status'] = FILE_EDITED
      kwargs['_delete_old_blob'] = False

    return super(FileVersioningMixin, self).write(**kwargs)

  @utils.compose_method_kwargs
  def delete(self, **kwargs):
    if not self.changeset:
      raise InvalidChangesetError(
          'File modification requires an associated changeset.')

    # A delete in the files world is a revert in the versions world.
    # The file should be removed entirely from the staging changeset.
    _verify_is_new_changeset(self.changeset)
    self.changeset.DisassociateFile(self)
    return super(FileVersioningMixin, self).delete(**kwargs)

# ------------------------------------------------------------------------------

class Changeset(object):
  """Unit of consistency over a group of files.

  Attributes:
    num: An integer of the changeset number.
    created: datetime.datetime object of when the changeset was created.
    created_by: The User object of who created this changeset.
    status: An integer of one of the CHANGESET_* constants.
    base_path: The path prefix for all files in this changeset,
        for example: '/_titan/ver/123'
    linked_changeset_base_path: Same as base_path, but for the linked changeset.
    exists: If the given changeset exists.
  """

  def __init__(self, num, changeset_ent=None):
    self._changeset_ent = changeset_ent
    self._num = int(num)
    self._associated_files = []
    self._finalized_files = False

  def __eq__(self, other):
    """Compare equality of two Changeset objects."""
    return isinstance(other, Changeset) and self.num == other.num

  def __repr__(self):
    return '<Changeset %d evaluated: %s>' % (self._num,
                                             bool(self._changeset_ent))

  @property
  def changeset_ent(self):
    """Lazy-load the _Changeset entity."""
    if not self._changeset_ent:
      self._changeset_ent = _Changeset.get_by_id(
          str(self._num), parent=_Changeset.get_root_key())
      if not self._changeset_ent:
        raise ChangesetError('Changeset %s does not exist.' % self._num)
    return self._changeset_ent

  @property
  def num(self):
    return self._num

  @property
  def created(self):
    return self.changeset_ent.created

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
      return Changeset(num=self.linked_changeset_num)

  @property
  def linked_changeset_num(self):
    if self.status not in (CHANGESET_NEW, CHANGESET_DELETED):
      return int(self.changeset_ent.linked_changeset.id())

  @property
  def created_by(self):
    return self.changeset_ent.created_by

  @property
  def associated_paths(self):
    return set([f.path for f in self._associated_files])

  @property
  def exists(self):
    try:
      return bool(self.changeset_ent)
    except ChangesetError:
      return False

  def get_files(self):
    """Get all files associated with this changeset.

    Guarantees strong consistency, but requires that associated file paths
    have been finalized on this specific Changeset instance.

    Raises:
      ChangesetError: If associated file paths have not been finalized.
    Returns:
      A populated files.Files object.
    """
    if not self._finalized_files:
      raise ChangesetError(
          'Cannot guarantee strong consistency when associated file paths '
          'have not been finalized. Perhaps you want list_files?')
    titan_files = files.Files(files=self._associated_files)
    # Force File objects to load so that they are not lazily loaded inside
    # of the commit transaction.
    titan_files.load()
    return titan_files

  def list_files(self):
    """Queries and returns a Files object containing this changeset's files.

    This method is always eventually consistent and may not contain recently
    changed files.

    Returns:
      A populated files.Files object.
    """
    changeset = self
    if changeset.status == CHANGESET_SUBMITTED:
      # The files stored for submitted changesets are actually stored under the
      # the staging changeset's number, since they are never moved.
      changeset = changeset.linked_changeset
    versioned_files = files.Files.list(changeset.base_path, recursive=True)
    versioned_files.load()
    # Recreate a Files object to get rid of versioned paths in the keys:
    return files.Files(files=versioned_files.values())

  def serialize(self):
    """Serializes changeset data into simple types."""
    data = {
        'num': self.num,
        'created': self.created,
        'status': self.status,
        'base_path': self.base_path,
        'linked_changeset_base_path': self.linked_changeset_base_path,
        'linked_changeset_num': self.linked_changeset_num,
        'created_by': str(self.created_by) if self.created_by else None,
    }
    return data

  def AssociateFile(self, titan_file):
    """Associate a file temporally to this changeset object before commit.

    Args:
      titan_file: File object.
    """
    self._associated_files.append(titan_file)
    self._finalized_files = False

  def DisassociateFile(self, titan_file):
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

  Attributes:
    num: Integer of the entity's key.id().
    created: datetime.datetime object of when this entity was created.
    status: A string status of the changeset.
    linked_changeset: A reference between staging and finalized changesets.
    created_by: A users.TitanUser object of the user who created the changeset.
  """
  num = ndb.IntegerProperty(required=True)
  created = ndb.DateTimeProperty(auto_now_add=True)
  status = ndb.StringProperty(choices=[CHANGESET_NEW,
                                       CHANGESET_PRE_SUBMIT,
                                       CHANGESET_SUBMITTED,
                                       CHANGESET_DELETED,
                                       CHANGESET_DELETED_BY_SUBMIT])
  linked_changeset = ndb.KeyProperty(kind='_Changeset')
  created_by = users.TitanUserProperty()

  def __repr__(self):
    return '<_Changeset %d status:%s>' % (self.num, self.status)

  @staticmethod
  def get_root_key():
    """Get the root key, the parent of all changeset entities."""
    # All changesets are in the same entity group by being children of the
    # arbitrary, non-existent "0" changeset.
    return ndb.Key('_Changeset', '0')

class FileVersion(object):
  """Metadata about a committed file version.

  NOTE: Always trust FileVersions as the canonical source of a file's revision
  history metadata. Don't use the 'status' meta property or other properties of
  File objects as authoritative.

  Attributes:
    path: The committed file path. Example: /foo.html
    versioned_path: The path of the versioned file. Ex: /_titan/ver/123/foo.html
    changeset: A final Changeset object.
    changeset_created_by: The TitanUser who created the changeset.
    created_by: The TitanUser who created the file version. This usually is the
        same as changeset_created_by.
    created: datetime.datetime object of when the file version was created.
    status: The edit type of the affected file.
  """

  def __init__(self, path, changeset, file_version_ent=None):
    self._path = path
    self._file_version_ent = file_version_ent
    self._changeset = changeset
    if isinstance(changeset, int):
      self._changeset = Changeset(changeset)

  @property
  def _file_version(self):
    """Lazy-load the _FileVersion entity."""
    if not self._file_version_ent:
      file_version_id = _FileVersion.make_key_name(self._changeset, self._path)
      self._file_version_ent = _FileVersion.get_by_id(
          file_version_id, parent=self._changeset.changeset_ent.key)
      if not self._file_version_ent:
        raise FileVersionError('No file version of %s at %s.'
                               % (self._path, self._changeset.num))
    return self._file_version_ent

  def __repr__(self):
    return ('<FileVersion path: %s versioned_path: %s created: %s '
            'status: %s>' % (self.path, self.versioned_path, self.created,
                             self.status))

  @property
  def path(self):
    return self._path

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
    if self.changeset.status != CHANGESET_SUBMITTED:
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
  # else added here increases the amount of time that Commit() will take,
  # and decreases the number of files that can be committed at once.
  path = ndb.StringProperty()
  changeset_num = ndb.IntegerProperty()
  changeset_created_by = users.TitanUserProperty()
  # In limited cases, the user who created the file version may be different
  # than the changeset user (such as with microversions, where the changeset
  # user is None, but each file version has an overwritten real author).
  created_by = users.TitanUserProperty()
  created = ndb.DateTimeProperty(auto_now_add=True)
  status = ndb.StringProperty(required=True,
                              choices=[FILE_CREATED, FILE_EDITED, FILE_DELETED])

  def __repr__(self):
    return ('<_FileVersion id:%s path:%s changeset_num:%s created:%s '
            'status:%s>' % (self.key.id(), self.path, self.changeset_num,
                            self.created, self.status))

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
    versioned_path: Versioned file path. Example: '/_titan/ver/1/foo.html'
  """
  # NOTE: This model should be kept as lightweight as possible. Anything
  # else added here increases the amount of time that Commit() will take,
  # and decreases the number of files that can be committed at once.
  changeset_num = ndb.IntegerProperty()

  def __repr__(self):
    return '<_FilePointer %s Current changeset: %s>' % (self.key.id(),
                                                        self.changeset_num)

  @property
  def versioned_path(self):
    return VERSIONS_PATH_FORMAT % (self.changeset_num, self.key.id())

  @staticmethod
  def get_root_key():
    # The parent of all _FilePointers is a non-existent _FilePointer arbitrarily
    # named '/', since no file path can be a single slash.
    return ndb.Key('_FilePointer', '/')

class VersionControlService(object):
  """A service object providing version control methods."""

  def new_staging_changeset(self, created_by=None):
    """Create a new staging changeset with a unique number ID.

    Args:
      created_by: A users.TitanUser object, will default to the current user.
    Returns:
      A Changeset.
    """
    return self._new_changeset(status=CHANGESET_NEW, created_by=created_by)

  def _new_changeset(self, status, created_by):
    """Create a changeset with the given status."""
    new_changeset_num = strong_counters.Increment(_CHANGESET_COUNTER_NAME)
    changeset_ent = _Changeset(
        # NDB can support integer keys, but this needs to be a string for
        # support of legacy IDs created when using db.
        id=str(new_changeset_num),
        num=new_changeset_num,
        status=status,
        parent=_Changeset.get_root_key())
    if created_by:
      changeset_ent.created_by = created_by
    else:
      changeset_ent.created_by = users.get_current_user()
    changeset_ent.put()
    return Changeset(num=new_changeset_num, changeset_ent=changeset_ent)

  def get_last_submitted_changeset(self):
    """Returns a Changeset object of the last submitted changeset."""
    changeset_root_key = _Changeset.get_root_key()
    # Use an ancestor query to maintain strong consistency.
    changeset_query = _Changeset.query(ancestor=changeset_root_key)
    changeset_query = changeset_query.filter(
        _Changeset.status == CHANGESET_SUBMITTED)
    changeset_query = changeset_query.order(-_Changeset.num)
    latest_changeset = list(changeset_query.fetch(1))
    if not latest_changeset:
      raise ChangesetError('No changesets have been submitted')
    return Changeset(num=latest_changeset[0].num)

  def get_file_versions(self, path, limit=1000):
    """Get FileVersion objects of the revisions of this file path.

    Args:
      path: An absolute file path.
      limit: The limit to the number of objects returned.
    Returns:
      A list of FileVersion objects, ordered from latest to earliest.
    """
    file_version_ents = _FileVersion.query()
    file_version_ents = file_version_ents.filter(_FileVersion.path == path)

    # Order in descending chronological order, which will also happen to
    # order by changeset_num.
    file_version_ents = file_version_ents.order(-_FileVersion.created)

    # Encapsulate all the _FileVersion objects in public FileVersion objects.
    file_versions = []
    for file_version_ent in file_version_ents.fetch(limit=limit):
      file_versions.append(
          FileVersion(path=file_version_ent.path,
                      changeset=Changeset(file_version_ent.changeset_num),
                      file_version_ent=file_version_ent))
    return file_versions

  def commit(self, staged_changeset, force=False):
    """Commit the given changeset.

    Args:
      staged_changeset: A Changeset object with a status of CHANGESET_NEW.
      force: Commit a changeset even if using an eventually-consistent query.
          This could cause files recently added to the changeset to be missed
          on commit.
    Raises:
      CommitError: If a changeset contains no files or it is already committed.
    Returns:
      The final Changeset object.
    """
    if staged_changeset.status != CHANGESET_NEW:
      raise CommitError('Cannot commit changeset with status "%s".'
                        % staged_changeset.status)

    try:
      staged_files = staged_changeset.get_files()
    except ChangesetError:
      if not force:
        raise
      # Got force=True, get files with an eventually-consistent query.
      staged_files = staged_changeset.list_files()
    if not staged_files:
      raise CommitError('Changeset %d contains no file changes.'
                        % staged_changeset.num)

    # Can't nest transactions, so we get a unique final changeset number here.
    # This has the potential to orphan a changeset number (if this submit works
    # but the following transaction does not). However, we don't care.
    final_changeset = self._new_changeset(
        status=CHANGESET_PRE_SUBMIT, created_by=staged_changeset.created_by)

    transaction_func = (
        lambda: self._commit(staged_changeset, final_changeset, staged_files))
    ndb.transaction(transaction_func, xg=True)

    return final_changeset

  @staticmethod
  def _commit(staged_changeset, final_changeset, staged_files):
    """Commit a staged changeset."""
    manifest = ['%s: %s' % (f.meta.status, f.path)
                for f in staged_files.values()]
    logging.info('Submitting changeset %d as changeset %d with %d files:\n%s',
                 staged_changeset.num, final_changeset.num,
                 len(staged_files), '\n'.join(manifest))

    # Update status of the staging and final changesets.
    staged_changeset_ent = staged_changeset.changeset_ent
    staged_changeset_ent.status = CHANGESET_DELETED_BY_SUBMIT
    staged_changeset_ent.linked_changeset = final_changeset.changeset_ent.key
    final_changeset_ent = final_changeset.changeset_ent
    final_changeset_ent.status = CHANGESET_SUBMITTED
    final_changeset_ent.linked_changeset = staged_changeset.changeset_ent.key
    ndb.put_multi([
        staged_changeset.changeset_ent,
        final_changeset.changeset_ent,
    ])

    # Get a mapping of paths to current _FilePointers (or None).
    file_pointers = {}
    root_file_pointer = _FilePointer.get_root_key()
    ordered_paths = staged_files.keys()
    file_pointer_keys = [ndb.Key(_FilePointer, path, parent=root_file_pointer)
                         for path in ordered_paths]
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
      if titan_file.meta.status == FILE_EDITED and not file_pointer:
        status = FILE_CREATED

      # Create a _FileVersion entity containing revision metadata.
      new_file_version = _FileVersion(
          id=_FileVersion.make_key_name(final_changeset, titan_file.path),
          path=titan_file.path,
          changeset_num=final_changeset.num,
          changeset_created_by=final_changeset.created_by,
          # This is correctly modified_by, not created_by. We want to store the
          # user who made this file revision, not the original created_by user.
          created_by=titan_file.modified_by,
          status=status,
          parent=final_changeset.changeset_ent.key)
      new_file_versions.append(new_file_version)

      # Create or change the _FilePointer for this file.
      if not file_pointer and status != FILE_DELETED:
        # New file, setup the pointer.
        file_pointer = _FilePointer(id=titan_file.path,
                                    parent=root_file_pointer)
      if file_pointer:
        # Important: the file pointer is pointed to the staged changeset number,
        # since a file is not copied on commit from ver/1/file to ver/2/file.
        file_pointer.changeset_num = staged_changeset.num

      # Files versions marked as "deleted" should delete the _FilePointer.
      if status == FILE_DELETED:
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

    logging.info('Submitted changeset %d as changeset %d.',
                 staged_changeset.num, final_changeset.num)

def _make_versioned_path(path, changeset):
  """Return a two-tuple of (versioned paths, is_multiple)."""
  # Make sure we're not accidentally using non-strings,
  # which could create a path like /_titan/ver/123<Some object>
  if not isinstance(path, basestring):
    raise TypeError('path argument must be a string: %r' % path)
  return VERSIONS_PATH_FORMAT % (changeset.num, path)

def _verify_is_new_changeset(changeset):
  """If changeset is committed, don't allow files to be changed."""
  if changeset.status != CHANGESET_NEW:
    raise ChangesetError('Cannot write files in a "%s" changeset.'
                         % changeset.status)

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

def _copy_file_from_root(path, changeset, **kwargs):
  """Copy a root file (if it exists) to a new versioned path.

  Args:
    path: An absolute filename.
    changeset: A Changeset object.
  Returns:
    The newly created files.File object or None (if the root path didn't exist).
  """
  root_file = files.File(path, **kwargs)
  versioned_file = files.File(path, changeset=changeset,
                              _disable_root_copy=True, **kwargs)
  if not root_file.exists:
    return
  # Copy the root file to the versioned path if:
  # 1) The root file exists.
  # 2) The versioned file doesn't exist or it is being un-deleted.
  if not versioned_file.exists or versioned_file.meta.status == FILE_DELETED:
    root_file.copy_to(versioned_file)
  return versioned_file
