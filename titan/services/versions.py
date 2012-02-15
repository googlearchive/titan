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

"""Titan version control system, including atomic commits of groups of files.

Documentation:
  http://code.google.com/p/titan-files/wiki/VersionsService
"""

# TODO(user): Add caching of all top-level entities, primarily _Changesets.

import logging
import re
from google.appengine.ext import db
import diff_match_patch
from titan.common import strong_counters
from titan.common import hooks
from titan.files import files

SERVICE_NAME = 'versions'

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

class ChangesetError(Exception):
  pass

class FileVersionError(Exception):
  pass

class CommitError(db.TransactionFailedError):
  pass

def RegisterService():
  """Method required for all Titan service plugins."""
  hooks.RegisterHook(SERVICE_NAME, 'file-exists', hook_class=HookForExists)
  hooks.RegisterHook(SERVICE_NAME, 'file-get', hook_class=HookForGet)
  hooks.RegisterHook(SERVICE_NAME, 'file-write', hook_class=HookForWrite)
  hooks.RegisterHook(SERVICE_NAME, 'file-touch', hook_class=HookForTouch)
  hooks.RegisterHook(SERVICE_NAME, 'file-delete', hook_class=HookForDelete)
  hooks.RegisterHook(SERVICE_NAME, 'list-files', hook_class=HookForListFiles)

  # Register validator for exposed arguments in the HTTP handlers.
  http_hook_names = (
      'http-file-exists',
      'http-file-get',
      'http-file-write',
      'http-file-touch',
      'http-file-delete',
      'http-list-files',
  )
  for hook_name in http_hook_names:
    hooks.RegisterParamValidator(
        SERVICE_NAME, hook_name, validator_func=_GetValidParams)

class VersionedFile(files.File):
  """Subclass of File class for magic hiding of versioned file paths."""

  def __init__(self, file_obj):
    self._file_obj = file_obj
    self._versioned_path = file_obj.path
    changeset_num = VERSIONS_PATH_BASE_REGEX.match(file_obj.path).group(1)
    self._changeset_num = int(changeset_num)
    self._path = re.sub(VERSIONS_PATH_BASE_REGEX, '', file_obj.path)

  def __repr__(self):
    return '<VersionedFile %s (Changeset %d)>' % (self._path,
                                                  self._changeset_num)

  def __getattr__(self, name):
    return getattr(self._file_obj, name)

  @property
  def path(self):
    return self._path

  @property
  def versioned_path(self):
    return self._versioned_path

# Hooks for Titan Files require Pre and Post methods, take specific arguments,
# and return specific result structures. See here for more info:
# http://code.google.com/p/titan-files/wiki/Services

class HookForExists(hooks.Hook):
  """A hook for files.Exists()."""

  def Pre(self, changeset=None, **kwargs):
    """Pre-hook method."""
    self.changeset = changeset
    if self.changeset is None:
      # If FilePointer for path exists, the file exists.
      root_file_pointer = _FilePointer.GetRootKey()
      file_pointer = _FilePointer.get_by_key_name(kwargs['path'],
                                                  parent=root_file_pointer)
      return hooks.TitanMethodResult(bool(file_pointer))

    # Check the file existence in a changeset. Deleted files will return True.
    path, _ = _MakeVersionedPaths(kwargs['path'], self.changeset)
    return {'path': path}

class HookForGet(hooks.Hook):
  """A hook for files.Get()."""

  def Pre(self, changeset=None, **kwargs):
    """Pre-hook method."""
    self.changeset = changeset
    paths = files.ValidatePaths(kwargs['paths'])
    is_multiple = hasattr(paths, '__iter__')
    if self.changeset is None:
      # Follow latest FilePointers and use those files.
      root_file_pointer = _FilePointer.GetRootKey()
      file_pointers = _FilePointer.get_by_key_name(paths,
                                                   parent=root_file_pointer)
      file_pointers = file_pointers if is_multiple else [file_pointers]
      versioned_paths = [fp.versioned_path for fp in file_pointers if fp]
      if not versioned_paths:
        # No files exist.
        return {} if is_multiple else None

      versioned_paths = versioned_paths if is_multiple else versioned_paths[0]
      return {'paths': versioned_paths}

    paths, is_multiple = _MakeVersionedPaths(paths, self.changeset)
    return {'paths': paths}

  def Post(self, file_objs):
    """Post-hook method."""
    # Single path result.
    is_multiple = hasattr(file_objs, '__iter__')
    if not is_multiple:
      return VersionedFile(file_objs) if file_objs else None

    # Multi-path result.
    new_file_objs = {}
    for path in file_objs:
      nonversioned_path = re.sub(VERSIONS_PATH_BASE_REGEX, '', path)
      if path in file_objs:
        new_file_objs[nonversioned_path] = VersionedFile(file_objs[path])
    return new_file_objs

class HookForWrite(hooks.Hook):
  """A hook for files.Write()."""

  def Pre(self, changeset, delete=False, **kwargs):
    """Pre-hook method."""
    _VerifyIsNewChangeset(changeset)
    changed_kwargs = {}

    root_path = files.ValidatePaths(kwargs['path'])
    changeset.AssociatePaths(root_path)

    # Modify where the file is written by prepending the versioned path.
    versioned_path, _ = _MakeVersionedPaths(root_path, changeset)
    changed_kwargs['path'] = versioned_path

    # Update meta data.
    changed_kwargs['meta'] = kwargs.get('meta') or {}
    if delete:
      changed_kwargs['content'] = ''
      changed_kwargs['meta']['status'] = FILE_DELETED
      # This will orphan blobs if a large file is uploaded many times in a
      # changeset without committing, but that's better than losing the data.
      # TODO(user): add a flag to entities signifying if they have been
      # copied or deleted, so that we can notice and delete orphaned blobs.
      changed_kwargs['_delete_old_blob'] = False
    else:
      # The first time the versioned file is created (or un-deleted), we have
      # to branch all content and properties from the current root file version.
      versioned_file = _CopyFilesFromRoot(root_path, versioned_path, changeset)
      changed_kwargs['meta']['status'] = FILE_EDITED
      changed_kwargs['_delete_old_blob'] = False

    return changed_kwargs

  def Post(self, file_obj):
    """Post-hook method."""
    return VersionedFile(file_obj)

class HookForTouch(hooks.Hook):
  """A hook for files.Touch()."""

  def Pre(self, changeset, **kwargs):
    """Pre-hook method."""
    _VerifyIsNewChangeset(changeset)
    changed_kwargs = {}

    root_paths = files.ValidatePaths(kwargs['paths'])
    changeset.AssociatePaths(root_paths)

    # Modify where the file is written by prepending the versioned path.
    versioned_paths, _ = _MakeVersionedPaths(root_paths, changeset)
    changed_kwargs['paths'] = versioned_paths

    versioned_files = _CopyFilesFromRoot(root_paths, versioned_paths, changeset)

    # Update meta data.
    changed_kwargs['meta'] = kwargs.get('meta') or {}
    changed_kwargs['meta']['status'] = FILE_EDITED
    return changed_kwargs

  def Post(self, file_objs):
    """Post-hook method."""
    is_multiple = hasattr(file_objs, '__iter__')
    if is_multiple:
      file_objs = [VersionedFile(file_obj) for file_obj in file_objs]
    else:
      file_objs = VersionedFile(file_objs)
    return file_objs

class HookForDelete(hooks.Hook):
  """A hook for files.Delete().

  A delete in the files world is a revert in the versions world.
  """

  def Pre(self, changeset, **kwargs):
    """Pre-hook method."""
    _VerifyIsNewChangeset(changeset)

    root_paths = files.ValidatePaths(kwargs['paths'])
    changeset.AssociatePaths(root_paths)

    # Modify where the file is written by prepending the versioned path.
    versioned_paths, _ = _MakeVersionedPaths(root_paths, changeset)
    return {'paths': versioned_paths}

class HookForListFiles(hooks.Hook):
  """A hook for files.ListFiles()."""

  def Pre(self, changeset=None, **kwargs):
    """Pre-hook method."""
    self.changeset = changeset
    if self.changeset is None:
      # -----
      # TODO(user): Since there is no complete and walkable root tree,
      # ListFiles without a changeset basically becomes meaningless and contains
      # no data. This should probably be fixed by deferring tasks after each
      # commit into a serial queue. The tasks will update the normal file tree
      # to contain an eventually-consistent view of the latest file revisions.
      # ListFiles should then walk this and ignore all versioned files.
      # However, this behavior needs to be able to be turned off in the
      # microversions module to avoid overwriting the already-written root tree.
      # -----
      raise NotImplementedError('Cannot ListFiles with versions service.')

    # Modify which directory is listed by prepending the versioned path.
    dir_path, _ = _MakeVersionedPaths(kwargs['dir_path'], self.changeset)
    return {'dir_path': dir_path}

  def Post(self, file_objs):
    """Post-hook method."""
    if self.changeset is not None:
      # Undo the prepended versioned paths.
      return [VersionedFile(file_obj) for file_obj in file_objs]
    return file_objs

def _GetValidParams(request_params):
  """Expose certain parameters for this service in the HTTP handlers.

  Args:
    request_params: Parameters from HTTP request.
  Returns:
    Validated dictionary of request params.
  """
  valid_kwargs = {}
  if 'changeset' in request_params:
    valid_kwargs['changeset'] = Changeset(int(request_params.get('changeset')))
  if 'delete' in request_params:
    delete = request_params.get('delete')
    valid_kwargs['delete'] = False if delete == 'false' else True
  return valid_kwargs

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
  """

  def __init__(self, num, changeset_ent=None):
    self._changeset_ent = changeset_ent
    self._num = int(num)
    self._associated_paths = []
    self._finalized_paths = False

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
      self._changeset_ent = _Changeset.get_by_key_name(
          str(self._num), parent=_Changeset.GetRootKey())
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
    return VERSIONS_PATH_FORMAT % (self.linked_changeset_num, '')

  @property
  def linked_changeset(self):
    return Changeset(num=self.linked_changeset_num)

  @property
  def linked_changeset_num(self):
    # Use protected property to avoid dereferencing the SelfReferenceProperty
    # and performing an unnecessary RPC since we only need the key_name.
    return int(self.changeset_ent._linked_changeset.name())

  @property
  def created_by(self):
    return self.changeset_ent.created_by

  def GetFiles(self):
    """Get all files associated with this changeset.

    Guarantees strong consistency, but requires that associated file paths
    have been finalized on this specific Changeset instance.

    Raises:
      ChangesetError: If associated file paths have not been finalized.
    Returns:
      A dictionary mapping root file paths to VersionedFile objects.
    """
    if not self._finalized_paths:
      raise ChangesetError(
          'Cannot guarantee strong consistency when associated file paths '
          'have not been finalized. Perhaps you want ListFiles?')
    return files.Get(self._associated_paths, changeset=self)

  def ListFiles(self):
    """Perform a query and return VersionedFiles of the changeset file paths.

    This method is always eventually consistent and may not contain recently
    changed files.

    Returns:
      A dictionary mapping root file paths to VersionedFile objects.
    """
    changeset = self
    if self.changeset_ent.status == CHANGESET_SUBMITTED:
      # The files stored for submitted changesets are actually stored under the
      # the staging changeset's number, since they are never moved.
      changeset = self.changeset_ent.linked_changeset
    versioned_file_objs = files.ListFiles('/', recursive=True,
                                          changeset=changeset)
    # Transform into a dictionary that maps non-versioned paths to the
    # VersionedFile objects.
    return dict([(file_obj.path, file_obj) for file_obj in versioned_file_objs])

  def AssociatePaths(self, paths):
    """Associate a path temporally to this changeset object before commit.

    Args:
      paths: Absolute file path or iterable of absolute file paths to associate.
    """
    paths = files.ValidatePaths(paths)
    _VerifyRootPaths(paths)

    is_multiple = hasattr(paths, '__iter__')
    if is_multiple:
      self._associated_paths.extend(paths)
    else:
      self._associated_paths.append(paths)
    self._finalized_paths = False

  def FinalizeAssociatedPaths(self):
    """Indicate that this specific Changeset object was used for all operations.

    This flag is used during commit to indicate if this object can be trusted
    for strong consistency guarantees of which files paths will be committed.
    Only call this method if you are sure that this same Changeset instance was
    passed in for all file operations associated with this changeset.
    """
    if not self._associated_paths:
      raise ChangesetError('Cannot finalize: associated paths is empty.')
    self._finalized_paths = True

class _Changeset(db.Model):
  """Model representing a changeset.

  Attributes:
    num: Integer of the entity's key().name().
    created: datetime.datetime object of when this entity was created.
    status: A string status of the changeset.
    linked_changeset: A reference between staging and finalized changesets.
    created_by: A users.User object of the user who created the changeset.
  """
  num = db.IntegerProperty(required=True)
  created = db.DateTimeProperty(auto_now_add=True)
  status = db.StringProperty(choices=[CHANGESET_NEW,
                                      CHANGESET_PRE_SUBMIT,
                                      CHANGESET_SUBMITTED,
                                      CHANGESET_DELETED,
                                      CHANGESET_DELETED_BY_SUBMIT])
  linked_changeset = db.SelfReferenceProperty()
  created_by = db.UserProperty(auto_current_user_add=True)

  def __repr__(self):
    return '<_Changeset %d status:%s>' % (self.num, self.status)

  @staticmethod
  def GetRootKey():
    """Get the root key, the parent of all changeset entities."""
    # All changesets are in the same entity group by being children of the
    # arbitrary, non-existent "0" changeset.
    return db.Key.from_path('_Changeset', '0')

class FileVersion(object):
  """Metadata about a committed file version.

  NOTE: Always trust FileVersions as the canonical source of a file's revision
  history metadata. Don't use the 'status' meta property or other properties of
  VersionedFile objects as authoritative.

  Attributes:
    path: The committed file path. Example: /foo.html
    versioned_path: The path of the versioned file. Ex: /_titan/ver/123/foo.html
    changeset: A final Changeset object.
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
      key_name = _FileVersion.MakeKeyName(self._changeset, self._path)
      self._file_version_ent = _FileVersion.get_by_key_name(
          key_name, parent=self._changeset.changeset_ent)
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
  def changeset_created_by(self):
    return self._file_version.changeset_created_by

  @property
  def created(self):
    return self._file_version.created

  @property
  def status(self):
    return self._file_version.status

  def Serialize(self):
    created_by = self.changeset_created_by
    result = {
        'path': self.path,
        'versioned_path': self.versioned_path,
        'created': self.created,
        'status': self.status,
        'changeset_num': self._changeset.num,
        'changeset_created_by': str(created_by) if created_by else None,
        'linked_changeset_num': self.changeset.linked_changeset_num,
    }
    return result

class _FileVersion(db.Model):
  """Model representing metadata about a committed file version.

  A _FileVersion entity will only exist for committed file changes.

  Attributes:
    key().name(): '<changeset num>:<path>', such as '123:/foo.html'.
    path: The Titan File path.
    changeset_num: The changeset number in which the file was changed.
    changeset_created_by: A users.User object of who created the changeset.
    created: datetime.datetime object of when the entity was created.
    status: The edit type of the file at this version.
  """
  # NOTE: This model should be kept as lightweight as possible. Anything
  # else added here increases the amount of time that Commit() will take,
  # and decreases the number of files that can be committed at once.
  path = db.StringProperty()
  changeset_num = db.IntegerProperty()
  changeset_created_by = db.UserProperty()
  created = db.DateTimeProperty(auto_now_add=True)
  status = db.StringProperty(required=True,
                             choices=[FILE_CREATED, FILE_EDITED, FILE_DELETED])

  def __repr__(self):
    return ('<_FileVersion __key__:%s path:%s changeset_num:%s created:%s '
            'status:%s>' % (self.key().name(), self.path, self.changeset_num,
                            self.created, self.status))

  @staticmethod
  def MakeKeyName(changeset, path):
    return ':'.join([str(changeset.num), path])

class _FilePointer(db.Model):
  """Pointer from a root file path to its current file version.

  All _FilePointers are in the same entity group. As such, the entities
  are updated atomically to point a set of files at new versions.

  Attributes:
    key().name(): Root file path string. Example: '/foo.html'
    changeset_num: An integer pointing to the file's latest committed changeset.
    versioned_path: Versioned file path. Example: '/_titan/ver/1/foo.html'
  """
  # NOTE: This model should be kept as lightweight as possible. Anything
  # else added here increases the amount of time that Commit() will take,
  # and decreases the number of files that can be committed at once.
  changeset_num = db.IntegerProperty()

  def __repr__(self):
    return '<_FilePointer %s Current changeset: %s>' % (self.key().name(),
                                                        self.changeset_num)

  @property
  def versioned_path(self):
    return VERSIONS_PATH_FORMAT % (self.changeset_num, self.key().name())

  @staticmethod
  def GetRootKey():
    # The parent of all _FilePointers is a non-existent _FilePointer arbitrarily
    # named '/', since no file path can be a single slash.
    return db.Key.from_path('_FilePointer', '/')

class VersionControlService(object):
  """A service object providing version control methods."""

  def NewStagingChangeset(self, created_by=None):
    """Create a new staging changeset with a unique number ID.

    Args:
      created_by: A users.User object, will default to the current user.
    """
    return self._NewChangeset(status=CHANGESET_NEW, created_by=created_by)

  def _NewChangeset(self, status, created_by):
    """Create a changeset with the given status."""
    new_changeset_num = strong_counters.Increment(_CHANGESET_COUNTER_NAME)
    changeset_ent = _Changeset(
        key_name=str(new_changeset_num),
        num=new_changeset_num,
        status=status,
        parent=_Changeset.GetRootKey())
    if created_by:
      changeset_ent.created_by = created_by
    changeset_ent.put()
    return Changeset(num=new_changeset_num, changeset_ent=changeset_ent)

  def GetLastSubmittedChangeset(self):
    """Returns a Changeset object of the last submitted changeset."""
    changeset_root_key = _Changeset.GetRootKey()
    changeset = db.Query(_Changeset, keys_only=True)
    # Use an ancestor query to maintain strong consistency.
    changeset.ancestor(changeset_root_key)
    changeset.filter('status =', CHANGESET_SUBMITTED)
    changeset.order('-num')
    latest_changeset = list(changeset.fetch(1))
    if not latest_changeset:
      raise ChangesetError('No changesets have been submitted')
    return Changeset(num=int(latest_changeset[0].name()))

  def GetFileVersions(self, path, limit=1000):
    """Get FileVersion objects of the revisions of this file path.

    Args:
      path: An absolute file path.
      limit: The limit to the number of objects returned.
    Returns:
      A list of FileVersion objects, ordered from latest to earliest.
    """
    file_version_ents = _FileVersion.all()
    file_version_ents.filter('path =', path)

    # Order in descending chronological order, which will also happen to
    # order by changeset_num.
    file_version_ents.order('-created')

    # Encapsulate all the _FileVersion objects in public FileVersion objects.
    file_versions = []
    for file_version_ent in file_version_ents.fetch(limit=limit):
      file_versions.append(
          FileVersion(path=file_version_ent.path,
                      changeset=Changeset(file_version_ent.changeset_num),
                      file_version_ent=file_version_ent))
    return file_versions

  @staticmethod
  def GenerateDiff(file_version_before, file_version_after,
                   semantic_cleanup=False, diff_lines=False, edit_cost=None):
    """Generate a diff using the diff_match_patch API.

    Args:
      file_version_before: An older FileVersion object.
      file_version_after: A younger FileVersion object.
      semantic_cleanup: Whether or not to produce a human-readable diff.
      diff_lines: Whether or not to perform a line-level diff.
      edit_cost: Efficiency cleanup edit cost. The larger the edit cost,
          the more aggressive the cleanup. Sets diff_match_patch.Edit_Cost.
          This should usually not be combined with semantic_cleanup=True.
    Returns:
      A list of two-tuples, following the diff_match_patch return structure.
      http://code.google.com/p/google-diff-match-patch/wiki/API
    """
    file_obj_before = files.Get(
        file_version_before.path,
        changeset=file_version_before.changeset.linked_changeset)
    assert file_obj_before

    file_obj_after = files.Get(
        file_version_after.path,
        changeset=file_version_after.changeset.linked_changeset)
    assert file_obj_after

    differ = diff_match_patch.diff_match_patch()
    before = file_obj_before.content
    after = file_obj_after.content
    if diff_lines:
      diffs = differ.diff_lineMode(before, after, deadline=None)
    else:
      # TODO(user): support diff deadline.
      diffs = differ.diff_main(before, after)
    if edit_cost is not None:
      differ.Diff_EditCost = edit_cost
      differ.diff_cleanupEfficiency(diffs)
    if semantic_cleanup:
      differ.diff_cleanupSemantic(diffs)
    return diffs

  @staticmethod
  def MakeNiceDualLineDiffs(diffs):
    """Convert a set of line-level diffs to an easier-to-output format.

    Args:
      diffs: A set of diff_match_patch diffs from GenerateDiff().
    Returns:
      Two lists in this format (each dict element is data for exactly one line):
      [
          {
              'diff_types': [0, 1],
              'diffs': [(0, 'foo'), (1, 'bar')],
          },
          ...
      ]
      The return is a two-tuple of two of the above structures, representing
      the left-hand lines and the right-hand lines in a side-by-side diff:
          (<lines for "before" diff>, <lines for "after" diff>)
      Both lists will contain diffs for deletes, equalities, and inserts, but
      lines breaks will naturally be in different places.
    """
    #
    # EXPERIMENTAL. This function currently has known correctness issues.
    #
    line_diffs_before = VersionControlService._MakeNiceLineDiffs(
        diffs, before=True)
    line_diffs_after = VersionControlService._MakeNiceLineDiffs(
        diffs, after=True)
    return line_diffs_before, line_diffs_after

  @staticmethod
  def _MakeNiceLineDiffs(diffs, before=False, after=False):
    # ---
    # TODO(user): 2012-02-02...re-write this. It doesn't correctly
    # determine newline break differences and could probably be much simpler.
    # ---
    line_diffs = []
    pending_lines = []
    current_line = {'diff_types': [], 'diffs': []}
    for diff_type, diff_content in diffs:
      multiline_content = diff_content.splitlines(True)
      if not multiline_content:
        # diff is (#, '') -- skip.
        pass
      elif len(multiline_content) == 1:
        # No extra lines within this diff, append diff info to current line.
        single_line_content = multiline_content[0]
        current_line['diff_types'].append(diff_type)
        current_line['diffs'].append((diff_type, single_line_content))
        if single_line_content.endswith('\n'):
          # End of line. Append to the result and then reset current_line:
          line_diffs.append(current_line)
          current_line = {'diff_types': [], 'diffs': []}
      else:
        # The current diff's content contains multiple lines all associated to
        # the same diff_type. For example: (1, 'foo\nbar\nbaz') is three
        # inserted lines. Split these up into their own lines with the same
        # diff_type, like (1, 'foo\n'), (1, 'bar\n'), (1, 'baz').
        #
        # multiline_content = ['foo\n', 'bar\n', 'baz']
        for single_line_content in multiline_content:
          if single_line_content.endswith('\n'):
            if before and current_line['diff_types'] and diff_type == 1:
              # Special case: for the left-hand diff, we are in the middle
              # of compiling the diffs for a line, and a new "insert" line
              # is presented (which should come after the line we haven't
              # finished yet). Put the line in a pending queue.
              single_line = {
                  'diff_types': [diff_type],
                  'diffs': [(diff_type, single_line_content)],
              }

              # Don't add pending lines which are partials of a previous line.
              # For example, in this set of diffs:
              #   (0, 'the quick'), (-1, '\nbrown'), (1, 'fox\n')
              # "fox" is a new addition to a line we have already completed, so
              # we discard it here to avoid adding additional lines.
              if pending_lines or single_line_content.startswith('\n'):
                pending_lines.append(single_line)
            else:
              # Could be one of two things:
              # - A partial which completes the current_line.
              # - Or, an entirely self-contained line.
              if current_line['diff_types']:
                # End of line. Append and reset.
                current_line['diff_types'].append(diff_type)
                current_line['diffs'].append((diff_type, single_line_content))

                if (before and diff_type != 1) or (after and diff_type != -1):
                  line_diffs.append(current_line)
                  current_line = {'diff_types': [], 'diffs': []}

                  # Add any pending lines into their correct spot.
                  line_diffs.extend(pending_lines)
                  pending_lines = []
              else:
                # Self-contained line.
                single_line = {
                    'diff_types': [diff_type],
                    'diffs': [(diff_type, single_line_content)],
                }
                line_diffs.append(single_line)
          else:
            # Partial line (example: the ending "baz" isn't followed by a
            # newline, so this diff doesn't tell us where the line ends).
            # Append the diff to the current_line, and one of the next
            # iterations will find the end of the line.
            current_line['diff_types'].append(diff_type)
            current_line['diffs'].append((diff_type, single_line_content))

    # Done with main loop. If the last diff didn't end with a newline, then we
    # have some partial data still around in current_line. Append this
    # final content as its own line.
    if current_line['diff_types']:
      line_diffs.append(current_line)
    return line_diffs

  def Commit(self, staged_changeset, force=False):
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
      staged_file_objs = staged_changeset.GetFiles()
    except ChangesetError:
      if not force:
        raise
      # Got force=True, get files with an eventually-consistent query.
      staged_file_objs = staged_changeset.ListFiles()
    if not staged_file_objs:
      raise CommitError('Changeset %d contains no file changes.'
                        % staged_changeset.num)

    # Can't nest transactions, so we get a unique final changeset number here.
    # This has the potential to orphan a changeset number (if this submit works
    # but the following transaction does not). However, we don't care.
    final_changeset = self._NewChangeset(
        status=CHANGESET_PRE_SUBMIT, created_by=staged_changeset.created_by)

    xg_transaction_options = db.create_transaction_options(xg=True)
    db.run_in_transaction_options(
        xg_transaction_options, self._Commit, staged_changeset,
        final_changeset, staged_file_objs)

    return final_changeset

  @staticmethod
  def _Commit(staged_changeset, final_changeset, staged_file_objs):
    """Commit a staged changeset."""
    manifest = ['%s: %s' % (f.status, f.path)
                for f in staged_file_objs.values()]
    logging.info('Submitting changeset %d as changeset %d with %d files:\n%s',
                 staged_changeset.num, final_changeset.num,
                 len(staged_file_objs), '\n'.join(manifest))

    # Update status of the staging and final changesets.
    staged_changeset_ent = staged_changeset.changeset_ent
    staged_changeset_ent.status = CHANGESET_DELETED_BY_SUBMIT
    staged_changeset_ent.linked_changeset = final_changeset.changeset_ent
    final_changeset_ent = final_changeset.changeset_ent
    final_changeset_ent.status = CHANGESET_SUBMITTED
    final_changeset_ent.linked_changeset = staged_changeset.changeset_ent
    db.put([staged_changeset.changeset_ent, final_changeset.changeset_ent])

    # Get a mapping of paths to current _FilePointers (or None).
    file_pointers = {}
    root_file_pointer = _FilePointer.GetRootKey()
    ordered_paths = staged_file_objs.keys()
    file_pointer_ents = _FilePointer.get_by_key_name(ordered_paths,
                                                     parent=root_file_pointer)
    for i, file_pointer_ent in enumerate(file_pointer_ents):
      file_pointers[ordered_paths[i]] = file_pointer_ent

    new_file_versions = []
    updated_file_pointers = []
    deleted_file_pointers = []
    for path, file_obj in staged_file_objs.iteritems():
      file_pointer = file_pointers[file_obj.path]

      # Update "edited" status to be "created" on commit if file doesn't exist.
      status = file_obj.status
      if file_obj.status == FILE_EDITED and not file_pointer:
        status = FILE_CREATED

      # Create a _FileVersion entity containing revision metadata.
      new_file_version = _FileVersion(
          key_name=_FileVersion.MakeKeyName(final_changeset, file_obj.path),
          path=file_obj.path,
          changeset_num=final_changeset.num,
          changeset_created_by=final_changeset.created_by,
          status=status,
          parent=final_changeset.changeset_ent)
      new_file_versions.append(new_file_version)

      # Create or change the _FilePointer for this file.
      if not file_pointer and status != FILE_DELETED:
        # New file, setup the pointer.
        file_pointer = _FilePointer(key_name=file_obj.path,
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
      db.put(new_file_versions)
    if updated_file_pointers:
      db.put(updated_file_pointers)
    if deleted_file_pointers:
      db.delete(deleted_file_pointers)

    logging.info('Submitted changeset %d as changeset %d.',
                 staged_changeset.num, final_changeset.num)

def _MakeVersionedPaths(paths, changeset):
  """Return a two-tuple of (versioned paths, is_multiple)."""
  is_multiple = hasattr(paths, '__iter__')
  new_paths = []
  for path in paths if is_multiple else [paths]:
    # Make sure we're not accidentally using non-strings,
    # which could create a path like /_titan/ver/123<Some object>
    if not isinstance(path, basestring):
      raise TypeError('path argument must be a string: %r' % path)
    new_paths.append(VERSIONS_PATH_FORMAT % (changeset.num, path))
  return new_paths if is_multiple else new_paths[0], is_multiple

def _VerifyIsNewChangeset(changeset):
  """If changeset is committed, don't allow files to be changed."""
  if changeset.status != CHANGESET_NEW:
    raise ChangesetError('Cannot write files in a "%s" changeset.'
                         % changeset.status)

def _VerifyRootPaths(paths):
  """Make sure all given paths are not versioned paths."""
  is_multiple = hasattr(paths, '__iter__')
  for path in paths if is_multiple else [paths]:
    if VERSIONS_PATH_BASE_REGEX.match(path):
      raise ValueError('Not a root file path: %s' % path)

def _CopyFilesFromRoot(root_paths, versioned_paths, changeset):
  """Copy current root files to their versioned file paths.

  Args:
    root_paths: An absolute filename or iterable of absolute filenames.
    versioned_paths: An absolute filename or iterable of absolute filenames.
    changeset: A Changeset object.
  Returns:
    For single paths: None if no versioned file exists, or the VersionedFile
        object.
    For multiple paths: A dictionary of existing root paths to VersionedFile
        objects.
  """
  is_multiple = hasattr(root_paths, '__iter__')

  root_files = files.Get(root_paths)
  versioned_files = files.Get(root_paths, changeset=changeset)

  if not root_files:
    return {} if is_multiple else None

  # For each existing root file, copy it to the versioned path (only if the
  # versioned file doesn't exist or is being un-deleted).
  for i, root_path in enumerate(root_paths if is_multiple else [root_paths]):
    if is_multiple:
      root_file = root_files.get(root_path)
      if not root_file:
        # If nothing to copy from root, skip.
        continue
      versioned_file = versioned_files.get(root_file.path)
      versioned_path = versioned_paths[i]
    else:
      root_file = root_files
      if not root_file:
        # If nothing to copy from root, skip.
        continue
      versioned_file = versioned_files
      versioned_path = versioned_paths

    # The first time a versioned file is created (or un-deleted) in a changeset,
    # we copy all content and properties from the current root file version.
    if not versioned_file or versioned_file.status == FILE_DELETED:
      # Unfortunate high-coupling to the microversions service: use a file
      # object's versioned_path, but microversions actually come from the root
      # tree so we fallback to the root_file itself if no versioned_path exists.
      source_path = getattr(root_file, 'versioned_path', root_file)
      files.Copy(source_path=source_path,
                 destination_path=versioned_path)
  return files.Get(root_paths, changeset=changeset)
