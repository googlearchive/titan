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

"""A filesystem abstraction for App Engine apps.

Documentation:
  http://code.google.com/p/titan-files/

Usage:
  titan_file = files.File('/some/file')
  titan_file.Write(content='hello world')
  titan_file.Delete()
  titan_file.CopyTo(files.File('/destination/file'))
  files.File.ValidatePath('/some/file')

  titan_files = files.Files.List('/some/dir')
  titan_files.CopyTo('/destination/', strip_prefix='/some')
  titan_files.Load()
  titan_files.Delete()
  files.Files.ValidatePaths(['/some/file', '/other/file'])
"""

try:
  # Load appengine_config here to guarantee that custom factories can be
  # registered before any File operation takes place.
  import appengine_config
except ImportError:
  appengine_config = None

import collections
import datetime
import hashlib
import logging
import os

from concurrent import futures
from google.appengine.ext import blobstore
from google.appengine.ext import ndb

from titan.common import utils
from titan.files import files_cache
from titan.users import users

# Arbitrary cutoff for when content will be stored in blobstore.
# This value should be mirrored by titan_client.DIRECT_TO_BLOBSTORE_SIZE.
MAX_CONTENT_SIZE = 1 << 19  # 500 KiB

DEFAULT_BATCH_SIZE = 100
DEFAULT_MAX_WORKERS = 25

class Error(Exception):
  pass

class BadFileError(Error):
  pass

class InvalidMetaError(Error):
  pass

class CopyFileError(Error):

  def __init__(self, titan_file):
    super(CopyFileError, self).__init__()
    self.titan_file = titan_file

class MoveFileError(Error):

  def __init__(self, titan_file):
    super(MoveFileError, self).__init__()
    self.titan_file = titan_file

class CopyFilesError(Error):
  pass

class File(object):
  """A file abstraction.

  Usage:
    titan_file = File('/path/to/file.html')
    titan_file.Write('Some content')
    titan_file.Delete()
    titan_file.CopyTo(destination_file)
    titan_file.MoveTo(destination_file)
    titan_file.Serialize()
  Attributes:
    name: Filename without path. Example: file.html
    path: Full filename and path. Might be a virtual path from a mixin.
        Example: /path/to/some/file.html
    real_path: Full filename and path. Always the actual backend storage path.
        Example: /path/to/some/file.html
    dir_path: The containing directory. Might be a virtual path.
        Example: /path/to/some
    real_dir_path: The containing directory. Always the actual storage dir path.
        Example: /path/to/some
    paths: A list of containing directories.
        Example: ['/', '/path', '/path/to', '/path/to/some']
    mime_type: Content type of the file.
    created: Created datetime.
    modified: Last modified datetime.
    content: The file contents, read from either datastore or blobstore.
    blob: If the content is stored in blobstore, this is the BlobInfo object
        pointing to it. If only static serving is needed, a client should
        check for 'blob' and use webapp's send_blob() to avoid loading the
        content blob into app memory.
    exists: Boolean of if the file exists.
    created_by: A users.TitanUser for who first created the file, or None.
    modified_by: A users.TitanUser for who last modified the file, or None.
    size: The number of bytes of this file's content.
    md5_hash: Pre-computed md5 hash of the file's content.
    meta: An object exposing File metadata as attributes. Use the Write()
        method to update meta information.

  Note: be aware that this object performs optimizations to avoid unnecessary
      RPCs. As such, some properties (like .exists) could return stale info
      especially if a File object is long-lived.
  """

  def __new__(cls, path, _file_ent=None, _from_factory=False, **kwargs):
    """Factory handling for File objects.

    Args:
      path: Mirrored from __init__.
      _file_ent: Mirrored from __init__.
      _from_factory: A flag for the recursive base case.
    Returns:
      File instance.
    """
    # Validate path before class creation and mixins.
    File.ValidatePath(path)
    if _global_file_factory.is_registered and not _from_factory:
      # Get the correct class instance for this path, determined by the factory:
      file_class = _global_file_factory(path=path, **kwargs)
      # Instantiate an object of the class and return it:
      return file_class(
          path=path, _file_ent=_file_ent, _from_factory=True, **kwargs)
    return super(File, cls).__new__(cls)

  def __init__(self, path, _file_ent=None, _from_factory=False, **kwargs):
    """File object constructor.

    Args:
      path: An absolute filename.
      _file_ent: An internal-only optimization argument which helps avoid
          unnecessary RPCs.
      _from_factory: An internal-only flag for factory handling.
    """
    File.ValidatePath(path)
    self._path = path
    self._real_path = None
    self._name = os.path.basename(self._path)
    self._file_ent = _file_ent
    self._meta = None
    self._original_kwargs = kwargs
    self._original_kwargs['path'] = path

  def __nonzero__(self):
    return self.exists

  def __eq__(self, other_file):
    return (isinstance(other_file, File)
            and self.real_path == other_file.real_path)

  def __repr__(self):
    return '<%s: %s>' % (self.__class__.__name__, self.real_path)

  def __reduce__(self):
    # This method allows a File object to be pickled, such as when it is passed
    # to a deferred task. This works by expecting that if we pass the same
    # arguments to init a File object at any time, it will go through any
    # registered factory and be created in the same way. No other state is
    # maintained, we simply save the original arguments and re-init a files.File
    # object with the same kwargs when unpickling, instead of
    # implementing __getstate__ and __setstate__.
    #
    # http://docs.python.org/library/pickle.html#object.__reduce__
    return _UnpickleFile, (self._original_kwargs,), {}

  @property
  def _file(self):
    """Internal property that allows lazy-loading of the public properties."""
    # NOTE: this is not the only way _file_ent can be set on this object.
    # Because of this, don't rely on .is_loaded for limiting permission checks.
    try:
      if self._file_ent:
        return self._file_ent
      # Haven't initialized a File object yet.
      self._file_ent = _TitanFile.get_by_id(self.real_path)
      if not self._file_ent:
        raise BadFileError('File does not exist: %s' % self.real_path)
      return self._file_ent
    except AttributeError:
      # Without this, certain attribute errors are masked and harder to debug.
      logging.exception('Internal AttributeError: ')
      raise

  @property
  def is_loaded(self):
    """Whether or not this lazy object has been evaluated."""
    return bool(self._file_ent)

  @property
  def name(self):
    return self._name

  @property
  def path(self):
    """The file's path. Can be override to display a virtual path."""
    # NOTE: All internal methods should use .real_path so that this
    # property can be overridden in subclasses without interference.
    return self._path

  @property
  def real_path(self):
    """The canonical backend path of this file.

    Subclasses can override self._real_path to use a different storage location.

    Returns:
      Actual storage path of the file.
    """
    return self._real_path or self._path

  @property
  def dir_path(self):
    """The file's containing directory path."""
    # Don't use self._file.dir_path to avoid evaluation.
    return os.path.dirname(self.path)

  @property
  def real_dir_path(self):
    """The actualy backend storage dir path of this file."""
    # Don't use self._file.dir_path to avoid evaluation.
    return os.path.dirname(self.real_path)

  @property
  def paths(self):
    return self._file.paths

  @property
  def mime_type(self):
    return self._file.mime_type

  @property
  def created(self):
    return self._file.created

  @property
  def modified(self):
    return self._file.modified

  @property
  def content(self):
    return _ReadContentOrBlob(self)

  @property
  def blob(self):
    """The BlobInfo of this File, if the file content is stored in blobstore."""
    # Backwards-compatibility with deprecated "blobs" property.
    if not self._file.blob and not self._file.blobs:
      return
    if self._file.blob:
      return blobstore.BlobInfo.get(self._file.blob)
    return blobstore.get(self._file.blobs[0])

  @property
  def exists(self):
    try:
      return bool(self._file)
    except BadFileError:
      return False

  @property
  def created_by(self):
    return self._file.created_by

  @property
  def modified_by(self):
    return self._file.modified_by

  @property
  def size(self):
    if self.blob:
      return self.blob.size
    content = self.content
    if isinstance(content, unicode):
      return len(content.encode('utf-8'))
    return len(content)

  @property
  def md5_hash(self):
    return self.blob.md5_hash if self.blob else self._file.md5_hash

  @property
  def meta(self):
    """File meta data."""
    if self._meta:
      return self._meta
    meta = {}
    for key in self._file.meta_properties:
      meta[key] = getattr(self._file, key)
    self._meta = utils.DictAsObject(meta)
    return self._meta

  def read(self):
    return self.content

  def close(self):
    pass

  def _MaybeEncodeContent(self, content, encoding):
    # If given unicode, encode it as UTF-8 and flag it for future decoding.
    if isinstance(content, unicode):
      if encoding is not None:
        raise TypeError(
            'If given a unicode object for "content", the "encoding" '
            'argument cannot be given.')
      encoding = 'utf-8'
      content = content.encode(encoding)
    return content, encoding

  def _MaybeWriteToBlobstore(self, content, blob):
    if content and blob:
      raise TypeError('Exactly one of "content" or "blob" must be given.')
    if content and len(content) > MAX_CONTENT_SIZE:
      logging.debug('Content size %s exceeds %s bytes, uploading to blobstore.',
                    len(content), MAX_CONTENT_SIZE)
      old_blobinfo = self.blob if self.exists else None
      blob = utils.WriteToBlobstore(content, old_blobinfo=old_blobinfo)
      files_cache.StoreBlob(self.real_path, content)
      content = None
    return content, blob

  def _GetCreatedByUser(self):
    """Returns the user that should be saved when a file is first created.

    Can be overridden by subclasses.

    Returns:
      A users.TitanUser object.
    """
    return users.GetCurrentUser()

  def _GetModifiedByUser(self):
    """Returns the user that should be saved when a file is modified.

    Can be overridden by subclasses.

    Returns:
      A users.TitanUser object.
    """
    return users.GetCurrentUser()

  # TODO(user): remove _delete_old_blob, refactor into versions subclass.
  def Write(self, content=None, blob=None, mime_type=None, meta=None,
            encoding=None, _delete_old_blob=True):
    """Write or update a File.

    Updates: if the File already exists, Write will accept any of the given args
    and only perform an update of the given data, without affecting other data.

    Args:
      content: File contents, either as a str or unicode object.
          This will handle content greater than the 1MB limit by storing it in
          blobstore. However, this technique should only be used for relatively
          small files; it is much less efficient than directly uploading
          to blobstore and passing the resulting BlobKeys to the blobs argument.
      blob: If content is not provided, a BlobKey pointing to the file.
      mime_type: Content type of the file; will be guessed if not given.
      meta: A dictionary of properties to be added to the file.
      encoding: The optional encoding of the content if given a bytestring.
          The encoding will be automatically determined if "content" is passed
          a unicode string.
      _delete_old_blob: Whether or not to delete the old blob if it changed.
    Raises:
      TypeError: For missing arguments.
      BadFileError: If updating meta information on a non-existent file.
    Returns:
      Self-reference.
    """
    logging.info('Writing Titan file: %s', self.real_path)

    # Argument sanity checks.
    _TitanFile.ValidateMetaProperties(meta)
    is_content_update = content is not None or blob is not None
    is_meta_update = mime_type is not None or meta is not None
    if not is_content_update and not is_meta_update:
      raise TypeError('Arguments expected, but none given.')
    if not self.exists and is_meta_update and not is_content_update:
      raise BadFileError('File does not exist: %s' % self.real_path)

    # If given unicode, encode it as UTF-8 and flag it for future decoding.
    content, encoding = self._MaybeEncodeContent(content, encoding)

    # If big enough, store content in blobstore. Must come after encoding.
    content, blob = self._MaybeWriteToBlobstore(content, blob)

    if not self.exists:
      # Create new _File entity.
      # Guess the MIME type if not given.
      if not mime_type:
        mime_type = utils.GuessMimeType(self.real_path)

      # Create a new _File.
      paths = utils.SplitPath(self.real_path)
      self._file_ent = _TitanFile(
          id=self.real_path,
          name=os.path.basename(self.real_path),
          dir_path=paths[-1],
          paths=paths,
          # Root files are at depth 0.
          depth=len(paths) - 1,
          mime_type=mime_type,
          encoding=encoding,
          modified=datetime.datetime.now(),
          content=content,
          blob=blob,
          # Backwards-compatibility with deprecated "blobs" property:
          blobs=[],
          created_by=self._GetCreatedByUser(),
          modified_by=self._GetModifiedByUser(),
          md5_hash=None if blob else hashlib.md5(content).hexdigest(),
      )
      # Add meta attributes.
      if meta:
        for key, value in meta.iteritems():
          setattr(self._file, key, value)
      self._file.put()
    else:
      # Updating an existing _File.
      self._file.modified_by = self._GetModifiedByUser()

      if mime_type and self._file.mime_type != mime_type:
        self._file.mime_type = mime_type

      # Auto-migrate entities from old "blobs" to new "blob" property on write:
      if self._file.blobs:
        self._file.blob = self._file.blobs[0]
        self._file.blobs = []

      if content is not None and self._file.content != content:
        self._file.content = content
        self._file.md5_hash = hashlib.md5(content).hexdigest()
        if self._file.blob and _delete_old_blob:
          # Delete the actual blobstore data.
          blobstore.delete(self._file.blob)
          files_cache.ClearBlobsForFiles(self._file)
        # Clear the current blob association for this file.
        self._file.blob = None

      if blob is not None and self._file.blob != blob:
        if self._file.blob and _delete_old_blob:
          # Delete the actual blobstore data.
          blobstore.delete(self._file.blob)
          files_cache.ClearBlobsForFiles(self._file)
        # Associate the new blob to this file.
        self._file.blob = blob
        self._file.md5_hash = None
        self._file.content = None

      if encoding != self._file.encoding:
        self._file.encoding = encoding

      # Update meta attributes.
      if meta is not None:
        for key, value in meta.iteritems():
          if not hasattr(self._file, key) or getattr(self._file, key) != value:
            setattr(self._file, key, value)
      self._file.put()
    return self

  def Delete(self, _delete_old_blob=True):
    """Delete file.

    Args:
      _delete_old_blob: defaults to True.
    Returns:
      Self-reference.
    """
    if self.blob and _delete_old_blob:
      blobstore.delete(self._file.blob)
      files_cache.ClearBlobsForFiles(self._file)
    self._file.key.delete()
    self._file_ent = None
    self._meta = None
    return self

  def CopyTo(self, destination_file, exclude_meta=None):
    """Copy this and all of its properties to a different path.

    Args:
      destination_file: A File object of the destination path.
      exclude_meta: A list of meta keywords to exclude in the destination file.
    Returns:
      Self-reference.
    """
    assert isinstance(destination_file, File)
    logging.info('Copying Titan file: %s --> %s', self.real_path,
                 destination_file.real_path)
    try:
      if not self.exists:
        raise BadFileError('File does not exist: %s' % self.real_path)
      if destination_file.exists:
        # TODO(user): make this DeleteAsync when available.
        destination_file.Delete()

      # Copy meta attributes, except ones that are excluded.
      meta = self.meta.Serialize()
      if exclude_meta:
        for key in exclude_meta:
          if key in meta:
            del meta[key]

      destination_file.Write(
          content=self._file.content,
          blob=self._file.blob,
          mime_type=self.mime_type,
          meta=meta)
      return self
    except:
      logging.exception('Error copying file: %s', self.path)
      raise CopyFileError(destination_file)

  def MoveTo(self, destination_file):
    """Move this and all of its properties to a different path.

    Args:
      destination_file: A File object of the destination path.
    Returns:
      Self-reference.
    """
    assert isinstance(destination_file, File)
    logging.info('Moving Titan file: %s --> %s', self.real_path,
                 destination_file.real_path)
    try:
      self.CopyTo(destination_file)
      self.Delete(_delete_old_blob=False)
      return self
    except:
      logging.exception('Error moving file: %s', self.path)
      raise MoveFileError(destination_file)

  def Serialize(self, full=False):
    """Serialize the File object to native Python types.

    Args:
      full: Whether or not to include this object's content. Potentially
          expensive if the content is large and particularly if the content is
          stored in blobstore.
    Returns:
      A serializable dictionary of this File object's properties.
    """
    result = {
        'name': self.name,
        'path': self.path,
        'real_path': self.real_path,
        'paths': self.paths,
        'mime_type': self.mime_type,
        'created': self.created,
        'blob': str(self.blob.key()) if self.blob else None,
        'modified': self.modified,
        'created_by': str(self.created_by) if self.created_by else None,
        'modified_by': str(self.modified_by) if self.modified_by else None,
        'meta': {},
        'size': self.size,
        'md5_hash': self.md5_hash,
    }
    if full:
      result['content'] = self.content
    for key in self._file_ent.meta_properties:
      result['meta'][key] = getattr(self._file_ent, key)
    return result

  @staticmethod
  def ValidatePath(path):
    return utils.ValidateFilePath(path)

# This must be a top-level module function. The File class cannot be used
# directly in __reduce__ because pickle does not support keyword arguments.
def _UnpickleFile(kwargs):
  return File(**kwargs)

class FactoryState(object):
  """A container for factories, to avoid direct use of globals."""

  def __init__(self):
    self.factory = None

  @property
  def is_registered(self):
    return bool(self.factory)

  def __call__(self, *args, **kwargs):
    return self.factory(*args, **kwargs)

  def Register(self, factory):
    self.factory = factory

  def Unregister(self):
    self.factory = None

# Internal FileFactoryState. See RegisterFileFactory().
_global_file_factory = FactoryState()

def RegisterFileFactory(file_factory):
  """Register a global file factory, which returns a File subclass.

  In advanced usage, all File object instantiations can be magically
  overwritten by registering a factory which returns the correct File subclass
  based on a given path.

  This method will overwrite any previously-registered factory method.

  Args:
    file_factory: A callable which returns a File subclass.
  """
  _global_file_factory.Register(file_factory)

def UnregisterFileFactory():
  """Clear the global file factory."""
  _global_file_factory.Unregister()

def RegisterFileMixins(mixin_classes):
  """Registers a factory that returns a dynamic subclass of File with mixins.

  This method will overwrite any previously-registered factory method.

  Args:
    mixin_classes: A list of mixins classes in the order they will be applied.
  """

  def DynamicFileFactory(**kwargs):
    """Factory that dynamically creates a File subclass with mixins included."""
    base_classes = []
    shared_mixin_state = {}
    for mixin_cls in mixin_classes:
      # Mixins are enabled by default. Optionally, they can have a classmethod
      # named "ShouldApplyMixin" which tells if the mixin should be enabled
      # based on the given kwargs.
      should_apply_mixin_fn = getattr(mixin_cls, 'ShouldApplyMixin', None)
      if (not should_apply_mixin_fn
          or should_apply_mixin_fn(_mixin_state=shared_mixin_state, **kwargs)):
        base_classes.append(mixin_cls)
    # Dynamically create a files.File subclass with all of the given mixins.
    base_classes.append(File)
    return type('DynamicFile', tuple(base_classes), {})
  RegisterFileFactory(DynamicFileFactory)

class Files(collections.Mapping):
  """A mapping of paths to File objects."""

  def __init__(self, paths=None, files=None, **kwargs):
    """Constructor.

    Args:
      paths: An iterable of absolute filenames.
      files: An iterable of File objects. Required if paths not specified.
    Raises:
      ValueError: If given invalid paths.
      TypeError: If given both paths and files.
    """
    if paths is not None and files is not None:
      raise TypeError('Exactly one of "paths" or "files" args must be given.')
    self._titan_files = {}
    if paths and not hasattr(paths, '__iter__'):
      raise ValueError('"paths" must be an iterable.')
    if files and not hasattr(files, '__iter__'):
      raise ValueError('"files" must be an iterable.')
    if paths is not None:
      for path in paths:
        self._AddFile(File(path=path, **kwargs))
    elif files is not None:
      for titan_file in files:
        self._AddFile(titan_file)

  def __delitem__(self, path):
    self._RemoveFile(path)

  def __getitem__(self, path):
    return self._titan_files[path]

  def __setitem__(self, path, titan_file):
    raise AttributeError('Cannot directly set items on %s instance. '
                         'Try the update() method instead.'
                         % self.__class__.__name__)

  def __contains__(self, other):
    path = getattr(other, 'path', other)
    return path in self._titan_files

  def __iter__(self):
    for path in self._titan_files:
      yield path

  def __len__(self):
    return len(self._titan_files)

  def __eq__(self, other):
    if not isinstance(other, Files) or len(self) != len(other):
      return False
    for titan_file in other.itervalues():
      if titan_file not in self:
        return False
    return True

  def __repr__(self):
    return '<Files %r>' % self.keys()

  def _AddFile(self, titan_file):
    # Use .path here to allow virtual file path, not forcing real_path.
    self._titan_files[titan_file.path] = titan_file

  def _RemoveFile(self, path):
    del self._titan_files[path]

  def update(self, other_titan_files):
    for titan_file in other_titan_files.itervalues():
      self._AddFile(titan_file)

  def clear(self):
    self._titan_files = {}

  def Delete(self):
    """Delete all files in this container."""
    # TODO(user): implement batch operation. For now, the naive way:
    for titan_file in self.itervalues():
      titan_file.Delete()
    # Empty the container:
    self.clear()
    return self

  def CopyTo(self, dir_path, **kwargs):
    """Copy current files to the given dir_path.

    Args:
      dir_path: The destination dir_path.
      strip_prefix: The directory prefix to strip from source paths.
      timeout: The number of seconds to wait for futures to complete.
          If timeout is given and expires, the function will return
          without error but operations will continue in the background.
      result_files: An optional Files object which will be populated
          with the destination File objects created during the copy.
      failed_files: An optional Files object which will be populated
          with the source files that failed to copy.
      max_workers: Number of workers to forge in threading.
    Returns:
      Self-reference.
    """
    self._MoveOrCopyTo(dir_path, is_move=False, **kwargs)
    return self

  def MoveTo(self, dir_path, **kwargs):
    """Move current files to the given dir_path.

    Args:
      dir_path: The destination dir_path.
      strip_prefix: The directory prefix to strip from source paths.
      timeout: The number of seconds to wait for futures to complete.
          If timeout is given and expires, the function will return
          without error but operations will continue in the background.
      result_files: An optional Files object which will be populated
          with the destination File objects created during the move.
      failed_files: An optional Files object which will be populated
          with the source files that failed to move.
      max_workers: Number of workers to forge in threading.
    Returns:
      Self-reference.
    """
    self._MoveOrCopyTo(dir_path, is_move=True, **kwargs)
    return self

  def Load(self):
    """If not loaded, load associated paths and remove non-existing ones."""
    # pylint: disable=protected-access
    real_path_to_paths = {f.real_path: f.path for f in self.itervalues()}
    file_ents = _GetTitanFileEnts(real_path_to_paths.keys())
    paths_to_clear = []
    for real_path in real_path_to_paths:
      path = real_path_to_paths[real_path]
      if real_path not in file_ents:
        # Remove non-existent files.
        paths_to_clear.append(path)
      else:
        # Inject the fetched file entity into the current File object.
        self[path]._file_ent = file_ents[real_path]

    for path in paths_to_clear:
      del self[path]
    return self

  def Serialize(self, full=False):
    """Serialize the File object to native Python types.

    Args:
      full: Whether to return a full representation of the files.
    Returns:
      A serializable dictionary of this Files object's properties, i.e. a
      mapping of paths to serialized File objects found at a given path.
    """
    result = {}
    for path, titan_file in self._titan_files.iteritems():
      result[path] = titan_file.Serialize(full=full)
    return result

  def _MoveOrCopyTo(self, dir_path, is_move=False, strip_prefix=None,
                    timeout=None, result_files=None, failed_files=None,
                    max_workers=DEFAULT_MAX_WORKERS, **kwargs):
    """This encapsulate repeated logic for CopyTo and MoveTo methods."""
    utils.ValidateDirPath(dir_path)
    destination_map = utils.MakeDestinationPathsMap(
        self.keys(), destination_dir_path=dir_path, strip_prefix=strip_prefix)

    future_results = []
    with futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
      for source_path, destination_path in destination_map.iteritems():
        source_file = self[source_path]
        destination_file = File(destination_path, **kwargs)
        if result_files is not None:
          result_files.update(Files(files=[destination_file]))
        file_method = source_file.MoveTo if is_move else source_file.CopyTo
        future = executor.submit(file_method, destination_file)
        future_results.append(future)
      futures.wait(future_results, timeout=timeout)
    errors = []
    for future in future_results:
      try:
        future.result()
      except (CopyFileError, MoveFileError) as e:
        if failed_files is not None:
          failed_files.update(Files(files=[e.titan_file]))
        # Remove the failed file from successfully copied files collection.
        if result_files is not None:
          del result_files[e.titan_file.path]
        logging.exception('Operation failed:')
        errors.append(e)

    # Important: clear the in-context cache since we changed state in threads.
    ndb.get_context().clear_cache()

    if errors:
      raise CopyFilesError(
          'Failed to copy files: \n%s' % '\n'.join([str(e) for e in errors]))

  @classmethod
  def Merge(cls, first_files, second_files):
    """Return a new Files instance merged from two others."""
    new_titan_files = cls(first_files.keys())
    new_titan_files.update(cls(second_files.keys()))
    return new_titan_files

  @classmethod
  def List(cls, dir_path, recursive=False, depth=None, filters=None,
           limit=None, offset=None, **kwargs):
    """Factory method to return a lazy Files mapping for the given dir.

    Args:
      dir_path: Absolute directory path.
      recursive: Whether to list files recursively.
      depth: If recursive, a positive integer to limit the recursion depth.
          1 is one folder deep, 2 is two folders deep, etc.
      filters: An iterable of FileProperty objects.
      limit: An integer limiting the number of files returned.
      offset: Number of files to offset the query by.
    Raises:
      ValueError: If given an invalid depth argument.
    Returns:
      A populated Files mapping.
    """
    files_query = _CreateFilesQuery(dir_path, recursive=recursive, depth=depth,
                                    filters=filters)
    # TODO(user): support cursors.
    file_keys = files_query.fetch(limit=limit, offset=offset, keys_only=True)
    titan_files = cls([key.id() for key in file_keys], **kwargs)
    return titan_files

  @staticmethod
  def Count(dir_path, recursive=False, depth=None, filters=None):
    """Factory method to count the number of files within a directory.

    Args:
      dir_path: Absolute directory path.
      recursive: Whether to list files recursively.
      depth: If recursive, a positive integer to limit the recursion depth.
          1 is one folder deep, 2 is two folders deep, etc.
      filters: An iterable of FileProperty objects.
    Raises:
      ValueError: If given an invalid depth argument.
    Returns:
      A count of files that match the query.
    """
    files_query = _CreateFilesQuery(dir_path, recursive=recursive, depth=depth,
                                    filters=filters)
    return files_query.count()

  @staticmethod
  def ValidatePaths(paths):
    if not hasattr(paths, '__iter__'):
      raise ValueError('"paths" must be an iterable.')
    for path in paths:
      utils.ValidateFilePath(path)

class OrderedFiles(Files):
  """An ordered mapping of paths to File objects."""

  def __init__(self, *args, **kwargs):
    self._ordered_paths = []
    super(OrderedFiles, self).__init__(*args, **kwargs)

  def _AddFile(self, titan_file):
    if titan_file.path not in self._titan_files:
      self._ordered_paths.append(titan_file.path)
    # pylint: disable=protected-access
    super(OrderedFiles, self)._AddFile(titan_file)

  def _RemoveFile(self, path):
    self._ordered_paths.remove(path)
    super(OrderedFiles, self)._RemoveFile(path)

  def clear(self):
    super(OrderedFiles, self).clear()
    self._ordered_paths = []

  def Sort(self):
    self._ordered_paths.sort()

  def __eq__(self, other):
    result = super(OrderedFiles, self).__eq__(other)
    # If equal, ensure order is also equal:
    if result is True and self.keys() != other.keys():
      return False
    return result

  def __iter__(self):
    for path in self._ordered_paths:
      yield path

class FileProperty(ndb.GenericProperty):
  """A convenience wrapper for creating filters for Files.List.

  Usage:
    filters = [files.FileProperty('color') == 'blue']
    files.Files.List('/', recursive=True, filters=filters)
  """

class _TitanFile(ndb.Expando):
  """Model for representing a file; don't use directly outside of this module.

  This model is intentionally free of data methods. All management of _TitanFile
  entities must go through File methods to maintain data integrity.

  Attributes:
    id: Full filename and path. Example: /path/to/some/file.html
    name: Filename without path. Example: file.html
    dir_path: Full path string. Example: /path/to/some
    paths: A list of containing directories.
        Example: ['/', '/path', '/path/to', '/path/to/some']
    depth: The depth in the directory tree, starting at 0 for root files.
    mime_type: Content type of the file.
    encoding: The content encoding. Right now, only null or 'utf-8'
        and this encoding is intentionally not exposed by higher layers.
    created: Created datetime.
    modified: Last-modified datetime.
    content: Byte string of the file's contents.
    blob: If content is null, a BlobKey pointing to the file.
    blobs: Deprecated; use "blob" instead.
    created_by: A users.TitanUser of who first created the file, or None.
    modified_by: A users.TitanUser of who last modified the file, or None.
    md5_hash: Pre-computed md5 hash of the entity's content or blob.
  """
  name = ndb.StringProperty()
  dir_path = ndb.StringProperty()
  paths = ndb.StringProperty(repeated=True)
  depth = ndb.IntegerProperty()
  mime_type = ndb.StringProperty()
  encoding = ndb.StringProperty()
  created = ndb.DateTimeProperty(auto_now_add=True)
  modified = ndb.DateTimeProperty(auto_now=True)
  content = ndb.BlobProperty()
  blob = ndb.BlobKeyProperty()
  # Deprecated; use "blob" instead.
  blobs = ndb.BlobKeyProperty(repeated=True)
  created_by = users.TitanUserProperty()
  modified_by = users.TitanUserProperty()
  md5_hash = ndb.StringProperty(indexed=False)

  BASE_PROPERTIES = frozenset((
      'name',
      'dir_path',
      'paths',
      'depth',
      'mime_type',
      'encoding',
      'created',
      'modified',
      'content',
      'blob',
      'blobs',
      'created_by',
      'modified_by',
      'md5_hash',
  ))

  @classmethod
  def _get_kind(cls):
    # For backwards-compatibility.
    return '_File'

  def __repr__(self):
    return '<_TitanFile (_File): %s>' % self.key.id()

  @property
  def path(self):
    return self.key.id()

  @property
  def meta_properties(self):
    """A dictionary containing any expando properties."""
    meta_properties = self.to_dict()
    for name in self.BASE_PROPERTIES:
      meta_properties.pop(name)
    return meta_properties

  @staticmethod
  def ValidateMetaProperties(meta):
    """Verify that meta properties are valid."""
    if not meta:
      return
    for key in meta:
      if key in _TitanFile.BASE_PROPERTIES:
        raise InvalidMetaError('Invalid name for meta property: "%s"' % key)

# ------------------------------------------------------------------------------

def _GetTitanFileEnts(paths):
  """Internal method for getting _File entities.

  Args:
    paths: An already-validated list of absolute filenames.
  Returns:
    An OrderedDict mapping paths to file entities which exist.
  """
  file_ents = ndb.get_multi([ndb.Key(_TitanFile, path) for path in paths])
  # Use an OrderedDict to preserve the alphabetical ordering from the query.
  file_objs = collections.OrderedDict()
  for f in file_ents:
    if f:
      file_objs[f.path] = f
  return file_objs

def _GetFileEntities(titan_files):
  """Get _TitanFile entities from File objects; use sparingly."""
  # This function should be the only place we access the protected _file attr
  # on File objects. This centralizes the high coupling to one location.
  is_multiple = hasattr(titan_files, '__iter__')
  if not is_multiple:
    return titan_files._file if titan_files else None
  file_ents = []
  for titan_file in titan_files:
    file_ents.append(titan_file._file if titan_file else None)
  return file_ents

def _CreateFilesQuery(dir_path, recursive=False, depth=None, filters=None):
  """Creates a ndb.Query object for listing _TitanFile entities."""
  if depth is not None and depth <= 0:
    raise ValueError('depth argument must be a positive integer.')
  if filters is not None and not hasattr(filters, '__iter__'):
    raise ValueError('"filters" must be an iterable.')
  utils.ValidateDirPath(dir_path)

  # Strip trailing slash.
  if dir_path != '/' and dir_path.endswith('/'):
    dir_path = dir_path[:-1]

  files_query = _TitanFile.query()
  if recursive:
    files_query = files_query.filter(_TitanFile.paths == dir_path)
    if depth is not None:
      dir_path_depth = 0 if dir_path == '/' else dir_path.count('/')
      depth_filter = _TitanFile.depth <= dir_path_depth + depth
      files_query = files_query.filter(depth_filter)
    files_query = files_query.filter(_TitanFile.paths == dir_path)
  else:
    files_query = files_query.filter(_TitanFile.dir_path == dir_path)

  if filters:
    files_query = files_query.filter(*filters)
  return files_query

def _ReadContentOrBlob(titan_file):
  file_ent = _GetFileEntities(titan_file)
  if not file_ent:
    raise BadFileError('File does not exist: %s' % titan_file.path)
  if file_ent.content is not None:
    content = file_ent.content
  else:
    content = files_cache.GetBlob(file_ent.path)
    if content is None:
      blob = file_ent.blob
      if not file_ent.blob:
        # Backwards-compatibility with deprecated "blobs" property:
        blob = blobstore.BlobInfo.get(file_ent.blobs[0])
      if not isinstance(blob, blobstore.BlobInfo):
        blob = blobstore.BlobInfo(blob)
      try:
        content = blob.open().read()
      except blobstore.BlobNotFoundError:
        raise blobstore.BlobNotFoundError(
            'Blob associated to path was not found: %s' % titan_file.path)
      files_cache.StoreBlob(file_ent.path, content)
  if file_ent.encoding:
    return content.decode(file_ent.encoding)
  return content
