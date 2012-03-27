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

"""A filesystem abstraction for AppEngine apps.

Documentation:
  http://code.google.com/p/titan-files/

Usage:
  See class docstring below for how to use File objects.

  files.Exists('/some/file.html')
  files.Get('/some/file.html')
  files.Write('/some/file.html', content='Hello')
  files.Delete('/some/file.html')
  files.Touch('/some/file.html')
  files.Copy('/some/file.html', '/other/file.html')
  files.ListFiles('/')
  files.ListDir('/')
  files.DirExists('/some/dir')

  Some of these methods can take async=True and return asynchronous RPC objects.
"""

try:
  # Load appengine_config here to guarantee that that hooks.LoadServices
  # can register all services for any request paths.
  import appengine_config
except ImportError:
  appengine_config = None

import collections
import cStringIO
import datetime
import logging
import mimetypes
import os
from google.appengine.api import files as blobstore_files
from google.appengine.ext import blobstore
from google.appengine.ext import db
from google.appengine.ext import deferred
from titan.common import hooks
from titan.files import files_cache

# Arbitrary cutoff for when content will be stored in blobstore.
# This value should be mirrored by titan_client.DIRECT_TO_BLOBSTORE_SIZE.
MAX_CONTENT_SIZE = 1 << 19  # 500 KiB

BLOBSTORE_APPEND_CHUNK_SIZE = 1 << 19 # 500 KiB

DEFAULT_BATCH_SIZE = 100

class BadFileError(db.BadKeyError):
  pass

class File(object):
  """A file abstraction.

  Usage:
    file_obj = File('/path/to/file.html')
    file_obj.Write('Some content')
    file_obj.Touch()
    file_obj.Delete()
    file_obj.Serialize()
  Attributes:
    name: Filename without path. Example: file.html
    path: Full filename and path. Example: /path/to/some/file.html
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
    created_by: A users.User object of who first created the file, or None.
    modified_by: A users.User object of who last modified the file, or None.
    size: The number of bytes of this file's content.
    [meta]: File objects also expose meta data as object attrs. Use the Write()
        method to update meta information.

  Note: be aware that this object performs optimizations to avoid unnecessary
      RPCs. As such, some properties (like .exists) could return stale info if
      you mix usage of File methods with module-level methods, especially
      when a File object is long-lived.
  """

  def __init__(self, path, _file_ent=None):
    """File object constructor.

    Args:
      path: An absolute filename.
      _file_ent: An internal-only optimization argument which helps avoid
          unnecessary RPCs.
    """
    self._path = ValidatePaths(path) if not _file_ent else _file_ent.path
    self._name = os.path.basename(self._path)
    self._meta = None
    self._file_ent = _file_ent
    self._exists = None

  def __eq__(self, other_file):
    return isinstance(other_file, File) and self._path == other_file._path

  def __repr__(self):
    return '<File: %s>' % self._path

  def __getattr__(self, name):
    """Attempt to pull from a File's stored meta data."""
    # Ignore builtins, like __setstate__ when being unpickled.
    if name.startswith('__') and name.endswith('__'):
      raise AttributeError("%s instance has no attribute '%s'"
                           % (self.__class__.__name__, name))
    # Load dynamic attributes if not initialized.
    if self._meta is None:
      self._meta = {}
      for key in self._file.dynamic_properties():
        self._meta[key] = getattr(self._file, key)
    try:
      return self._meta[name]
    except KeyError, e:
      raise AttributeError("%s instance has no attribute '%s'"
                           % (self.__class__.__name__, e))

  @property
  def _file(self):
    """Internal property that allows lazy-loading of the public properties."""
    if self._file_ent:
      return self._file_ent
    # Haven't initialized a File object yet.
    temp_file_obj, _ = _GetFilesOrDie(self._path)
    self._file_ent = _GetFileEntities(temp_file_obj)
    return self._file_ent

  @property
  def blob(self):
    """The BlobInfo of this File, if the file content is stored in blobstore."""
    # Backwards-compatibility with deprecated "blobs" property:
    if not self._file.blob and not self._file.blobs:
      return
    return self._file.blob or blobstore.get(self._file.blobs[0])

  @property
  def is_loaded(self):
    """Whether or not this lazy object has been evaluated."""
    return bool(self._file_ent)

  @property
  def name(self):
    return self._name

  @property
  def path(self):
    # NOTE: All internal methods should use ._path directly so that this
    # property can be overridden in subclasses without interference.
    return self._path

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
    return _ReadContentOrBlobs(self)

  @property
  def exists(self):
    if self._exists is not None:
      return self._exists
    return Exists(self._path)

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

  def read(self):
    return self.content

  def close(self):
    pass

  def Write(self, *args, **kwargs):
    self._file_ent = None
    self._exists = True
    return Write(self._path, *args, **kwargs)
  write = Write

  def Delete(self, async=False):
    self._file_ent = None
    self._exists = False
    return Delete(self._path, async=async)

  def Touch(self, async=False):
    self._file_ent = None
    self._exists = True
    return Touch(self._path, async=async)

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
        'path': self._path,
        'paths': self.paths,
        'mime_type': self.mime_type,
        'created': self.created,
        'blob': str(self.blob.key()) if self.blob else None,
        'modified': self.modified,
        'exists': self.exists,
        'created_by': str(self.created_by) if self.created_by else None,
        'modified_by': str(self.modified_by) if self.modified_by else None,
    }
    if full:
      result['content'] = self.content
    for key in self._file_ent.dynamic_properties():
      result[key] = getattr(self._file_ent, key)
    return result

class SmartFileList(object):
  """Smart list of File objects to optimize RPCs by iterative batch loading."""

  def __init__(self, file_objs, batch_size=DEFAULT_BATCH_SIZE):
    # Make sure an iterable was given.
    assert hasattr(file_objs, '__iter__')
    self._file_objs = file_objs
    self._batch_size = batch_size

  def __iter__(self):
    """Generator method which loads lazy File objects in batches."""
    for i, file_obj in enumerate(self._file_objs):
      if not i % self._batch_size:
        # On a batch boundary, load the next set.
        _LoadFiles(self._file_objs[i:i + DEFAULT_BATCH_SIZE])
      yield file_obj

  def __len__(self):
    return len(self._file_objs)

  def __getattr__(self, name):
    # Support list methods like .sort(), etc.
    return getattr(self._file_objs, name)

  def __getitem__(self, i):
    return self._file_objs[i]

  def __getslice__(self, i, j):
    return SmartFileList(self._file_objs[i:j])

  def __repr__(self):
    return repr(self._file_objs)

class _File(db.Expando):
  """Model for representing a file; don't use directly outside of this module.

  This model is intentionally free of methods. All management of _File entities
  must go through Write() and other module-level functions (or through the
  File object) in order to maintain data integrity.

  Attributes:
    key_name: Full filename and path. Example: /path/to/some/file.html
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
    created_by: A users.User object of who first created the file, or None.
    modified_by: A users.User object of who last modified the file, or None.
  """
  name = db.StringProperty()
  dir_path = db.StringProperty()
  paths = db.StringListProperty()
  depth = db.IntegerProperty()
  mime_type = db.StringProperty()
  encoding = db.StringProperty()
  created = db.DateTimeProperty(auto_now_add=True)
  modified = db.DateTimeProperty()
  content = db.BlobProperty()
  blob = blobstore.BlobReferenceProperty()
  # Deprecated; use "blob" instead.
  blobs = db.ListProperty(blobstore.BlobKey)
  created_by = db.UserProperty(auto_current_user_add=True)
  modified_by = db.UserProperty(auto_current_user=True)

  @property
  def path(self):
    return self.key().name()

  def __repr__(self):
    return '<_File: %s>' % self.path

@hooks.ProvideHook('file-exists')
def Exists(path):
  """Check if a File exists.

  Args:
    path: Absolute filename or File object.
  Returns:
    Boolean of whether or not the file exists.
  """
  file_obj, _ = _GetFiles(path)
  file_ent = _GetFileEntities(file_obj)
  return bool(file_ent)

@hooks.ProvideHook('file-get')
def Get(paths):
  """Get pre-loaded File objects.

  Args:
    paths: Absolute filename, iterable of absolute filenames, or File objects.
  Raises:
    BadFileError: If any given file paths don't exist.
  Returns:
    None: If given single path which didn't exist.
    A pre-loaded File object: If given a single path which did exist.
    Dict: When given multiple paths, returns a dict of paths --> pre-loaded File
        objects. Non-existent file paths are not included in the result.
  """
  file_objs, is_multiple = _GetFiles(paths)
  if not is_multiple:
    return file_objs if file_objs else None
  # Transform from [<File>, None, ...] to {'file path': <File>, ...}.
  file_objs_result = {}
  for i, file_obj in enumerate(file_objs):
    if file_obj:
      file_objs_result[file_obj.path] = file_obj
  return file_objs_result

@hooks.ProvideHook('file-write')
def Write(path, content=None, blob=None, mime_type=None, meta=None,
          async=False, _delete_old_blob=True):
  """Write or update a File. Supports asynchronous writes.

  Updates: if the File already exists, Write will accept any of the given args
  and only perform an update of the given data, without affecting other attrs.

  Args:
    path: An absolute filename or a File object.
    content: File contents, either as a str or unicode object.
        This will handle content greater than the 1MB limit by storing it in
        blobstore. However, this technique should only be used for relatively
        small files; it is significantly less efficient than directly uploading
        to blobstore and passing the resulting BlobKeys to the blobs argument.
    blob: If content is not provided, a BlobKey pointing to the file.
    mime_type: Content type of the file; will be guessed if not given.
    meta: A dictionary of properties to be added to the file.
    async: Whether or not to perform put() operations asynchronously.
    _delete_old_blob: Whether or not to delete the old blob if it changed.
  Raises:
    ValueError: If paths are invalid.
    TypeError: For missing arguments.
    BadFileError: If attempting to update meta information on non-existent file.
  Returns:
    File object: if async is False.
    Datastore RPC object: if async is True.
    None: if async is True and the file didn't change.
  """
  file_obj, _ = _GetFiles(path)
  file_ent = _GetFileEntities(file_obj)
  path = ValidatePaths(path)
  logging.info('Writing Titan file: %s', path)

  # Argument sanity checks.
  is_content_update = content is not None or blob is not None
  is_meta_update = mime_type is not None or meta is not None
  if not is_content_update and not is_meta_update:
    raise TypeError('Arguments expected, but none given.')
  if not file_ent and is_meta_update and not is_content_update:
    raise BadFileError('File does not exist: %s' % path)
  if content and blob:
    raise TypeError('Exactly one of "content" or "blob" must be given.')

  # If given unicode content, flag it so that Read() can decode back to unicode.
  if isinstance(content, unicode):
    encoding = 'utf-8'
    content = content.encode('utf-8')
  else:
    encoding = None

  # Determine if we should store content in blobstore. Must come after encoding.
  if content and len(content) > MAX_CONTENT_SIZE:
    logging.debug('Content size %s exceeds %s bytes, uploading to blobstore.',
                  len(content), MAX_CONTENT_SIZE)

    filename = blobstore_files.blobstore.create()
    content_file = cStringIO.StringIO(content)
    blobstore_file = blobstore_files.open(filename, 'a')
    # Blobstore writes cannot exceed the RPC size limit, so we chunk the writes.
    while True:
      content_chunk = content_file.read(BLOBSTORE_APPEND_CHUNK_SIZE)
      if not content_chunk:
        break
      blobstore_file.write(content_chunk)
    blobstore_file.close()
    blobstore_files.finalize(filename)
    blob = blobstore_files.blobstore.get_blob_key(filename)
    files_cache.StoreBlob(path, content)
    content = None

  if not file_ent:
    # Create new _File entity.
    # Guess the MIME type if not given.
    if not mime_type:
      mime_type = _GuessMimeType(path)
    # Create a new _File.
    paths = _MakePaths(path)
    file_ent = _File(
        key_name=path,
        name=os.path.basename(path),
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
    )
    # Add meta attributes.
    if meta:
      for key, value in meta.iteritems():
        setattr(file_ent, key, value)
    rpc = db.put_async(file_ent)

    # Cache the entity.
    files_cache.StoreFiles(file_ent)
    files_cache.UpdateSubdirsForFiles(file_ent)
  else:
    # Update an existing _File.
    changed = False
    if mime_type and file_ent.mime_type != mime_type:
      file_ent.mime_type = mime_type
      changed = True

    # Auto-migrate entities from old "blobs" to new "blob" property on write:
    if file_ent.blobs:
      file_ent.blob = file_ent.blobs[0]
      file_ent.blobs = []
      changed = True

    if content is not None and file_ent.content != content:
      file_ent.content = content
      if file_ent.blob and _delete_old_blob:
        # Delete the actual blobstore data.
        file_ent.blob.delete()
      # Clear the current blob association for this file.
      file_ent.blob = None
      changed = True

    if blob is not None and file_ent.blob != blob:
      if file_ent.blob and _delete_old_blob:
        # Delete the actual blobstore data.
        file_ent.blob.delete()
      # Associate the new blob to this file.
      file_ent.blob = blob
      file_ent.content = None
      changed = True

    if encoding != file_ent.encoding:
      file_ent.encoding = encoding
      changed = True

    # Update meta attributes.
    if meta is not None:
      for key, value in meta.iteritems():
        if not hasattr(file_ent, key) or getattr(file_ent, key) != value:
          setattr(file_ent, key, value)
          changed = True

    # Preserve the old modified time if nothing has changed.
    if changed:
      file_ent.modified = datetime.datetime.now()
      rpc = db.put_async(file_ent)
      # Update the cache.
      files_cache.StoreFiles(file_ent)
    else:
      return File(path, _file_ent=file_ent) if not async else None

  result = rpc
  if not async:
    rpc.get_result()
    result = File(path, _file_ent=file_ent)
  return result

@hooks.ProvideHook('file-delete')
def Delete(paths, async=False, update_subdir_caches=False,
           _delete_old_blobs=True):
  """Delete a File. Supports asynchronous and batch deletes.

  Args:
    paths: Absolute filename, iterable of absolute filenames, or File objects.
    async: Whether or not to do asynchronous deletes.
    update_subdir_caches: Whether to defer a potentially expensive task that
        will repopulate the subdir caches by calling ListDir. If False, the
        dir caches will just be cleared and subsequent ListDirs may be slower.
    _delete_old_blobs: Whether or not to delete the old blobs.
  Raises:
    BadFileError: if single file doesn't exist.
    datastore_errors.BadArgumentError: if one file in a batch delete fails.
  Returns:
    None: if async is False and delete succeeded.
    Datastore RPC object: if async is True.
  """
  # With one RPC, delete any associated blobstore files for all paths.
  file_objs, is_multiple = _GetFilesOrDie(paths)
  # Only use paths which are already validated by _GetFilesOrDie.
  paths = [f.path for f in file_objs] if is_multiple else file_objs.path
  blob_keys = []
  files_list = file_objs if is_multiple else [file_objs]
  for file_obj in files_list:
    if file_obj.blob:
      blob_keys.append(file_obj.blob.key())
  if blob_keys and _delete_old_blobs:
    blobstore.delete(blob_keys)

  # Flag these files in cache as non-existent, cleanup subdir and blob caches.
  file_ents = _GetFileEntities(file_objs)
  files_cache.SetFileDoesNotExist(paths)
  files_cache.ClearSubdirsForFiles(file_ents)
  if _delete_old_blobs:
    files_cache.ClearBlobsForFiles(file_ents)

  if update_subdir_caches:
    paths = paths if is_multiple else [paths]
    deferred.defer(ListDir, _GetCommonDir(paths))

  rpc = db.delete_async(file_ents)
  return rpc if async else rpc.get_result()

@hooks.ProvideHook('file-touch')
def Touch(paths, meta=None, async=False):
  """Create or update File objects by updating modified times.

  Supports batch and asynchronous touches.

  Args:
    paths: Absolute filename, iterable of absolute filenames, or File objects.
    meta: A dictionary of properties to be added to the file.
    async: Whether or not to do asynchronous touches.
  Returns:
    File object: if only one path is given and async is False.
    List of File objects: if multiple paths given and async is False.
    Datastore RPC object: if async is True.
  """
  now = datetime.datetime.now()
  paths = ValidatePaths(paths)
  file_objs, is_multiple = _GetFiles(paths)
  file_ents = _GetFileEntities(file_objs)
  files_list = file_ents if is_multiple else [file_ents]
  paths_list = paths if is_multiple else [paths]
  for i, file_ent in enumerate(files_list):
    if file_ent and meta is not None:
      # File exists, update meta information if given.
      for key, value in meta.iteritems():
        if not hasattr(file_ent, key) or getattr(file_ent, key) != value:
          setattr(file_ent, key, value)
    elif not file_ent:
      # File doesn't exist, touch it.
      # We disable Write() hooks by passing disabled_services=True since we
      # assume that all hook behavior/manipulation is done around Touch()
      # itself. We trust that the arguments passed here to Touch() have already
      # been through any necessary modifications and the service hooks shouldn't
      # be called again.
      file_obj = Write(paths_list[i], content='', meta=meta,
                       disabled_services=True)
      file_ent = _GetFileEntities(file_obj)
      # Inject new _File entity back into the object that will be put().
      if is_multiple:
        file_ents[i] = file_ent
      else:
        file_ents = file_ent
    file_ent.modified = now

  # Start the put, then update the file cache and subdir caches.
  rpc = db.put_async(file_ents)
  files_cache.StoreFiles(file_ents)
  files_cache.UpdateSubdirsForFiles(file_ents)

  result = rpc
  if not async:
    rpc.get_result()
    if is_multiple:
      result = [File(f.path, _file_ent=f) for f in file_ents]
    else:
      result = File(file_ents.path, _file_ent=file_ents)
  return result

@hooks.ProvideHook('file-copy')
def Copy(source_path, destination_path, async=False):
  """Copy a File and all of its properties to a different path.

  Args:
    source_path: An absolute filename or File object.
    destination_path: An absolute filename or File object indicating where
        the file will be copied.
    async: Whether or not to perform put() operations asynchronously.
  Raises:
    BadFileError: If the source_path doesn't exist.
  Returns:
    The result of the Write() call to the destination path.
  """
  # First _GetFiles (in order to accept pre-loaded File objects and avoid RPCs),
  # then ValidatePaths to overwrite the paths.
  file_objs, _ = _GetFiles([source_path, destination_path])
  source_path, destination_path = ValidatePaths([source_path, destination_path])

  source_file_obj, destination_file_obj = file_objs
  source_file_ent, destination_file_ent = _GetFileEntities(file_objs)
  if not source_file_ent:
    raise BadFileError('Copy failed, source file does not exist: %s'
                       % source_path)
  logging.info('Copying Titan file: %s --> %s', source_path, destination_path)

  # Delete the file if it currently exists so old properties don't persist.
  try:
    delete_rpc = Delete(destination_path, async=True, disabled_services=True)
  except BadFileError:
    delete_rpc = None

  # Copy all source file properties.
  content = source_file_ent.content
  # Use file_obj here, to correct handle old "blobs" property.
  blob = source_file_obj.blob
  mime_type = source_file_ent.mime_type
  meta = {}
  for key in source_file_ent.dynamic_properties():
    meta[key] = getattr(source_file_ent, key)

  if delete_rpc:
    delete_rpc.wait()

  # Write the file.
  result = Write(destination_path, content=content, blob=blob,
                 mime_type=mime_type, meta=meta, async=async,
                 disabled_services=True)
  return result

@hooks.ProvideHook('copy-dir')
def CopyDir(source_dir_path, destination_dir_path, dry_run=False):
  """Copy a directory's contents recursively to another directory path.

  Destination files which exist will be overwritten.

  Args:
    source_dir_path: An absolute directory path.
    destination_dir_path: An absolute directory path.
    dry_run: Whether or not to actually perform the copy.
  Returns:
    Default: a list of the new File objects which were created.
    If dry_run is True, a list of new paths.
  """
  # Strip trailing slashes.
  if source_dir_path != '/' and source_dir_path.endswith('/'):
    source_dir_path = source_dir_path[:-1]
  if destination_dir_path != '/' and destination_dir_path.endswith('/'):
    destination_dir_path = destination_dir_path[:-1]

  source_file_objs = ListFiles(
      source_dir_path, recursive=True, disabled_services=True)
  # If not a dry-run, wrap in a SmartFileList to optimize batch fetching
  # of source file objects.
  if not dry_run:
    source_file_objs = SmartFileList(source_file_objs)

  new_paths = []
  async_results = []
  for source_file_obj in source_file_objs:
    new_path = source_file_obj.path.replace(source_dir_path,
                                            destination_dir_path, 1)
    if not dry_run:
      rpc = Copy(source_file_obj, new_path, async=True, disabled_services=True)
      async_results.append(rpc)
    new_paths.append(new_path)

  if dry_run:
    return new_paths
  file_keys = [rpc.get_result() for rpc in async_results]
  return [File(key.name()) for key in file_keys]

@hooks.ProvideHook('list-files')
def ListFiles(dir_path, recursive=False, depth=None, filters=None):
  """Get list of File objects in the given directory path.

  Args:
    dir_path: Absolute directory path.
    recursive: Whether to list files recursively.
    depth: If recursive, a positive integer to limit the recursion depth. 1 is
        one folder deep, 2 is two folders deep, etc.
    filters: A two-tuple or list of two-tuples. The first element of each
        tuple is a datastore filter expression, the second is the value.
        This is used to filter on meta properties or other File properties.
        Example: ('color =', 'blue')
        Example: [('type =', 'foo'), ('color =', 'blue')]
  Raises:
    ValueError: If given an invalid depth argument.
  Returns:
    A list of File objects.
  """
  if depth is not None and depth <= 0:
    raise ValueError('depth argument must be a positive integer.')
  dir_path = ValidatePaths(dir_path)

  # Strip trailing slash.
  if dir_path != '/' and dir_path.endswith('/'):
    dir_path = dir_path[:-1]

  file_keys = _File.all(keys_only=True)
  if recursive:
    file_keys.filter('paths =', dir_path)
    if depth is not None:
      dir_path_depth = 0 if dir_path == '/' else dir_path.count('/')
      file_keys.filter('depth <=', dir_path_depth + depth)
    file_keys.filter('paths =', dir_path)
  else:
    file_keys.filter('dir_path =', dir_path)

  if filters:
    filters_list = filters if hasattr(filters[0], '__iter__') else [filters]
    for expression, value in filters_list:
      file_keys.filter(expression, value)

  return [File(key.name()) for key in file_keys]

@hooks.ProvideHook('list-dir')
def ListDir(dir_path):
  """List a directory's contents.

  This is a potentially expensive operation. Titan Files does not represent
  directories by design, so on cache miss this method must query and
  manipulate the filename strings of all entities in dir_path's subtree.

  Args:
    dir_path: Absolute directory path.
  Returns:
    A two-tuple of (dirs, file_objs) with directory names and File objects.
  """
  dir_path = ValidatePaths(dir_path)
  # Strip trailing slash.
  if dir_path != '/' and dir_path.endswith('/'):
    dir_path = dir_path[:-1]
  is_root_dir = dir_path == '/'

  # Return immediately if subdir list is cached.
  dirs = files_cache.GetSubdirs(dir_path)
  if dirs is not None:
    return list(dirs), ListFiles(dir_path, disabled_services=True)

  # Since Titan doesn't represent directories, recursively find files inside
  # of dir_path and see if any returned files have a longer path. To avoid
  # datastore fetches, we simply count path slashes, pull out dir strings,
  # and avoid any properties which will cause the lazy File object to evaluate.
  file_objs = ListFiles(dir_path, recursive=True, disabled_services=True)
  dir_level = 0 if is_root_dir else dir_path.count('/')
  all_subdirs = collections.defaultdict(set)
  first_level_files = []
  for file_obj in file_objs:
    file_level = file_obj.path.count('/') - 1
    if file_level == dir_level:
      # File is at the root listing level.
      first_level_files.append(file_obj)
    elif file_level > dir_level:
      # File is at a deeper level, meaning that at least one subdir exists.
      subdirs = file_obj.path.split('/')[dir_level + 1:-1]

      # Since we have gone through the expense of walking the whole tree
      # rooted at dir_path, update all subdir caches from dir_path down.
      for i, subdir in enumerate(subdirs):
        temp_dir_path = '' if is_root_dir else dir_path
        if i:
          # Make "<dir_path>/first_subdir/second_subdir" key for current depth.
          temp_dir_path = '%s/%s' % (temp_dir_path, '/'.join(subdirs[:i]))
        all_subdirs[temp_dir_path or '/'].add(subdir)

  # Cache all the directories found in the dir_path subtree.
  files_cache.StoreSubdirs(all_subdirs)

  return list(all_subdirs.get(dir_path, [])), first_level_files

@hooks.ProvideHook('dir-exists')
def DirExists(dir_path):
  """Returns True if any files exist within the given directory path."""
  # TODO(user): optimize this to pull from cache.
  dir_path = ValidatePaths(dir_path)
  # Strip trailing slash.
  if dir_path != '/' and dir_path.endswith('/'):
    dir_path = dir_path[:-1]
  file_keys = _File.all(keys_only=True)
  file_keys.filter('paths =', dir_path)
  return bool(file_keys.fetch(1))

def ValidatePaths(paths):
  """Validate that a given path or list of paths is valid.

  Args:
    paths: A string or File object, or a list containing these types.
  Raises:
    ValueError: If any path is invalid.
  Returns:
    A string path or a list of string paths.
  """
  is_multiple = hasattr(paths, '__iter__')
  if not is_multiple:
    paths = [paths]
  else:
    if not paths:
      return []
    # Make a copy of the paths list since we manipulate it in-place below.
    paths = list(paths[:])

  for i, path in enumerate(paths):
    # Support _File and File objects by pulling out the path.
    if isinstance(path, File) or isinstance(path, _File):
      path = paths[i] = path.path
    if not path:
      raise ValueError('Path is invalid: "%s"' % path)
    if not path.startswith('/'):
      raise ValueError('Path must have a leading /: %s' % path)
    if '//' in path:
      raise ValueError('Double-slashes (//) are not allowed in path: %s' % path)
    if '..' in path:
      raise ValueError('Double-dots (..) are not allowed in path: %s' % path)

  return paths if is_multiple else paths[0]

# ------------------------------------------------------------------------------

def _GetFiles(paths_or_file_objs):
  """Get a non-lazy File object or a list of non-lazy File objects, or None."""
  is_multiple = hasattr(paths_or_file_objs, '__iter__')

  # Slightly magical optimizations: if given all loaded file objects (usually by
  # a service plugin), return them immediately and avoid any more RPCs.
  paths_list = paths_or_file_objs if is_multiple else [paths_or_file_objs]
  is_all_file_objs = True
  for maybe_file_obj in paths_list:
    is_loaded_file_obj = getattr(maybe_file_obj, 'is_loaded', False)
    if not is_loaded_file_obj:
      # At least one of the given objects was not a loaded File object,
      # break and follow the non-optimized path.
      is_all_file_objs = False
      break
  if is_all_file_objs:
    return paths_or_file_objs, is_multiple

  # Get the file entities from cache or from the datastore.
  paths = ValidatePaths(paths_or_file_objs)
  file_ents, cache_hit = files_cache.GetFiles(paths)
  if not cache_hit:
    file_ents = _File.get_by_key_name(paths)
    if is_multiple:
      # Make the data dictionary of {path: <_File entity or None>, ...}
      data = dict([(path, file_ents[i]) for i, path in enumerate(paths)])
    else:
      data = {paths: file_ents}
    files_cache.StoreAll(data)

  # Wrap all the _File entities in <File> objects.
  file_objs = []
  for f in file_ents if is_multiple else [file_ents]:
    file_objs.append(File(f.path, _file_ent=f) if f else None)
  return file_objs if is_multiple else file_objs[0], is_multiple

def _GetFileEntities(file_objs):
  """Get _File entities from File objects; use sparingly."""
  # This function should be the only place we access the protected _file attr
  # on File objects. This centralizes the high coupling to one location.
  is_multiple = hasattr(file_objs, '__iter__')
  if not is_multiple:
    return file_objs._file if file_objs else None
  file_ents = []
  for file_obj in file_objs:
    file_ents.append(file_obj._file if file_obj else None)
  return file_ents

def _LoadFiles(file_objs):
  """Given File objects, load all of them in-place and with batch RPCs."""
  is_multiple = hasattr(file_objs, '__iter__')
  loaded_file_objs = Get([f.path for f in file_objs if not f.is_loaded])
  file_objs = file_objs if is_multiple else [file_objs]
  for file_obj in file_objs:
    if file_obj.path in loaded_file_objs:
      file_obj._file_ent = loaded_file_objs[file_obj.path]._file_ent

def _GetFilesOrDie(paths_or_file_objs):
  """Same as _GetFiles, but raises BadFileError if a path doesn't exist."""
  file_objs, is_multiple = _GetFiles(paths_or_file_objs)
  if not is_multiple:
    # Single path argument.
    if file_objs is None:
      raise BadFileError('File does not exist: %s' % paths_or_file_objs)
  else:
    # Multiple paths.
    for i, file_obj in enumerate(file_objs):
      if file_obj is None:
        raise BadFileError('File does not exist: %s' % paths_or_file_objs[i])
  return file_objs, is_multiple

def _ReadContentOrBlobs(file_obj):
  file_ent = _GetFileEntities(file_obj)
  if file_ent.content is not None:
    content = file_ent.content
  else:
    content = files_cache.GetBlob(file_ent.path)
    if content is None:
      blob = file_ent.blob
      if not file_ent.blob:
        # Backwards-compatibility with deprecated "blobs" property:
        blob = blobstore.BlobInfo.get(file_ent.blobs[0])
      content = blob.open().read()
      files_cache.StoreBlob(file_ent.path, content)
  if file_ent.encoding == 'utf-8':
    return content.decode('utf-8')
  return content

def _MakePaths(path):
  """Make a list of all containing dirs given a full filename including path."""
  # '/path/to/some/file' --> ['/', '/path', '/path/to', '/path/to/some']
  if path == '/':
    return []
  path = os.path.split(path)[0]
  return _MakePaths(path) + [path]

def _GuessMimeType(path):
  return mimetypes.guess_type(path)[0] or 'application/octet-stream'

def _GetCommonDir(paths):
  """Given an iterable of file paths, return the top common prefix."""
  common_dir = os.path.commonprefix(paths)
  if common_dir != '/' and common_dir.endswith('/'):
    return common_dir[:-1]
  # If common_dir doesn't end in a slash, we need to pop the last directory off.
  return os.path.split(common_dir)[0]
