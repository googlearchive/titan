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
  files.Read('/some/file.html')
  files.Write('/some/file.html', content='Hello')
  files.Delete('/some/file.html')
  files.Touch('/some/file.html')
  files.ListFiles('/')
  files.ListDir('/')
  files.DirExists('/some/dir')

  Some of these methods can take async=True and return asynchronous RPC objects.
"""

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

# Load registered service hooks.
hooks.LoadServices()

# Arbitrary cutoff for when content will be stored in blobstore.
MAX_CONTENT_SIZE = 1 << 19  # 500 KiB

BLOBSTORE_APPEND_CHUNK_SIZE = 1 << 19 # 500 KiB

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
    blobs: If the content is stored in blobstore, this is the list of BlobKeys
        comprising it. If only static serving is needed, a client could simply
        check for 'blobs' and use webapp's send_blob() to avoid loading the
        content blob into app memory.
    exists: Boolean of if the file exists.
    [meta]: File objects also expose meta data as object attrs. Use the Write()
        method to update meta information.

  Note: be aware that this object performs optimizations to avoid unnecessary
      RPCs. As such, some properties (like .exists) could return stale info if
      you mix usage of File methods with module-level methods, especially
      when a File object is long-lived.
  """

  def __init__(self, path, file_ent=None):
    self._path = ValidatePaths(path)
    self._name = os.path.split(self._path)[1]
    self._meta = None
    self._file_ent = file_ent
    self._exists = None

  def __eq__(self, other):
    return isinstance(other, File) and self.path == other.path

  def __repr__(self):
    return '<File: %s>' % self.path

  def __getattr__(self, name):
    """Attempt to pull from a File's stored meta data."""
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
    self._file_ent, _ = _GetFilesOrDie(self.path)
    return self._file_ent

  @property
  def name(self):
    return self._name

  @property
  def path(self):
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
    return _ReadContentOrBlobs(self.path, file_ent=self._file)

  @property
  def blobs(self):
    return self._file.blobs

  @property
  def exists(self):
    if self._exists is not None:
      return self._exists
    return Exists(self.path)

  def read(self):
    return self.content

  def close(self):
    pass

  def Write(self, *args, **kwargs):
    self._file_ent = None
    self._exists = True
    return Write(self.path, *args, **kwargs)
  write = Write

  def Delete(self, async=False):
    self._file_ent = None
    self._exists = False
    return Delete(self.path, async=async)

  def Touch(self, async=False):
    self._file_ent = None
    self._exists = True
    return Touch(self.path, async=async)

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
        'paths': self.paths,
        'mime_type': self.mime_type,
        'created': self.created,
        'blobs': [str(blob_key) for blob_key in self.blobs],
        'modified': self.modified,
        'exists': self.exists,
    }
    if full:
      result['content'] = self.content
    for key in self._file_ent.dynamic_properties():
      result[key] = getattr(self._file_ent, key)
    return result

class _File(db.Expando):
  """Model for representing a file; don't use directly outside of this module.

  This model is intentionally free of methods. All management of _File entities
  must go through Write() and other module-level functions (or through the
  File object) in order to maintain data integrity.

  Attributes:
    key_name: Full filename and path. Example: /path/to/some/file.html
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
    blobs: If content is null, a list of BlobKeys comprising the file.
        Right now this is only ever one key, allowing up to the 2GB limit.
  """
  dir_path = db.StringProperty()
  paths = db.StringListProperty()
  depth = db.IntegerProperty()
  mime_type = db.StringProperty()
  encoding = db.StringProperty()
  created = db.DateTimeProperty(auto_now_add=True)
  modified = db.DateTimeProperty()
  content = db.BlobProperty()
  blobs = db.ListProperty(blobstore.BlobKey)

  @property
  def path(self):
    return self.key().name()

  def __repr__(self):
    return '<_File: %s>' % self.path

@hooks.ProvideHook('file-exists')
def Exists(path, file_ent=None):
  """Check if a File exists."""
  ValidatePaths(path)
  return bool(_GetFiles(file_ent or path)[0])

@hooks.ProvideHook('file-get')
def Get(paths, file_ents=None):
  """Get a File, or batch get a list of File objects.

  Args:
    paths: Absolute filename or iterable of absolute filenames.
    file_ents: File entities, usually provided by an internal service plugin to
        avoid duplicate RPCs for the same file entities.
  Raises:
    BadFileError: If any given file paths don't exist.
  Returns:
    Non-lazy File object or tuple of File objects.
  """
  file_ents, is_multiple = _GetFilesOrDie(file_ents or paths)
  if not is_multiple:
    return File(paths, file_ent=file_ents)
  return tuple([File(f.key().name(), file_ent=f) for f in file_ents])

@hooks.ProvideHook('file-read')
def Read(path, file_ent=None):
  """Get the contents of a File."""
  return _ReadContentOrBlobs(path, file_ent=file_ent)

@hooks.ProvideHook('file-write')
def Write(path, content=None, blobs=None, mime_type=None, meta=None,
          async=False, file_ent=None):
  """Write or update a File. Supports asynchronous writes.

  Updates: if the File already exists, Write will accept any of the given args
  and only perform an update of the given data, without affecting other attrs.

  Args:
    path: An absolute filename.
    content: File contents, either as a str or unicode object.
        This will handle content greater than the 1MB limit by storing it in
        blobstore. However, this technique should only be used for relatively
        small files; it is significantly less efficient than directly uploading
        to blobstore and passing the resulting BlobKeys to the blobs argument.
    blobs: If content is not provided, takes BlobKeys comprising the file.
    mime_type: Content type of the file; will be guessed if not given.
    meta: A dictionary of attributes to be added to the File Expando object.
    async: Whether or not to perform put() operations asynchronously.
    file_ent: A file entity, usually provided by an internal service plugin to
        avoid a duplicate RPC for the same file entity.
  Raises:
    ValueError: If paths are invalid.
    TypeError: For missing arguments.
    BadFileError: If attempting to update meta information on non-existent file.
  Returns:
    db.Key object: if async is False.
    Datastore RPC object: if async is True.
    None: if async is True and the file didn't change.
  """
  path = ValidatePaths(path)
  file_ent, _ = _GetFiles(file_ent or path)
  logging.info('Writing Titan file: %s', path)

  # Argument sanity checks.
  is_content_update = content is not None or blobs is not None
  is_meta_update = mime_type is not None or meta is not None
  if not is_content_update and not is_meta_update:
    raise TypeError('Arguments expected, but none given.')
  if not file_ent and is_meta_update and not is_content_update:
    raise BadFileError('File does not exist: %s' % path)
  if content and blobs:
    raise TypeError('Exactly one of "content" or "blobs" must be given.')

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
    blobstore_path = '/blobstore/' + path
    filename = blobstore_files.blobstore.create(blobstore_path)
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
    blob_key = blobstore_files.blobstore.get_blob_key(filename)
    blobs = [blob_key]
    files_cache.StoreBlob(path, content)
    content = None

  if not file_ent:
    # Create new _File entity.
    if not blobs:
      blobs = []
    # Guess the MIME type if not given.
    if not mime_type:
      mime_type = _GuessMimeType(path)
    # Create a new _File.
    paths = _MakePaths(path)
    file_ent = _File(
        key_name=path,
        dir_path=paths[-1],
        paths=paths,
        # Root files are at depth 0.
        depth=len(paths) - 1,
        mime_type=mime_type,
        encoding=encoding,
        modified=datetime.datetime.now(),
        content=content,
        blobs=blobs)
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

    if content is not None and file_ent.content != content:
      file_ent.content = content
      if file_ent.blobs:
        # Clear all current blobstore blobs for this file.
        blobstore.delete(file_ent.blobs)
      file_ent.blobs = []
      changed = True

    if blobs is not None and file_ent.blobs != blobs:
      if file_ent.blobs:
        # Clear all current blobstore blobs for this file.
        blobstore.delete(file_ent.blobs)
      file_ent.blobs = blobs
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
      return file_ent.key() if not async else None
  return rpc if async else rpc.get_result()

@hooks.ProvideHook('file-delete')
def Delete(paths, async=False, file_ents=None, update_subdir_caches=False):
  """Delete a File. Supports asynchronous and batch deletes.

  Args:
    paths: Absolute filename or iterable of absolute filenames.
    async: Whether or not to do asynchronous deletes.
    file_ents: File entities, usually provided by an internal service plugin to
        avoid duplicate RPC for the same file entities.
    update_subdir_caches: Whether to defer a potentially expensive task that
        will repopulate the subdir caches by calling ListDir. If False, the
        dir caches will just be cleared and subsequent ListDirs may be slower.
  Raises:
    BadFileError: if single file doesn't exist.
    datastore_errors.BadArgumentError: if one file in a batch delete fails.
  Returns:
    None: if async is False and delete succeeded.
    Datastore RPC object: if async is True.
  """
  # With one RPC, delete any associated blobstore files for all paths.
  file_ents, is_multiple = _GetFilesOrDie(file_ents or paths)
  blob_keys = []
  files_list = file_ents if is_multiple else [file_ents]
  for file_ent in files_list:
    if file_ent.blobs:
      blob_keys += file_ent.blobs
  if blob_keys:
    blobstore.delete(blob_keys)

  # Flag these files in cache as non-existent, cleanup subdir and blob caches.
  files_cache.SetFileDoesNotExist(paths)
  files_cache.ClearSubdirsForFiles(file_ents)
  files_cache.ClearBlobsForFiles(file_ents)

  if update_subdir_caches:
    path_strings = ValidatePaths(paths)
    path_strings = path_strings if is_multiple else [path_strings]
    deferred.defer(ListDir, _GetCommonDir(path_strings))

  rpc = db.delete_async(file_ents)
  return rpc if async else rpc.get_result()

@hooks.ProvideHook('file-touch')
def Touch(paths, async=False, file_ents=None):
  """Create or update File objects by updating modified times.

  Supports batch and asynchronous touches.

  Args:
    paths: Absolute filename or iterable of absolute filenames.
    async: Whether or not to do asynchronous touches.
    file_ents: File entities, usually provided by an internal service plugin to
        avoid duplicate RPCs for the same file entities.
  Returns:
    db.Key: if only one path is given and async is False.
    List of db.Key objects: if multiple paths given and async is False.
    Datastore RPC object: if async is True.
  """
  now = datetime.datetime.now()
  paths = ValidatePaths(paths)
  file_ents, is_multiple = _GetFiles(file_ents or paths)
  files_list = file_ents if is_multiple else [file_ents]
  paths_list = paths if is_multiple else [paths]
  for i, file_ent in enumerate(files_list):
    if not file_ent:
      # File doesn't exist, touch it.
      file_ent = _File.get(Write(paths_list[i], content=''))
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

  return rpc if async else rpc.get_result()

@hooks.ProvideHook('list-files')
def ListFiles(dir_path, recursive=False, depth=None):
  """Get list of File objects in the given directory path.

  Args:
    dir_path: Absolute directory path.
    recursive: Whether to list files recursively.
    depth: If recursive, a positive integer to limit the recursion depth. 1 is
        one folder deep, 2 is two folders deep, etc.
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
    return list(dirs), ListFiles(dir_path)

  # Since Titan doesn't represent directories, recursively find files inside
  # of dir_path and see if any returned files have a longer path. To avoid
  # datastore fetches, we simply count path slashes, pull out dir strings,
  # and avoid any properties which will cause the lazy File object to evaluate.
  file_objs = ListFiles(dir_path, recursive=True)
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
  elif not paths:
    return []

  for i, path in enumerate(paths[:]):
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

def _GetFiles(paths):
  """Get a _File entity or a list of _File entities."""
  ValidatePaths(paths)
  is_multiple = hasattr(paths, '__iter__')

  # Slightly magical: if already given file entities (usually by a service
  # plugin), return them immediately and avoid any more RPCs.
  paths_list = paths if is_multiple else [paths]
  all_file_entities = True
  for possible_file_ent in paths_list:
    if not isinstance(possible_file_ent, _File):
      all_file_entities = False
      break
  if all_file_entities:
    return paths, is_multiple

  # Get the file entities from cache or from the datastore.
  file_ents, cache_hit = files_cache.GetFiles(paths)
  if not cache_hit:
    file_ents = _File.get_by_key_name(paths)
    if is_multiple:
      # Make the data dictionary of {path: <_File entity or None>, ...}
      data = dict([(path, file_ents[i]) for i, path in enumerate(paths)])
    else:
      data = {paths: file_ents}
    files_cache.StoreAll(data)
  return file_ents, is_multiple

def _GetFilesOrDie(paths):
  """Get a _File entity or a list of _File entities if all paths exist."""
  file_ents, is_multiple = _GetFiles(paths)
  if not is_multiple:
    # Single path argument.
    if not file_ents:
      raise BadFileError('File does not exist: %s' % paths)
  else:
    # Multiple paths.
    for i, file_ent in enumerate(file_ents):
      if not file_ent:
        raise BadFileError('File does not exist: %s' % paths[i])
  return file_ents, is_multiple

def _ReadContentOrBlobs(path, file_ent=None):
  if not file_ent:
    file_ent, _ = _GetFilesOrDie(path)
  if file_ent.content is not None:
    content = file_ent.content
  else:
    content = files_cache.GetBlob(path)
    if content is None:
      content = blobstore.BlobReader(file_ent.blobs[0]).read()
      files_cache.StoreBlob(path, content)
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
