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

"""Tests for files.py."""

from tests import testing

import copy
import datetime
from google.appengine.api import files as blobstore_files
from google.appengine.api import memcache
from google.appengine.ext import blobstore
from titan.common.lib.google.apputils import app
from titan.common.lib.google.apputils import basetest
from titan.common import sharded_cache
from titan.files import files_cache
from titan.files import files

# Content larger than the arbitrary max content size and the 1MB RPC limit.
LARGE_FILE_CONTENT = 'a' * (1 << 21)  # 2 MiB

class FileTestCase(testing.BaseTestCase):

  def setUp(self):
    super(FileTestCase, self).setUp()

    # Create a blob and blob_reader for testing.
    filename = blobstore_files.blobstore.create(
        mime_type='application/octet-stream')
    with blobstore_files.open(filename, 'a') as fp:
      fp.write('Blobstore!')
    blobstore_files.finalize(filename)

    self.blob_key = blobstore_files.blobstore.get_blob_key(filename)
    self.blob_reader = blobstore.BlobReader(self.blob_key)

  @testing.DisableCaching
  def testExists(self):
    self.assertFalse(files.Exists('/foo/bar.html'))
    files.Write('/foo/bar.html', content='Test')
    self.assertTrue(files.Exists('/foo/bar.html'))
    self.assertFalse(files.Exists('/fake'))
    self.assertRaises(ValueError, files.Exists, '')

  @testing.DisableCaching
  def testFileObject(self):
    meta = {'color': 'blue', 'flag': False}
    key = files.Write('/foo/bar.html', content='Test', meta=meta)
    file_ent = files._File.get(key)

    # Init with path only, verify lazy-loading properties.
    file_obj = files.File('/foo/bar.html')
    self.assertEqual(None, file_obj._file_ent)
    _ = file_obj.mime_type
    self.assertNotEqual(None, file_obj._file_ent)

    # Init with a _File entity.
    file_obj = files.File('/foo/bar.html', file_ent=file_ent)
    self.assertEqual('/foo/bar.html', file_obj.path)
    self.assertEqual('bar.html', file_obj.name)
    self.assertNotEqual(None, file_obj._file_ent)

    # Write() and write().
    self.assertEqual(file_obj.content, 'Test')
    file_obj.Write('New content')
    self.assertEqual(file_obj.content, 'New content')
    rpc = file_obj.write('', async=True)
    rpc.get_result()
    self.assertEqual(file_obj.content, '')

    # Delete() and exists property.
    self.assertTrue(file_obj.exists)
    file_obj.Delete()
    self.assertFalse(file_obj.exists)
    self.assertEqual(None, file_obj._file_ent)
    key = files.Write('/foo/bar.html', content='Test', meta=meta)
    rpc = file_obj.Delete(async=True)
    self.assertEqual(None, rpc.get_result())

    # The exists property should be memoized, so that it only makes one RPC.
    file_obj = files.File('/foo/bar/baz')
    # Verify that no more RPCs are made unless the file changes.
    file_obj.Touch()
    self.stubs.Set(files, 'Exists', lambda x: self.fail('Lost memoization!'))
    self.assertTrue(file_obj.exists)
    file_obj.Delete()
    self.assertFalse(file_obj.exists)
    file_obj.Write('test')
    self.assertTrue(file_obj.exists)
    self.stubs.UnsetAll()

    # Touch().
    file_obj = files.File('/foo/bar/baz')
    file_obj.Touch()
    old_modified = file_obj.modified
    rpc = file_obj.Touch(async=True)
    self.assertEqual(u'/foo/bar/baz', rpc.get_result().name())
    self.assertNotEqual(old_modified, file_obj.modified)

    # Properties: paths, mime_type, created, modified, and blobs.
    file_obj = files.File('/foo/bar/baz.html')
    file_obj.Touch()
    self.assertEqual(file_obj.paths, ['/', '/foo', '/foo/bar'])
    self.assertEqual(file_obj.mime_type, 'text/html')
    self.assertTrue(isinstance(file_obj.created, datetime.datetime))
    self.assertTrue(isinstance(file_obj.modified, datetime.datetime))
    self.assertEqual(file_obj.blobs, [])

    # read() and content property.
    self.assertEqual(file_obj.content, file_obj.read())

    # close().
    self.assertEqual(None, file_obj.close())

    # Error handling: init with non-existent path.
    file_obj = files.File('/foo/fake.html')
    self.assertRaises(files.BadFileError, lambda: file_obj.paths)
    self.assertRaises(files.BadFileError, lambda: file_obj.content)
    self.assertRaises(files.BadFileError, file_obj.Delete)
    self.assertRaises(files.BadFileError, file_obj.Serialize)

  @testing.DisableCaching
  def testGet(self):
    meta = {'color': 'blue', 'flag': False}
    key = files.Write('/foo/bar.html', content='Test', meta=meta)
    actual_file = files._File.get(key)

    expected = {
        'name': 'bar.html',
        'path': '/foo/bar.html',
        'paths': [u'/', u'/foo'],
        'mime_type': u'text/html',
        'created': actual_file.created,
        'modified': actual_file.modified,
        'blobs': [],
        'exists': True,
        # meta attributes:
        'color': u'blue',
        'flag': False,
    }
    self.assertDictEqual(expected, files.Get('/foo/bar.html').Serialize())
    expected['content'] = 'Test'
    self.assertDictEqual(expected,
                         files.Get('/foo/bar.html').Serialize(full=True))

    # Batch get.
    # Get() should always return non-lazy, pre-populated File objects to avoid
    # multiple fetches, and should error if any file does not exist.
    files.Touch(['/foo/bar.html', '/foo/bar/baz', '/qux'])
    file_objs = files.Get(['/foo/bar.html', '/foo/bar/baz', '/qux'])
    self.assertEqual(3, len(file_objs))
    self.assertTrue(all([f.exists for f in file_objs]))
    self.assertEqual((), files.Get([]))

    # Error handling.
    self.assertRaises(files.BadFileError, files.Get, '/fake.html')
    self.assertRaises(files.BadFileError, files.Get, ['/foo/bar.html', '/fake'])

  @testing.DisableCaching
  def testRead(self):
    # Reading File contents from datastore.
    files.Write('/foo/bar.html', content='Test')
    self.assertEqual('Test', files.Read('/foo/bar.html'))
    files.Write('/foo/bar.html', content='')
    self.assertEqual('', files.Read('/foo/bar.html'))

    # Reading File contents from blobstore.
    # Also, writing with blobs should nullify content.
    files.Write('/foo/bar.html', blobs=[self.blob_key])
    self.assertEqual(self.blob_reader.read(), files.Read('/foo/bar.html'))

    # Returned content should maintain its encoding.
    # Byte string:
    files.Write('/foo/bar.html', content='Test')
    content = files.Read('/foo/bar.html')
    self.assertTrue(isinstance(content, str))
    self.assertEqual('Test', files.Read('/foo/bar.html'))
    # Unicode string:
    files.Write('/foo/bar.html', content=u'Test')
    content = files.Read('/foo/bar.html')
    self.assertTrue(isinstance(content, unicode))
    self.assertEqual(u'Test', files.Read('/foo/bar.html'))

    # Error handling.
    self.assertRaises(files.BadFileError, files.Read, '/fake.html')

  @testing.DisableCaching
  def testWrite(self):
    expected_file = files._File(
        key_name='/foo/bar.html',
        content='Test',
        dir_path='/foo',
        paths=[u'/', u'/foo'],
        depth=1,
        mime_type=u'text/html',
        # Arbitrary meta data for expando:
        color=u'blue',
        flag=False,
    )
    original_expected_file = copy.deepcopy(expected_file)
    meta = {'color': 'blue', 'flag': False}
    new_meta = {'color': 'blue', 'flag': True}
    dates = ['modified', 'created']

    # Synchronous write of a new file.
    key = files.Write('/foo/bar.html', content='Test', meta=meta)
    actual_file = files._File.get(key)
    self.assertEntityEqual(expected_file, actual_file, ignore=dates)
    self.assertNotEqual(None, actual_file.modified, 'modified is not being set')

    # Synchronous update without changes.
    old_modified = actual_file.modified
    key = files.Write('/foo/bar.html', meta=meta)
    actual_file = files._File.get(key)
    self.assertEntityEqual(expected_file, actual_file, ignore=dates)
    self.assertEqual(old_modified, actual_file.modified)

    # Synchronous update with changes.
    old_modified = actual_file.modified
    key = files.Write('/foo/bar.html', content='New content', meta=new_meta,
                      mime_type='fake/type')
    expected_file.content = 'New content'
    expected_file.flag = True
    expected_file.mime_type = 'fake/type'
    actual_file = files._File.get(key)
    self.assertEntityEqual(expected_file, actual_file, ignore=dates)
    self.assertNotEqual(old_modified, actual_file.modified)

    # Allow writing blank files.
    self.assertTrue(files.Write('/foo/bar.html', content=''))
    actual_file = files._File.get(key)
    self.assertEqual(actual_file.content, '')

    # Allow overwriting mime_type and meta without touching content.
    files.Write('/foo/bar.html', content='Test')
    files.Write('/foo/bar.html', mime_type='fake/mimetype')
    actual_file = files._File.get_by_key_name('/foo/bar.html')
    self.assertEqual('fake/mimetype', actual_file.mime_type)
    self.assertEqual('Test', actual_file.content)

    files.Write('/foo/bar.html', meta=new_meta)
    actual_file = files._File.get_by_key_name('/foo/bar.html')
    self.assertEqual(True, actual_file.flag)
    self.assertEqual('Test', actual_file.content)

    # Cleanup.
    expected_file = original_expected_file
    files.Delete('/foo/bar.html')

    # Asynchronous write of a new file.
    rpc = files.Write('/foo/bar.html', content='Test', meta=meta, async=True)
    actual_file = files._File.get(rpc.get_result())
    self.assertEntityEqual(expected_file, actual_file, ignore=dates)

    # Asynchronous update without changes.
    old_modified = actual_file.modified
    result = files.Write('/foo/bar.html', content='Test', meta=meta, async=True)
    self.assertEqual(None, result)
    actual_file = files._File.get_by_key_name('/foo/bar.html')
    self.assertEntityEqual(expected_file, actual_file, ignore=dates)
    self.assertEqual(old_modified, actual_file.modified)

    # Asynchronous update with changes.
    old_modified = actual_file.modified
    rpc = files.Write('/foo/bar.html', meta=new_meta, async=True)
    expected_file.flag = True
    actual_file = files._File.get(rpc.get_result())
    self.assertEntityEqual(expected_file, actual_file, ignore=dates)
    self.assertNotEqual(old_modified, actual_file.modified)

    # Write large content to blobstore.
    key = files.Write('/foo/bar.html', content=LARGE_FILE_CONTENT)
    file_obj = files.Get('/foo/bar.html')
    blob_keys = file_obj.blobs
    self.assertEqual(1, len(blob_keys))
    self.assertEqual(LARGE_FILE_CONTENT, file_obj.content)
    self.assertEqual(None, file_obj._file_ent.content)
    self.assertEqual(LARGE_FILE_CONTENT, files.Read('/foo/bar.html'))
    # Make sure the blobs are deleted with the file:
    file_obj.Delete()
    self.assertEqual([None], blobstore.get(blob_keys))
    self.assertRaises(files.BadFileError, lambda: file_obj.blobs)
    # Make sure blobs are deleted if the file gets smaller:
    key = files.Write('/foo/bar.html', content=LARGE_FILE_CONTENT)
    file_obj = files.Get('/foo/bar.html')
    blob_keys = file_obj.blobs
    file_obj.Write(content='Test')
    self.assertEqual([None], blobstore.get(blob_keys))
    actual_file = files._File.get_by_key_name('/foo/bar.html')
    self.assertEqual('Test', actual_file.content)
    self.assertEqual([], file_obj.blobs)

    # Cleanup.
    expected_file = original_expected_file
    files.Delete('/foo/bar.html')

    # Error handling:
    # Updating mime_type or meta when entity doesn't exist.
    self.assertRaises(files.BadFileError, files.Write, '/foo/bar.html',
                      meta=meta)
    self.assertRaises(files.BadFileError, files.Write, '/foo/bar.html',
                      mime_type='fake/mimetype')
    # No leading / on path.
    self.assertRaises(ValueError, files.Write, '')
    self.assertRaises(ValueError, files.Write, 'bar.html')
    self.assertRaises(TypeError, files.Write)
    self.assertRaises(TypeError, files.Write, content='', blobs=[])
    # Attempt to set 'path' as dynamic property.
    meta = {'path': False}
    self.assertRaises(AttributeError, files.Write, '/bar.html',
                      content='', meta=meta)
    # Specifying both content and blobs.
    blobs = [self.blob_key]
    self.assertRaises(TypeError, files.Write, content='Test', blobs=blobs)

  @testing.DisableCaching
  def testDelete(self):
    # Synchronous delete.
    files.Touch('/foo/bar.html')
    self.assertEqual(None, files.Delete('/foo/bar.html'))
    self.assertEqual(None, files._File.get_by_key_name('/foo/bar.html'))

    # Asynchronous delete.
    files.Touch('/foo/bar.html')
    rpc = files.Delete('/foo/bar.html', async=True)
    self.assertEqual(None, rpc.get_result())
    self.assertEqual(None, files._File.get_by_key_name('/foo/bar.html'))

    # Batch delete.
    files.Touch('/foo/bar.html')
    files.Touch('/foo/bar/baz')
    files.Touch('/qux')
    result = files.Delete(['/foo/bar.html', '/foo/bar/baz', '/qux'])
    self.assertEqual(None, result)

    # Error handling.
    self.assertRaises(files.BadFileError, files.Delete, '/fake.html')

  @testing.DisableCaching
  def testTouch(self):
    old_file = files._File.get(files.Write('/foo/bar.html', content='Test'))

    # Synchronous touch.
    key = files.Touch('/foo/bar.html')
    touched_file = files._File.get(key)
    self.assertNotEqual(touched_file.modified, old_file.modified)

    # Asynchronous touch.
    rpc = files.Touch('/foo/bar.html', async=True)
    touched_file = files._File.get(rpc.get_result())
    self.assertNotEqual(touched_file.modified, old_file.modified)

    # Batch touch.
    paths = ['/foo/bar.html', '/foo/bar/baz', '/qux']
    keys = files.Touch(paths)
    self.assertEqual(len(keys), 3)
    file_objs = files.Get(paths)
    self.assertEqual(file_objs[0].modified, file_objs[1].modified)
    self.assertEqual(file_objs[1].modified, file_objs[2].modified)

    # Error handling.
    self.assertRaises(ValueError, files.Touch, '')
    self.assertRaises(ValueError, files.Touch, 'root-file')

  @testing.DisableCaching
  def testListFiles(self):
    # Create files for testing.
    root_level = [
        files.File('/index.html'),
        files.File('/qux'),
    ]
    first_level = [
        files.File('/foo/bar'),
    ]
    second_level = [
        files.File('/foo/bar/baz'),
        files.File('/foo/bar/baz.html'),
        files.File('/foo/bar/baz.txt'),
    ]
    all_files = root_level + first_level + second_level
    for file_obj in all_files:
      file_obj.Touch()

    # Since Titan doesn't represent directories, non-existent paths will be [].
    self.assertSameObjects([], files.ListFiles('/fake/path'))

    # From root.
    self.assertSameObjects(root_level, files.ListFiles('/'))
    file_objs = files.ListFiles('/', recursive=True)
    self.assertSameObjects(all_files, file_objs)

    # From first level dir.
    self.assertSameObjects(first_level, files.ListFiles('/foo'))
    self.assertSameObjects(first_level, files.ListFiles('/foo/'))
    file_objs = files.ListFiles('/foo', recursive=True)
    self.assertSameObjects(first_level + second_level, file_objs)

    # From second level dir.
    self.assertSameObjects(second_level, files.ListFiles('/foo/bar'))
    file_objs = files.ListFiles('/foo/bar', recursive=True)
    self.assertSameObjects(second_level, file_objs)

    # Limit recursion depth.
    file_objs = files.ListFiles('/', recursive=True, depth=1)
    self.assertSameObjects(root_level + first_level, file_objs)
    file_objs = files.ListFiles('/', recursive=True, depth=2)
    self.assertSameObjects(all_files, file_objs)
    file_objs = files.ListFiles('/foo/', recursive=True, depth=1)
    self.assertSameObjects(first_level + second_level, file_objs)

    # Support trailing slashes.
    self.assertSameObjects(second_level, files.ListFiles('/foo/bar/'))
    file_objs = files.ListFiles('/foo/bar/', recursive=True)
    self.assertSameObjects(second_level, file_objs)

    # Error handling.
    self.assertRaises(ValueError, files.ListFiles, '')
    self.assertRaises(ValueError, files.ListFiles, '//')
    self.assertRaises(ValueError, files.ListFiles, '/..')
    self.assertRaises(ValueError, files.ListFiles, '/', recursive=True, depth=0)

  @testing.DisableCaching
  def testListDir(self):
    # Create files for testing.
    root_level = [
        files.File('/index.html'),
        files.File('/qux'),
    ]
    first_level = [
        files.File('/foo/bar'),
    ]
    second_level = [
        files.File('/foo/bar/baz'),
        files.File('/foo/bar/baz.txt'),
        files.File('/foo/qux/baz.html'),
    ]
    root_level_dirs = ['foo']
    first_level_dirs = ['bar', 'qux']
    all_files = root_level + first_level + second_level
    for file_obj in all_files:
      file_obj.Touch()

    # Since Titan doesn't represent directories, non-existent paths will be [].
    dirs, file_objs = files.ListDir('/fake/path')
    self.assertEqual([], dirs)
    self.assertEqual([], file_objs)

    # From root.
    dirs, file_objs = files.ListDir('/')
    self.assertSameObjects(root_level_dirs, dirs)
    self.assertSameObjects(files.ListFiles('/'), file_objs)

    # From first level dir.
    dirs, file_objs = files.ListDir('/foo')
    self.assertSameObjects(first_level_dirs, dirs)
    self.assertSameObjects(files.ListFiles('/foo'), file_objs)

    # Support trailing slashes.
    self.assertSameObjects(files.ListDir('/foo'), files.ListDir('/foo/'))

    # Verify correct inner directory listing when only a deep file exists.
    files.Touch('/not-foo/bar/baz/qux/box/foo.html')
    dirs, file_objs = files.ListDir('/')
    self.assertSameElements(root_level_dirs + ['not-foo'], dirs)
    self.assertSameObjects(root_level, file_objs)
    dirs, file_objs = files.ListDir('/not-foo/bar/baz')
    self.assertEqual(['qux'], dirs)
    self.assertEqual([], file_objs)
    files.Delete('/not-foo/bar/baz/qux/box/foo.html')

    # Error handling.
    self.assertRaises(ValueError, files.ListDir, '')
    self.assertRaises(ValueError, files.ListDir, '//')
    self.assertRaises(ValueError, files.ListDir, '/..')

  @testing.DisableCaching
  def testDirExists(self):
    self.assertFalse(files.DirExists('/foo'))
    files.Touch('/foo/bar.html')
    self.assertTrue(files.DirExists('/foo'))
    self.assertTrue(files.DirExists('/foo/'))

    # Error handling.
    self.assertRaises(ValueError, files.DirExists, '/..')

  def testCaching(self):
    namespace = files_cache.DEFAULT_NAMESPACE
    files.Write('/foo', 'Test')

    # Get: should store in memcache after first fetch.
    memcache.flush_all()
    self.assertEqual(None, memcache.get('/foo'))
    file_obj = files.Get('/foo')
    cache_item = memcache.get('/foo', namespace=namespace)
    self.assertEntityEqual(file_obj._file, cache_item)

    # Read: should rely on Get behavior.
    memcache.flush_all()
    self.assertEqual(None, memcache.get('/foo'))
    content = files.Read('/foo')
    cache_item = memcache.get('/foo', namespace=namespace)
    self.assertEqual(content, cache_item.content)

    # Write of new file: should add to memcache.
    memcache.flush_all()
    file_obj = files.File('/foo/bar')
    self.assertEqual(None, memcache.get('/foo/bar'))
    file_obj.Write('Test')
    cache_item = memcache.get('/foo/bar', namespace=namespace)
    self.assertEntityEqual(file_obj._file, cache_item)

    # Write with changes: should update memcache.
    file_obj = files.File('/foo/bar')
    self.assertEqual(None, memcache.get('/foo/bar'))
    file_obj.Write('New content')
    cache_item = memcache.get('/foo/bar', namespace=namespace)
    self.assertEntityEqual(file_obj._file, cache_item)
    self.assertEqual('New content', cache_item.content)

    # Delete: should flag file as non-existent in memcache.
    memcache.flush_all()
    files.Touch(['/foo', '/bar'])
    files.Delete(['/foo', '/bar'])
    cache_item = memcache.get('/foo', namespace=namespace)
    self.assertEqual(files_cache._NO_FILE_FLAG, cache_item)
    cache_item = memcache.get('/bar', namespace=namespace)
    self.assertEqual(files_cache._NO_FILE_FLAG, cache_item)

    # Touch: should update file memcache.
    memcache.flush_all()
    files.Touch(['/foo', '/bar'])
    cache_item = memcache.get('/foo', namespace=namespace)
    self.assertEntityEqual(files.File('/foo')._file, cache_item)
    cache_item = memcache.get('/bar', namespace=namespace)
    self.assertEntityEqual(files.File('/bar')._file, cache_item)

    # ListDir: should set subdir caches for entire subtree.
    memcache.flush_all()
    # After ListDir, subdir caches should be populated.
    files.Touch('/foo/bar/baz.html')
    files.ListDir('/foo')
    self.assertEqual(None, memcache.get('dir:/', namespace=namespace))
    self.assertEqual(None, memcache.get('dir:/foo/bar', namespace=namespace))
    cache_item = memcache.get('dir:/foo', namespace=namespace)
    self.assertEqual(set(['bar']), cache_item['subdirs'])
    files.ListDir('/')
    cache_item = memcache.get('dir:/', namespace=namespace)
    self.assertEqual(set(['foo']), cache_item['subdirs'])

    # Write: subdir caches should be updated.
    memcache.flush_all()
    files.Touch('/foo/bar/baz.html')
    files.ListDir('/')
    cache_item = memcache.get('dir:/foo', namespace=namespace)
    self.assertEqual(set(['bar']), cache_item['subdirs'])

    # Write blobs: should store in sharded cache.
    files.Write('/foo/bar.html', content=LARGE_FILE_CONTENT)
    cache_item = memcache.get('/foo/bar.html',
                              namespace=sharded_cache.NAMESPACE)
    blob_content = files.Read('/foo/bar.html')
    self.assertEqual(LARGE_FILE_CONTENT, blob_content)
    blob_content = files.files_cache.GetBlob('/foo/bar.html')
    self.assertEqual(LARGE_FILE_CONTENT, blob_content)

    # Delete: should delete blobs from sharded cache.
    files.Delete('/foo/bar.html')
    cache_item = memcache.get('/foo/bar.html',
                              namespace=sharded_cache.NAMESPACE)
    self.assertEqual(None, cache_item)

    # Delete: should clear each subdir cache for the file's paths.
    memcache.flush_all()
    files.Touch('/foo/bar/baz.html')
    files.ListDir('/')
    files.Delete('/foo/bar/baz.html', update_subdir_caches=True)
    self.assertEqual({}, memcache.get('dir:/foo', namespace=namespace))
    self.assertEqual({}, memcache.get('dir:/', namespace=namespace))
    files.Touch('/foo/bar/baz.html')
    files.Delete([files.File('/foo/bar/baz.html')], update_subdir_caches=True)

  @testing.DisableCaching
  def testPrivateGetCommonDir(self):
    paths = ['/foo/bar/baz/test.html', '/foo/bar/test.html']
    self.assertEqual('/foo/bar', files._GetCommonDir(paths))
    paths = ['/foo/bar/baz/test.html', '/z/test.html']
    self.assertEqual('/', files._GetCommonDir(paths))
    paths = ['/foo/bar/baz/test.html', '/footest/bar.html']
    self.assertEqual('/', files._GetCommonDir(paths))

  @testing.DisableCaching
  def testPrivateValidatePaths(self):
    # Support of File objects.
    self.assertEqual('/foo', files.ValidatePaths(files.File('/foo')))
    self.assertListEqual(['/foo'], files.ValidatePaths([files.File('/foo')]))
    self.assertListEqual(['/foo', '/bar'],
                         files.ValidatePaths([files.File('/foo'), '/bar']))

    # Invalid paths.
    self.assertRaises(ValueError, files.ValidatePaths, None)
    self.assertRaises(ValueError, files.ValidatePaths, '')
    self.assertRaises(ValueError, files.ValidatePaths, '//')
    self.assertRaises(ValueError, files.ValidatePaths, '/..')

  @testing.DisableCaching
  def testPrivateMakePaths(self):
    # No containing paths of '/'.
    self.assertEqual([], files._MakePaths('/'))
    # / contains /file
    self.assertEqual(['/'], files._MakePaths('/file'))
    # / and /foo contain /foo/bar
    self.assertEqual(['/', '/foo'], files._MakePaths('/foo/bar'))

    expected = ['/', '/path', '/path/to', '/path/to/some']
    self.assertEqual(expected, files._MakePaths('/path/to/some/file.txt'))
    self.assertEqual(expected, files._MakePaths('/path/to/some/file'))

def main(unused_argv):
  basetest.main()

if __name__ == '__main__':
  app.run()
