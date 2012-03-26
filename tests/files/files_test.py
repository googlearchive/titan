#!/usr/bin/env python
# -*- coding: utf-8 -*-
#!/usr/bin/python2.4
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

from tests.common import testing

import copy
import datetime
from google.appengine.api import files as blobstore_files
from google.appengine.api import memcache
from google.appengine.api import users
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
    files.Write('/foo/bar.html', content='Test', meta=meta)

    # Init with path only, verify lazy-loading properties.
    file_obj = files.File('/foo/bar.html')
    self.assertFalse(file_obj.is_loaded)
    self.assertIsNone(file_obj._file_ent)
    _ = file_obj.mime_type
    self.assertNotEqual(None, file_obj._file_ent)

    # Init with a _File entity.
    file_ent = files._File.get_by_key_name('/foo/bar.html')
    file_obj = files.File('/foo/bar.html', _file_ent=file_ent)
    self.assertEqual('/foo/bar.html', file_obj.path)
    self.assertEqual('bar.html', file_obj.name)
    self.assertTrue(file_obj.is_loaded)
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
    self.assertIsNone(file_obj._file_ent)
    key = files.Write('/foo/bar.html', content='Test', meta=meta)
    rpc = file_obj.Delete(async=True)
    self.assertIsNone(rpc.get_result())

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

    # Properties: paths, mime_type, created, modified, blob, created_by,
    # modified_by, and size.
    file_obj = files.File('/foo/bar/baz.html')
    file_obj.Touch()
    self.assertEqual(file_obj.paths, ['/', '/foo', '/foo/bar'])
    self.assertEqual(file_obj.mime_type, 'text/html')
    self.assertTrue(isinstance(file_obj.created, datetime.datetime))
    self.assertTrue(isinstance(file_obj.modified, datetime.datetime))
    self.assertIsNone(file_obj.blob)
    self.assertEqual(file_obj.created_by, users.User('titanuser@example.com'))
    self.assertEqual(file_obj.modified_by, users.User('titanuser@example.com'))
    # Size:
    file_obj.Write('foo')
    self.assertEqual(file_obj.size, 3)
    file_obj.Write(u'f♥♥')
    # "size" should represent the number of bytes, not the number of characters.
    # 'f♥♥' == 'f\xe2\x99\xa5\xe2\x99\xa5' == 1 + 3 + 3 == 7
    self.assertEqual(file_obj.size, 7)
    # "size" should use blob size if present:
    file_obj.Write(LARGE_FILE_CONTENT)
    self.assertEqual(file_obj.size, 1 << 21)

    # read() and content property.
    self.assertEqual(file_obj.content, file_obj.read())

    # close().
    self.assertIsNone(file_obj.close())

    # Error handling: init with non-existent path.
    file_obj = files.File('/foo/fake.html')
    self.assertRaises(files.BadFileError, lambda: file_obj.paths)
    self.assertRaises(files.BadFileError, lambda: file_obj.content)
    self.assertRaises(files.BadFileError, file_obj.Delete)
    self.assertRaises(files.BadFileError, file_obj.Serialize)

  @testing.DisableCaching
  def testSmartFileList(self):
    # 256 File objects in a FileList which load in batches of 100.
    file_objs = [files.File('/foo%s' % i) for i in range(256)]
    files.Touch(file_objs)
    file_list = files.SmartFileList(file_objs, batch_size=100)

    # When iterating over a files list, file objects should evaluate in batches.
    self.assertFalse(file_objs[0].is_loaded)
    for i, file_obj in enumerate(file_list):
      self.assertTrue(file_obj.is_loaded, 'not loaded: %s' % i)
      # When rolling over each boundary, verify the correct batches were loaded.
      if i == 99:
        self.assertFalse(file_objs[100].is_loaded)
      elif i == 100:
        self.assertTrue(file_objs[199].is_loaded)
      elif i == 199:
        self.assertFalse(file_objs[200].is_loaded)
      elif i == 200:
        self.assertTrue(file_objs[255].is_loaded)

    # Verify slicing.
    file_objs = [files.File('/foo%s' % i) for i in range(10)]
    file_list = files.SmartFileList(file_objs)
    new_file_list = file_list[5:]
    self.assertTrue(isinstance(new_file_list, files.SmartFileList))
    self.assertTrue(new_file_list[0].path == '/foo5')
    self.assertTrue(new_file_list[-1].path == '/foo9')

    # Error handling.
    self.assertRaises(IndexError, lambda: file_list[20])

  @testing.DisableCaching
  def testGet(self):
    meta = {'color': 'blue', 'flag': False}
    actual_file = files.Write('/foo/bar.html', content='Test', meta=meta)

    expected = {
        'name': 'bar.html',
        'path': '/foo/bar.html',
        'paths': [u'/', u'/foo'],
        'mime_type': u'text/html',
        'created': actual_file.created,
        'modified': actual_file.modified,
        'blob': None,
        'exists': True,
        'created_by': 'titanuser@example.com',
        'modified_by': 'titanuser@example.com',
        # meta attributes:
        'color': u'blue',
        'flag': False,
    }
    self.assertDictEqual(expected, files.Get('/foo/bar.html').Serialize())
    expected['content'] = 'Test'
    self.assertDictEqual(expected,
                         files.Get('/foo/bar.html').Serialize(full=True))

    # Batch get.
    # Get() should always return a dictionary of non-lazy, pre-populated File
    # objects to avoid multiple fetches. Non-existent files will be missing
    # from the dict.
    files.Touch(['/foo/bar.html', '/foo/bar/baz', '/qux'])
    file_objs = files.Get(['/foo/bar.html', '/foo/bar/baz', '/qux', '/fake'])
    expected = {
        '/foo/bar.html': files.File('/foo/bar.html'),
        '/foo/bar/baz': files.File('/foo/bar/baz'),
        '/qux': files.File('/qux'),
    }
    self.assertDictEqual(expected, file_objs)
    self.assertTrue(all([f.exists for f in file_objs.values()]))
    self.assertEqual({}, files.Get([]))

    # Reading File contents from datastore.
    files.Write('/foo/bar.html', content='Test')
    self.assertEqual('Test', files.Get('/foo/bar.html').content)
    files.Write('/foo/bar.html', content='')
    self.assertEqual('', files.Get('/foo/bar.html').content)

    # Reading File contents from blobstore.
    # Also, writing with a blob should nullify content.
    files.Write('/foo/bar.html', blob=self.blob_key)
    blob_content = self.blob_reader.read()
    self.assertEqual(blob_content, files.Get('/foo/bar.html').content)

    # Returned content should maintain its encoding.
    # Byte string:
    files.Write('/foo/bar.html', content='Test')
    content = files.Get('/foo/bar.html').content
    self.assertTrue(isinstance(content, str))
    self.assertEqual('Test', files.Get('/foo/bar.html').content)
    # Unicode string:
    files.Write('/foo/bar.html', content=u'Test')
    content = files.Get('/foo/bar.html').content
    self.assertTrue(isinstance(content, unicode))
    self.assertEqual(u'Test', files.Get('/foo/bar.html').content)

    # Gets return None or an empty dict for non-existent files.
    self.assertIsNone(files.Get('/fake.html'))
    self.assertEqual({}, files.Get(['/fake']))

  @testing.DisableCaching
  def testWrite(self):
    expected_file = files._File(
        key_name='/foo/bar.html',
        name='bar.html',
        content='Test',
        dir_path='/foo',
        paths=[u'/', u'/foo'],
        depth=1,
        mime_type=u'text/html',
        created_by=users.User('titanuser@example.com'),
        modified_by=users.User('titanuser@example.com'),
        # Arbitrary meta data for expando:
        color=u'blue',
        flag=False,
    )
    original_expected_file = copy.deepcopy(expected_file)
    meta = {'color': 'blue', 'flag': False}
    new_meta = {'color': 'blue', 'flag': True}
    dates = ['modified', 'created']

    # Synchronous write of a new file.
    actual_file = files.Write('/foo/bar.html', content='Test', meta=meta)
    self.assertEntityEqual(expected_file, actual_file._file, ignore=dates)
    self.assertNotEqual(None, actual_file.modified, 'modified is not being set')

    # Synchronous update without changes.
    old_modified = actual_file.modified
    actual_file = files.Write('/foo/bar.html', meta=meta)
    self.assertEntityEqual(expected_file, actual_file._file, ignore=dates)
    self.assertEqual(old_modified, actual_file.modified)

    # Synchronous update with changes.
    old_modified = actual_file.modified
    actual_file = files.Write('/foo/bar.html', content='New content',
                              meta=new_meta, mime_type='fake/type')
    expected_file.content = 'New content'
    expected_file.flag = True
    expected_file.mime_type = 'fake/type'
    self.assertEntityEqual(expected_file, actual_file._file, ignore=dates)
    self.assertNotEqual(old_modified, actual_file.modified)

    # Allow writing blank files.
    actual_file = files.Write('/foo/bar.html', content='')
    self.assertEqual(actual_file.content, '')

    # Allow overwriting mime_type and meta without touching content.
    files.Write('/foo/bar.html', content='Test')
    actual_file = files.Write('/foo/bar.html', mime_type='fake/mimetype')
    self.assertEqual('fake/mimetype', actual_file.mime_type)
    self.assertEqual('Test', actual_file.content)

    actual_file = files.Write('/foo/bar.html', meta=new_meta)
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
    self.assertIsNone(result)
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
    file_obj = files.Write('/foo/bar.html', content=LARGE_FILE_CONTENT)
    blob_key = file_obj.blob.key()
    self.assertTrue(blob_key)
    self.assertEqual(LARGE_FILE_CONTENT, file_obj.content)
    self.assertIsNone(file_obj._file_ent.content)
    self.assertEqual(LARGE_FILE_CONTENT, files.Get('/foo/bar.html').content)
    # Make sure the blob is deleted with the file:
    file_obj.Delete()
    self.assertIsNone(blobstore.get(blob_key))
    self.assertRaises(files.BadFileError, lambda: file_obj.blob)
    # Make sure the blob is deleted if the file gets smaller:
    file_obj = files.Write('/foo/bar.html', content=LARGE_FILE_CONTENT)
    blob_key = file_obj.blob.key()
    file_obj.Write(content='Test')
    self.assertIsNone(blobstore.get(blob_key))
    self.assertEqual('Test', file_obj.content)
    self.assertIsNone(file_obj.blob)

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
    self.assertRaises(TypeError, files.Write, content='', blob=None)
    # Attempt to set 'path' as dynamic property.
    meta = {'path': False}
    self.assertRaises(AttributeError, files.Write, '/bar.html',
                      content='', meta=meta)
    # Specifying both content and blob.
    blob = self.blob_key
    self.assertRaises(TypeError, files.Write, content='Test', blob=blob)

  @testing.DisableCaching
  def testDelete(self):
    # Synchronous delete.
    files.Touch('/foo/bar.html')
    self.assertIsNone(files.Delete('/foo/bar.html'))
    self.assertIsNone(files._File.get_by_key_name('/foo/bar.html'))

    # Asynchronous delete.
    files.Touch('/foo/bar.html')
    rpc = files.Delete('/foo/bar.html', async=True)
    self.assertIsNone(rpc.get_result())
    self.assertIsNone(files._File.get_by_key_name('/foo/bar.html'))

    # Batch delete.
    files.Touch('/foo/bar.html')
    files.Touch('/foo/bar/baz')
    files.Touch('/qux')
    result = files.Delete(['/foo/bar.html', '/foo/bar/baz', '/qux'])
    self.assertIsNone(result)

    # Support of lazy, unevaluated File objects.
    files.Touch('/foo/bar.html')
    files.Delete(files.File('/foo/bar.html'))
    self.assertIsNone(files._File.get_by_key_name('/foo/bar.html'))
    files.Touch('/foo/bar.html')
    files.Delete([files.File('/foo/bar.html')])
    self.assertIsNone(files._File.get_by_key_name('/foo/bar.html'))

    # Support of evaluated File objects.
    files.Touch('/foo/bar.html')
    files.Delete(files.Get('/foo/bar.html'))
    self.assertIsNone(files._File.get_by_key_name('/foo/bar.html'))
    files.Touch('/foo/bar.html')
    files.Delete(files.Get(['/foo/bar.html']).values())
    self.assertIsNone(files._File.get_by_key_name('/foo/bar.html'))

    # Error handling.
    self.assertRaises(files.BadFileError, files.Delete, '/fake.html')
    self.assertRaises(files.BadFileError, files.Delete, ['/fake.html'])

  @testing.DisableCaching
  def testTouch(self):
    old_file = files.Write('/foo/bar.html', content='Test')

    # Synchronous touch.
    touched_file = files.Touch('/foo/bar.html')
    self.assertNotEqual(touched_file.modified, old_file.modified)

    # Asynchronous touch.
    rpc = files.Touch('/foo/bar.html', async=True)
    touched_file = files._File.get(rpc.get_result())
    self.assertNotEqual(touched_file.modified, old_file.modified)

    # Batch touch.
    paths = ['/foo/bar.html', '/foo/bar/baz', '/qux']
    file_objs = files.Touch(paths, meta={'color': 'blue'})
    file_objs = files.Get(file_objs)
    self.assertEqual(len(file_objs), 3)
    modified_datetime = file_objs['/foo/bar.html'].modified
    self.assertEqual(modified_datetime, file_objs['/foo/bar.html'].modified)
    self.assertEqual(modified_datetime, file_objs['/foo/bar/baz'].modified)
    self.assertEqual(modified_datetime, file_objs['/qux'].modified)
    self.assertEqual('blue', file_objs['/foo/bar.html'].color)
    self.assertEqual('blue', file_objs['/foo/bar/baz'].color)
    self.assertEqual('blue', file_objs['/qux'].color)

    # Error handling.
    self.assertRaises(ValueError, files.Touch, '')
    self.assertRaises(ValueError, files.Touch, 'root-file')

  @testing.DisableCaching
  def testCopy(self):
    files.Write('/foo.html', 'Test', mime_type='test/mimetype',
                meta={'color': 'blue'})
    files.Copy('/foo.html', '/bar/qux.html')
    file_obj = files.Get('/bar/qux.html')
    self.assertEqual('/bar/qux.html', file_obj.path)
    self.assertEqual(['/', '/bar'], file_obj.paths)
    self.assertEqual('test/mimetype', file_obj.mime_type)
    self.assertEqual('Test', file_obj.content)
    self.assertEqual('blue', file_obj.color)

    # Blobs instead of content.
    files.Delete('/foo.html')
    files.Write('/foo.html', blob=self.blob_key, meta={'flag': False})
    files.Copy('/foo.html', '/bar/qux.html')
    file_obj = files.Get('/bar/qux.html')
    blob_content = self.blob_reader.read()
    self.assertEqual(blob_content, file_obj.content)
    self.assertEqual('text/html', file_obj.mime_type)
    self.assertEqual(False, file_obj.flag)
    # Copy should overwrite previous versions and their properties.
    self.assertRaises(AttributeError, lambda: file_obj.color)

  @testing.DisableCaching
  def testCopyDir(self):
    files.Touch('/foo/a.html')
    files.Touch('/foo/baz/a.html')
    files.Write('/bar/a.html', 'foo')
    files.Touch('/bar/baz/b.html')

    # Test dry-run copy.
    new_file_paths = files.CopyDir('/foo/', '/bar', dry_run=True)
    expected_paths = [
        '/bar/a.html',
        '/bar/baz/a.html',
    ]
    self.assertListEqual(expected_paths, new_file_paths)
    self.assertFalse(files.Exists('/bar/baz/a.html'))

    # Test actual copy.
    new_file_objs = files.CopyDir('/foo/', '/bar')
    expected_files = [
        files.File('/bar/a.html'),
        files.File('/bar/baz/a.html'),
    ]
    self.assertListEqual(expected_files, new_file_objs)
    self.assertEqual('', files.Get('/bar/a.html').content)
    # b.html should still exist:
    self.assertTrue(files.Exists('/bar/baz/b.html'))

    # Error handling.
    self.assertEqual([], files.CopyDir('/fake/', '/foo/'))

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

    # Custom filters:
    foo_file_obj = files.Touch('/a/bar', meta={'color': 'red', 'count': 1})
    bar_file_obj = files.Touch('/a/baz/qux', meta={'color': 'red', 'count': 2})
    baz_file_obj = files.Touch('/a/bzz', meta={'color': 'blue', 'count': 3})
    # Single filter:
    filters = ('color =', 'red')
    file_objs = files.ListFiles('/a', filters=filters)
    self.assertSameObjects([foo_file_obj], file_objs)
    # Multiple filters:
    filters = [('color =', 'blue'), ('count =', 3)]
    file_objs = files.ListFiles('/', recursive=True, filters=filters)
    self.assertSameObjects([baz_file_obj], file_objs)
    # Recursive:
    filters = [('color =', 'red')]
    file_objs = files.ListFiles('/', recursive=True, filters=filters)
    self.assertSameObjects([foo_file_obj, bar_file_obj], file_objs)
    # Non-meta property:
    filters = [('created_by =', users.User('titanuser@example.com')),
               ('count =', 2)]
    file_objs = files.ListFiles('/a/', recursive=True, filters=filters)
    self.assertSameObjects([bar_file_obj], file_objs)

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
    files.Write('/foo', 'Test')

    # Get: should store in memcache after first fetch.
    memcache.flush_all()
    self.assertIsNone(memcache.get('/foo'))
    file_obj = files.Get('/foo')
    cache_item = memcache.get(files_cache.FILE_MEMCACHE_PREFIX + '/foo')
    self.assertEntityEqual(file_obj._file, cache_item)

    # Write of new file: should add to memcache.
    memcache.flush_all()
    file_obj = files.File('/foo/bar')
    self.assertIsNone(memcache.get('/foo/bar'))
    file_obj.Write('Test')
    cache_item = memcache.get(files_cache.FILE_MEMCACHE_PREFIX + '/foo/bar')
    self.assertEntityEqual(file_obj._file, cache_item)

    # Write with changes: should update memcache.
    file_obj = files.File('/foo/bar')
    self.assertIsNone(memcache.get('/foo/bar'))
    file_obj.Write('New content')
    cache_item = memcache.get(files_cache.FILE_MEMCACHE_PREFIX + '/foo/bar')
    self.assertEntityEqual(file_obj._file, cache_item)
    self.assertEqual('New content', cache_item.content)

    # Delete: should flag file as non-existent in memcache.
    memcache.flush_all()
    files.Touch(['/foo', '/bar'])
    files.Delete(['/foo', '/bar'])
    cache_item = memcache.get(files_cache.FILE_MEMCACHE_PREFIX + '/foo')
    self.assertEqual(files_cache._NO_FILE_FLAG, cache_item)
    cache_item = memcache.get(files_cache.FILE_MEMCACHE_PREFIX + '/bar')
    self.assertEqual(files_cache._NO_FILE_FLAG, cache_item)

    # Touch: should update file memcache.
    memcache.flush_all()
    files.Touch(['/foo', '/bar'])
    cache_item = memcache.get(files_cache.FILE_MEMCACHE_PREFIX + '/foo')
    self.assertEntityEqual(files.File('/foo')._file, cache_item)
    cache_item = memcache.get(files_cache.FILE_MEMCACHE_PREFIX + '/bar')
    self.assertEntityEqual(files.File('/bar')._file, cache_item)

    # ListDir: should set subdir caches for entire subtree.
    memcache.flush_all()
    # After ListDir, subdir caches should be populated.
    files.Touch('/foo/bar/baz.html')
    files.ListDir('/foo')
    self.assertIsNone(memcache.get(files_cache.DIR_MEMCACHE_PREFIX + '/'))
    self.assertIsNone(
        memcache.get(files_cache.DIR_MEMCACHE_PREFIX + '/foo/bar'))
    cache_item = memcache.get(files_cache.DIR_MEMCACHE_PREFIX + '/foo')
    self.assertEqual(set(['bar']), cache_item['subdirs'])
    files.ListDir('/')
    cache_item = memcache.get(files_cache.DIR_MEMCACHE_PREFIX + '/')
    self.assertEqual(set(['foo']), cache_item['subdirs'])

    # Write: subdir caches should be updated.
    memcache.flush_all()
    files.Touch('/foo/bar/baz.html')
    files.ListDir('/')
    cache_item = memcache.get(files_cache.DIR_MEMCACHE_PREFIX + '/foo')
    self.assertEqual(set(['bar']), cache_item['subdirs'])

    # Write blob: should store in sharded cache.
    files.Write('/foo/bar.html', content=LARGE_FILE_CONTENT)
    cache_item = memcache.get(sharded_cache.MEMCACHE_PREFIX + '/foo/bar.html')
    blob_content = files.Get('/foo/bar.html').content
    self.assertEqual(LARGE_FILE_CONTENT, blob_content)
    blob_content = files.files_cache.GetBlob('/foo/bar.html')
    self.assertEqual(LARGE_FILE_CONTENT, blob_content)

    # Delete: should delete blob from sharded cache.
    files.Delete('/foo/bar.html')
    cache_item = memcache.get(sharded_cache.MEMCACHE_PREFIX + '/foo/bar.html')
    self.assertIsNone(cache_item)

    # Delete: should clear each subdir cache for the file's paths.
    memcache.flush_all()
    files.Touch('/foo/bar/baz.html')
    files.ListDir('/')
    files.Delete('/foo/bar/baz.html', update_subdir_caches=True)
    self.assertEqual({}, memcache.get(files_cache.DIR_MEMCACHE_PREFIX + '/foo'))
    self.assertEqual({}, memcache.get(files_cache.DIR_MEMCACHE_PREFIX + '/'))
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
