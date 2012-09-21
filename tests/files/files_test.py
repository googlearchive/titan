#!/usr/bin/env python
# -*- coding: utf-8 -*-
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

"""Tests for files.py."""

from tests.common import testing

import copy
import datetime
import hashlib
from google.appengine.api import files as blobstore_files
from google.appengine.api import users
from google.appengine.ext import blobstore
from titan.common.lib.google.apputils import app
from titan.common.lib.google.apputils import basetest
from titan.files import files
from titan.common import utils

# Content larger than the arbitrary max content size and the 1MB RPC limit.
LARGE_FILE_CONTENT = 'a' * (1 << 21)  # 2 MiB.

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

  def tearDown(self):
    files.UnregisterFileFactory()
    super(FileTestCase, self).tearDown()

  def testFile(self):
    meta = {'color': 'blue', 'flag': False}
    titan_file = files.File('/foo/bar.html')
    titan_file.Write('Test', meta=meta)

    # Init with path only, verify lazy-loading properties.
    titan_file = files.File('/foo/bar.html')
    self.assertFalse(titan_file.is_loaded)
    self.assertIsNone(titan_file._file_ent)
    _ = titan_file.mime_type
    self.assertNotEqual(None, titan_file._file_ent)
    self.assertTrue(titan_file.is_loaded)

    # Init with a _TitanFile entity.
    file_ent = files._TitanFile.get_by_id('/foo/bar.html')
    titan_file = files.File('/foo/bar.html', _file_ent=file_ent)
    self.assertEqual('/foo/bar.html', titan_file.path)
    self.assertEqual('bar.html', titan_file.name)
    self.assertTrue(titan_file.is_loaded)
    self.assertIsNotNone(titan_file._file_ent)

    # Write().
    self.assertEqual(titan_file.content, 'Test')
    titan_file.Write('New content')
    self.assertEqual(titan_file.content, 'New content')
    titan_file.Write('')
    self.assertEqual(titan_file.content, '')
    # Check meta data.
    self.assertEqual('blue', titan_file.meta.color)
    self.assertEqual(False, titan_file.meta.flag)

    # Delete().
    self.assertTrue(titan_file.exists)
    titan_file.Delete()
    self.assertFalse(titan_file.exists)
    titan_file.Write(content='Test', meta=meta)
    titan_file.Delete()
    self.assertFalse(titan_file.exists)

    # Serialize().
    titan_file = files.File('/foo/bar/baz').Write('', meta=meta)
    expected_data = {
        'path': '/foo/bar/baz',
        'real_path': '/foo/bar/baz',
        'name': 'baz',
        'paths': ['/', '/foo', '/foo/bar'],
        'mime_type': u'application/octet-stream',
        'created': titan_file.created,
        'modified': titan_file.modified,
        'content': '',
        'blob': None,
        'created_by': 'titanuser@example.com',
        'modified_by': 'titanuser@example.com',
        'meta': {
            'color': 'blue',
            'flag': False,
        },
        'size': 0,
        'md5_hash': hashlib.md5('').hexdigest(),
    }
    self.assertEqual(expected_data,
                     files.File('/foo/bar/baz').Serialize(full=True))

    # Properties: paths, mime_type, created, modified, blob, created_by,
    # modified_by, and size.
    titan_file = files.File('/foo/bar/baz.html')
    # Check bool handling:
    self.assertFalse(titan_file)
    titan_file.Write('')
    self.assertTrue(titan_file)
    self.assertEqual(titan_file.paths, ['/', '/foo', '/foo/bar'])
    self.assertEqual(titan_file.mime_type, 'text/html')
    self.assertTrue(isinstance(titan_file.created, datetime.datetime))
    self.assertTrue(isinstance(titan_file.modified, datetime.datetime))
    self.assertIsNone(titan_file.blob)
    self.assertEqual(titan_file.created_by, users.User('titanuser@example.com'))
    self.assertEqual(titan_file.modified_by,
                     users.User('titanuser@example.com'))
    # Size:
    titan_file.Write('foo')
    self.assertEqual(titan_file.size, 3)
    titan_file.Write(u'f♥♥')
    # "size" should represent the number of bytes, not the number of characters.
    # 'f♥♥' == 'f\xe2\x99\xa5\xe2\x99\xa5' == 1 + 3 + 3 == 7
    self.assertEqual(titan_file.size, 7)
    # "size" should use blob size if present:
    titan_file.Write(LARGE_FILE_CONTENT)
    self.assertEqual(titan_file.size, 1 << 21)

    # read() and content property.
    self.assertEqual(titan_file.content, titan_file.read())

    # close().
    self.assertIsNone(titan_file.close())

    # Error handling: init with non-existent path.
    titan_file = files.File('/foo/fake.html')
    self.assertRaises(files.BadFileError, lambda: titan_file.paths)
    self.assertRaises(files.BadFileError, lambda: titan_file.content)
    self.assertRaises(files.BadFileError, titan_file.Delete)
    self.assertRaises(files.BadFileError, titan_file.Serialize)

    # Bad path arguments:
    self.assertRaises(ValueError, files.File, None)
    self.assertRaises(ValueError, files.File, '')
    self.assertRaises(ValueError, files.File, 'bar.html')
    self.assertRaises(ValueError, files.File, '/a/b/')
    self.assertRaises(ValueError, files.File, '/a//b')
    self.assertRaises(ValueError, files.File, '..')
    self.assertRaises(ValueError, files.File, '/a/../b')
    self.assertRaises(ValueError, files.File, '/')

  def testWrite(self):
    expected_file = files._TitanFile(
        id='/foo/bar.html',
        name='bar.html',
        content='Test',
        dir_path='/foo',
        paths=[u'/', u'/foo'],
        depth=1,
        mime_type=u'text/html',
        created_by=users.User('titanuser@example.com', _user_id='1'),
        modified_by=users.User('titanuser@example.com', _user_id='1'),
        # Arbitrary meta data for expando:
        color=u'blue',
        flag=False,
        md5_hash=hashlib.md5('Test').hexdigest(),
    )
    original_expected_file = copy.deepcopy(expected_file)
    meta = {'color': 'blue', 'flag': False}
    new_meta = {'color': 'blue', 'flag': True}
    dates = ['modified', 'created']

    # Synchronous write of a new file.
    actual_file = files.File('/foo/bar.html').Write('Test', meta=meta)
    self.assertNdbEntityEqual(expected_file, actual_file._file, ignore=dates)
    self.assertNotEqual(None, actual_file.modified, 'modified is not being set')

    # Synchronous update without changes.
    actual_file = files.File('/foo/bar.html').Write(meta=meta)
    self.assertNdbEntityEqual(expected_file, actual_file._file, ignore=dates)

    # Synchronous update with changes.
    old_modified = actual_file.modified
    actual_file = files.File('/foo/bar.html')
    actual_file.Write('New content', meta=new_meta, mime_type='fake/type')
    expected_file.content = 'New content'
    expected_file.md5_hash = hashlib.md5('New content').hexdigest()
    expected_file.flag = True
    expected_file.mime_type = 'fake/type'
    self.assertNdbEntityEqual(expected_file, actual_file._file, ignore=dates)
    self.assertNotEqual(old_modified, actual_file.modified)

    # Allow writing blank files.
    actual_file = files.File('/foo/bar.html').Write('')
    self.assertEqual(actual_file.content, '')

    # Allow overwriting mime_type and meta without touching content.
    files.File('/foo/bar.html').Write(content='Test')
    actual_file = files.File('/foo/bar.html').Write(mime_type='fake/mimetype')
    self.assertEqual('fake/mimetype', actual_file.mime_type)
    self.assertEqual('Test', actual_file.content)

    actual_file = files.File('/foo/bar.html').Write(meta=new_meta)
    self.assertEqual(True, actual_file.meta.flag)
    self.assertEqual('Test', actual_file.content)

    # Cleanup.
    expected_file = original_expected_file
    files.File('/foo/bar.html').Delete()

    # Write large content to blobstore.
    titan_file = files.File('/foo/bar.html').Write(content=LARGE_FILE_CONTENT)
    blob_key = titan_file.blob.key()
    self.assertTrue(blob_key)
    self.assertEqual(LARGE_FILE_CONTENT, titan_file.content)
    self.assertIsNone(titan_file._file_ent.content)
    self.assertEqual(LARGE_FILE_CONTENT, files.File('/foo/bar.html').content)
    self.assertEqual(hashlib.md5(LARGE_FILE_CONTENT).hexdigest(),
                     titan_file.md5_hash)

    # De-duping check: verify the blob key doesn't change if the content
    # doesn't change.
    old_blob_key = blob_key
    titan_file = files.File('/foo/bar.html').Write(content=LARGE_FILE_CONTENT)
    blob_key = titan_file.blob.key()
    self.assertEqual(old_blob_key, blob_key)
    self.assertEqual(LARGE_FILE_CONTENT, titan_file.content)
    self.assertIsNone(titan_file._file_ent.content)
    self.assertEqual(LARGE_FILE_CONTENT, files.File('/foo/bar.html').content)
    self.stubs.SmartUnsetAll()

    # Write with a blob key and encoding; verify proper decoding.
    encoded_foo = u'f♥♥'.encode('utf-8')
    blob_key = utils.WriteToBlobstore(encoded_foo)
    titan_file = files.File('/foo/bar.html')
    # Verify that without encoding, the encoded bytestring is returned.
    titan_file.Write(blob=blob_key)
    self.assertEqual(encoded_foo, titan_file.content)
    # Verify that with encoding, a unicode string is returned.
    titan_file.Write(blob=blob_key, encoding='utf-8')
    self.assertEqual(u'f♥♥', titan_file.content)
    # Argument error handling for mixing encoding and unicode content:
    self.assertRaises(TypeError, titan_file.Write, content=u'Test',
                      encoding='utf-8')

    # Make sure the blob is deleted with the file:
    titan_file.Delete()
    self.assertIsNone(blobstore.get(blob_key))
    self.assertRaises(files.BadFileError, lambda: titan_file.blob)
    # Make sure the blob is deleted if the file gets smaller:
    titan_file = files.File('/foo/bar.html').Write(content=LARGE_FILE_CONTENT)
    blob_key = titan_file.blob.key()
    titan_file.Write(content='Test')
    self.assertIsNone(blobstore.get(blob_key))
    # Test the current object and a new instance:
    self.assertEqual('Test', titan_file.content)
    self.assertEqual('Test', files.File('/foo/bar.html').content)
    self.assertIsNone(titan_file.blob)
    self.assertIsNone(files.File('/foo/bar.html').blob)

    # Write with a BlobKey:
    titan_file = files.File('/foo/bar.html').Write(blob=self.blob_key)
    blob_content = self.blob_reader.read()
    # Test the current object and a new instance:
    self.assertEqual(blob_content, files.File('/foo/bar.html').content)
    self.assertEqual(blob_content, titan_file.content)
    self.assertEqual(hashlib.md5('Blobstore!').hexdigest(),
                     titan_file.md5_hash)

    # Cleanup.
    expected_file = original_expected_file
    files.File('/foo/bar.html').Delete()

    # Error handling:
    # Updating mime_type or meta when entity doesn't exist.
    titan_file = files.File('/fake/file')
    self.assertRaises(files.BadFileError, titan_file.Write, meta=meta)
    self.assertRaises(files.BadFileError, titan_file.Write,
                      mime_type='fake/mimetype')

    # Bad arguments:
    self.assertRaises(TypeError, titan_file.Write)
    self.assertRaises(TypeError, titan_file.Write, content=None, blob=None)
    self.assertRaises(TypeError, titan_file.Write, content='Test',
                      blob=self.blob_key)

    # Attempt to set invalid dynamic property.
    meta = {'path': False}
    self.assertRaises(AttributeError, titan_file.Write, content='', meta=meta)
    meta = {'mime_type': 'fail'}
    self.assertRaises(files.InvalidMetaError,
                      titan_file.Write, content='', meta=meta)

  def testDelete(self):
    # Synchronous delete.
    titan_file = files.File('/foo/bar.html').Write('')
    self.assertTrue(titan_file.exists)
    titan_file.Delete()
    # Recreate object, just in case memoization is hiding an error:
    self.assertFalse(files.File('/foo/bar.html').exists)

    # TODO(user): add checks for blob handling.

    # Error handling.
    self.assertRaises(files.BadFileError, files.File('/fake.html').Delete)

  def testFileMixins(self):
    # Support behavior: subclass File and make Write() also touch a
    # centralized file, while avoiding infinite recursion.

    class TouchRootMixin(files.File):

      def Write(self, *args, **kwargs):
        # Note: this mixin is a bad idea in practice. Don't directly touch a
        # centralized file on every write, due to write rate limits.
        if self.path != '/root-touched-file':
          files.File('/root-touched-file').Write('')
        super(TouchRootMixin, self).Write(*args, **kwargs)

    class CustomFile(TouchRootMixin, files.File):
      pass

    custom_file = CustomFile('/foo/bar')
    custom_file.Write('foo')
    self.assertTrue(custom_file.exists)
    self.assertTrue(files.File('/foo/bar').exists)
    self.assertTrue(files.File('/root-touched-file').exists)

  def testCopyTo(self):
    files.File('/foo.html').Write('Test', mime_type='test/mimetype',
                                  meta={'color': 'blue'})
    files.File('/foo.html').CopyTo(files.File('/bar/qux.html'))
    titan_file = files.File('/bar/qux.html')
    self.assertEqual('/bar/qux.html', titan_file.path)
    self.assertEqual(['/', '/bar'], titan_file.paths)
    self.assertEqual('test/mimetype', titan_file.mime_type)
    self.assertEqual('Test', titan_file.content)
    self.assertEqual('blue', titan_file.meta.color)

    # Blobs instead of content.
    files.File('/foo.html').Delete()
    files.File('/foo.html').Write(blob=self.blob_key, meta={'flag': False})
    files.File('/foo.html').CopyTo(files.File('/bar/qux.html'))
    titan_file = files.File('/bar/qux.html')
    blob_content = self.blob_reader.read()
    self.assertEqual(blob_content, titan_file.content)
    self.assertEqual('text/html', titan_file.mime_type)
    self.assertEqual(False, titan_file.meta.flag)
    # Copy should overwrite previous versions and their properties.
    self.assertRaises(AttributeError, lambda: titan_file.meta.color)

    # Error handling:
    self.assertRaises(AssertionError, files.File('/foo.html').CopyTo, '/test')
    self.assertRaises(files.CopyFileError, files.File('/fake').CopyTo,
                      files.File('/test/fake'))
    # Verify that the exception is populated with the failed destination file.
    try:
      files.File('/fake').CopyTo(files.File('/test/fake'))
    except files.CopyFileError as e:
      self.assertEqual('/test/fake', e.titan_file.path)

  def testMoveTo(self):
    files.File('/foo.html').Write('Test', meta={'color': 'blue'})
    files.File('/foo.html').MoveTo(files.File('/bar/qux.html'))
    self.assertFalse(files.File('/foo.html').exists)
    self.assertTrue(files.File('/bar/qux.html').exists)

    # Blob instead of content.
    files.File('/foo.html').Write(blob=self.blob_key, meta={'flag': False})
    files.File('/foo.html').MoveTo(files.File('/bar/qux.html'))
    titan_file = files.File('/bar/qux.html')
    blob_content = self.blob_reader.read()
    self.assertEqual(blob_content, titan_file.content)
    self.assertEqual('text/html', titan_file.mime_type)
    self.assertEqual(False, titan_file.meta.flag)
    self.assertRaises(AttributeError, lambda: titan_file.meta.color)

    self.assertRaises(AssertionError, files.File('/foo.html').MoveTo, '/test')

  def testRegisterFileFactory(self):

    class FooFile(files.File):
      pass

    class BarFile(files.File):
      pass

    def TitanFileFactory(path):
      if path.startswith('/foo/files/'):
        return FooFile
      elif path.startswith('/bar/files/'):
        return BarFile
      return files.File

    files.RegisterFileFactory(TitanFileFactory)
    foo_file = files.File('/foo/files/a')
    bar_file = files.File('/bar/files/b')
    normal_file = files.File('/c')
    self.assertTrue(isinstance(foo_file, FooFile))
    self.assertTrue(isinstance(bar_file, BarFile))
    self.assertTrue(isinstance(normal_file, files.File))

class FilesTestCase(testing.BaseTestCase):

  def testFilesList(self):
    # Create files for testing.
    root_level = files.Files(['/index.html', '/qux'])
    first_level = files.Files(['/foo/bar'])
    second_level = files.Files([
        '/foo/bar/baz',
        '/foo/bar/baz.html',
        '/foo/bar/baz.txt',
    ])
    root_and_first_levels = files.Files.Merge(root_level, first_level)
    first_and_second_levels = files.Files.Merge(first_level, second_level)

    # files.Files.update().
    all_files = files.Files([])
    all_files.update(root_level)
    all_files.update(first_level)
    all_files.update(second_level)
    self.assertEqual(6, len(all_files))

    # Test __eq__ (don't use assertEqual).
    self.assertTrue(files.Files(['/a', '/b']) == files.Files(['/a', '/b']))
    self.assertFalse(files.Files(['/a', '/b']) == files.Files(['/a']))

    for titan_file in all_files.itervalues():
      titan_file.Write('')

    # Empty.
    self.assertSameObjects(files.Files(), files.Files.List('/fake/path'))
    self.assertSameObjects(files.Files([]), files.Files.List('/fake/path'))

    # From root.
    self.assertSameObjects(root_level, files.Files.List('/'))
    titan_files = files.Files.List('/', recursive=True)
    self.assertSameObjects(all_files, titan_files)

    # From first level dir.
    self.assertSameObjects(first_level, files.Files.List('/foo'))
    self.assertSameObjects(first_level, files.Files.List('/foo/'))
    titan_files = files.Files.List('/foo', recursive=True)
    self.assertSameObjects(first_and_second_levels, titan_files)

    # From second level dir.
    self.assertSameObjects(second_level, files.Files.List('/foo/bar'))
    titan_files = files.Files.List('/foo/bar', recursive=True)
    self.assertSameObjects(second_level, titan_files)

    # Limit recursion depth.
    titan_files = files.Files.List('/', recursive=True, depth=1)
    self.assertSameObjects(root_and_first_levels, titan_files)
    titan_files = files.Files.List('/', recursive=True, depth=2)
    self.assertSameObjects(all_files, titan_files)
    titan_files = files.Files.List('/foo/', recursive=True, depth=1)
    self.assertSameObjects(first_and_second_levels, titan_files)

    # Limit the number of files returned.
    titan_files = files.Files.List('/foo', recursive=True, limit=1)
    self.assertEqual(1, len(titan_files))

    # Support trailing slashes.
    self.assertSameObjects(second_level, files.Files.List('/foo/bar/'))
    titan_files = files.Files.List('/foo/bar/', recursive=True)
    self.assertSameObjects(second_level, titan_files)

    # Custom filters:
    files.File('/a/foo').Write('', meta={'color': 'red', 'count': 1})
    files.File('/a/bar/qux').Write('', meta={'color': 'red', 'count': 2})
    files.File('/a/baz').Write('', meta={'color': 'blue', 'count': 3})
    # Single filter:
    filters = [files.FileProperty('color') == 'red']
    titan_files = files.Files.List('/a', filters=filters)
    self.assertSameObjects(['/a/foo'], titan_files)
    # Multiple filters:
    filters = [
        files.FileProperty('color') == 'blue',
        files.FileProperty('count') == 3,
    ]
    titan_files = files.Files.List('/', recursive=True, filters=filters)
    self.assertEqual(files.Files(['/a/baz']), titan_files)
    # Recursive:
    filters = [files.FileProperty('color') == 'red']
    titan_files = files.Files.List('/', recursive=True, filters=filters)
    self.assertEqual(files.Files(['/a/foo', '/a/bar/qux']), titan_files)
    # Non-meta property:
    filters = [
        files.FileProperty('created_by') == users.User('titanuser@example.com'),
        files.FileProperty('count') == 2,
    ]
    titan_files = files.Files.List('/a/', recursive=True, filters=filters)
    self.assertEqual(files.Files(['/a/bar/qux']), titan_files)

    # Error handling.
    self.assertRaises(ValueError, files.Files.List, '')
    self.assertRaises(ValueError, files.Files.List, '//')
    self.assertRaises(ValueError, files.Files.List, '/..')
    self.assertRaises(ValueError, files.Files.List, '/',
                      recursive=True, depth=0)

  def testOrderedFiles(self):
    # Create files for testing.
    root_level = files.OrderedFiles([
        # These need to be alphabetically ordered here because they will be
        # usually returned that way from queries inside files.Files.List(),
        # except for when other filters are applied.
        '/bar',
        '/baz',
        '/foo',
    ])
    for titan_file in root_level.itervalues():
      titan_file.Write('')

    # Verify that equality handles order checking. Do this first to make sure
    # that following assertEqual() calls are also checking for order.
    root_level_same_order = files.OrderedFiles([
        # Intentionally not the same order, to test Sort() right below.
        '/baz',
        '/foo',
        '/bar',
    ])
    root_level_same_order.Sort()
    root_level_different_order = files.OrderedFiles([
        '/foo',
        '/baz',
        '/bar',
    ])
    self.assertEqual(root_level, root_level_same_order)
    self.assertNotEqual(root_level, root_level_different_order)
    self.assertNotEqual(files.OrderedFiles([]), root_level)

    # Test adding, removing, and updating.
    new_root_level = files.OrderedFiles([
        '/bar',
        '/baz',
        '/foo',
    ])
    new_root_level['/qux'] = files.File('/qux')
    self.assertNotEqual(root_level, new_root_level)
    del new_root_level['/qux']
    self.assertEqual(root_level, new_root_level)

    new_root_level.update(files.Files(['/qux']))
    self.assertNotEqual(root_level, new_root_level)
    del new_root_level['/qux']
    self.assertEqual(root_level, new_root_level)

    # Test files.OrderedFiles.List().
    self.assertEqual(root_level, files.OrderedFiles.List('/'))
    self.assertNotEqual(root_level_different_order,
                        files.OrderedFiles.List('/'))

  def testCopyTo(self):
    # Populate the in-context cache by reading the file before creation.
    self.assertFalse(files.File('/x/b/foo').exists)

    files.File('/foo').Write('')
    files.File('/a/foo').Write('')
    files.File('/a/b/foo').Write('')
    files.File('/c/foo').Write('')

    copied_files = files.Files()
    failed_files = files.Files()
    files.Files.List('/').CopyTo(dir_path='/x', copied_files=copied_files,
                                 failed_files=failed_files)
    expected_paths = [
        '/x/foo',
    ]
    self.assertSameElements(expected_paths, copied_files.keys())
    self.assertEqual([], failed_files.keys())
    titan_files = files.Files.List('/a/', recursive=True)
    titan_files.CopyTo('/x', strip_prefix='/a/', copied_files=copied_files)
    expected_paths = [
        '/x/foo',
        '/x/b/foo',
    ]
    self.assertSameElements(expected_paths, copied_files.keys())
    # With trailing slashes should be the same.
    copied_files.clear()
    titan_files.CopyTo('/x/', strip_prefix='/a/', copied_files=copied_files)
    self.assertSameElements(expected_paths, copied_files.keys())

    copied_files = files.Files()
    failed_files = files.Files()
    files_paths = ['/foo', '/fake']
    self.assertRaises(files.CopyFilesError, files.Files(files_paths).CopyTo,
                      dir_path='/x', copied_files=copied_files,
                      failed_files=failed_files)
    expected_paths = [
        '/x/foo',
    ]
    failed_paths = [
        '/x/fake',
    ]
    self.assertSameElements(expected_paths, copied_files.keys())
    self.assertSameElements(failed_paths, failed_files.keys())

    # Verify that the NDB in-context cache was cleared correctly.
    self.assertTrue(files.File('/x/b/foo').exists)

  def testGet(self):
    files.File('/foo').Write('')
    files.File('/bar').Write('')
    titan_files = files.Files.Get(['/foo', '/fake'])

    self.assertIn('/foo', titan_files)
    self.assertNotIn('/fake', titan_files)

    # Error handling.
    self.assertRaises(ValueError, files.Files.Get, '/foo')

  def testDelete(self):
    files.File('/foo').Write('')
    files.File('/bar').Write('')
    files.File('/qux').Write('')
    files.Files(['/foo', '/bar']).Delete()
    self.assertEqual(files.Files([]), files.Files.Get(['/foo', '/bar']))

    # Test chaining with Get.
    files.File('/foo').Write('')
    files.Files.Get(['/foo']).Delete()
    self.assertEqual(files.Files([]), files.Files.Get(['/foo']))

    # Error handling.
    self.assertRaises(files.BadFileError, files.Files(['/fake', '/qux']).Delete)

  def testSerialize(self):
    # Serialize().
    first_file = files.File('/foo/bar').Write('foobar')
    second_file = files.File('/foo/bat/baz').Write('foobatbaz')
    first_file_data = {
        u'name': u'bar',
        u'path': u'/foo/bar',
        u'paths': [u'/', u'/foo'],
        u'real_path': u'/foo/bar',
        u'mime_type': u'application/octet-stream',
        u'created': first_file.created,
        u'modified': first_file.modified,
        u'content': u'foobar',
        u'blob': None,
        u'created_by': u'titanuser@example.com',
        u'modified_by': u'titanuser@example.com',
        u'meta': {},
        u'md5_hash': hashlib.md5('foobar').hexdigest(),
        u'size': len('foobar'),
    }
    second_file_data = {
        u'name': u'baz',
        u'path': u'/foo/bat/baz',
        u'paths': [u'/', u'/foo', u'/foo/bat'],
        u'real_path': u'/foo/bat/baz',
        u'mime_type': u'application/octet-stream',
        u'created': second_file.created,
        u'modified': second_file.modified,
        u'content': u'foobatbaz',
        u'blob': None,
        u'created_by': u'titanuser@example.com',
        u'modified_by': u'titanuser@example.com',
        u'meta': {},
        u'md5_hash': hashlib.md5('foobatbaz').hexdigest(),
        u'size': len('foobatbaz'),
    }
    self.maxDiff = None
    expected_data = {
        first_file_data['path']: first_file_data,
        second_file_data['path']: second_file_data,
    }

    self.assertEqual(expected_data,
                     files.Files.List(dir_path='/foo/',
                                      recursive=True).Serialize(full=True))

def main(unused_argv):
  basetest.main()

if __name__ == '__main__':
  app.run()
