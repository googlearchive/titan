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
from titan.common import sharded_cache
from titan.files import files
from titan.common import utils

# Imports for deprecated test case code.
from google.appengine.api import memcache
from titan.files import files_cache

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

    # TODO(user): When file_service_stub correctly sets md5_hash,
    # use this code. Until then, check that the AttributeError is raised.
    # self.assertEqual(hashlib.md5(LARGE_FILE_CONTENT).hexdigest(),
    #                 titan_file.md5_hash)
    self.assertRaises(AttributeError, lambda: titan_file.md5_hash)

    # De-duping check: verify the blob key doesn't change if the content
    # doesn't change.
    old_blob_key = blob_key
    # Monkey-patch the md5_hash property for testing.
    # See note about file_service_stub below.
    self.stubs.SmartSet(titan_file.blob.__class__, 'md5_hash', property(
        lambda _: hashlib.md5(LARGE_FILE_CONTENT).hexdigest()))
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

    # TODO(user): When file_service_stub correctly sets md5_hash,
    # use this code. Until then, check that the AttributeError is raised.
    # self.assertEqual(hashlib.md5('Blobstore!').hexdigest(),
    #                  titan_file.md5_hash)
    self.assertRaises(AttributeError, lambda: titan_file.md5_hash)

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

  def testMoveTo(self):
    files.File('/foo.html').Write('Test', meta={'color': 'blue'})
    files.File('/foo.html').MoveTo(files.File('/bar/qux.html'))
    self.assertFalse(files.Exists('/foo.html'))
    self.assertTrue(files.Exists('/bar/qux.html'))

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

#-------------------------------------------------------------------------------
# YARR, THERE BE DEPRECATED CODE BELOW. Will be removed!
#-------------------------------------------------------------------------------

class DeprecatedFileTestCase(testing.BaseTestCase):

  def setUp(self):
    super(DeprecatedFileTestCase, self).setUp()

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
    file_obj = files.DeprecatedFile('/foo/bar.html')
    self.assertFalse(file_obj.is_loaded)
    self.assertIsNone(file_obj._file_ent)
    _ = file_obj.mime_type
    self.assertNotEqual(None, file_obj._file_ent)

    # Init with a _File entity.
    file_ent = files._File.get_by_key_name('/foo/bar.html')
    file_obj = files.DeprecatedFile('/foo/bar.html', _file_ent=file_ent)
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
    self.assertFalse(file_obj._file_ent)
    key = files.Write('/foo/bar.html', content='Test', meta=meta)
    rpc = file_obj.Delete(async=True)
    self.assertIsNone(rpc.get_result())

    # The exists property should be memoized, so that it only makes one RPC.
    file_obj = files.DeprecatedFile('/foo/bar/baz')
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
    file_obj = files.DeprecatedFile('/foo/bar/baz')
    file_obj.Touch()
    old_modified = file_obj.modified
    rpc = file_obj.Touch(async=True)
    self.assertEqual(u'/foo/bar/baz', rpc.get_result().name())
    self.assertNotEqual(old_modified, file_obj.modified)

    # Properties: paths, mime_type, created, modified, blob, created_by,
    # modified_by, and size.
    file_obj = files.DeprecatedFile('/foo/bar/baz.html')
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
    file_obj = files.DeprecatedFile('/foo/fake.html')
    self.assertRaises(files.BadFileError, lambda: file_obj.paths)
    self.assertRaises(files.BadFileError, lambda: file_obj.content)
    self.assertRaises(files.BadFileError, file_obj.Delete)
    self.assertRaises(files.BadFileError, file_obj.Serialize)

  @testing.DisableCaching
  def testSmartFileList(self):
    # 256 DeprecatedFile objects in a FileList which load in batches of 100.
    file_objs = [files.DeprecatedFile('/foo%s' % i) for i in range(256)]
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
    file_objs = [files.DeprecatedFile('/foo%s' % i) for i in range(10)]
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
        '/foo/bar.html': files.DeprecatedFile('/foo/bar.html'),
        '/foo/bar/baz': files.DeprecatedFile('/foo/bar/baz'),
        '/qux': files.DeprecatedFile('/qux'),
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
    files.Delete(files.DeprecatedFile('/foo/bar.html'))
    self.assertIsNone(files._File.get_by_key_name('/foo/bar.html'))
    files.Touch('/foo/bar.html')
    files.Delete([files.DeprecatedFile('/foo/bar.html')])
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
        files.DeprecatedFile('/bar/a.html'),
        files.DeprecatedFile('/bar/baz/a.html'),
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
        files.DeprecatedFile('/index.html'),
        files.DeprecatedFile('/qux'),
    ]
    first_level = [
        files.DeprecatedFile('/foo/bar'),
    ]
    second_level = [
        files.DeprecatedFile('/foo/bar/baz'),
        files.DeprecatedFile('/foo/bar/baz.html'),
        files.DeprecatedFile('/foo/bar/baz.txt'),
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
        files.DeprecatedFile('/index.html'),
        files.DeprecatedFile('/qux'),
    ]
    first_level = [
        files.DeprecatedFile('/foo/bar'),
    ]
    second_level = [
        files.DeprecatedFile('/foo/bar/baz'),
        files.DeprecatedFile('/foo/bar/baz.txt'),
        files.DeprecatedFile('/foo/qux/baz.html'),
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
    file_obj = files.DeprecatedFile('/foo/bar')
    self.assertIsNone(memcache.get('/foo/bar'))
    file_obj.Write('Test')
    cache_item = memcache.get(files_cache.FILE_MEMCACHE_PREFIX + '/foo/bar')
    self.assertEntityEqual(file_obj._file, cache_item)

    # Write with changes: should update memcache.
    file_obj = files.DeprecatedFile('/foo/bar')
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
    self.assertEntityEqual(files.DeprecatedFile('/foo')._file, cache_item)
    cache_item = memcache.get(files_cache.FILE_MEMCACHE_PREFIX + '/bar')
    self.assertEntityEqual(files.DeprecatedFile('/bar')._file, cache_item)

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
    files.Delete([files.DeprecatedFile('/foo/bar/baz.html')],
                 update_subdir_caches=True)

  @testing.DisableCaching
  def testPrivateValidatePaths(self):
    # Support of DeprecatedFile objects.
    self.assertEqual('/foo', files.ValidatePaths(files.DeprecatedFile('/foo')))
    self.assertListEqual(['/foo'],
                         files.ValidatePaths([files.DeprecatedFile('/foo')]))
    self.assertListEqual(
        ['/foo', '/bar'],
        files.ValidatePaths([files.DeprecatedFile('/foo'), '/bar']))

    # Invalid paths.
    self.assertRaises(ValueError, files.ValidatePaths, None)
    self.assertRaises(ValueError, files.ValidatePaths, '')
    self.assertRaises(ValueError, files.ValidatePaths, '//')
    self.assertRaises(ValueError, files.ValidatePaths, '/..')

def main(unused_argv):
  basetest.main()

if __name__ == '__main__':
  app.run()
