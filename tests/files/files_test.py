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

import cPickle as pickle
import copy
import datetime
import hashlib
from google.appengine.api import files as blobstore_files
from google.appengine.ext import blobstore
from titan.common.lib.google.apputils import app
from titan.common.lib.google.apputils import basetest
from titan.files import files
from titan.common import utils
from titan import users

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
    files.unregister_file_factory()
    super(FileTestCase, self).tearDown()

  def testFile(self):
    meta = {'color': 'blue', 'flag': False}
    titan_file = files.File('/foo/bar.html')
    titan_file.write('Test', meta=meta)

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
    self.assertEqual('bar', titan_file.name_clean)
    self.assertEqual('.html', titan_file.extension)
    self.assertTrue(titan_file.is_loaded)
    self.assertIsNotNone(titan_file._file_ent)

    # write().
    self.assertEqual(titan_file.content, 'Test')
    titan_file.write('New content')
    self.assertEqual(titan_file.content, 'New content')
    titan_file.write('')
    self.assertEqual(titan_file.content, '')
    # Check meta data.
    self.assertEqual('blue', titan_file.meta.color)
    self.assertEqual(False, titan_file.meta.flag)

    # delete().
    self.assertTrue(titan_file.exists)
    titan_file.delete()
    self.assertFalse(titan_file.exists)
    titan_file.write(content='Test', meta=meta)
    titan_file.delete()
    self.assertFalse(titan_file.exists)

    # __hash__().
    self.assertEqual(hash(files.File('/foo')), hash(files.File('/foo')))
    self.assertNotEqual(hash(files.File('/foo')), hash(files.File('/bar')))
    self.assertNotEqual(
        hash(files.File('/foo')), hash(files.File('/foo', namespace='aaa')))

    # serialize().
    titan_file = files.File('/foo/bar/baz').write('', meta=meta)
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
                     files.File('/foo/bar/baz').serialize(full=True))

    # Properties: name, name_clean, extension, paths, mime_type, created,
    # modified, blob, created_by, modified_by, and size.
    titan_file = files.File('/foo/bar/baz.html')
    self.assertEqual('baz.html', titan_file.name)
    self.assertEqual('baz', titan_file.name_clean)
    self.assertEqual('.html', titan_file.extension)
    # Check bool handling:
    self.assertFalse(titan_file)
    titan_file.write('')
    self.assertTrue(titan_file)
    self.assertEqual(['/', '/foo', '/foo/bar'], titan_file.paths)
    self.assertEqual('text/html', titan_file.mime_type)
    self.assertTrue(isinstance(titan_file.created, datetime.datetime))
    self.assertTrue(isinstance(titan_file.modified, datetime.datetime))
    self.assertIsNone(titan_file.blob)
    self.assertEqual(users.TitanUser('titanuser@example.com'),
                     titan_file.created_by)
    self.assertEqual(users.TitanUser('titanuser@example.com'),
                     titan_file.modified_by)
    # Size:
    titan_file.write('foo')
    self.assertEqual(3, titan_file.size)
    titan_file.write(u'f♥♥')
    # "size" should represent the number of bytes, not the number of characters.
    # 'f♥♥' == 'f\xe2\x99\xa5\xe2\x99\xa5' == 1 + 3 + 3 == 7
    self.assertEqual(7, titan_file.size)
    # "size" should use blob size if present:
    titan_file.write(LARGE_FILE_CONTENT)
    self.assertEqual(1 << 21, titan_file.size)

    # read() and content property.
    self.assertEqual(titan_file.content, titan_file.read())

    # close().
    self.assertIsNone(titan_file.close())

    # Error handling: init with non-existent path.
    titan_file = files.File('/foo/fake.html')
    self.assertRaises(files.BadFileError, lambda: titan_file.paths)
    self.assertRaises(files.BadFileError, lambda: titan_file.content)
    self.assertRaises(files.BadFileError, titan_file.delete)
    self.assertRaises(files.BadFileError, titan_file.serialize)

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
        created_by=users.TitanUser('titanuser@example.com'),
        modified_by=users.TitanUser('titanuser@example.com'),
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
    actual_file = files.File('/foo/bar.html').write('Test', meta=meta)
    self.assertNdbEntityEqual(expected_file, actual_file._file, ignore=dates)
    self.assertNotEqual(None, actual_file.modified, 'modified is not being set')

    # Synchronous update without changes.
    actual_file = files.File('/foo/bar.html').write(meta=meta)
    self.assertNdbEntityEqual(expected_file, actual_file._file, ignore=dates)

    # Synchronous update with changes.
    old_modified = actual_file.modified
    actual_file = files.File('/foo/bar.html')
    actual_file.write('New content', meta=new_meta, mime_type='fake/type')
    expected_file.content = 'New content'
    expected_file.md5_hash = hashlib.md5('New content').hexdigest()
    expected_file.flag = True
    expected_file.mime_type = 'fake/type'
    self.assertNdbEntityEqual(expected_file, actual_file._file, ignore=dates)
    self.assertNotEqual(old_modified, actual_file.modified)

    # Allow writing blank files.
    actual_file = files.File('/foo/bar.html').write('')
    self.assertEqual(actual_file.content, '')

    # Allow overwriting mime_type and meta without touching content.
    files.File('/foo/bar.html').write(content='Test')
    actual_file = files.File('/foo/bar.html').write(mime_type='fake/mimetype')
    self.assertEqual('fake/mimetype', actual_file.mime_type)
    self.assertEqual('Test', actual_file.content)

    actual_file = files.File('/foo/bar.html').write(meta=new_meta)
    self.assertEqual(True, actual_file.meta.flag)
    self.assertEqual('Test', actual_file.content)

    # Allow overwriting created and modified without touching content.
    files.File('/foo/bar.html').write(content='Test')
    now = datetime.datetime.now() + datetime.timedelta(days=1)
    actual_file = files.File('/foo/bar.html').write(created=now, modified=now)
    self.assertEqual(now, actual_file.created)
    self.assertEqual(now, actual_file.modified)
    # Verify the same behavior for the file creation codepath.
    files.File('/foo/bar.html').delete().write(
        'Test', created=now, modified=now)
    self.assertEqual(now, actual_file.created)
    self.assertEqual(now, actual_file.modified)
    # Error handling.
    self.assertRaises(ValueError, files.File('/a').write, '', created='foo')
    self.assertRaises(ValueError, files.File('/a').write, '', modified='foo')

    # Allow overwriting created_by and modified_by without touching content.
    files.File('/foo/bar.html').write(content='Test')
    user = users.TitanUser('bob@example.com')  # Not the current logged in user.
    actual_file = files.File('/foo/bar.html').write(
        created_by=user, modified_by=user)
    self.assertEqual(user, actual_file.created_by)
    self.assertEqual(user, actual_file.modified_by)
    # Verify the same behavior for the file creation codepath.
    files.File('/foo/bar.html').delete().write(
        'Test', created_by=user, modified_by=user)
    self.assertEqual(user, actual_file.created_by)
    self.assertEqual(user, actual_file.modified_by)
    # Error handling.
    self.assertRaises(ValueError, files.File('/a').write, '', created_by='foo')
    self.assertRaises(ValueError, files.File('/a').write, '', modified_by='foo')

    # Cleanup.
    expected_file = original_expected_file
    files.File('/foo/bar.html').delete()

    # write large content to blobstore.
    titan_file = files.File('/foo/bar.html').write(content=LARGE_FILE_CONTENT)
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
    titan_file = files.File('/foo/bar.html').write(content=LARGE_FILE_CONTENT)
    blob_key = titan_file.blob.key()
    self.assertEqual(old_blob_key, blob_key)
    self.assertEqual(LARGE_FILE_CONTENT, titan_file.content)
    self.assertIsNone(titan_file._file_ent.content)
    self.assertEqual(LARGE_FILE_CONTENT, files.File('/foo/bar.html').content)
    self.stubs.SmartUnsetAll()

    # write with a blob key and encoding; verify proper decoding.
    encoded_foo = u'f♥♥'.encode('utf-8')
    blob_key = utils.write_to_blobstore(encoded_foo)
    titan_file = files.File('/foo/bar.html')
    # Verify that without encoding, the encoded bytestring is returned.
    titan_file.write(blob=blob_key)
    self.assertEqual(encoded_foo, titan_file.content)
    # Verify that with encoding, a unicode string is returned.
    titan_file.write(blob=blob_key, encoding='utf-8')
    self.assertEqual(u'f♥♥', titan_file.content)
    # Argument error handling for mixing encoding and unicode content:
    self.assertRaises(TypeError, titan_file.write, content=u'Test',
                      encoding='utf-8')

    # Make sure the blob is deleted with the file:
    titan_file.delete()
    self.assertIsNone(blobstore.get(blob_key))
    self.assertRaises(files.BadFileError, lambda: titan_file.blob)
    # Make sure the blob is deleted if the file gets smaller:
    titan_file = files.File('/foo/bar.html').write(content=LARGE_FILE_CONTENT)
    blob_key = titan_file.blob.key()
    titan_file.write(content='Test')
    self.assertIsNone(blobstore.get(blob_key))
    # Test the current object and a new instance:
    self.assertEqual('Test', titan_file.content)
    self.assertEqual('Test', files.File('/foo/bar.html').content)
    self.assertIsNone(titan_file.blob)
    self.assertIsNone(files.File('/foo/bar.html').blob)

    # write with a BlobKey:
    titan_file = files.File('/foo/bar.html').write(blob=self.blob_key)
    blob_content = self.blob_reader.read()
    # Test the current object and a new instance:
    self.assertEqual(blob_content, files.File('/foo/bar.html').content)
    self.assertEqual(blob_content, titan_file.content)
    self.assertEqual(hashlib.md5('Blobstore!').hexdigest(),
                     titan_file.md5_hash)

    # Cleanup.
    expected_file = original_expected_file
    files.File('/foo/bar.html').delete()

    # Error handling:
    # Updating mime_type or meta when entity doesn't exist.
    titan_file = files.File('/fake/file')
    self.assertRaises(files.BadFileError, titan_file.write, meta=meta)
    self.assertRaises(files.BadFileError, titan_file.write,
                      mime_type='fake/mimetype')

    # Bad arguments:
    self.assertRaises(TypeError, titan_file.write)
    self.assertRaises(TypeError, titan_file.write, content=None, blob=None)
    self.assertRaises(TypeError, titan_file.write, content='Test',
                      blob=self.blob_key)

    # There are some reserved words that cannot be used in meta properties.
    invalid_meta_keys = [
        # Titan reserved:
        'name',
        'path',
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
        # NDB reserved:
        'key',
        'app',
        'id',
        'parent',
        'namespace',
        'projection',
    ]
    for key in invalid_meta_keys:
      try:
        titan_file.write(content='', meta={key: ''})
      except files.InvalidMetaError:
        pass
      else:
        self.fail(
            'Invalid meta key should have failed: {!r}'.format(key))

  def testDelete(self):
    # Synchronous delete.
    titan_file = files.File('/foo/bar.html').write('')
    self.assertTrue(titan_file.exists)
    titan_file.delete()
    # Recreate object, just in case memoization is hiding an error:
    self.assertFalse(files.File('/foo/bar.html').exists)

    # Error handling.
    self.assertRaises(files.BadFileError, files.File('/fake.html').delete)

  def testFileMixins(self):
    # Support behavior: subclass File and make write() also touch a
    # centralized file, while avoiding infinite recursion.

    class TouchRootMixin(files.File):

      def write(self, *args, **kwargs):
        # Note: this mixin is a bad idea in practice. Don't directly touch a
        # centralized file on every write, due to write rate limits.
        if self.path != '/root-touched-file':
          files.File('/root-touched-file').write('')
        super(TouchRootMixin, self).write(*args, **kwargs)

    class CustomFile(TouchRootMixin, files.File):
      pass

    custom_file = CustomFile('/foo/bar')
    custom_file.write('foo')
    self.assertTrue(custom_file.exists)
    self.assertTrue(files.File('/foo/bar').exists)
    self.assertTrue(files.File('/root-touched-file').exists)

  def testCopyTo(self):
    files.File('/foo.html').write('Test', mime_type='test/mimetype',
                                  meta={'color': 'blue'})
    files.File('/foo.html').copy_to(files.File('/bar/qux.html'))
    titan_file = files.File('/bar/qux.html')
    self.assertEqual('/bar/qux.html', titan_file.path)
    self.assertEqual(['/', '/bar'], titan_file.paths)
    self.assertEqual('test/mimetype', titan_file.mime_type)
    self.assertEqual('Test', titan_file.content)
    self.assertEqual('blue', titan_file.meta.color)

    # Exclude some meta properties.
    meta = {
        'color': 'blue',
        'size': 'large',
        'full_name': 'John Doe',
    }
    titan_file = files.File('/meta.html').write('hello', meta=meta)
    dest_file = files.File('/meta2.html')
    titan_file.copy_to(dest_file, exclude_meta=['full_name'])
    self.assertEqual('blue', dest_file.meta.color)
    self.assertEqual('large', dest_file.meta.size)
    self.assertRaises(AttributeError, lambda: dest_file.meta.full_name)

    # Blobs instead of content.
    files.File('/foo.html').delete()
    files.File('/foo.html').write(blob=self.blob_key, meta={'flag': False})
    files.File('/foo.html').copy_to(files.File('/bar/qux.html'))
    titan_file = files.File('/bar/qux.html')
    blob_content = self.blob_reader.read()
    self.assertEqual(blob_content, titan_file.content)
    self.assertEqual('text/html', titan_file.mime_type)
    self.assertEqual(False, titan_file.meta.flag)
    # Copy should overwrite previous versions and their properties.
    self.assertRaises(AttributeError, lambda: titan_file.meta.color)

    # Error handling:
    self.assertRaises(AssertionError, files.File('/foo.html').copy_to, '/test')
    self.assertRaises(files.CopyFileError, files.File('/fake').copy_to,
                      files.File('/test/fake'))
    # Verify that the exception is populated with the failed destination file.
    try:
      files.File('/fake').copy_to(files.File('/test/fake'))
    except files.CopyFileError as e:
      self.assertEqual('/test/fake', e.titan_file.path)

  def testMoveTo(self):
    files.File('/foo.html').write('Test', meta={'color': 'blue'})
    files.File('/foo.html').move_to(files.File('/bar/qux.html'))
    self.assertFalse(files.File('/foo.html').exists)
    self.assertTrue(files.File('/bar/qux.html').exists)

    # Blob instead of content.
    files.File('/foo.html').write(blob=self.blob_key, meta={'flag': False})
    files.File('/foo.html').move_to(files.File('/bar/qux.html'))
    titan_file = files.File('/bar/qux.html')
    blob_content = self.blob_reader.read()
    self.assertEqual(blob_content, titan_file.content)
    self.assertEqual('text/html', titan_file.mime_type)
    self.assertEqual(False, titan_file.meta.flag)
    self.assertRaises(AttributeError, lambda: titan_file.meta.color)

    self.assertRaises(AssertionError, files.File('/foo.html').move_to, '/test')

class NamespaceTestCase(testing.BaseTestCase):

  def write_namespace_testdata(self):
    files.File('/foo').write('foo')
    files.File('/b/bar').write('bar')
    # 'aaa' namespace, overlapping filenames with the default namespace.
    files.File('/foo', namespace='aaa').write('aaa-foo')
    files.File('/b/bar', namespace='aaa').write('aaa-bar')
    # 'bbb' namespace, no overlapping filenames.
    files.File('/b/qux', namespace='bbb').write('bbb-qux')

  def testNamespaces(self):
    self.write_namespace_testdata()

    # Verify the state of the filesystem in the default namespace.
    self.assertEqual('foo', files.File('/foo').content)
    self.assertEqual('bar', files.File('/b/bar').content)
    self.assertFalse(files.File('/b/qux').exists)

    titan_files = files.Files.list('/', recursive=True)
    self.assertEqual({'/foo', '/b/bar'}, set(titan_files))

    titan_files = files.Files(paths=['/foo', '/b/bar', '/b/qux']).load()
    self.assertEqual({'/foo', '/b/bar'}, set(titan_files))

    # Verify the state of the filesystem in the 'aaa' namespace.
    self.assertEqual('aaa-foo', files.File('/foo', namespace='aaa').content)
    self.assertEqual('aaa-bar', files.File('/b/bar', namespace='aaa').content)
    self.assertFalse(files.File('/b/qux', namespace='aaa').exists)

    titan_files = files.Files.list('/', recursive=True, namespace='aaa')
    self.assertEqual({'/foo', '/b/bar'}, set(titan_files))
    self.assertEqual('aaa-foo', titan_files['/foo'].content)
    self.assertEqual('aaa-bar', titan_files['/b/bar'].content)

    titan_files = files.Files(
        paths=['/foo', '/b/bar', '/b/qux'], namespace='aaa').load()
    self.assertEqual({'/foo', '/b/bar'}, set(titan_files))
    self.assertEqual('aaa-foo', titan_files['/foo'].content)
    self.assertEqual('aaa-bar', titan_files['/b/bar'].content)

    # Verify the state of the filesystem in the 'bbb' namespace.
    self.assertEqual('bbb-qux', files.File('/b/qux', namespace='bbb').content)
    self.assertFalse(files.File('/foo', namespace='bbb').exists)
    self.assertFalse(files.File('/b/bar', namespace='bbb').exists)

    titan_files = files.Files.list('/', recursive=True, namespace='bbb')
    self.assertEqual({'/b/qux'}, set(titan_files))
    self.assertEqual('bbb-qux', titan_files['/b/qux'].content)

    titan_files = files.Files(
        paths=['/foo', '/b/bar', '/b/qux'], namespace='bbb').load()
    self.assertEqual({'/b/qux'}, set(titan_files))
    self.assertEqual('bbb-qux', titan_files['/b/qux'].content)

    # Namespace is not affected by file existence.
    self.assertIsNone(files.File('/foo').namespace)
    self.assertIsNone(files.File('/fake').namespace)
    self.assertEqual('aaa', files.File('/foo', namespace='aaa').namespace)
    self.assertEqual('zzz', files.File('/fake', namespace='zzz').namespace)

    # Cannot mix namespaces in files.Files.
    titan_files = files.Files()
    with self.assertRaises(files.NamespaceMismatchError):
      other_files = files.Files(paths=['/foo'], namespace='aaa')
      titan_files.update(other_files)

    # Files are not the same if their namespace is different.
    self.assertNotEqual(files.File('/foo'), files.File('/foo', namespace='aaa'))
    self.assertNotEqual(
        files.File('/foo', namespace='aaa'),
        files.File('/foo', namespace='zzz'))

    # Error handling (more extensive namespace validate tests in utils_test.py).
    self.assertRaises(ValueError, files.File, '/a', namespace='/')
    self.assertRaises(ValueError, files.File, '/a', namespace=u'∆')

  def testCopyInNamespace(self):
    self.write_namespace_testdata()

    # Default namespace --> 'aaa' namespace.
    files.File('/foo').copy_to(files.File('/foo-copy', namespace='aaa'))
    self.assertEqual('foo', files.File('/foo-copy', namespace='aaa').content)
    self.assertFalse(files.File('/foo-copy').exists)

    # 'aaa' namespace --> default namespace.
    self.assertFalse(files.File('/foo-copy').exists)
    files.File('/foo', namespace='aaa').copy_to(files.File('/foo-copy'))
    self.assertEqual('aaa-foo', files.File('/foo-copy').content)

    # 'aaa' namespace --> 'bbb' namespace.
    self.assertFalse(files.File('/foo-copy', namespace='bbb').exists)
    files.File('/foo', namespace='aaa').copy_to(
        files.File('/foo-copy', namespace='bbb'))
    self.assertEqual(
        'aaa-foo', files.File('/foo-copy', namespace='bbb').content)

  def testMoveInNamespace(self):
    self.write_namespace_testdata()

    # Default namespace --> 'aaa' namespace.
    files.File('/foo').move_to(files.File('/foo-moved', namespace='aaa'))
    self.assertEqual('foo', files.File('/foo-moved', namespace='aaa').content)
    self.assertFalse(files.File('/foo-moved').exists)
    self.assertFalse(files.File('/foo').exists)

    # 'aaa' namespace --> default namespace.
    self.assertFalse(files.File('/foo-moved').exists)
    files.File('/foo', namespace='aaa').move_to(files.File('/foo-moved'))
    self.assertEqual('aaa-foo', files.File('/foo-moved').content)

    # 'aaa' namespace --> 'bbb' namespace.
    self.assertFalse(files.File('/b/bar-moved', namespace='bbb').exists)
    files.File('/b/bar', namespace='aaa').move_to(
        files.File('/b/bar-moved', namespace='bbb'))
    self.assertEqual(
        'aaa-bar', files.File('/b/bar-moved', namespace='bbb').content)

  def testMultiCopyInNamespace(self):
    # Default namespace --> 'aaa' namespace.
    self.write_namespace_testdata()
    titan_files = files.Files.list('/', recursive=True).load()
    titan_files.copy_to('/', namespace='aaa')

    titan_files = files.Files.list('/', recursive=True, namespace='aaa').load()
    self.assertEqual({'/foo', '/b/bar'}, set(titan_files))
    self.assertEqual('foo', titan_files['/foo'].content)
    self.assertEqual('bar', titan_files['/b/bar'].content)

    # 'aaa' namespace --> 'bbb' namespace.
    self.write_namespace_testdata()
    titan_files = files.Files.list('/', namespace='aaa', recursive=True).load()
    titan_files.copy_to('/', namespace='bbb')

    titan_files = files.Files.list('/', recursive=True, namespace='bbb').load()
    self.assertEqual({'/foo', '/b/bar', '/b/qux'}, set(titan_files))
    self.assertEqual('aaa-foo', titan_files['/foo'].content)
    self.assertEqual('aaa-bar', titan_files['/b/bar'].content)
    self.assertEqual('bbb-qux', titan_files['/b/qux'].content)

  def testMultiMoveInNamespace(self):
    # Default namespace --> 'aaa' namespace.
    self.write_namespace_testdata()
    titan_files = files.Files.list('/', recursive=True).load()
    titan_files.move_to('/', namespace='aaa')

    titan_files = files.Files.list('/', recursive=True, namespace='aaa').load()
    self.assertEqual({'/foo', '/b/bar'}, set(titan_files))
    self.assertEqual('foo', titan_files['/foo'].content)
    self.assertEqual('bar', titan_files['/b/bar'].content)
    titan_files = files.Files.list('/', recursive=True).load()
    self.assertEqual(set(), set(titan_files))

    # 'aaa' namespace --> 'bbb' namespace.
    self.write_namespace_testdata()
    titan_files = files.Files.list('/', namespace='aaa', recursive=True).load()
    titan_files.move_to('/', namespace='bbb')

    titan_files = files.Files.list('/', recursive=True, namespace='bbb').load()
    self.assertEqual({'/foo', '/b/bar', '/b/qux'}, set(titan_files))
    self.assertEqual('aaa-foo', titan_files['/foo'].content)
    self.assertEqual('aaa-bar', titan_files['/b/bar'].content)
    self.assertEqual('bbb-qux', titan_files['/b/qux'].content)

class MixinsTestCase(testing.BaseTestCase):

  def testRegisterFileFactory(self):

    class FooFile(files.File):
      pass

    class BarFile(files.File):
      pass

    def TitanFileFactory(path, **unused_kwargs):
      if path.startswith('/foo/files/'):
        return FooFile
      elif path.startswith('/bar/files/'):
        return BarFile
      return files.File

    files.register_file_factory(TitanFileFactory)
    foo_file = files.File('/foo/files/a')
    bar_file = files.File('/bar/files/b')
    normal_file = files.File('/c')
    self.assertTrue(isinstance(foo_file, FooFile))
    self.assertTrue(isinstance(bar_file, BarFile))
    self.assertTrue(isinstance(normal_file, files.File))

  def testRegisterFileMixins(self):

    class FooFileMixin(files.File):
      pass

    class BarFileMixin(files.File):

      @classmethod
      def should_apply_mixin(cls, **kwargs):
        if kwargs['path'].startswith('/bar/files/'):
          return True
        return False

    files.register_file_mixins([FooFileMixin, BarFileMixin])

    # Pickle and unpickle each file to verify __reduce__ behavior.
    foo_file = pickle.loads(pickle.dumps(files.File('/foo/files/a')))
    bar_file = pickle.loads(pickle.dumps(files.File('/bar/files/b')))

    self.assertTrue(isinstance(foo_file, FooFileMixin))
    self.assertFalse(isinstance(foo_file, BarFileMixin))

    self.assertTrue(isinstance(bar_file, BarFileMixin))
    self.assertTrue(isinstance(bar_file, FooFileMixin))

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
    root_and_first_levels = files.Files.merge(root_level, first_level)
    first_and_second_levels = files.Files.merge(first_level, second_level)

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
      titan_file.write('')

    # Empty.
    self.assertSameObjects(files.Files(), files.Files.list('/fake/path'))
    self.assertSameObjects(files.Files([]), files.Files.list('/fake/path'))

    # From root.
    self.assertSameObjects(root_level, files.Files.list('/'))
    titan_files = files.Files.list('/', recursive=True)
    self.assertSameObjects(all_files, titan_files)

    # From first level dir.
    self.assertSameObjects(first_level, files.Files.list('/foo'))
    self.assertSameObjects(first_level, files.Files.list('/foo/'))
    titan_files = files.Files.list('/foo', recursive=True)
    self.assertSameObjects(first_and_second_levels, titan_files)

    # From second level dir.
    self.assertSameObjects(second_level, files.Files.list('/foo/bar'))
    titan_files = files.Files.list('/foo/bar', recursive=True)
    self.assertSameObjects(second_level, titan_files)

    # Limit recursion depth.
    titan_files = files.Files.list('/', recursive=True, depth=1)
    self.assertSameObjects(root_and_first_levels, titan_files)
    titan_files = files.Files.list('/', recursive=True, depth=2)
    self.assertSameObjects(all_files, titan_files)
    titan_files = files.Files.list('/foo/', recursive=True, depth=1)
    self.assertSameObjects(first_and_second_levels, titan_files)

    # Limit the number of files returned.
    titan_files = files.Files.list('/foo', recursive=True, limit=1)
    self.assertEqual(1, len(titan_files))

    # Support trailing slashes.
    self.assertSameObjects(second_level, files.Files.list('/foo/bar/'))
    titan_files = files.Files.list('/foo/bar/', recursive=True)
    self.assertSameObjects(second_level, titan_files)

    # Custom filters:
    files.File('/a/foo').write('', meta={'color': 'red', 'count': 1})
    files.File('/a/bar/qux').write('', meta={'color': 'red', 'count': 2})
    files.File('/a/baz').write('', meta={'color': 'blue', 'count': 3})
    # Single filter:
    filters = [files.FileProperty('color') == 'red']
    titan_files = files.Files.list('/a', filters=filters)
    self.assertSameObjects(['/a/foo'], titan_files)
    # Multiple filters:
    filters = [
        files.FileProperty('color') == 'blue',
        files.FileProperty('count') == 3,
    ]
    titan_files = files.Files.list('/', recursive=True, filters=filters)
    self.assertEqual(files.Files(['/a/baz']), titan_files)
    # Recursive:
    filters = [files.FileProperty('color') == 'red']
    titan_files = files.Files.list('/', recursive=True, filters=filters)
    self.assertEqual(files.Files(['/a/foo', '/a/bar/qux']), titan_files)
    # Non-meta property:
    user = users.TitanUser('titanuser@example.com')
    filters = [
        files.FileProperty('created_by') == str(user),
        files.FileProperty('count') == 2,
    ]
    titan_files = files.Files.list('/a/', recursive=True, filters=filters)
    self.assertEqual(files.Files(['/a/bar/qux']), titan_files)

    # Error handling.
    self.assertRaises(ValueError, files.Files.list, '')
    self.assertRaises(ValueError, files.Files.list, '//')
    self.assertRaises(ValueError, files.Files.list, '/..')
    self.assertRaises(ValueError, files.Files.list, '/',
                      recursive=True, depth=0)
    self.assertRaises(ValueError, files.Files.list, '/',
                      recursive=False, depth=1)

  def testFilesCount(self):
    # Create files for testing.
    root_level = files.Files(['/index.html', '/qux'])
    first_level = files.Files(['/foo/bar'])
    second_level = files.Files([
        '/foo/bar/baz',
        '/foo/bar/baz.html',
        '/foo/bar/baz.txt',
    ])
    root_and_first_levels = files.Files.merge(root_level, first_level)
    all_files = files.Files(root_level.keys() +
                            first_level.keys() +
                            second_level.keys())

    for titan_file in all_files.itervalues():
      titan_file.write('')

    # Empty.
    self.assertEqual(0, files.Files.count('/fake/path'))

    # From root.
    self.assertEqual(len(root_level), files.Files.count('/'))
    self.assertEqual(len(all_files), files.Files.count('/', recursive=True))

    # Limit recursion depth.
    self.assertEqual(len(root_and_first_levels),
                     files.Files.count('/', recursive=True, depth=1))

    # Custom filters:
    files.File('/a/foo').write('', meta={'color': 'red', 'item_id': 1})
    files.File('/a/bar/qux').write('', meta={'color': 'red', 'item_id': 2})
    files.File('/a/baz').write('', meta={'color': 'blue', 'item_id': 3})
    # Single filter:
    filters = [files.FileProperty('color') == 'blue']
    self.assertEqual(1, files.Files.count('/', recursive=True, filters=filters))
    # Multiple filters:
    filters = [
        files.FileProperty('color') == 'red',
        files.FileProperty('item_id') == 2,
    ]
    self.assertEqual(1, files.Files.count('/', recursive=True, filters=filters))

  def testOrderedFiles(self):
    # Create files for testing.
    root_level = files.OrderedFiles([
        # These need to be alphabetically ordered here because they will be
        # usually returned that way from queries inside files.Files.list(),
        # except for when other filters are applied.
        '/bar',
        '/baz',
        '/foo',
    ])
    for titan_file in root_level.itervalues():
      titan_file.write('')

    # Verify that equality handles order checking. Do this first to make sure
    # that following assertEqual() calls are also checking for order.
    root_level_same_order = files.OrderedFiles([
        # Intentionally not the same order, to test Sort() right below.
        '/baz',
        '/foo',
        '/bar',
    ])
    root_level_same_order.sort()
    root_level_different_order = files.OrderedFiles([
        '/foo',
        '/baz',
        '/bar',
    ])
    self.assertEqual(root_level, root_level_same_order)
    self.assertNotEqual(root_level, root_level_different_order)
    self.assertNotEqual(files.OrderedFiles([]), root_level)

    # Test updating and removing items.
    new_root_level = files.OrderedFiles([
        '/bar',
        '/baz',
        '/foo',
    ])
    new_root_level.update(files.Files(['/qux']))
    self.assertNotEqual(root_level, new_root_level)
    del new_root_level['/qux']
    self.assertEqual(root_level, new_root_level)

    # Test files.OrderedFiles.list().
    self.assertEqual(root_level, files.OrderedFiles.list('/'))
    self.assertNotEqual(root_level_different_order,
                        files.OrderedFiles.list('/'))

    # Test files.OrderedFiles.list() with order= kwarg.
    self.Login('bob@example.com')
    files.File('/a/middle').write('')
    self.Login('charlie@example.com')
    files.File('/a/last').write('')
    self.Login('alice@example.com')
    files.File('/a/first').write('')
    order = [files.FileProperty('created_by')]
    results = files.OrderedFiles.list('/a', order=order)
    expected = files.OrderedFiles([
        '/a/first',
        '/a/middle',
        '/a/last',
    ])
    self.assertEqual(expected, results)

    # Test reverse order.
    order = [-files.FileProperty('created_by')]
    results = files.OrderedFiles.list('/a', order=order)
    expected = files.OrderedFiles([
        '/a/last',
        '/a/middle',
        '/a/first',
    ])
    self.assertEqual(expected, results)

    # Error handling.
    self.assertRaises(
        AttributeError, new_root_level.__setitem__, '/qux', files.File('/qux'))

  def testCopyTo(self):
    # Populate the in-context cache by reading the file before creation.
    self.assertFalse(files.File('/x/b/foo').exists)

    files.File('/foo').write('')
    files.File('/a/foo').write('')
    files.File('/a/b/foo').write('')
    files.File('/c/foo').write('')

    result_files = files.Files()
    failed_files = files.Files()
    files.Files.list('/').copy_to(dir_path='/x', result_files=result_files,
                                 failed_files=failed_files)
    expected_paths = [
        '/x/foo',
    ]
    self.assertSameElements(expected_paths, result_files.keys())
    self.assertEqual([], failed_files.keys())
    titan_files = files.Files.list('/a/', recursive=True)
    titan_files.copy_to('/x', strip_prefix='/a/', result_files=result_files)
    expected_paths = [
        '/x/foo',
        '/x/b/foo',
    ]
    self.assertSameElements(expected_paths, result_files.keys())
    # With trailing slashes should be the same.
    result_files.clear()
    titan_files.copy_to('/x/', strip_prefix='/a/', result_files=result_files)
    self.assertSameElements(expected_paths, result_files.keys())

    result_files = files.Files()
    failed_files = files.Files()
    files_paths = ['/foo', '/fake']
    self.assertRaises(files.CopyFilesError, files.Files(files_paths).copy_to,
                      dir_path='/x', result_files=result_files,
                      failed_files=failed_files)
    expected_paths = [
        '/x/foo',
    ]
    failed_paths = [
        '/x/fake',
    ]
    self.assertSameElements(expected_paths, result_files.keys())
    self.assertSameElements(failed_paths, failed_files.keys())

    # Verify that the NDB in-context cache was cleared correctly.
    self.assertTrue(files.File('/x/b/foo').exists)

  def testMoveTo(self):
    # Populate the in-context cache by reading the file before creation.
    self.assertFalse(files.File('/x/b/foo').exists)

    files.File('/foo').write('')
    files.File('/a/foo').write('')
    files.File('/a/b/foo').write('')
    files.File('/c/foo').write('')

    result_files = files.Files()
    titan_files = files.Files.list('/a/', recursive=True)
    titan_files.move_to('/x', strip_prefix='/a/', result_files=result_files)
    expected_paths = [
        '/x/foo',
        '/x/b/foo',
    ]
    self.assertSameElements(expected_paths, result_files.keys())

    # Verify files has been deleted from the old directory.
    self.assertFalse(files.File('/a/foo').exists)
    self.assertFalse(files.File('/a/b/foo').exists)

    result_files = files.Files()
    failed_files = files.Files()
    files_paths = ['/foo', '/fake']
    self.assertRaises(files.CopyFilesError, files.Files(files_paths).move_to,
                      dir_path='/x', result_files=result_files,
                      failed_files=failed_files)
    expected_paths = [
        '/x/foo',
    ]
    failed_paths = [
        '/x/fake',
    ]
    self.assertSameElements(expected_paths, result_files.keys())
    self.assertSameElements(failed_paths, failed_files.keys())

    # Verify files has been deleted from the old directory.
    self.assertFalse(files.File('/foo').exists)

    # Verify that the NDB in-context cache was cleared correctly.
    self.assertTrue(files.File('/x/b/foo').exists)

  def testLoad(self):
    files.File('/foo').write('')
    files.File('/bar').write('')
    titan_files = files.Files(paths=['/foo', '/bar', '/fake'])
    titan_files.load()

    self.assertIn('/foo', titan_files)
    self.assertIn('/bar', titan_files)
    self.assertTrue(titan_files['/foo'].is_loaded)
    self.assertTrue(titan_files['/bar'].is_loaded)
    self.assertNotIn('/fake', titan_files)

  def testDelete(self):
    files.File('/foo').write('')
    files.File('/bar').write(LARGE_FILE_CONTENT)
    files.File('/qux').write('')
    blob_key = files.File('/bar').blob.key()
    files.Files(['/foo', '/bar']).delete()
    self.assertEqual(
        files.Files(['/qux']), files.Files(['/foo', '/bar', '/qux']).load())
    # Verify that the blob is also deleted.
    self.assertIsNone(blobstore.get(blob_key))

  def testSerialize(self):
    # serialize().
    first_file = files.File('/foo/bar').write('foobar')
    second_file = files.File('/foo/bat/baz').write('foobatbaz')
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
                     files.Files.list(dir_path='/foo/',
                                      recursive=True).serialize(full=True))

class FileCacheTestCase(testing.BaseTestCase):

  def testCacheHelpers(self):
    result = files._store_blob_cache('/foo.html', 'Test')
    self.assertTrue(result)
    self.assertEqual('Test', files._get_blob_cache('/foo.html'))
    files._clear_blob_cache_for_paths(['/foo.html'])
    self.assertIsNone(files._get_blob_cache('/foo.html'))

def main(unused_argv):
  basetest.main()

if __name__ == '__main__':
  app.run()
