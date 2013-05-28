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

"""Core functionality for Titan Files command-line.

The objects here are decoupled from appcommands and flags.
"""

import os
import sys
import time

from concurrent import futures

import logging
from titan.common import runners
from titan.common import utils
from titan.files import files_client

NUM_THREADS = 100

API_PATH_BASE = files_client.FILE_API_PATH_BASE

# The cutoff size of when to upload a local file directly to blobstore.
# This value should match files.MAX_CONTENT_SIZE.
DIRECT_TO_BLOBSTORE_SIZE = 1 << 19  # 500 KiB

class Error(Exception):
  pass

class UploadFileError(Error):

  def __init__(self, message, target_path):
    self.target_path = target_path
    super(UploadFileError, self).__init__(message)

class DownloadFileError(Error):

  def __init__(self, message, target_path):
    self.target_path = target_path
    super(DownloadFileError, self).__init__(message)

class BaseRunner(runners.BaseRunner):
  """Base class for runners."""

  def __init__(self, host, port=None, remote_file_factory=None,
               vcs_factory=None, api_base_path=API_PATH_BASE,
               num_threads=NUM_THREADS, force=False, secure=True, quiet=False,
               *args, **kwargs):
    super(BaseRunner, self).__init__(*args, **kwargs)
    self._host = host
    self.port = port
    self.remote_file_factory = remote_file_factory
    self.vcs_factory = vcs_factory
    self.api_base_path = api_base_path
    self.num_threads = num_threads
    self.force = force
    self.secure = secure
    self.quiet = quiet

  @property
  def host(self):
    return '%s:%d' % (self._host, self.port) if self.port else self._host

  def ThreadPoolExecutor(self):
    return futures.ThreadPoolExecutor(max_workers=self.num_threads)

class BaseRunnerWithVersions(BaseRunner):

  def _commit_changeset_or_exit(self, changeset_num, force_commit=False,
                                manifest=None, save_manifest=True):
    """If confirmed, commits the given changeset."""
    assert not force_commit and manifest
    # Confirm action.
    msg = 'Commit changeset %d?' % changeset_num
    if not self.confirm(msg):
      sys.exit('Commit aborted.')

    start = time.time()
    remote_vcs = self.vcs_factory.make_remote_vcs()
    staging_changeset = self.vcs_factory.make_remote_changeset(changeset_num)

    if manifest:
      # We have full knowledge of the files uploaded to a changeset, associate
      # those files to staging_changeset to ensure a strong consistency commit.
      for path in manifest:
        assert not path.startswith('/_titan/ver/')
        remote_file = self.remote_file_factory.make_remote_file(path)
        staging_changeset.associate_file(remote_file)
      staging_changeset.finalize_associated_files()

    final_remote_changeset = remote_vcs.commit(
        staging_changeset, force=force_commit, save_manifest=save_manifest)
    elapsed_time = time.time() - start
    print 'Finished commit in %s.' % utils.humanize_duration(elapsed_time)
    return final_remote_changeset

class UploadRunner(BaseRunnerWithVersions):
  """Uploads files to Titan Files."""

  def Run(self, filenames, root_dir=None, target_path='/', changeset=None,
          commit=False, confirm_manifest=False):
    """Upload runner.

    Args:
      filenames: A list of local filenames to upload.
      root_dir: Local root dir containing all the files. Default: current dir.
      target_path: Remote root target dir for uploaded documents.
      changeset: A changeset number or "new".
      commit: Whether or not to commit the changeset.
      confirm_manifest: Whether or not the current list of files given to
          upload can be trusted as a complete manifest of the files in the
          changeset. Required when combining "commit" and "changeset=<num>".
    Returns:
      A dictionary containing "success", "manifest", "paths", and
      "changeset_num" if uploading to a changeset.
    """
    if root_dir is None:
      root_dir = os.path.curdir

    # Compose mapping of absolute local path to absolute remote path.
    root_dir = os.path.abspath(root_dir)
    filename_to_paths = {}
    for filename in filenames:
      absolute_filename = os.path.abspath(filename)
      if not absolute_filename.startswith(root_dir):
        self.print_error('Path "%s" not contained within "%s".'
                         % (absolute_filename, root_dir))
        sys.exit()
      remote_path = absolute_filename[len(root_dir) + 1:]
      remote_path = utils.safe_join(target_path, remote_path)
      filename_to_paths[absolute_filename] = remote_path

    # Confirm action.
    print '\nUploading files to %s (%s):' % (self.host, self.api_base_path)
    for filename, path in filename_to_paths.iteritems():
      if root_dir == os.path.abspath(os.path.curdir):
        # Strip the root_dir from view if it's already the working directory.
        filename = '.' + filename[len(root_dir):]
      print '  %s --> %s' % (filename, path)
    if not self.confirm('Upload files?'):
      sys.exit('Upload aborted.')

    # Versions Mixin:
    file_kwargs = {}
    changeset_num = None
    if changeset == 'new':
      self.vcs_factory.validate_client_auth()
      vcs = self.vcs_factory.make_remote_vcs()
      staging_changeset = vcs.new_staging_changeset()
      changeset_num = staging_changeset.num
      print 'New staging changeset created: %d' % changeset_num
    elif changeset:
      changeset_num = int(changeset)
    if changeset and changeset_num:
      file_kwargs['changeset'] = changeset_num
      print 'Uploading %d files to changeset %d...' % (len(filenames),
                                                       changeset_num)

    start = time.time()
    self.remote_file_factory.validate_client_auth()
    future_results = []
    with self.ThreadPoolExecutor() as executor:
      for filename, target_path in filename_to_paths.iteritems():
        future = executor.submit(
            self._upload_file, filename, target_path, file_kwargs=file_kwargs)
        future_results.append(future)

    failed = False
    total_bytes = 0
    for future in futures.as_completed(future_results):
      try:
        remote_file = future.result()
        print 'Uploaded %s' % remote_file.real_path
        total_bytes += remote_file.size
      except UploadFileError as e:
        self.print_error('Error uploading %s. Error was: %s %s'
                         % (e.target_path, e.__class__.__name__, str(e)))
        failed = True

    if failed:
      self.print_error('Could not upload one or more files.')
      return

    manifest = filename_to_paths.values()
    if commit:
      if changeset != 'new' and not confirm_manifest:
        self.print_error('Must use --changeset=new with --commit, or pass '
                         '--confirm_manifest.')
        return
      self._commit_changeset_or_exit(changeset_num, manifest=manifest)

    elapsed_time = time.time() - start
    print 'Uploaded %d files in %s.' % (
        len(filenames),
        utils.humanize_duration(elapsed_time))
    result = {}
    if changeset_num:
      result['changeset_num'] = changeset_num
    result['success'] = not failed
    result['manifest'] = manifest
    result['paths'] = filename_to_paths.values()
    return result

  def _upload_file(self, filename, target_path,
                   file_kwargs=None, method_kwargs=None):
    """Uploads a file."""
    try:
      file_kwargs = file_kwargs or {}
      remote_file = self.remote_file_factory.make_remote_file(target_path,
                                                              **file_kwargs)
      with open(filename) as fp:
        method_kwargs = method_kwargs or {}
        if os.path.getsize(filename) > DIRECT_TO_BLOBSTORE_SIZE:
          remote_file.write(fp=fp, **method_kwargs)
        else:
          remote_file.write(content=fp.read(), **method_kwargs)
      # Load the RemoteFile to avoid synchronous fetches in the main thread.
      _ = remote_file.real_path
      return remote_file
    except Exception as e:
      logging.exception('Traceback:')
      raise UploadFileError(e, target_path=target_path)

class DownloadRunner(BaseRunner):
  """Downloads files from Titan files to local directory."""

  def Run(self, dir_path=None, recursive=False, depth=None, file_paths=None,
          target_dir=None):
    """Download runner.

    Args:
      dir_path: A list of remote directories to download.
      recursive: Whether or not to download directory recursively.
      depth: Depth of recursion if specified.
      file_paths: A list of remote files to download.
      target_dir: The target local directory to upload to.
    Returns:
      A list of mappings between remote_path -> local_path.
    Raises:
      DownloadFileError
    """
    if not file_paths and not dir_path:
      self.print_error('No files to download. Use --file_path or '
                       '--dir_path.')
      return

    if target_dir is None:
      target_dir = '.'

    path_map = []
    if file_paths:
      remote_files = self.remote_file_factory.make_remote_files(
          paths=file_paths)
      for remote_file in remote_files.itervalues():
        if not remote_file.exists:
          print 'File %s does not exist' % remote_file.path

        target = os.path.abspath(utils.safe_join(target_dir, remote_file.name))
        path_map.append([remote_file, target])

    elif dir_path:
      dir_kwargs = {'recursive': recursive, 'depth': depth}
      remote_files = self.remote_file_factory.make_remote_files(paths=[])
      remote_files.list(dir_path, **dir_kwargs)

      for remote_file in remote_files.itervalues():
        target = os.path.abspath(
            utils.safe_join(target_dir, remote_file.path[1:]))
        path_map.append([remote_file, target])

    conf_message = ['The following will be downloaded from %s' % self.host]
    for remote_file, target in path_map:
      conf_message.append('  %s --> %s' % (remote_file.path, target))
    print '\n'.join(conf_message)
    if not self.confirm(' Are you sure?'):
      sys.exit('Download aborted.')
    else:
      print 'Downloading...'

    utils.make_dirs(target_dir)

    # Start the download.
    start = time.time()
    self.remote_file_factory.validate_client_auth()
    future_results = []
    with self.ThreadPoolExecutor() as executor:
      for remote_file, target in path_map:

        target_base_dir = os.path.dirname(target)
        utils.make_dirs(target_base_dir)

        future = executor.submit(self._DownloadFile, remote_file, target)
        future_results.append(future)

    failed = False
    for future in futures.as_completed(future_results):
      try:
        downloaded_file = future.result()
        self.print_message(
            'Downloaded %s to %s' %
            (downloaded_file['path'], downloaded_file['target']))
      except DownloadFileError as e:
        self.print_error('Error downloading %s. Error was: %s %s' %
                        (e.target_path, e.__class__.__name__, str(e)))
        failed = True

    if failed:
      self.print_error('Could not download one or more files.')
      return
    elapsed_time = time.time() - start
    print 'Downloaded %d files in %s.' % (len(path_map),
                                          utils.humanize_duration(elapsed_time))

  def _DownloadFile(self, remote_file, target):
    """Downloads a file."""

    content = remote_file.content
    fp = open(target, 'w')
    try:
      fp.write(content)
    finally:
      fp.close()
    return {'path': remote_file.path, 'target': target}

class CommitRunner(BaseRunnerWithVersions):

  def Run(self, changeset, force_commit=False, manifest=None,
          save_manifest=True):
    """Commit runner.

    Args:
      changeset: The changeset number to commit.
      force_commit: Confirmation for passing consistency error message.
      manifest: An optional list of remote paths in the changeset to guarantee
          a strongly consistent commit.
      save_manifest: Whether or not to save a manifest of the entire
          filesystem state upon commit. This requires that the associated
          base_changeset was also committed with a snapshot.
    Raises:
      TypeError: if both force_commit and manifest are given.
    """
    self.remote_file_factory.validate_client_auth()
    if force_commit and manifest:
      raise TypeError('Exactly one of force_commit or manifest is allowed.')
    if not manifest and not force_commit:
      self.print_error(
          'You are attempting to commit a changeset directly, outside of\n'
          'an upload request. This relies on an eventually-consistent list\n'
          'of the files, which has the the potential to miss files if you are\n'
          'performing a commit quickly after the upload.\n'
          '\n'
          'You can either:\n'
          '  Pass --force_commit to ignore this error.\n'
          'Or:\n'
          '  Use "upload --changeset=new --commit <filenames>" instead.')
      return

    changeset_num = int(changeset)
    self._commit_changeset_or_exit(
        changeset_num, force_commit=force_commit, manifest=manifest,
        save_manifest=save_manifest)
