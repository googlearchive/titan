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

"""Titan Files command-line tool.

For help docs, run:
  titanfiles help [command]
"""

import json
import os
import sys
import time

from concurrent import futures

from titan.common.lib.google.apputils import appcommands
import gflags as flags
from titan.common.lib.google.apputils import humanize
from titan.common.lib.google.apputils import logging
from titan.common import colors
from titan.files import files_client
from titan.files.mixins import versions_client

FLAGS = flags.FLAGS

flags.DEFINE_string(
    'host', '',
    'Hostname of the App Engine app.')

flags.DEFINE_integer(
    'port', None,
    'Number of threads to use.')

flags.DEFINE_string(
    'api_base_path', files_client.FILE_API_PATH_BASE,
    'Base path of Titan Files API.')

flags.DEFINE_integer(
    'threads', 100,
    'Number of threads to use.')

flags.DEFINE_bool(
    'force', False,
    'Whether or not to confirm action.')

flags.DEFINE_bool(
    'insecure', False,
    'Whether or not to disable SSL.')

# The cutoff size of when to upload a local file directly to blobstore.
# This value should match files.MAX_CONTENT_SIZE.
DIRECT_TO_BLOBSTORE_SIZE = 1 << 19  # 500 KiB

class Error(Exception):
  pass

class UploadFileError(Error):

  def __init__(self, message, target_path):
    self.target_path = target_path
    super(UploadFileError, self).__init__(message)

class BaseCommand(appcommands.Cmd):
  """Base class for commands."""

  def __init__(self, *args, **kwargs):
    super(BaseCommand, self).__init__(*args, **kwargs)
    # Cannot access FLAGS until after initialized.
    self._remote_file_factory = None
    self._vcs_factory = None

  @property
  def host(self):
    return '%s:%d' % (FLAGS.host, FLAGS.port) if FLAGS.port else FLAGS.host

  @property
  def secure(self):
    return False if FLAGS.insecure else True

  def ThreadPoolExecutor(self):
    return futures.ThreadPoolExecutor(max_workers=FLAGS.threads)

  @property
  def remote_file_factory(self):
    if not self._remote_file_factory:
      self._remote_file_factory = files_client.RemoteFileFactory(
          host=self.host, secure=self.secure)
    return self._remote_file_factory

  @property
  def vcs_factory(self):
    if not self._vcs_factory:
      self._vcs_factory = versions_client.RemoteVcsFactory(
          host=self.host, secure=self.secure)
    return self._vcs_factory

  def PrintError(self, msg):
    print colors.Format('<red>ERROR</red>: %s', msg)

  def PrintWarning(self, msg):
    print colors.Format('<red>WARNING</red>: %s', msg)

  def PrintJson(self, data):
    print json.dumps(data, sort_keys=True, indent=2)

  def Confirm(self, message, default=False):
    yes = 'Y' if default else 'y'
    no = 'n' if default else 'N'
    result = raw_input('%s [%s/%s]: ' % (message, yes, no))
    return True if result.lower() == yes else False

class BaseCommandWithVersions(BaseCommand):

  def _CommitChangesetOrExit(self, changeset_num, force=False, manifest=None):
    """If confirmed, commits the given changeset."""
    # Confirm action.
    msg = 'Commit changeset %d?' % changeset_num
    if not FLAGS.force and not self.Confirm(msg):
      sys.exit('Commit aborted.')

    start = time.time()
    remote_vcs = self.vcs_factory.MakeRemoteVersionControlService()
    staging_changeset = self.vcs_factory.MakeRemoteChangeset(num=changeset_num)

    if manifest:
      # We have full knowledge of the files uploaded to a changeset, associate
      # those files to staging_changeset to ensure a strong consistency commit.
      for path in manifest:
        remote_file = self.remote_file_factory.MakeRemoteFile(path)
        staging_changeset.AssociateFile(remote_file)
      staging_changeset.FinalizeAssociatedFiles()

    final_remote_changeset = remote_vcs.Commit(staging_changeset, force=force)
    elapsed_time = time.time() - start
    print 'Finished commit in %s.' % humanize.Duration(elapsed_time)
    return final_remote_changeset

class UploadCommand(BaseCommandWithVersions):
  """Uploads files to a Titan Files handler.

  Usage:
    upload [--root_dir=.] [--target_path=/] [--changeset=<num or "new">] \
        <filenames>
  """

  def __init__(self, name, flag_values, **kwargs):
    super(UploadCommand, self).__init__(name, flag_values, **kwargs)
    # Command-specific flags.
    flags.DEFINE_string(
        'root_dir', os.path.curdir,
        'Local root dir of all files.',
        flag_values=flag_values)
    flags.DEFINE_string(
        'target_path', '/',
        'Remote root target path for uploaded documents.',
        flag_values=flag_values)

    # Mixin-specific flags:
    flags.DEFINE_string(
        'changeset', None,
        'If versions mixin is enabled, the changeset number or "new".',
        flag_values=flag_values)
    flags.DEFINE_bool(
        'commit', False,
        'Whether or not to commit the changeset. This option requires that '
        '--changeset=new is given.',
        flag_values=flag_values)

  def Run(self, argv):
    filenames = argv[1:]
    if not filenames or not FLAGS.host:
      sys.exit('Usage: --host=example.com '
               'upload [--root_dir=.] [--target_path=/] <filename>')

    # Compose mapping of absolute local path to absolute remote path.
    FLAGS.root_dir = os.path.abspath(FLAGS.root_dir)
    filename_to_paths = {}
    for filename in filenames:
      absolute_filename = os.path.join(FLAGS.root_dir, filename)
      if not absolute_filename.startswith(FLAGS.root_dir):
        self.PrintError('Path "%s" not contained within "%s".'
                        % (absolute_filename, FLAGS.root_dir))
        sys.exit()
      remote_path = absolute_filename[len(FLAGS.root_dir) + 1:]
      remote_path = os.path.join(FLAGS.target_path, remote_path)
      filename_to_paths[absolute_filename] = remote_path

    # Confirm action.
    print '\nUploading files to %s (%s):' % (FLAGS.host, FLAGS.api_base_path)
    for filename, path in filename_to_paths.iteritems():
      if FLAGS.root_dir == os.path.abspath(os.path.curdir):
        # Strip the root_dir from view if it's already the working directory.
        filename = '.' + filename[len(FLAGS.root_dir):]
      print '  %s --> %s' % (filename, path)
    if not FLAGS.force and not self.Confirm('Upload files?'):
      sys.exit('Upload aborted.')

    # Versions Mixin:
    file_kwargs = {}
    changeset = FLAGS.changeset
    if changeset == 'new':
      self.vcs_factory.ValidateClientAuth()
      vcs = self.vcs_factory.MakeRemoteVersionControlService()
      staging_changeset = vcs.NewStagingChangeset()
      changeset_num = staging_changeset.num
      print 'New staging changeset created: %d' % changeset_num
    elif changeset:
      changeset_num = int(changeset)
    if changeset and changeset_num:
      file_kwargs['changeset'] = changeset_num
      print 'Uploading %d files to changeset %d...' % (len(filenames),
                                                       changeset_num)

    start = time.time()
    self.remote_file_factory.ValidateClientAuth()
    future_results = []
    with self.ThreadPoolExecutor() as executor:
      for filename, target_path in filename_to_paths.iteritems():
        future = executor.submit(
            self._UploadFile, filename, target_path, file_kwargs=file_kwargs)
        future_results.append(future)

    failed = False
    total_bytes = 0
    for future in futures.as_completed(future_results):
      try:
        remote_file = future.result()
        print 'Uploaded %s' % remote_file.real_path
        total_bytes += remote_file.size
      except UploadFileError as e:
        self.PrintError('Error uploading %s. Error was: %s %s'
                        % (e.target_path, e.__class__.__name__, str(e)))
        failed = True

    if failed:
      self.PrintError('Could not upload one or more files.')
      return

    if FLAGS.commit:
      if changeset != 'new':
        self.PrintError('Must use --changeset=new with --commit.')
        return
      manifest = filename_to_paths.values()
      self._CommitChangesetOrExit(changeset_num, manifest=manifest)

    elapsed_time = time.time() - start
    print 'Uploaded %d files (%s) in %s.' % (
        len(filenames),
        humanize.BinaryPrefix(total_bytes, 'B'),
        humanize.Duration(elapsed_time))

  def _UploadFile(self, filename, target_path,
                  file_kwargs=None, method_kwargs=None):
    """Uploads a document."""
    try:
      file_kwargs = file_kwargs or {}
      remote_file = self.remote_file_factory.MakeRemoteFile(target_path,
                                                            **file_kwargs)
      with open(filename) as fp:
        method_kwargs = method_kwargs or {}
        if os.path.getsize(filename) > DIRECT_TO_BLOBSTORE_SIZE:
          remote_file.Write(fp=fp, **method_kwargs)
        else:
          remote_file.Write(content=fp.read(), **method_kwargs)
      # Load the RemoteFile to avoid synchronous fetches in the main thread.
      _ = remote_file.real_path
      return remote_file
    except Exception as e:
      logging.exception('Traceback:')
      raise UploadFileError(e, target_path=target_path)

class CommitCommand(BaseCommandWithVersions):
  """Commit a versions Changeset.

  Usage:
    commit --changeset=<changeset num>
  """

  def __init__(self, name, flag_values, **kwargs):
    super(CommitCommand, self).__init__(name, flag_values, **kwargs)
    flags.DEFINE_bool(
        'force_commit', False,
        'Must be given to ensure commit consistency.',
        flag_values=flag_values)
    flags.DEFINE_string(
        'changeset', None,
        'The changeset number to commit.',
        flag_values=flag_values)

  def Run(self, argv):
    if not FLAGS.changeset:
      sys.exit('Usage: --host=example.com '
               'commit --changeset=<changeset num>')

    # TODO(user): output which files are associated to the changeset
    # when the /_titan/files RESTful endpoint is created.
    if not FLAGS.force_commit:
      self.PrintError(
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

    changeset_num = int(FLAGS.changeset)
    self._CommitChangesetOrExit(changeset_num, force=True)

def main(unused_argv):
  appcommands.AddCmd('upload', UploadCommand)
  appcommands.AddCmd('commit', CommitCommand)

if __name__ == '__main__':
  appcommands.Run()
