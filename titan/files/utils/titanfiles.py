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

import os
import sys

from titan.common.lib.google.apputils import appcommands
import gflags as flags
from titan.files import files_client
from titan.files.mixins import versions_client
from titan.files.utils import titanfiles_runners

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

flags.DEFINE_bool(
    'quiet', False,
    'Whether or not to show logging.')

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
  def remote_file_factory(self):
    if not self._remote_file_factory:
      self._remote_file_factory = files_client.RemoteFileFactory(
          host=self.host, secure=not FLAGS.insecure)
    return self._remote_file_factory

  @property
  def vcs_factory(self):
    if not self._vcs_factory:
      self._vcs_factory = versions_client.RemoteVcsFactory(
          host=self.host, secure=not FLAGS.insecure)
    return self._vcs_factory

class UploadCommand(BaseCommand):
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
    flags.DEFINE_bool(
        'confirm_manifest', False,
        'Whether or not the current list of files given to upload can be '
        'trusted as a complete manifest of the files in the changeset. This '
        'allows you to combine --changeset=<num> with --commit.',
        flag_values=flag_values)

  def Run(self, argv):
    """Command runner."""
    filenames = argv[1:]
    if not filenames or not FLAGS.host:
      sys.exit('Usage: --host=example.com '
               'upload [--root_dir=.] [--target_path=/] <filename>')

    upload_runner = titanfiles_runners.UploadRunner(
        host=FLAGS.host,
        port=FLAGS.port,
        remote_file_factory=self.remote_file_factory,
        vcs_factory=self.vcs_factory,
        api_base_path=FLAGS.api_base_path,
        num_threads=FLAGS.threads,
        force=FLAGS.force,
        secure=not FLAGS.insecure,
    )
    upload_runner.Run(
        filenames=filenames,
        root_dir=FLAGS.root_dir,
        target_path=FLAGS.target_path,
        changeset=FLAGS.changeset,
        commit=FLAGS.commit,
        confirm_manifest=FLAGS.confirm_manifest,
    )

class DownloadCommand(BaseCommand):
  """Downloads files from Titan.

  Usage:
    download --file_path=<remote file path>  [target local directory]
    download --dir_path=<remote directory path> [target local directory]
  """

  def __init__(self, name, flag_values, **kwargs):
    super(DownloadCommand, self).__init__(name, flag_values, **kwargs)
    # Command-specific flags.
    flags.DEFINE_multistring(
        'file_path', [],
        'Remote file to download.',
        flag_values=flag_values)
    flags.DEFINE_string(
        'dir_path', None,
        'Remote directory to download.',
        flag_values=flag_values)
    flags.DEFINE_bool(
        'recursive', False,
        'Downloads from a directory recursively',
        flag_values=flag_values)
    flags.DEFINE_integer(
        'depth', None,
        'Specifies recusion depth if "recursive" is specified',
        flag_values=flag_values)

  def Run(self, argv):
    """Command runner."""
    if len(argv) >= 2:
      target = argv[1]
    else:
      target = None

    download_runner = titanfiles_runners.DownloadRunner(
        host=FLAGS.host,
        port=FLAGS.port,
        remote_file_factory=self.remote_file_factory,
        vcs_factory=self.vcs_factory,
        api_base_path=FLAGS.api_base_path,
        num_threads=FLAGS.threads,
        force=FLAGS.force,
        secure=not FLAGS.insecure,
        quiet=FLAGS.quiet
    )
    download_runner.Run(
        file_paths=FLAGS.file_path,
        dir_path=FLAGS.dir_path,
        recursive=FLAGS.recursive,
        depth=FLAGS.depth,
        target_dir=target,
    )

class CommitCommand(BaseCommand):
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
    """Command runner."""
    if not FLAGS.changeset:
      sys.exit('Usage: --host=example.com '
               'commit --changeset=<changeset num>')

    commit_runner = titanfiles_runners.CommitRunner(
        host=FLAGS.host,
        port=FLAGS.port,
        remote_file_factory=self.remote_file_factory,
        vcs_factory=self.vcs_factory,
        api_base_path=FLAGS.api_base_path,
        num_threads=FLAGS.threads,
        force=FLAGS.force,
        secure=not FLAGS.insecure,
    )
    commit_runner.Run(
        changeset=FLAGS.changeset,
        force_commit=FLAGS.force_commit,
    )

def main(unused_argv):
  appcommands.AddCmd('upload', UploadCommand)
  appcommands.AddCmd('commit', CommitCommand)
  appcommands.AddCmd('download', DownloadCommand)

if __name__ == '__main__':
  appcommands.Run()
