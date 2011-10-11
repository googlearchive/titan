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

"""Server which synchronizes local and remote file changes."""

# TODO(user): currently only supports sync from local to remote files.

import functools
import logging
import os
import re
import pyinotify
from titan.common.lib.google.apputils import app
from titan.files import client
from titan.utils import titan_client

# http://pyinotify.sourceforge.net/#The_EventsCodes_Class
WATCHED_EVENTS = pyinotify.IN_CREATE
WATCHED_EVENTS |= pyinotify.IN_MODIFY
WATCHED_EVENTS |= pyinotify.IN_DELETE
WATCHED_EVENTS |= pyinotify.IN_MOVED_FROM
WATCHED_EVENTS |= pyinotify.IN_MOVED_TO
WATCHED_EVENTS |= pyinotify.IN_UNMOUNT
WATCHED_EVENTS |= pyinotify.IN_ISDIR

# Known temporary files and other cruft files that shouldn't sync.
EXCLUDED_FILENAMES_REGEX = re.compile(
    # Many editors end temporary files with a "~".
    r'.+~$'
    # Vim uses ".swp", ".swx", and ".swpx" file extensions.
    r'|.+\.swp$|.+\.swx$|.+\.swpx$'
    # Vim also thought "4913" up to "5036" were appropriate temporary filenames.
    r'|^(491[3-9]|49[2-9]\d)$'
    # Emacs does ".#filename".
    r'|^\.#.+$'
)

def IgnoreDirectories(func):

  @functools.wraps(func)
  def Wrapper(self, event):
    if event.dir:
      logging.debug('Ignoring dir: %s', os.path.join(event.path, event.name))
      return
    func(self, event)

  return Wrapper

def IgnoreTemporaryFiles(func):

  @functools.wraps(func)
  def Wrapper(self, event):
    if (self.excluded_filenames_regex
        and self.excluded_filenames_regex.search(event.name)):
      logging.debug('Ignoring file: %s', os.path.join(event.path, event.name))
      return
    func(self, event)

  return Wrapper

class LocalFileEventHandler(pyinotify.ProcessEvent):
  """Event handler for local filesystem events."""

  def __init__(self, base_dir, target_path, titan_rpc_client,
               excluded_filenames_regex=None):
    super(LocalFileEventHandler, self).__init__()
    self.base_dir = base_dir
    self.target_path = target_path
    self.titan_rpc_client = titan_rpc_client
    self.excluded_filenames_regex = excluded_filenames_regex

  def _GetRemoteTarget(self, event):
    filename = event.pathname
    rel_file_path = filename[len(self.base_dir):]
    # Strip leading slash.
    if rel_file_path.startswith('/'):
      rel_file_path = rel_file_path[1:]
    target = os.path.join(self.target_path, rel_file_path)
    return target

  def _GetFilenameAndTarget(self, event):
    filename = event.pathname
    target = self._GetRemoteTarget(event)
    return filename, target

  @IgnoreDirectories
  @IgnoreTemporaryFiles
  def process_IN_CREATE(self, event):
    # TODO(user): there is an unhandled condition where IN_CREATE events
    # are not fired for files created in a new directory. Figure out why the
    # fix committed in pyinotify 0.9.2 isn't working.
    # https://github.com/seb-m/pyinotify/issues/2
    filename, target = self._GetFilenameAndTarget(event)
    logging.info('Created: %s --> %s:%s',
                 filename, self.titan_rpc_client.host, target)
    try:
      self.titan_rpc_client.Write(target, open(filename).read())
    except IOError, e:
      logging.error('Unable to upload %s. Error was: %s', filename, e)

  @IgnoreDirectories
  @IgnoreTemporaryFiles
  def process_IN_MODIFY(self, event):
    filename, target = self._GetFilenameAndTarget(event)
    logging.info('Modified: %s --> %s:%s',
                 filename, self.titan_rpc_client.host, target)
    try:
      self.titan_rpc_client.Write(target, open(filename).read())
    except IOError, e:
      logging.error('Unable to upload %s. Error was: %s', filename, e)

  @IgnoreDirectories
  @IgnoreTemporaryFiles
  def process_IN_DELETE(self, event):
    filename, target = self._GetFilenameAndTarget(event)
    logging.info('Removed: %s --> %s:%s',
                 filename, self.titan_rpc_client.host, target)
    try:
      self.titan_rpc_client.Delete(target)
    except client.BadFileError:
      logging.error('Unable to delete, file does not exist: %s', target)

  @IgnoreDirectories
  @IgnoreTemporaryFiles
  def process_IN_MOVED_FROM(self, event):
    filename, target = self._GetFilenameAndTarget(event)
    logging.info('Deleted by move: %s --> %s:%s',
                 filename, self.titan_rpc_client.host, target)
    try:
      self.titan_rpc_client.Delete(target)
    except client.BadFileError:
      logging.error('Unable to delete, file does not exist: %s', target)

  @IgnoreDirectories
  @IgnoreTemporaryFiles
  def process_IN_MOVED_TO(self, event):
    filename, target = self._GetFilenameAndTarget(event)
    logging.info('Created by move: %s --> %s:%s',
                 filename, self.titan_rpc_client.host, target)
    try:
      self.titan_rpc_client.Write(target, open(filename).read())
    except IOError, e:
      logging.error('Unable to upload %s. Error was: %s', filename, e)

  def process_UNMOUNT(self, event):
    logging.error('ERROR: Backing filesystem was unmounted: %s', event)

class TitanFilesystemCommands(titan_client.TitanCommands):
  """Commands subclass which defines the available titanfs commands."""

  def __init__(self):
    super(TitanFilesystemCommands, self).__init__()
    self.ResetCommands()
    self.RegisterCommand('start', self.Start)

  @titan_client.RequireFlags('host', 'sync_dir', 'target_path')
  def Start(self, base_dir=None, sync_dirs=None,
            excluded_filenames_regex=EXCLUDED_FILENAMES_REGEX):
    """Start watching and syncing the local and remote filesystem changes.

    Usage:
      titanfs start --host=<hostname> --sync_dir=<local dir>
          [--base_dir=<directory name to strip from sync_dirs>]
          [--target_path=<remote base path>]
    """
    # TODO(user): add syncing from remote changes to local files.

    target_path = self.flags['target_path']
    if not sync_dirs:
      sync_dirs = self.flags.get('sync_dir')
    if not base_dir and sync_dirs:
      # Determine the directory that contains all given sync dirs.
      base_dir = os.path.commonprefix(sync_dirs)
      if not base_dir.endswith('/'):
        base_dir = os.path.split(base_dir)[0]
    # Passing True to the default arg of .get doesn't work, so set it directly:
    recursive = True if self.flags.get('recursive') is None else False

    titan_rpc_client = self._GetTitanClient()
    self.ValidateAuth(titan_rpc_client)

    watch_manager = pyinotify.WatchManager()
    local_file_handler = LocalFileEventHandler(
        base_dir=base_dir,
        target_path=target_path,
        titan_rpc_client=titan_rpc_client,
        excluded_filenames_regex=excluded_filenames_regex)

    logging.info('Starting directory watcher, please wait...')
    notifier = pyinotify.Notifier(watch_manager, local_file_handler)
    for sync_dir in sync_dirs:
      watch_manager.add_watch(sync_dir, WATCHED_EVENTS, rec=recursive)
    logging.info('Ready. Watching for changes...')
    notifier.loop()

def AddTitanFilesystemOptions(parser):
  """Adds TitanFS-specific options to command line parser."""

  parser.AddOption(
      '--target_path', dest='target_path',
      help='A remote Titan directory as the base of all uploaded files.',
      default='/')

  parser.AddOption(
      '--sync_dir', dest='sync_dir',
      action='append', default=[],
      help=('The local base directory to watch for changes.'))

def main(unused_argv):
  logger = logging.getLogger()
  logger.setLevel(logging.INFO)

  parser = titan_client.GetDefaultParser()
  parser.RemoveOption('--file_path')
  parser.RemoveOption('--dir_path')
  parser.RemoveOption('--num_threads')
  parser.RemoveOption('--target_path')
  AddTitanFilesystemOptions(parser)

  titan_commands = TitanFilesystemCommands()
  titan_client.RunCommandLine(titan_commands, parser)

if __name__ == '__main__':
  app.run()
