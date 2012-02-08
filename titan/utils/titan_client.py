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

"""Titan command-line file management tool.

For a list of available commands, run:
  titan_client help [command]
"""

import copy
import errno
import functools
import getpass
import glob
import inspect
from multiprocessing import pool
import optparse
import os
import re
import sys
import time
from titan.common.lib.google.apputils import app
from titan.files import client

DEFAULT_NUM_THREADS = 30

# The cutoff size of when to upload a local file directly to blobstore.
# This value should match files.MAX_CONTENT_SIZE.
DIRECT_TO_BLOBSTORE_SIZE = 1 << 19  # 500 KiB

class RequireFlags(object):
  """Decorator that ensures all of the required flags are provided.

  Attributes:
    required_flags: A list of required flags.
  """

  def __init__(self, *args):
    self.required_flags = args

  def __call__(self, func):
    """Returns a decorated function that checks for required flags."""

    @functools.wraps(func)
    def WrappedFunc(instance, *args, **kwargs):
      for flag in self.required_flags:
        if instance.flags.get(flag) is None:
          raise CommandValueError('Missing required flag: --%s' % flag)
      return func(instance, *args, **kwargs)

    return WrappedFunc

class TitanCommands(object):
  """Class containing Titan command-line commands.

  The "help" command is self-documenting based on the docstrings of the
  corresponding functions for a command, so be sure to add appropriate
  documentation.

  Attributes:
    args: List of command-line arguments.
    flags: Dict of command-line flags.
    titan_rpc: A Titan RPC client object.
    host: Titan host with port number.
  """

  def __init__(self):
    self.args = None
    self.flags = None
    self._password = None
    self._thread_pool = None
    self._original_titan_rpc_client = None

    self.ResetCommands()
    self.RegisterCommand('upload', self.Upload)
    self.RegisterCommand('download', self.Download)

  @property
  def host(self):
    """Returns the Titan host with port number."""
    host = self.flags['host']
    if self.flags['port'] != 80:
      host = '%s:%d' % (host, self.flags['port'])
    return host

  @RequireFlags('target_path', 'host')
  def Upload(self):
    """Uploads files from the local filesystem to a Titan file service.

    Upload files:
      titan_client upload --target_path=<remote dir> <local filenames>

    Upload directories:
      titan_client upload --target_path=<remote dir> --recursive <local dirs>

    Required flags:
      --host, -H: Host of the Titan service.
      --target_path: The target directory on Titan to upload to.

    Optional flags:
      --force, -f: Ignore any confirmation messages and force upload.
      --recursive, -r: Used to upload a directory to Titan.
      --port, -P: Port of the Titan service.
      --username, -u: Username (full email address) to authenticate with.
      --password_file, -p: Path to a file containing a password.
      --insecure: If requests should be made over plaintext HTTP.

    Returns:
      A list of the mapping between local_path => remote_path.
    Raises:
      CommandValueError: If directory was provided but --recursive was not set
          or if no paths were provided to upload.
    """
    target_path = self.flags['target_path']
    recursive = self.flags.get('recursive')
    force_upload = self.flags.get('force')
    paths = self._FlattenGlobs(self.args)

    if not paths:
      raise CommandValueError('No files to upload.')

    # Create a mapping of local_path => remote_path.
    path_map = []
    for path in paths:
      if os.path.isdir(path):
        if not recursive:
          raise CommandValueError(
              'A dir was provided but missing --recursive flag: %s' % path)

        dir_name = os.path.split(path)[1]
        for dir_path, _, filenames in os.walk(path):
          for filename in filenames:
            file_path = os.path.relpath(os.path.join(dir_path, filename))
            rel_path_from_dir = file_path[len(path) + 1:]
            target = os.path.join(target_path, dir_name, rel_path_from_dir)
            path_map.append([file_path, target])
      else:
        filename = os.path.split(path)[1]
        target = os.path.join(target_path, filename)
        path_map.append([path, target])

    if sys.stdin.isatty() and not force_upload:
      conf_message = ['The following will be uploaded to %s:' % self.host]
      for path, target in path_map:
        conf_message.append('  %s --> %s' % (path, target))
      if not self._ConfirmAction('\n'.join(conf_message)):
        raise CommandValueError('Upload aborted.')

    # Verify the auth function is valid before making requests.
    titan_rpc_client = self._GetTitanClient()
    self.ValidateAuth(titan_rpc_client)

    def UploadFile(path, target, force_blobs):
      # Ensure thread-safety by instantiating a new titan_rpc_client:
      titan_rpc_client = self._GetTitanClient()

      with open(path) as fp:
        if force_blobs or os.path.getsize(path) > DIRECT_TO_BLOBSTORE_SIZE:
          titan_rpc_client.Write(target, fp=fp)
        else:
          titan_rpc_client.Write(target, content=fp.read())
      print 'Uploaded %s to %s' % (path, target)

    # Start upload of files.
    thread_pool = ThreadPool(self.flags['num_threads'])
    force_blobs = self.flags['force_blobs']
    start = time.time()
    for path, target in path_map:
      thread_pool.EnqueueThread(UploadFile, path, target, force_blobs)
    thread_pool.Wait()
    self.ExitForErrors(thread_pool.Errors())

    seconds = int(time.time() - start)
    print '%s files uploaded in %s' % (
        len(path_map), HumanizeDuration(seconds))
    return path_map

  @RequireFlags('host')
  def Download(self, dir_paths=None, file_paths=None, target_dir=None,
               quiet=False):
    """Downloads files from a Titan file service to the local filesystem.

    Usage:
      titan_client download --file_path=<remote path> [local target dir]
      titan_client download --dir_path=<remote dir> [local target dir]

    Required flags:
      --host, -H: Host of the Titan service.

    Optional flags:
      --file_path: Titan file path to download from.
      --dir_path: Titan directory path to download.
      --force, -f: Ignore any confirmation messages and force download.
      --port, -P: Port of the Titan service.
      --username, -u: Username (full email address) to authenticate with.
      --password_file, -p: Path to a file containing a password.

    Args:
      dir_paths: A list of remote directories to download.
      file_paths: A list of remote files to download.
      target_dir: The target local directory to upload to.
      quiet: Whether to show logging.
    Returns:
      A list of mapping between remote_path => local_path.
    Raises:
      CommandValueError: If an invalid target dir was provided or if no files
          were provided to download.
    """
    if dir_paths is None:
      dir_paths = self.flags.get('dir_path', [])
    if file_paths is None:
      file_paths = self.flags.get('file_path', [])
    force_download = self.flags.get('force')

    if not file_paths and not dir_paths:
      raise CommandValueError('No files to download. Use --file_path and/or '
                              '--dir_path.')

    if target_dir is None:
      target_dir = self.args[0] if self.args else '.'

    if not os.path.isdir(target_dir):
      raise CommandValueError('Not a directory: %s' % target_dir)

    # Verify the auth function is valid before making requests.
    titan_rpc_client = self._GetTitanClient()
    self.ValidateAuth(titan_rpc_client)

    # Create a mapping of remote_path => local_path.
    path_map = []

    for path in file_paths:
      if not titan_rpc_client.Exists(path):
        raise CommandValueError('%s does not exist.' % path)

      filename = os.path.split(path)[1]
      target = os.path.relpath(os.path.join(target_dir, filename))
      path_map.append([path, target])

    for dir_path in dir_paths:
      if not titan_rpc_client.DirExists(dir_path):
        raise CommandValueError('%s does not exist.' % dir_path)

      files = titan_rpc_client.ListFiles(dir_path, recursive=True)
      paths = [fp['path'] for fp in files]
      if dir_path.endswith('/'):
        dir_path = dir_path[:-1]
      dir_name = os.path.split(dir_path)[1]
      for path in paths:
        target = os.path.join(target_dir, dir_name, path[len(dir_path) + 1:])
        target = os.path.relpath(target)
        path_map.append([path, target])

    if sys.stdin.isatty() and not force_download:
      conf_message = ['The following will be downloaded from %s' % self.host]
      for path, target in path_map:
        conf_message.append('  %s --> %s' % (path, target))
      if not self._ConfirmAction('\n'.join(conf_message)):
        raise CommandValueError('Download aborted.')

    def DownloadFile(path, target):
      """Downloads a file from a Titan path to a target dir."""
      # Ensure the directory exists.
      MakeDirs(os.path.dirname(os.path.abspath(target)))

      # Ensure thread-safety by instantiating a new titan_rpc_client:
      titan_rpc_client = self._GetTitanClient()

      # Write the content to the file.
      content = titan_rpc_client.Read(path)
      fp = open(target, 'w')
      try:
        fp.write(content)
      finally:
        fp.close()
      if not quiet:
        print 'Downloaded %s to %s' % (path, target)

    # Start the downloads.
    thread_pool = ThreadPool(self.flags['num_threads'])
    start = time.time()
    for path, target in path_map:
      thread_pool.EnqueueThread(DownloadFile, path, target)
    thread_pool.Wait()
    self.ExitForErrors(thread_pool.Errors())

    seconds = int(time.time() - start)
    if not quiet:
      print '%s files downloaded in %s' % (
          len(path_map), HumanizeDuration(seconds))
    return path_map

  def Help(self, method=None):
    """Prints the help message.

    Basic usage:
      <binary> help

    Help with a specific command:
      <binary> help <command>

    Args:
      method: The function pointer whose docstring will be parsed.
    Returns:
      The help message.
    """
    if method:
      helpdoc = inspect.getdoc(method)
    elif self.args:
      command = self.args[0]
      method = self._GetMethodForCommand(command)
      helpdoc = inspect.getdoc(method)
    else:
      helpdoc = [
          'Basic usage:',
          '  %s <command> [--flags] [args]' % sys.argv[0],
          '\nAvailable commands:',
      ]
      for command in self._GetAvailableCommands():
        helpdoc.append('  %s: %s' % (command['name'],
                                     command['description']))

      helpdoc.append('\nFor help with a specific command:')
      helpdoc.append('  %s help <command>' % sys.argv[0])
      helpdoc = '\n'.join(helpdoc)

    # Remove internal docstring comments (anything after Args, Returns, etc.)
    clean_helpdoc = re.search('(.*)(?:Args|Returns|Raises):\n', helpdoc)
    if clean_helpdoc:
      helpdoc = clean_helpdoc.group(1)
    print helpdoc
    return helpdoc

  def ResetCommands(self):
    """Primarily for subclasses to clear the default registered commands."""
    self._commands = {}
    self.RegisterCommand('help', self.Help)

  def RegisterCommand(self, command, method):
    """Registers a new command function.

    Args:
      command: The name of the command used on the command-line.
      method: Method to call for the command.
    """
    self._commands[command] = method

  def RunCommand(self, command, args=None, flags=None):
    """Runs the corresponding method for a command.

    Args:
      command: The name of the command to run.
      args: A list of command-line arguments.
      flags: A dict of command-line flags.
    Returns:
      The response from the method.
    """
    self.args = args or []
    self.flags = flags or {}
    try:
      return self._GetMethodForCommand(command)()
    except CommandValueError, e:
      sys.exit(e)

  def ValidateAuth(self, titan_rpc_client):
    """Get the user's password and verify it is valid for future requests."""
    # Password already set.
    if self._password is not None:
      return
    try:
      password_file = self.flags.get('password_file')
      if password_file:
        self._password = open(password_file).read().strip()
      elif sys.stdin.isatty():
        print 'Username: %s' % self.flags['username']
        self._password = getpass.getpass()
      else:
        self._password = ''
      # Try given credentials, may raise appengine_rpc.ClientLoginError.
      titan_rpc_client.ValidateClientAuth()
    except client.AuthenticationError, e:
      raise CommandValueError(e)

  def ExitForErrors(self, errors):
    """If errors, nicely print a list of exception strings and then exit."""
    if errors:
      for error in errors:
        if hasattr(error, 'code') and error.code == 302:
          print 'Error occurred: %s: Location: %s' % (
              error, error.headers.get('Location'))
        else:
          print 'Error occurred: %s' % error
      sys.exit('The operation failed.')

  def _AuthFunc(self):
    """Returns a tuple of (username, password) used for authentication."""
    return self.flags['username'], self._password

  def _ConfirmAction(self, description, prompt='Are you sure? (y/n) '):
    """Prompts the user for confirmation to perform an action.

    Args:
      description: A string describing the action to be performed.
      prompt: The confirmation prompt.
    Returns:
      True if user replies with 'y' or 'yes' (case-insensitive), False
      otherwise.
    """
    print description
    confirm = raw_input(prompt)
    return confirm.lower() in ['y', 'yes']

  def _FlattenGlobs(self, globs):
    """Flattens a list of file globs into a sorted set of relative paths."""
    paths = set()
    for glob_path in globs:
      paths.update([os.path.relpath(path) for path in glob.glob(glob_path)])
    return sorted(paths)

  def _GetAvailableCommands(self):
    """Returns a list of dicts of commands and documentation."""
    commands = []
    for command in sorted(self._commands):
      docstring = inspect.getdoc(self._commands[command]) or 'No description.'
      commands.append({
          'name': command,
          'description': docstring.splitlines()[0],
      })
    return commands

  def _GetMethodForCommand(self, command):
    """Returns the corresponding method for a command."""
    method = self._commands.get(command)
    if not method or not callable(method):
      raise CommandValueError('Unknown command: %s' % command)
    return method

  def _GetTitanClient(self):
    """Returns a client.TitanClient object."""
    # Allow a new titan client to be init'd without need to re-authenticate.
    if self._original_titan_rpc_client:
      titan_rpc_client = copy.copy(self._original_titan_rpc_client)
      # For thread-safety, we need to make sure that the extra_headers dict
      # isn't shared among TitanClient objects.
      titan_rpc_client.extra_headers = titan_rpc_client.extra_headers.copy()
      return titan_rpc_client

    self._original_titan_rpc_client = client.TitanClient(
        self.host, self._AuthFunc, user_agent='TitanClient/1.0', source='-',
        secure=self.flags['secure'])
    return self._GetTitanClient()

class ThreadPool(object):
  """A light convenience wrapper around multiprocessing.pool.ThreadPool."""

  def __init__(self, num_threads=DEFAULT_NUM_THREADS):
    self._thread_pool = pool.ThreadPool(num_threads)
    self.async_results = []

  def EnqueueThread(self, target, *args, **kwargs):
    """Enqueue the given target function with arguments in the thread pool."""
    result = self._thread_pool.apply_async(target, args, kwargs)
    self.async_results.append(result)

  def Wait(self):
    """Block until all threads in the pool have finished."""
    self._thread_pool.close()
    self._thread_pool.join()

  def Errors(self):
    """Return a list of any exception objects raised by threads."""
    errors = []
    for result in self.async_results:
      try:
        result.get()
      except Exception, e:
        errors.append(e)
    return errors

class ArgumentParser(object):
  """A command-line arument parser.

  NOTE(user): This class was created because optparse is being deprecated in
  py27 in favor of argparse. This'll allow our methods to interface this class
  and gives us the flexibility to migrate to argparse in the future without much
  refactoring.

  Attributes:
    parser: An optparse.OptionParser object.
  """

  def __init__(self):
    self.parser = optparse.OptionParser()

  def AddOption(self, *args, **kwargs):
    """Adds a flag option to the parser."""
    self.parser.add_option(*args, **kwargs)

  def RemoveOption(self, *args, **kwargs):
    """Removes a flag from the parser."""
    self.parser.remove_option(*args, **kwargs)

  def ParseArgs(self):
    """Returns list of args and dict of flags."""
    flags, args = self.parser.parse_args()
    flags = vars(flags)  # Convert the object to a dict.
    return args, flags

def GetDefaultParser():
  """Returns a ArgmentParser object."""
  parser = ArgumentParser()

  parser.AddOption(
      '-u', '--username', dest='username',
      help='Full email address of your account.',
      default='%s@google.com' % getpass.getuser())

  parser.AddOption(
      '-p', '--password_file', dest='password_file',
      help='Path to password file.')

  parser.AddOption(
      '-H', '--host', dest='host',
      help='Host to make requests to.')

  parser.AddOption(
      '-P', '--port', dest='port',
      type='int',
      help='Port to make requests to.',
      default=80)

  parser.AddOption(
      '--insecure', dest='secure',
      help='If requests should made over plaintext HTTP.',
      action='store_false',
      default=True)

  parser.AddOption(
      '-f', '--force', dest='force',
      help='Ignore confirmation messages.',
      action='store_true')

  parser.AddOption(
      '--force_blobs', dest='force_blobs',
      help='Force all files to upload to blobstore.',
      default=False,
      action='store_true')

  parser.AddOption(
      '-r', '--recursive', dest='recursive',
      help='Recursively upload/download a directory.',
      action='store_true')

  parser.AddOption(
      '--target_path', dest='target_path',
      help='The target path')

  parser.AddOption(
      '--file_path', dest='file_path',
      help='The Titan file path.',
      action='append',
      default=[])

  parser.AddOption(
      '--dir_path', dest='dir_path',
      help='The Titan dir path.',
      action='append',
      default=[])

  parser.AddOption(
      '--num_threads', dest='num_threads',
      help='The number of threads to run concurrently.',
      type='int',
      action='store',
      default=DEFAULT_NUM_THREADS)

  return parser

def HumanizeDuration(seconds):
  hours = int(float(seconds / 3600))
  minutes = int(float(seconds % 3600) / 60)
  seconds %= 60
  return '%sh %sm %ss' % (hours, minutes, seconds)

def MakeDirs(dir_path):
  """A thread-safe version of os.makedirs."""
  if not os.path.exists(dir_path) or not os.path.isdir(dir_path):
    try:
      os.makedirs(dir_path)
    except OSError, e:
      if e.errno == errno.EEXIST:
        # The directory exists (probably from another thread creating it),
        # but we need to keep going all the way until the tail directory.
        MakeDirs(dir_path)
      else:
        raise

def RunCommandLine(titan_commands, parser):
  """Runs a command based on args from the command line.

  Args:
    titan_commands: A TitanCommands object.
    parser: A ArgumentParser object.
  Returns:
    The results of the corresponding command.
  """
  args, flags = parser.ParseArgs()
  if args:
    command = args[0]
    args = args[1:]
    return titan_commands.RunCommand(command, args=args, flags=flags)
  else:
    return titan_commands.RunCommand('help')

class CommandValueError(ValueError):
  pass

def main(unused_argv):
  parser = GetDefaultParser()
  titan_commands = TitanCommands()
  RunCommandLine(titan_commands, parser)

if __name__ == '__main__':
  app.run()
