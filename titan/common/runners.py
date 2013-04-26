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

"""Microframework for CLI Runners (decoupled from flags).

Configurable options:
  force: Ignore confirmation prompts.
  quiet: Ignore print statements.
  suppress_errors: Ignore errors.
  suppress_warnings: Ignore warnings.
"""

import json
import sys
import threading

from titan.common import colors

class BaseRunner(object):
  """Base class for runners."""

  def __init__(self):
    self.quiet = False
    self.force = False
    self.suppress_errors = False
    self.suppress_warnings = False
    self.quiet = False

    self._print_lock = threading.RLock()

  def print_message(self, msg):
    if self.quiet:
      return
    self._print(msg)

  def print_warning(self, msg):
    if self.suppress_warnings:
      return
    msg = colors.format('<red>WARNING</red>: %s', msg)
    self._print(msg, to_stderr=True)

  def print_error(self, msg):
    if self.suppress_errors:
      return
    msg = colors.format('<red>ERROR</red>: %s', msg)
    self._print(msg, to_stderr=True)

  def print_json(self, data):
    self.print_message(json.dumps(data, sort_keys=True, indent=2))

  def _print(self, msg, to_stderr=False):
    target = sys.stderr if to_stderr else sys.stdout
    with self._print_lock:
      target.write(msg + '\n')

  def confirm(self, message, default=False):
    if self.force:
      return True
    yes = 'Y' if default else 'y'
    no = 'n' if default else 'N'
    result = raw_input('%s [%s/%s]: ' % (message, yes, no))
    return result.lower() == yes
