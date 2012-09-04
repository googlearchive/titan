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

"""Microframework for CLI Runners (decoupled from flags)."""

import json
import sys

from titan.common import colors

class BaseRunner(object):
  """Base class for runners."""

  def __init__(self):
    self.quiet = False
    self.force = False

  def Print(self, msg):
    if self.quiet:
      return
    print msg

  def PrintError(self, msg):
    sys.stderr.write(colors.Format('<red>ERROR</red>: %s\n', msg))

  def PrintWarning(self, msg):
    print colors.Format('<red>WARNING</red>: %s', msg)

  def PrintJson(self, data):
    print json.dumps(data, sort_keys=True, indent=2)

  def Confirm(self, message, default=False):
    if self.force:
      return True
    yes = 'Y' if default else 'y'
    no = 'n' if default else 'N'
    result = raw_input('%s [%s/%s]: ' % (message, yes, no))
    return True if result.lower() == yes else False
