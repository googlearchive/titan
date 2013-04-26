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

"""Utility for printing colored messages to a bash shell.

Example usage:
  print colors.Format('<red>WARNING</red> Proceed with caution!')
"""

import re

# Color codes.
BLACK = '0;30'
BLUE = '0;34'
BROWN = '0;33'
GREEN = '0;32'
CYAN = '0;36'
RED = '0;31'
PURPLE = '0;35'
LIGHT_GREY = '0;37'
DARK_GRAY = '1;30'
LIGHT_BLUE = '1;34'
LIGHT_GREEN = '1;32'
LIGHT_CYAN = '1;36'
LIGHT_RED = '1;31'
LIGHT_PURPLE = '1;35'
YELLOW = '1;33'
WHITE = '1;37'

_CODE_MAP = {
    'black': BLACK,
    'blue': BLUE,
    'brown': BROWN,
    'cyan': CYAN,
    'green': GREEN,
    'purple': PURPLE,
    'red': RED,
    'light_grey': LIGHT_GREY,
    'dark_grey': DARK_GRAY,
    'light_blue': LIGHT_BLUE,
    'light_green': LIGHT_GREEN,
    'light_cyan': LIGHT_CYAN,
    'light_red': LIGHT_RED,
    'light_purple': LIGHT_PURPLE,
    'yellow': YELLOW,
    'white': WHITE,
}

_FMT_REGEX = re.compile(r'<(%s)>(.*?)<\/\1>' % '|'.join(_CODE_MAP.keys()))

def format(msg, *args, **kwargs):
  """Formats a message with bash colors.

  Uses an XML-like syntax to format specific snippets in color. For example:
    <green>SUCCESS:<green> yay!
    <red>WARNING:</red> WTF?!?

  Args:
    msg: A message containing XML-like <color> tags.
    *args: Any formatting args for the string.
    **kwargs: Any formatting kwargs for the string.
  Returns:
    A formatted message that can be used to print colors to a bash shell.
  """
  if args:
    msg %= args
  elif kwargs:
    msg %= kwargs

  for match in _FMT_REGEX.finditer(msg):
    color, snippet = match.group(1, 2)

    if color not in _CODE_MAP:
      continue

    color_code = _CODE_MAP[color]
    colored_msg = format_color(color_code, snippet)
    msg = msg.replace(match.group(), colored_msg)
  return msg

def format_color(color_code, msg, *args, **kwargs):
  """Formats a message using a specific color.

  Args:
    color_code: Bash color code.
    msg: The string to colorize.
    *args: Any formatting args for the string.
    **kwargs: Any formatting kwargs for the string.
  Returns:
    A formatted message that can be used to print colors to a bash shell.
  """
  if args:
    msg %= args
  elif kwargs:
    msg %= kwargs
  return '\033[%sm%s\033[m' % (color_code, msg)
