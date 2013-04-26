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

"""Tests for colors.py."""

from titan.common.lib.google.apputils import basetest
from titan.common import colors

class ColorsTest(basetest.TestCase):

  def testFormat(self):
    msg = '<red>WARNING:</red> hello world'
    expected = '\033[0;31mWARNING:\033[m hello world'
    actual = colors.format(msg)
    self.assertEqual(expected, actual)

    msg = '<red>WARNING:</red> %s'
    expected = '\033[0;31mWARNING:\033[m hello'
    actual = colors.format(msg, 'hello')
    self.assertEqual(expected, actual)

    msg = '<red>WARNING:<blue></red> foo</blue>'
    expected = '\033[0;31mWARNING:<blue>\033[m foo</blue>'
    actual = colors.format(msg)
    self.assertEqual(expected, actual)

    msg = '<red>WARNING:</red> foo <blue>bar</blue>'
    expected = '\033[0;31mWARNING:\033[m foo \033[0;34mbar\033[m'
    actual = colors.format(msg)
    self.assertEqual(expected, actual)

    msg = '<red>WARNING:</red> foo <yellow>bar</yellow>'
    expected = '\033[0;31mWARNING:\033[m foo \033[1;33mbar\033[m'
    actual = colors.format(msg)
    self.assertEqual(expected, actual)

    msg = '<red>WARNING:</red> foo <red>bar</red>'
    expected = '\033[0;31mWARNING:\033[m foo \033[0;31mbar\033[m'
    actual = colors.format(msg)
    self.assertEqual(expected, actual)

  def testFormatColor(self):
    msg = 'foo'
    expected = '\033[0;31mfoo\033[m'
    actual = colors.format_color(colors.RED, msg)
    self.assertEqual(expected, actual)

    msg = 'hello, %s!'
    expected = '\033[0;30mhello, world!\033[m'
    actual = colors.format_color(colors.BLACK, msg, 'world')
    self.assertEqual(expected, actual)

    msg = 'hello, %(name)s!'
    expected = '\033[0;34mhello, foo!\033[m'
    actual = colors.format_color(colors.BLUE, msg, name='foo')
    self.assertEqual(expected, actual)

    msg = 'foo'
    expected = '\033[1;33mfoo\033[m'
    actual = colors.format_color(colors.YELLOW, msg)
    self.assertEqual(expected, actual)

if __name__ == '__main__':
  basetest.main()
