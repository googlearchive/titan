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

"""Tests for utils.py."""

from titan.common.lib.google.apputils import app
from titan.common.lib.google.apputils import basetest
from titan.common import utils

class UtilsTestCase(basetest.TestCase):

  def testGetCommonDirPath(self):
    paths = ['/foo/bar/baz/test.html', '/foo/bar/test.html']
    self.assertEqual('/foo/bar', utils.GetCommonDirPath(paths))
    paths = ['/foo/bar/baz/test.html', '/z/test.html']
    self.assertEqual('/', utils.GetCommonDirPath(paths))
    paths = ['/foo/bar/baz/test.html', '/footest/bar.html']
    self.assertEqual('/', utils.GetCommonDirPath(paths))

  def testSplitPath(self):
    # No containing paths of '/'.
    self.assertEqual([], utils.SplitPath('/'))
    # / contains /file
    self.assertEqual(['/'], utils.SplitPath('/file'))
    # / and /foo contain /foo/bar
    self.assertEqual(['/', '/foo'], utils.SplitPath('/foo/bar'))

    expected = ['/', '/path', '/path/to', '/path/to/some']
    self.assertEqual(expected, utils.SplitPath('/path/to/some/file.txt'))
    self.assertEqual(expected, utils.SplitPath('/path/to/some/file'))

  def testComposeMethodKwargs(self):

    class Parent(object):

      def Method(self, foo, bar=None):
        return '%s-%s' % (foo, bar)

    class Child(Parent):

      @utils.ComposeMethodKwargs
      def Method(self, **kwargs):
        baz_result = '-%s' % kwargs.pop('baz', False)  # Consume "baz" arg.
        return super(Child, self).Method(**kwargs) + baz_result

    class GrandChild(Child):

      @utils.ComposeMethodKwargs
      def Method(self, **kwargs):
        return super(GrandChild, self).Method(**kwargs)

    child = Child()
    self.assertEqual('1-None-False', child.Method(1))
    child = GrandChild()
    self.assertEqual('1-None-False', child.Method(1))
    self.assertEqual('1-True-False', child.Method(1, True))
    self.assertEqual('1-True-False', child.Method(1, bar=True))
    self.assertEqual('1-True-False', child.Method(foo=1, bar=True))
    self.assertEqual('1-True-True', child.Method(foo=1, bar=True, baz=True))
    self.assertEqual('1-True-False', child.Method(bar=True, foo=1))

def main(unused_argv):
  basetest.main()

if __name__ == '__main__':
  app.run()
