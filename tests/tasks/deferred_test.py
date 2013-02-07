#!/usr/bin/env python
# Copyright 2013 Google Inc. All Rights Reserved.
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

"""Tests for deferred.py."""

from tests.common import testing

from titan.common.lib.google.apputils import basetest
from titan.files import files
from titan.tasks import deferred

class DeferredTest(testing.BaseTestCase):

  def testDefer(self):
    self.Login('alice@example.com')
    path = '/foo.txt'
    content = 'hello'
    deferred.Defer(WriteFile, path, content)
    self.RunDeferredTasks()

    # Verify the task ran.
    titan_file = files.File(path)
    self.assertTrue(titan_file.exists)
    self.assertEqual(content, titan_file.content)

    # Verify that alice@example.com is the created_by user.
    self.assertEqual('alice@example.com', titan_file.created_by.email)

def WriteFile(path, content):
  files.File(path).Write(content)

if __name__ == '__main__':
  basetest.main()
