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

"""Tests for datastructures.py."""

from titan.common.lib.google.apputils import basetest
from titan.common import datastructures

class DatastructuresTest(basetest.TestCase):

  def testMRUSet(self):
    mru_set = datastructures.MRUSet(max_size=3)
    self.assertEqual(0, len(mru_set))
    self.assertIsNone(mru_set.add('foo'))
    self.assertEqual(1, len(mru_set))
    self.assertIn('foo', mru_set)
    self.assertNotIn('bar', mru_set)

    self.assertListEqual(['foo'], mru_set._sorted_items)
    self.assertIsNone(mru_set.add('bar'))
    self.assertListEqual(['foo', 'bar'], mru_set._sorted_items)

    # Get "qux" to the highest-priority slot, position 0.
    self.assertIsNone(mru_set.add('qux'))
    self.assertListEqual(['foo', 'bar', 'qux'], mru_set._sorted_items)
    self.assertIsNone(mru_set.add('qux'))
    self.assertListEqual(['foo', 'qux', 'bar'], mru_set._sorted_items)
    self.assertIsNone(mru_set.add('qux'))
    self.assertListEqual(['qux', 'foo', 'bar'], mru_set._sorted_items)
    # Nothing happens if it's already at position 0.
    self.assertIsNone(mru_set.add('qux'))
    self.assertListEqual(['qux', 'foo', 'bar'], mru_set._sorted_items)

    # Exceed the max_size, which will pop the last item off.
    self.assertEqual('bar', mru_set.add('baz'))
    self.assertListEqual(['qux', 'foo', 'baz'], mru_set._sorted_items)

    # Verify clear() handling.
    mru_set.clear()
    self.assertNotIn('baz', mru_set)
    self.assertListEqual([], mru_set._sorted_items)

    # Verify remove() handling.
    mru_set.add('baz')
    mru_set.remove('baz')
    self.assertListEqual([], mru_set._sorted_items)

  def testMRUDict(self):
    mru_dict = datastructures.MRUDict(max_size=2)
    self.assertEqual(0, len(mru_dict))

    self.assertNotIn('foo', mru_dict)
    mru_dict['foo'] = True
    self.assertIn('foo', mru_dict)
    self.assertSameElements(['foo'], mru_dict.keys())
    mru_dict['bar'] = True
    # Push "bar" to more recently accessed.
    mru_dict['bar'] = True
    self.assertSameElements(['foo', 'bar'], mru_dict.keys())
    # Exceed max_size, "foo" gets evicted.
    mru_dict['baz'] = True
    self.assertSameElements(['bar', 'baz'], mru_dict.keys())

    # Push "baz" and "qux", which should advance "baz" and evict "bar".
    mru_dict.update({'baz': True, 'qux': True})
    self.assertSameElements(['baz', 'qux'], mru_dict.keys())

    # Verify clear() handling.
    mru_dict.clear()
    self.assertNotIn('baz', mru_dict._mru_set)
    mru_dict.update({'baz': True, 'qux': True})
    self.assertSameElements(['baz', 'qux'], mru_dict.keys())

    # Verify delete handling.
    del mru_dict['baz']
    self.assertSameElements(['qux'], mru_dict.keys())

    # Get a key which doesn't exist, then set a different key to verify
    # that __getitem__ doesn't modify the mru_set for non-existent keys.
    self.assertRaises(KeyError, lambda: mru_dict['fake'])
    mru_dict['new_fake'] = True

if __name__ == '__main__':
  basetest.main()
