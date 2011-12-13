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

"""Common datastructures."""

class MRUDict(object):
  """A most-recently-used dictionary of a specific size."""

  def __init__(self, max_size):
    self.max_size = max_size
    self._data = {}
    self._mru_set = MRUSet(max_size=max_size)

  def __contains__(self, key):
    return key in self._data

  def __len__(self):
    return len(self._data)

  def __getitem__(self, key):
    if key in self._data:
      popped_key = self._mru_set.add(key)
      if popped_key:
        del self._data[popped_key]
    return self._data[key]

  def __setitem__(self, key, value):
    popped_key = self._mru_set.add(key)
    if popped_key:
      del self._data[popped_key]
    self._data[key] = value

  def __delitem__(self, key):
    self._mru_set.remove(key)
    del self._data[key]

  def __iter__(self):
    return self._data.__iter__()

  def __repr__(self):
    return repr(self._data)

  def iteritems(self):
    return self._data.iteritems()

  def iterkeys(self):
    return self._data.iterkeys()

  def itervalues(self):
    return self._data.itervalues()

  def keys(self):
    return self._data.keys()

  def values(self):
    return self._data.values()

  def update(self, other_dict):
    # Update doesn't use __setitem__, so do it manually to preserve MRU state.
    # Sort the other dict's keys to ensure a deterministic eviction order.
    for key in sorted(other_dict.keys()):
      self[key] = other_dict[key]

  def clear(self):
    self._data.clear()
    self._mru_set.clear()

class MRUSet(object):
  """A most-recently-used set of items."""
  # TODO(user): support more set operations.

  def __init__(self, max_size):
    assert max_size > 1
    self.max_size = max_size
    # Ordered from most recently used to least recently used.
    self._sorted_items = []
    # Also store items in a set to provide O(1) existence checks.
    self._items = set()

  def __len__(self):
    return len(self._items)

  def __contains__(self, item):
    return item in self._items

  def add(self, item):
    """Add the item to the limited-size set; if existing, increase weight."""
    if item in self._items:
      # Item exists: move up in the list.
      pos = self._sorted_items.index(item)
      if pos:
        items = self._sorted_items
        items[pos - 1], items[pos] = items[pos], items[pos - 1]
    else:
      # Item did not exist: pop off an item (if at max) and put the new item in.
      evicted_item = None
      if len(self) >= self.max_size:
        evicted_item = self._sorted_items.pop()
        self._items.remove(evicted_item)
      self._sorted_items.append(item)
      self._items.add(item)
      return evicted_item

  def remove(self, item):
    self._sorted_items.remove(item)
    self._items.remove(item)

  def clear(self):
    self._sorted_items = []
    self._items.clear()
