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

"""Named, strongly-consistent counters for App Engine apps.

WARNING: These counters are slow. A single named counter can only support ~1
write per second per named counter in the high replication datastore, and only
about 5 writes per second per counter in the master/slave datastore. When there
is too much write contention, a TransactionFailedError may be raised.

Memcache is not used to in order to support strong consistency (as a memcache
increment operation is not guaranteed to succeed).
"""

from google.appengine.ext import db

class StrongCounter(db.Model):
  count = db.IntegerProperty(required=True, default=0)

def GetCount(name):
  """Retrieve the current value for the given strong counter."""
  counter = StrongCounter.get_or_insert(key_name=name)
  return counter.count

def Increment(name):
  """Increment the value for the given strong counter and return it."""

  def Transaction():
    counter = StrongCounter.get_by_key_name(name)
    if not counter:
      counter = StrongCounter(key_name=name)
    counter.count += 1
    counter.put()
    return counter.count

  return db.run_in_transaction(Transaction)
