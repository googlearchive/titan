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
write per second, per named counter, per namespace in the high replication
datastore. When there is too much write contention, a TransactionFailedError
may be raised.

Memcache is not used to guarantee strong consistency.
"""

from google.appengine.ext import ndb

class StrongCounter(ndb.Model):

  _use_cache = False
  _use_memcache = False

  count = ndb.IntegerProperty(default=0)

def GetCount(name, namespace=None):
  """Retrieve the current value for the given strong counter."""
  counter = StrongCounter.get_or_insert(name, namespace=namespace)
  return counter.count

def Increment(name, namespace=None, nested_transaction=False):
  """Increment the value for the given strong counter and return it.

  Args:
    name: String name of the counter.
    namespace: Namespace used for the underlying datastore namespace.
    nested_transaction: Whether or not this function is being called already
        from inside a transaction. Until NDB supports nested transactions,
        this simply does the read + increment without a transaction.
  Returns:
    The new counter integer.
  """

  def Transaction():
    counter = StrongCounter.get_by_id(name, namespace=namespace)
    if not counter:
      counter = StrongCounter(id=name, namespace=namespace)
    counter.count += 1
    counter.put()
    return counter.count

  if nested_transaction:
    return Transaction()
  return ndb.transaction(Transaction)
