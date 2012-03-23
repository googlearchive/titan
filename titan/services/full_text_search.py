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

"""Service to support full text search in Titan files."""

import base64
from google.appengine.api import search
from google.appengine.ext import deferred
from titan.common import hooks
from titan.files import files

SERVICE_NAME = 'full-text-search'
INDEX_NAME = 'titan-' +  SERVICE_NAME

def RegisterService():
  hooks.RegisterHook(SERVICE_NAME, 'file-write', hook_class=HookForWrite)
  hooks.RegisterHook(SERVICE_NAME, 'file-touch', hook_class=HookForWrite)
  hooks.RegisterHook(SERVICE_NAME, 'file-copy', hook_class=HookForWrite)
  hooks.RegisterHook(SERVICE_NAME, 'file-delete', hook_class=HookForDelete)

class HookForWrite(hooks.Hook):
  """Hook for files.Write(), files.Touch(), and files.Copy()."""

  def Post(self, file_obj):
    """Create a search document for the file entity."""
    # TODO(user): Support namespace.
    doc_id = _GetDocId(file_obj.path)
    fields = _GetSearchFields(file_obj)
    doc = search.Document(doc_id=doc_id, fields=fields)
    index = _GetSearchIndex()
    deferred.defer(index.add, doc, _queue=SERVICE_NAME)
    return file_obj

class HookForDelete(hooks.Hook):
  """Hook for files.Delete()."""

  def Pre(self, **kwargs):
    """Delete the search document."""
    paths = kwargs['paths']
    # TODO(user): Support namespace.
    paths = paths if hasattr(paths, '__iter__') else [paths]
    doc_ids = map(_GetDocId, paths)
    index = _GetSearchIndex()
    deferred.defer(index.remove, doc_ids, _queue=SERVICE_NAME)
    return kwargs

def _GetSearchIndex(index_name=INDEX_NAME, namespace=None):
  """Create a search index."""
  return search.Index(name=index_name,
                      namespace=namespace,
                      consistency=search.Index.PER_DOCUMENT_CONSISTENT)

def _GetDocId(path):
  """Create a unique doc id string for the search document."""
  return base64.urlsafe_b64encode(path)

def _KeyFromDocId(doc_id):
  return base64.urlsafe_b64decode(doc_id)

def _GetSearchFields(file_obj):
  """Create a list of search fields to be indexed for the document."""
  fields = [
      search.TextField(name='name', value=file_obj.name),
      search.TextField(name='path', value=file_obj.path),
      search.TextField(name='mime_type', value=file_obj.mime_type),
      search.DateField(name='created', value=file_obj.created.date()),
      search.DateField(name='modified', value=file_obj.modified.date()),
      search.TextField(name='created_by', value=file_obj.created_by.email()),
      search.TextField(name='modified_by',
                       value=file_obj.modified_by.email()),
  ]
  # If the content isn't using blobs, index it.
  if not file_obj.blob:
    fields.append(search.TextField(name='content', value=file_obj.content))
  # If the file exists, allow searches by is:exists.
  if file_obj.exists:
    fields.append(search.AtomField(name='is', value='exists'))
  # Each path needs to be added individually.
  fields.extend([search.TextField(name='paths', value=p)
                 for p in file_obj.paths])
  return fields

def _SearchRequest(query, index_name=INDEX_NAME, namespace=None, **kwargs):
  """Make a search request and return the raw results for processing."""
  matched = []
  index = _GetSearchIndex(index_name, namespace)
  response = index.search(query, **kwargs)
  if response.results:
    matched.extend(response.results)
  return matched

def SearchRequest(query, index_name=INDEX_NAME, namespace=None, **kwargs):
  """Make a search request and return the file key names (paths)."""
  results = _SearchRequest(query, index_name, namespace, **kwargs)
  documents = [_KeyFromDocId(x.document.doc_id) for x in results]
  return documents
