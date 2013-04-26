#!/usr/bin/env python
"""Titan Channel API for centralized, multi-subscriber broadcast channels."""

import datetime
import hashlib
import json
import logging

from google.appengine.api import channel as channel_lib
from titan import files

__all__ = [
    # Functions.
    'create_channel',
    # Classes.
    'BroadcastChannel',
]

CHANNELS_DIR = '/_titan/channel/'

def create_channel(client_id):
  """Simple convenience wrapper for App Engine's create_channel.

  Args:
    client_id: A abitrary string which uniquely identifies the client channel.
  Returns:
    Individual client token to be used in goog.appengine.Channel.
  """
  return channel_lib.create_channel(client_id=client_id)

class BroadcastChannel(object):
  """A multi-subscriber broadcast channel."""

  def __init__(self, key):
    self.key = key
    self._internal_client_ids = set()

    # Use a hashed version of the user key for the filename.
    self._internal_key = hashlib.md5(key).hexdigest()
    self._file_path = '%s%s' % (CHANNELS_DIR, self.key)
    self._file = files.File(self._file_path, _internal=True)

  @property
  def _data(self):
    return json.loads(self._file.content)

  @property
  def _client_ids(self):
    if not self._internal_client_ids:
      self._internal_client_ids = set(self._data['client_ids'])
    return self._internal_client_ids

  @property
  def exists(self):
    return self._file.exists

  def _Save(self):
    self._file.write(json.dumps(self.serialize()))

  def subscribe(self, client_id):
    """Adds an existing client to the broadcast channel.

    Can be called with any App Engine channel client_id, including ones created
    with the native API. This allows you to manage a single channel for
    connected clients which may or may not use BroadcastChannel messages.

    Args:
      client_id: A abitrary string which uniquely identifies the client channel.
    """
    if not self._file.exists:
      # Lazily create the channel file when the first client is added.
      self._Save()
    self._client_ids.add(client_id)
    self._Save()

  def send_message(self, message):
    """Sends a message to all clients registered to the channel.

    Args:
      message: The string message to broadcast.
    Raises:
      ValueError: If the message arg is not a string.
    """
    if not self._file.exists:
      logging.warning(
          'Skipping message to channel "%s" since no clients have been added.'
          % self.key)
      return
    if not isinstance(message, basestring):
      raise ValueError('"message" arg must be a string.')
    message = {
        'message': message,
        'broadcast_channel_key': self.key,
    }
    for client_id in self._client_ids:
      channel_lib.send_message(client_id, json.dumps(message))

  def serialize(self):
    data = {
        'key': self.key,
        'client_ids': list(self._client_ids) if self._file else [],
    }
    return data
