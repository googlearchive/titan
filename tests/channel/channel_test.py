#!/usr/bin/env python
"""Unit tests for channel.py."""

from tests.common import testing

import json
from titan import channel
from titan.common.lib.google.apputils import basetest

class BroadcastChannelTest(testing.BaseTestCase):

  def testBroadcastChannel(self):
    broadcast_channel = channel.BroadcastChannel('some-unique-channel-id')

    # This shouldn't error without any clients connected, but it shouldn't
    # queue up the message either.
    broadcast_channel.SendMessage('Hello world!')

    tokens = []
    self.assertFalse(broadcast_channel.exists)
    tokens.append(channel.CreateChannel('foo'))
    tokens.append(channel.CreateChannel('bar'))
    tokens.append(channel.CreateChannel('example@example.com'))
    self.assertTrue(all(tokens), 'Bad tokens: %r' % tokens)

    broadcast_channel.Subscribe('foo')
    broadcast_channel.Subscribe('bar')
    broadcast_channel.Subscribe('example@example.com')
    self.assertTrue(broadcast_channel.exists)

    # Must connect each client to the channel before broadcast.
    # This happens automatically by the client scripts in the real world.
    for token in tokens:
      self.channel_stub.connect_channel(token)

    broadcast_channel.SendMessage('Hello world!')
    for token in tokens:
      message = self.channel_stub.pop_first_message(token)
      message_data = {
          'message': 'Hello world!',
          'broadcast_channel_key': 'some-unique-channel-id'
      }
      self.assertEqual(json.dumps(message_data), message)

if __name__ == '__main__':
  basetest.main()
