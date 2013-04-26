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
    broadcast_channel.send_message('Hello world!')

    tokens = []
    self.assertFalse(broadcast_channel.exists)
    tokens.append(channel.create_channel('foo'))
    tokens.append(channel.create_channel('bar'))
    tokens.append(channel.create_channel('example@example.com'))
    self.assertTrue(all(tokens), 'Bad tokens: %r' % tokens)

    broadcast_channel.subscribe('foo')
    broadcast_channel.subscribe('bar')
    broadcast_channel.subscribe('example@example.com')
    self.assertTrue(broadcast_channel.exists)

    # Must connect each client to the channel before broadcast.
    # This happens automatically by the client scripts in the real world.
    for token in tokens:
      self.channel_stub.connect_channel(token)

    broadcast_channel.send_message('Hello world!')
    for token in tokens:
      message = self.channel_stub.pop_first_message(token)
      message_data = {
          'message': 'Hello world!',
          'broadcast_channel_key': 'some-unique-channel-id'
      }
      self.assertEqual(json.dumps(message_data), message)

if __name__ == '__main__':
  basetest.main()
