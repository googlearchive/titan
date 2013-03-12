/**
 * @fileoverview Titan Channel client for subscribing to broadcast channels.
 */

goog.provide('titan.channel.BroadcastChannel');

goog.require('goog.events.Event');
goog.require('goog.events.EventTarget');
goog.require('goog.json');



/**
 * A centralized, multi-subscriber, server-broadcast channel.
 * @param {string} key The key of the broadcast channel where messages are sent.
 * @constructor
 */
titan.channel.BroadcastChannel = function(key) {
  goog.base(this);

  /**
   * The key of the broadcast channel through which messages will be sent.
   * @type {string}
   * @private
   */
  this.key_ = key;

  /**
   * Channel object.
   * @type {?goog.appengine.Channel}
   * @private
   */
  this.channel_ = null;

  /**
   * Socket from the Channel API.
   * @type {?goog.appengine.Socket}
   * @private
   */
  this.socket_ = null;

  /**
   * Whether or not the broadcast channel is currently connected.
   * @type {boolean}
   * @private
   */
  this.isConnected_ = false;

  /**
   * Nasty hack because the App Engine Channel API decided to share the
   * same namespace as Closure, which is not supported by Closure compiler.
   * We could use externs for this, but that would require all users to manually
   * alias "appengine = goog.appengine" before the compiled code loads.
   *
   * groups.google.com/d/msg/closure-compiler-discuss/Z8sThaEEb34/UA75Xdi1fDoJ
   *
   * Instead, just pull the object directly from the uncompiled namespace.
   */
  if (!window['goog'] || !window['goog']['appengine']) {
    throw ('Failed to load /_ah/channel/jsapi. ' +
           'Aborting channel initialization.');
  }
  /**
   * A goog.appengine.Channel class.
   * @type {?goog.appengine.Channel}
   * @private
   */
  this.googAppEngineChannel_ = window['goog']['appengine']['Channel'];

};
goog.inherits(titan.channel.BroadcastChannel, goog.events.EventTarget);
goog.addSingletonGetter(titan.channel.BroadcastChannel);


/**
 * Events dispatched by BroadcastChannel.
 * @enum {string}
 */
titan.channel.BroadcastChannel.EventType = {
  CONNECTED: goog.events.getUniqueId('channel-connected'),
  MESSAGE: goog.events.getUniqueId('channel-message'),
  UNKNOWN_MESSAGE: goog.events.getUniqueId('channel-unknown-message')
};


/**
 * Connects to the client's unique channel.
 * @param {string} token The secret App Engine channel token for this client.
 */
titan.channel.BroadcastChannel.prototype.connect = function(token) {
  var EventType = titan.channel.BroadcastChannel.EventType;

  // https://developers.google.com/appengine/docs/python/channel/javascript
  this.channel_ = new this.googAppEngineChannel_(token);
  this.socket_ = this.channel_.open();
  this.socket_.onopen = goog.bind(function(e) {
      this.isConnected_ = true;
      this.dispatch_(EventType.CONNECTED);
  }, this);
};


/**
 * Setter for the current broadcast channel key.
 * @param {string} key The key of the broadcast channel where messages are sent.
 */
titan.channel.BroadcastChannel.prototype.setKey = function(key) {
  this.key_ = key;
};


/**
 * Getter for the current broadcast channel key.
 * @param {string} key The key of the broadcast channel where messages are sent.
 */
titan.channel.BroadcastChannel.prototype.getKey = function() {
  return this.key_;
};


/**
 * Getter for the state of the connection.
 * @return {boolean} If the socket is connected or not.
 */
titan.channel.BroadcastChannel.prototype.isConnected = function() {
  return this.isConnected_;
};


/**
 * Subscribes to the connected channel and listens for messages.
 */
titan.channel.BroadcastChannel.prototype.subscribe = function() {
  var EventType = titan.channel.BroadcastChannel.EventType;

  // TODO(user): Since App Engine limits to one open channel per page,
  // this and connect() will override any other open channels. To support
  // multiple channels, change this to subscribe to a shared message manager.
  this.socket_.onmessage = goog.bind(function(e) {
      var data = goog.json.parse(e['data']);
      if (!data['message'] || !data['broadcast_channel_key']) {
        // The message was not sent through a broadcast channel, dispatch
        // an event so that custom messages can be handled elsewhere.
        this.dispatch_(EventType.UNKNOWN_MESSAGE, message);
      } else if (data['broadcast_channel_key'] == this.key_) {
        var message = goog.json.parse(data['message']);
        this.dispatch_(EventType.MESSAGE, message);
      }
  }, this);
};


/**
 * BroadcastChannel event dispatcher.
 * @param {titan.channel.BroadcastChannel.Event} type Event type to dispatch.
 * @param {string} message Message from server.
 * @private
 */
titan.channel.BroadcastChannel.prototype.dispatch_ = function(type, message) {
  var e = new titan.channel.BroadcastChannel.Event(type, message);
  this.dispatchEvent(e);
};


/**
 * BroadcastChannel event.
 * @param {titan.channel.BroadcastChannel.Event} type Event type to dispatch.
 * @param {string} message Message from server.
 * @constructor
 * @extends {goog.events.Event}
 */
titan.channel.BroadcastChannel.Event = function(type, message) {
  goog.base(this, type);
  /**
   * Data sent from the server.
   * @type {string}
   */
  this.message = message;
};
goog.inherits(titan.channel.BroadcastChannel.Event, goog.events.Event);
