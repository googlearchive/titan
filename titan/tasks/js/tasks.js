/**
 * @fileoverview Client and helpers for Titan Tasks.
 */

goog.provide('titan.tasks.TaskManager');

goog.require('goog.events.Event');
goog.require('goog.events.EventTarget');
goog.require('goog.json');
goog.require('goog.net.EventType');
goog.require('goog.net.XhrIo');
goog.require('goog.uri.utils');
goog.require('titan.channel.BroadcastChannel');



/**
 * Client for Titan Tasks.
 * @param {string} key The server-generated ID.
 * @param {string} group The server-generated group ID.
 * @param {?string} opt_broadcastChannelKey The key identifying a
 *     BroadcastChannel to which this client can subscribe.
 * @constructor
 */
titan.tasks.TaskManager = function(key, group, opt_broadcastChannelKey) {
  goog.base(this);

  /**
   * The server-generated ID.
   * @type {string}
   */
  this.key = key;

  /**
   * The server-generated group ID.
   * @type {string}
   */
  this.group = group;

  /**
   * The ID of the associated broadcast channel.
   * @type {?string}
   */
  this.broadcastChannelKey = opt_broadcastChannelKey;

  /**
   * The broadcast channel object used for message passing and subscription.
   * @type {?titan.channel.BroadcastChannel}
   */
  this.broadcastChannel = null;

  this.initListeners_();
};
goog.inherits(titan.tasks.TaskManager, goog.events.EventTarget);


/**
 * The default Titan Tasks API subscription URL path.
 * @type {string}
 * @const
 * @private
 */
titan.tasks.TaskManager.SUBSCRIBE_URL_ = '/_titan/tasks/taskmanager/subscribe';


/**
 * Events dispatched by TaskManager.
 * @enum {string}
 */
titan.tasks.TaskManager.EventType = {
  SUBSCRIBED: goog.events.getUniqueId('task-manager-connected'),
  TASK_STATUS: goog.events.getUniqueId('task-status')
};


/**
 * Status message types from the TaskManager backend.
 * @enum {string}
 * @protected
 */
titan.tasks.TaskManager.TaskStatus = {
  QUEUED: 'queued',
  RUNNING: 'running',
  SUCCESSFUL: 'successful',
  FAILED: 'failed'
};


/**
 * Connects the client to the broadcast channel.
 * @param {string} token The secret App Engine channel token for this client.
 * @param {function} callback A callback which will be called upon connect.
 */
titan.tasks.TaskManager.prototype.connect = function(token, callback) {
  if (this.broadcastChannel.isConnected()) {
    callback();
    return;
  }
  goog.events.listen(
      this.broadcastChannel,
      titan.channel.BroadcastChannel.EventType.CONNECTED, function(e) {
    callback();
  }, false, this);
  this.broadcastChannel.connect(token);
};


/**
 * Connects the client to the broadcast channel and subscribes to task messages.
 * @param {string} clientId An application-created string the server uses
 *     to identify individual JavaScript clients. This is the same as the
 *     lower-level App Engine Channel API clientId (
 *     https://developers.google.com/appengine/docs/python/channel/functions ).
 */
titan.tasks.TaskManager.prototype.subscribe = function(clientId) {
  var EventType = titan.tasks.TaskManager.EventType;
  this.broadcastChannel.subscribe();
  var params = {
    'key': this.key,
    'group': this.group,
    'client_id': clientId
  };
  params = goog.uri.utils.buildQueryDataFromMap(params);
  var xhr = new goog.net.XhrIo();
  goog.events.listen(xhr, goog.net.EventType.COMPLETE, function(e) {
    if (!e.target.isSuccess()) {
      throw 'Error with subscribe: ' + e.target.getResponseText();
    }
    this.dispatch_(EventType.SUBSCRIBED);
  }, false, this);
  xhr.send(titan.tasks.TaskManager.SUBSCRIBE_URL_, 'POST', params);
};


/**
 * Initialize the event internal broadcast channel event listeners.
 * @private
 */
titan.tasks.TaskManager.prototype.initListeners_ = function() {
  var EventType = titan.tasks.TaskManager.EventType;
  var TaskStatus = titan.tasks.TaskManager.TaskStatus;
  if (!this.broadcastChannelKey) {
    return;
  }
  // Get or create a singleton broadcast channel so that the socket and
  // channel are re-used if they are open already, since goog.appengine.Socket
  // doesn't fire multiple connected events.
  // TODO(user): make this compatible with multiple broadcast channels
  // open on a single page.
  this.broadcastChannel = titan.channel.BroadcastChannel.getInstance();
  this.broadcastChannel.setKey(this.broadcastChannelKey);

  // If previously connected, listeners are already in place.
  if (!this.broadcastChannel.isConnected()) {
    goog.events.listen(
        this.broadcastChannel,
        titan.channel.BroadcastChannel.EventType.MESSAGE, function(e) {
      var message = goog.json.parse(e.message);
      var status = message['status'];
      var taskKey = message['task_key'];
      var error = message['error'];
      this.dispatch_(EventType.TASK_STATUS, taskKey, status, error);
    }, false, this);
  }
};


/**
 * Static method for returning a new TaskManager from a custom endpoint.
 * @param {string} url The custom API URL which will return a JSON-serialized
 *     TaskManager.
 * @param {function(titan.tasks.TaskManager)} callback The callback that will
 *     receive the populated TaskManager object.
 * @param {string=} opt_content Optional content for custom endpoint behavior.
 */
titan.tasks.TaskManager.newFromUrl = function(url, callback, opt_content) {
  goog.net.XhrIo.send(url, function() {
    var data = this.getResponseJson();
    if (!data['key'] || !data['group']) {
      throw ('Incorrect response from server, JSON-serialized task manager ' +
             'was not returned. Received: ' + data.getResponseText());
    }
    var taskManager = new titan.tasks.TaskManager(
        data['key'], data['group'], data['broadcast_channel_key']);
    callback(taskManager);
  }, 'POST', opt_content);
};


/**
 * TaskManager event dispatcher.
 * @param {titan.tasks.TaskManager.EventType} type Event type to dispatch.
 * @param {string=} opt_taskKey Unique ID identifying individual task.
 *     This is useful to relate backend messages to frontend UI components.
 * @param {titan.tasks.TaskManager.TaskStatus=} opt_status Task status.
 * @param {string=} opt_error String if the event is an error.
 * @private
 */
titan.tasks.TaskManager.prototype.dispatch_ = function(
    type, opt_taskKey, opt_status, opt_error) {
  var e = new titan.tasks.TaskManager.Event(
      type, opt_taskKey, opt_status, opt_error);
  this.dispatchEvent(e);
};


/**
 * TaskManager event.
 * @param {titan.tasks.TaskManager.EventType} type Event type to dispatch.
 * @param {string=} opt_taskKey Unique ID identifying individual task.
 *     This is useful to relate backend messages to frontend UI components.
 * @param {titan.tasks.TaskManager.TaskStatus=} opt_status Task status.
 * @param {string=} opt_error String if the event is an error.
 * @constructor
 * @extends {goog.events.Event}
 */
titan.tasks.TaskManager.Event = function(type, opt_taskKey, opt_status,
                                         opt_error) {
  goog.base(this, type);
  /**
   * The task key of the event.
   * @type {?string}
   */
  this.taskKey = opt_taskKey;

  /**
   * Error message sent from the server.
   * @type {?string}
   */
  this.status = opt_status;

  /**
   * Error message sent from the server.
   * @type {?string}
   */
  this.error = opt_error;
};
goog.inherits(titan.tasks.TaskManager.Event, goog.events.Event);
