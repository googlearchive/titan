// Copyright 2013 Google Inc. All Rights Reserved.

/**
 * @fileoverview Simple Titan Files RPC client.
 */

goog.provide('titan.dirs');
goog.provide('titan.files');
goog.require('titan.Rpc');


/**
 * Get data for a single file.
 * @param {Object} params The params to send to the handler.
 * @param {function(Object)=} callback A callback for the received data.
 * @param {?Object} opt_scope Optional scope for the callback.
 */
titan.files.getFile = function(params, callback, opt_scope) {
  var rpc = new titan.Rpc();
  rpc.send('file', 'GET', params, callback, opt_scope);
};


/**
 * List files for a specific directory.
 * @param {Object} params The params to send to the handler.
 * @param {function(Object)=} callback A callback for the received data.
 * @param {?Object} opt_scope Optional scope for the callback.
 */
titan.files.list = function(params, callback, opt_scope) {
  var rpc = new titan.Rpc();
  rpc.send('files', 'GET', params, callback, opt_scope);
};


/**
 * List directories in a directory.
 * @param {Object} params The params to send to the handler.
 * @param {function(Object)=} callback A callback for the received data.
 * @param {?Object} opt_scope Optional scope for the callback.
 */
titan.dirs.list = function(params, callback, opt_scope) {
  var rpc = new titan.Rpc();
  rpc.send('dirs', 'GET', params, callback, opt_scope);
};
