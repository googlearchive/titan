// Copyright 2013 Google Inc. All Rights Reserved.

/**
 * @fileoverview Base Titan RPC client which abstracts out XHR listeners.
 */

goog.provide('titan.Rpc');

goog.require('goog.Uri.QueryData');
goog.require('goog.events');
goog.require('goog.net.XhrIo');



/**
 * Manages RPCs to the App Engine app.
 * @constructor
 */
titan.Rpc = function() {
  /**
   * Root path to the App Engine app handlers.
   * @type {string}
   * @private
   */
  this.baseEndpointPath_ = '/_titan/';
};


/**
 * Sends an RPC to a Titan endpoint.
 * @param {string} endpoint RPC endpoint, often "file", "files" or "dirs".
 * @param {string=} opt_method The HTTP method, defaults to 'GET'.
 * @param {Object=} opt_params Params to pass to the API.
 * @param {function(Object)=} opt_callback Callback function.
 * @param {Object=} opt_scope Scope to bind callback function at.
 */
titan.Rpc.prototype.send =
    function(endpoint, opt_method, opt_params, opt_callback, opt_scope) {
  if (opt_callback && opt_scope) {
    opt_callback = goog.bind(opt_callback, opt_scope);
  }
  var url = this.baseEndpointPath_ + endpoint;
  var method = opt_method || 'GET';
  var headers = {
    'Content-Type': 'application/json'
  };
  if (opt_params) {
    var params = goog.Uri.QueryData.createFromMap(opt_params);
    if (method == 'GET') {
      url = url + '?' + params;
    }
  }
  goog.net.XhrIo.send(url, function(e) {
    if (opt_callback) {
      opt_callback(e.target.getResponseJson());
    }
  }, method, params, headers);
};
