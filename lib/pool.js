/*!
 *      Copyright (C) 2015 SequoiaDB Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

'use strict';

var EventEmitter = require('events');
var util = require('util');

var Connection = require('./connection');

var Pool = function (opts) {
  EventEmitter.call(this);
  this.count = opts.count || 1;
  this.opts = opts;
  this.conns = [];
  this.authorized = [];
  this.isBigEndian = false;
  this.init();
};
util.inherits(Pool, EventEmitter);

Pool.prototype.init = function () {
  var options = this.opts;
  var that = this;
  var ready = function () {
    if (!that.readyState) {
      that.readyState = true;
      that.emit('ready');
    }
  };

  // add connection to authorized pool
  var addAuthorizedConnection = function (conn) {
    that.isBigEndian = conn.isBigEndian;
    that.authorized.push(conn);
  };

  var reconnect = function (error, conn) {
    var index = that.authorized.indexOf(conn);
    that.authorized.splice(index, 1);
  };

  for (var i = 0; i < this.count; i++) {
    var conn = new Connection(options.port, options.host, options);
    conn.ready(ready);
    conn.on('authorized', addAuthorizedConnection);
    conn.on('disconnect', reconnect);
    this.conns.push(conn);
  }
};

Pool.prototype.ready = function (callback) {
  if (this.readyState) {
    callback();
  } else {
    this.once('ready', callback);
  }
};

Pool.prototype.allocate = function () {
  var random = Math.floor(Math.random() * this.authorized.length);
  return this.authorized[random];
};

/**
 * disconnect all connections of pool
 */
Pool.prototype.disconnect = function (buff, callback) {
  var total = this.authorized.length;
  var done = function () {
    total--;
    if (total === 0) {
      callback();
    }
  };

  var conn;
  while ((conn = this.authorized.shift())) {
    conn.socket.write(buff);
    conn.socket.end(done);
  }
};

module.exports = Pool;
