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

// native modules
var util = require('util');
var net = require('net');
var EventEmitter = require('events');

// third modules
var Long = require('long');

// file modules
var helper = require('./helper');
var errors = require('./error').errors;
var constants = require('./const');
var Parser = require('./parser');
var Queue = require('./queue');
var Message = require('./message');
var Item = require('./item');
var debug = require('debug')('sequoiadb:connection');

var connectionId = 0;

/**
 * The steps of connect to DB:
 * 1. C: connect
 * 2. S: connected
 * 3. C: send sysinfo request
 * 4. S: set endian
 * 5. C: Auth
 * 6. S: Auth response
 * @param {String} port port
 * @param {String} host hostname, or ip address
 */
var Connection = function (port, host, options) {
  EventEmitter.call(this);
  var opts = {
    'host' : host || 'localhost',
    'port' : port || 11810
  };
  this.opts = opts;
  this.user = options.user || "";
  this.pass = options.pass || "";
  this.longTrace = options.longTrace || false;
  this.queue = new Queue(this);
  this.connectionId = connectionId++;
  debug('create new connection to %j', opts);
  debug('connection id is %s', this.connectionId);
  this.init();
};
util.inherits(Connection, EventEmitter);

Connection.prototype.init = function () {
  this.connected = false;
  this.readyState = false;
  this.isBigEndian = false;
  this.socket = net.connect(this.opts);
  this.initParser();
  this.handleConnection();
};

Connection.prototype.handleConnection = function () {
  var that = this;
  this.socket.setNoDelay(false);
  this.socket.on("connect", function () {
    that.onConnect();
  });

  this.socket.on("data", function (chunk) {
    that.onData(chunk);
  });

  this.socket.on("error", function (err) {
    debug('socket has error: %j', err);
    that.onError(err);
    that.emit('error', err, that);
    that.reconnect();
  });

  this.socket.on("close", function (hadError) {
    debug('closed with transmission error? %s', hadError);
    that.connectionGone("close");
  });

  this.socket.on("end", function () {
    that.connectionGone("end");
  });

  this.socket.on("drain", function () {
    that.emit("drain");
  });
};

/**
 * the connect handle
 */
Connection.prototype.onConnect = function () {
  debug('connected');
  this.connected = true;
  this.emit("connect");
  this.sendSystemInfoRequest();
};

/**
 * The error handle
 */
Connection.prototype.onError = function (err) {
  this.queue.fail(err);
};

Connection.prototype.reconnect = function () {
  // remove old events
  this.socket.removeAllListeners();
  this.parser.removeAllListeners();
  // reinitialize socket and parser
  this.init();
};

/**
 * send system info request to db server
 */
Connection.prototype.sendSystemInfoRequest = function () {
  debug('send system info request');
  this.parser.state = "SystemInfoRequest";
  var buf = helper.buildSystemInfoRequest();
  this.socket.write(buf);
};

Connection.prototype.request = function (buff, message, state, callback) {
  var err = null;
  if (this.longTrace) {
    err = new Error();
  }
  var item = new Item(buff, message, state, err, callback);
  this.queue.enqueue(item);
};

/**
 * initialize parser
 */
Connection.prototype.initParser = function () {
  var that = this;
  this.parser = new Parser();
  this.parser.on('SystemInfoRequest', function (isBigEndian) {
    debug('received system info request reply');
    that.isBigEndian = isBigEndian;
    that.auth();
  });
  this.parser.on('response', function (message) {
    that.onResponse(message);
    if (!that.readyState) {
      that.readyState = true;
      that.emit('ready');
    }
    // continue to process queue
    that.queue.start();
  });
};


/**
 * handle data event
 */
Connection.prototype.onData = function (data) {
  debug('parser state: %s', this.parser.state);
  debug('get data: %j', data);
  this.parser.execute(data);
};

/**
 * handle response
 */
Connection.prototype.onResponse = function (message) {
  debug('received message is %j', message);

  var sendItem = this.queue.dequeue();

  var sendOpCode = sendItem.message.OperationCode;
  var recvOpCode = message.OperationCode;
  if ((sendOpCode | 0x80000000) !== recvOpCode) {
    debug('get unexpected result');
    message.Flags = errors.SDB_UNEXPECTED_RESULT;
  }

  var err;
  var flags = message.Flags;
  if (flags !== 0) {
    debug('get error flags: %s', flags);
    err = new Error(require('./error').getErrorMessage(flags));
    err.flags = flags;
    if (this.longTrace) {
      var delimiter = '\n    --------------------\n' ;
      err.stack += delimiter + sendItem.error.stack.replace(/.+\n/, '');
    }
  }

  if (typeof sendItem.callback === 'function') {
    var callback = sendItem.callback;
    callback(err, message);
  }
};

/**
 * send auth request
 */
Connection.prototype.auth = function (callback) {
  var that = this;
  var message = new Message(constants.Operation.MSG_AUTH_VERIFY_REQ);
  message.RequestID = Long.ZERO;
  var secret = helper.md5(this.pass);
  var buff = helper.buildAuthMessage(message, this.user, secret, this.isBigEndian);
  this.request(buff, message, "Request", function (err) {
    that.emit('authorized', that);
  });
};

Connection.prototype.ready = function (callback) {
  if (this.readyState) {
    callback();
  } else {
    this.once('ready', callback);
  }
};

Connection.prototype.connectionGone = function (exit) {
  debug("connection closed by %s", exit);
  this.socket = null;
};

module.exports = Connection;
