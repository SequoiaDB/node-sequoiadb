'use strict';

var net = require('net');
var EventEmitter = require('events');
var util = require('util');
var helper = require('./helper');
var Parser = require('./parser');
var Message = require('./message');
var SDBError = require('./error');
var constants = require('./const');
var Long = require("./long");
var debug = require('debug')('sequoiadb:connection');

var callbackWrap = function (callback) {
  return function (err, response) {
    callback(err);
  };
};

var connectionId = 0;

/**
 * 创建到数据库的网络连接
 * 1. C: connect
 * 2. S: connected
 * 3. C: send sysinfo request
 * 4. S: set endian
 * 5. C: Auth
 * 6. S: Auth response
 * @param {String} port 端口，默认为11810
 * @param {String} host 主机名，或者IP地址。默认为`localhost`
 */
var Connection = function (port, host, options) {
  EventEmitter.call(this);
  var opts = {
    'host' : host || 'localhost',
    'port' : port || 11810
  };
  this.user = options.user || "";
  this.pass = options.pass || "";
  this.connected = false;
  this.authorized = false;
  this.pending = []; // pending call
  this.mode = ""; // in request/response
  this.connectionId = connectionId++;
  debug('create new connection to %j', opts);
  debug('connection id is %s', this.connectionId);
  this.conn = net.connect(opts);
  this.initParser();
  this.handleConnection();
  this.readyState = false;
  this.isBigEndian = false;
};
util.inherits(Connection, EventEmitter);

/**
 * 初始化报文解析器
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
    if (!that.readyState) {
      that.readyState = true;
      that.emit('ready');
    }
    that.onResponse(message);
    that.processPending();
  });
};

Connection.prototype.ready = function (callback) {
  if (this.readyState) {
    callback();
  } else {
    this.once('ready', callback);
  }
};

/**
 * 处理连接的事件
 */
Connection.prototype.handleConnection = function () {
  var that = this;
  this.conn.on("connect", function () {
    that.onConnect();
  });

  this.conn.on("data", function (chunk) {
    that.onData(chunk);
  });

  this.conn.on("error", function (err) {
    that.onError(err);
  });

  this.conn.on("close", function () {
    that.connectionGone("close");
  });

  this.conn.on("end", function () {
    that.connectionGone("end");
  });

  this.conn.on("drain", function () {
    that.emit("drain");
  });
};

Connection.prototype.connectionGone = function (exit) {
  debug("connection closed by %s", exit);
};

Connection.prototype.isValid = function (callback) {
  if (!this.conn) {
    callback(null, false);
    return;
  }
  var message = new Message(constants.Operation.OP_KILL_CONTEXT);
  message.ContextIDList = [-1];
  var buff = helper.buildKillCursorMessage(message, this.parser.isBigEndian);
  this.send(buff, message, callbackWrap(callback));
};

Connection.prototype.processPending = function () {
  if (this.mode === "responsed") {
    var item = this.pending.shift();
    if (item) {
      this.send(item.buff, item.message, item.callback);
    }
  }
};


var Item = function (buff, message, callback) {
  this.buff = buff;
  this.message = message;
  this.callback = callback;
};

Connection.prototype.send = function (buff, message, callback) {
  debug('mode is %s', this.mode);
  if (this.mode !== "responsed") {
    debug('pending message for %s', message.OperationCode);
    var item = new Item(buff, message, callback);
    this.pending.push(item);
    return;
  }
  debug('send message is %j', message);
  debug('buff is %j', buff);
  this.sendMessage = message;
  this.callback = callback;
  this.parser.state = "Request";
  this.conn.write(buff);
};

/**
 * 处理连接的事件
 */
Connection.prototype.onError = function (err) {
  if (this.pending.length) {
    for (var i = 0; i < this.pending.length; i++) {
      var command = this.pending[i];
      command.callback(err);
    }
  }
  debug(err);
};

/**
 * 处理连接的事件
 */
Connection.prototype.onConnect = function () {
  debug('connected');
  this.connected = true;
  this.emit("connect");
  this.sendSystemInfoRequest();
};

/**
 * 处理连接的事件
 */
Connection.prototype.sendSystemInfoRequest = function () {
  debug('send system info request');
  this.mode = "requesting";
  this.parser.state = "SystemInfoRequest";
  var buf = helper.buildSystemInfoRequest();
  this.conn.write(buf);
};

/**
 * 处理连接的事件
 */
Connection.prototype.onData = function (data) {
  debug('parser state: %s', this.parser.state);
  this.parser.execute(data);
};

/**
 * 处理连接的事件
 */
Connection.prototype.onResponse = function (message) {
  this.mode = "responsed";
  debug('received message is %j', message);
  var sendOpCode = this.sendMessage.OperationCode;
  var recvOpCode = message.OperationCode;
  if ((sendOpCode | 0x80000000) !== recvOpCode) {
    debug('get unexpected result');
    message.Flags = SDBError.errors.SDB_UNEXPECTED_RESULT;
  }

  var flags = message.Flags;
  if (flags !== 0) {
    debug('get error flags: %s', flags);
    var err = new Error(SDBError.getErrorMessage(flags));
    err.flags = flags;
    this.callback && this.callback(err);
    return;
  }
  this.callback && this.callback(null, message);
};

/**
 * 发送登陆请求
 */
Connection.prototype.auth = function (callback) {
  var message = new Message(constants.Operation.MSG_AUTH_VERIFY_REQ);
  message.RequestID = Long.ZERO;
  var secret = helper.md5(this.pass);
  var buff = helper.buildAuthMessage(message, this.user, secret, this.isBigEndian);
  this.callback = callback || function (err) {
    debug('auth done');
  };
  this.sendMessage = message;
  this.parser.state = "Request";
  this.conn.write(buff);
};

Connection.prototype.isValid = function (callback) {
  if (!this.conn) {
    callback(null, false);
    return;
  }
  var message = new Message(constants.Operation.OP_KILL_CONTEXT);
  message.ContextIDList = [-1];
  var buff = helper.buildKillCursorMessage(message, this.parser.isBigEndian);
  this.send(buff, message, callbackWrap(callback));
};

/**
 * 发送断开连接请求，并断开连接
 */
Connection.prototype.disconnect = function (callback) {
  var buff = helper.buildDisconnectRequest(this.isBigEndian);
  this.conn.write(buff);
  this.conn.end(callback);
  this.conn = null;
};

Object.defineProperty(Connection.prototype, 'isClosed', {
  get: function () {
    if (!this.conn) {
      return true;
    }
    return this.conn.readyState === "closed";
  }
});

module.exports = Connection;
