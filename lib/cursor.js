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

var helper = require('./helper');
var Message = require('./message');
var SDBError = require('./error').SDBError;
var constants = require('./const');
var Long = require("long");
var debug = require('debug')('sequoiadb:cursor');

var Cursor = function (response, input) {
  this.collection = null;
  Object.defineProperty(this, 'conn', {
    value: null,
    writable: true,
    enumerable: false
  });
  this.index = -1;
  // 循环引用
  if (input instanceof require('./connection')) {
    this.conn = input;
  } else if (input instanceof require('./collection')) {
    this.collection = input;
    this.conn = input.collSpace.conn;
  } else {
    throw new SDBError("SDB_INVALIDARG");
  }
  var hint = {};
  hint[""] = constants.CLIENT_RECORD_ID_INDEX;
  this.hint = hint;
  this.message = new Message();
  this.reqId = response.RequestID;
  this.message.NodeID = constants.ZERO_NODEID;
  this.message.ContextIDList = response.ContextIDList;
  this.contextId = this.message.ContextIDList[0];
  this.message.NumReturned = -1;
  this.list = response.ObjectList;
  this.hasMore = true;
  this.isClosed = false;
};

Cursor.prototype.next = function (callback) {
  debug('next');
  if (this.isClosed) {
    return callback(new SDBError("SDB_DMS_CONTEXT_IS_CLOSE"));
  }

  var that = this;
  var handle = function () {
    if (!that.list) {
      return callback(null, null);
    }

    if (that.index < that.list.length - 1) {
      callback(null, that.list[++that.index]);
    } else {
      that.index = -1;
      that.list = null;
      that.next(callback);
    }
  };

  if ((this.index === -1) && this.hasMore && !this.list) {
    this._readNextBuffer(function (err) {
      if (err) {
        return callback(err);
      }
      handle();
    });
  } else {
    handle();
  }
};

Cursor.prototype._readNextBuffer = function (callback) {
  if (!this.conn) {
    return callback(new SDBError("SDB_NOT_CONNECTED"));
  }
  debug('read next buffer');

  // compare to -1
  if (Long.NEG_ONE.equals(this.contextId)) {
    this.hasMore = false;
    this.index = -1;
    this.collection = null;
    this.list = null;
    return callback(null);
  }

  this.message.OperationCode = constants.Operation.OP_GETMORE;
  this.message.RequestID = this.reqId;
  debug('the message is %j', this.message);
  var buff = helper.buildGetMoreRequest(this.message, this.conn.isBigEndian);
  var that = this;
  this.conn.send(buff, this.message, function (err, response) {
    if (!err) {
      that.reqId = response.RequestID;
      that.list = response.ObjectList;
      return callback(null);
    }
    if (err.flags === constants.SDB_DMS_EOC) {
      that.hasMore = false;
      that.index = -1;
      that.collection = null;
      that.list = null;
      callback(null);
    } else {
      callback(err);
    }
  });
};

Cursor.prototype.current = function (callback) {
  if (this.isClosed) {
    return callback(new SDBError("SDB_DMS_CONTEXT_IS_CLOSE"));
  }
  if (this.index === -1) {
    this.next(callback);
    return;
  } else {
    callback(null, this.list[this.index]);
  }
};

Cursor.prototype.close = function (callback) {
  if (this.isClosed) {
    return callback(null);
  }
  var that = this;
  this._killCursor(function (err) {
    if (err) {
      return callback(err);
    }
    that.isClosed = true;
    callback(null);
  });
};

Cursor.prototype._killCursor = function (callback) {
  if (!this.conn || Long.NEG_ONE.equals(this.contextId) || this.conn.isClosed) {
    return callback(null);
  }
  var that = this;

  var message = new Message(constants.Operation.OP_KILL_CONTEXT);
  message.ContextIDList = [that.contextId];
  var buff = helper.buildKillCursorMessage(message, that.conn.isBigEndian);
  that.conn.send(buff, message, function (err, response) {
    if (err) {
      return callback(err);
    }
    that.conn = null;
    that.contextId = Long.NEG_ONE;
    callback(null);
  });
};

module.exports = Cursor;
