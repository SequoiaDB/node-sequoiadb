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
var XBuffer = require('./buffer');
var Message = require('./message');
var debug = require('debug')('sequoiadb:parser');
var constants = require('./const');

/**
 * The parser of sequoiadb
 */
var Parser = function () {
  EventEmitter.call(this);
  this.buff = null;
  this.state = "";
  this.expect = 0;
};
util.inherits(Parser, EventEmitter);

/**
 * Excute buffer
 * @param {Buffer} buf the received buffer from socket
 */
Parser.prototype.execute = function (buf) {
  this.append(buf);
  this.parseResult();
};

/**
 * Parse the buffer
 */
Parser.prototype.parseResult = function () {
  debug('the state is: %s', this.state);
  switch (this.state) {
  case "SystemInfoRequest":
    debug('parse SystemInfoRequest reply');
    if (this.buff.length >= 128) {
      var eyeCatcher = this.buff.readUInt32LE(4); // UInt32
      debug('eye catcher is %s', eyeCatcher);
      var isBigEndian;
      if (eyeCatcher === constants.MSG_SYSTEM_INFO_EYECATCHER) {
        // little-endian
        isBigEndian = false;
      } else if (eyeCatcher === constants.MSG_SYSTEM_INFO_EYECATCHER_REVERT) {
        // big-endian
        isBigEndian = true;
      } else {
        this.emit('error', new Error("parse system info response failed"));
        return;
      }
      // remove used buffer
      this.buff = this.buff.slice(128);
      this.isBigEndian = isBigEndian;
      debug('set isBigEndian is %s', this.isBigEndian);
      this.emit('SystemInfoRequest', isBigEndian);
    }
    break;
  case "Request":
  case "LobRequest":
    debug('parse response. phase 1, check length.');
    debug('buffer length is %s', this.buff.length);
    // read first 4 bytes
    if (this.buff.length >= 4) {
      var buf = new XBuffer(this.buff.slice(0, 4), this.isBigEndian);
      this.state = this.state + "Body";
      var messageSize = buf.readInt32(0); // message size
      debug('expect message size: %s', messageSize);
      this.expect = messageSize;
    } else {
      break;
    }
  case "RequestBody":
  case "LobRequestBody":
    debug('parse response. phase 2. check body');
    debug('current buff size: %s', this.buff.length);
    if (this.buff.length >= this.expect) {
      var xbuff = new XBuffer(this.buff.slice(0, this.expect), this.isBigEndian);
      var message;
      if (this.state === "RequestBody") {
        message = Message.extractReply(xbuff);
      } else if (this.state === "LobRequestBody") {
        message = Message.extractLobReply(xbuff);
      }
      this.buff = this.buff.slice(this.expect);
      this.emit('response', message);
    }
    break;
  default:
    break;
  }
};

/**
 * Append buffer into temp buffer
 * @param {Buffer} buf the received buffer from socket
 */
Parser.prototype.append = function (buf) {
  if (this.buff) {
    var list = [this.buff, buf];
    var length = this.buff.length + buf.length;
    this.buff = Buffer.concat(list, length);
  } else {
    this.buff = buf;
  }
};

module.exports = Parser;
