/**
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

var debug = require('debug')('sequoiadb:message');
var padLength = require('./pad').padLength;
var BSON = require('bson').BSONPure.BSON;

var MESSAGE_HEADER_LENGTH = 28;

var Message = function (code) {
  this.OperationCode = code;
  this.RequestLength = undefined;
  this.RequestID = undefined;
  this.Version = undefined;
  this.W = undefined;
  this.Padding = undefined;
  this.Flags = undefined;
  this.CollectionFullNameLength = undefined;
  this.CollectionFullName = undefined;
  this.Matcher = undefined;
  this.Selector = undefined;
  this.OrderBy = undefined;
  this.Hint = undefined;
  this.Insertor = undefined;
  this.Modifier = undefined;
  this.ObjectList = undefined;
  this.NodeID = undefined;
  this.SkipRowsCount = undefined;
  this.ReturnRowsCount = undefined;
  this.StartFrom = undefined;
  this.NumReturned = undefined;
  this.KillCount = undefined;
  this.ContextIDList = undefined;
  this.MessageText = undefined;
  this.rc = undefined;
  // for lob
  this.BsonLen = undefined;
  this.LobLen = undefined;
  this.LobSequence = undefined;
  this.LobOffset = undefined;
  this.LobBuff = undefined;
};

Message.prototype.parseHeader = function (xbuff) {
  this.RequestLength = xbuff.readInt32(0); // 4 bytes
  this.OperationCode = xbuff.readInt32(4); // 4 bytes
  this.NodeID = xbuff.slice(8, 20).toBuffer(); // 12 bytes
  this.RequestID = xbuff.slice(20, 28).toBuffer(); // 8 bytes
};

var extractBsonObject = function (xbuff) {
  var length;
  var objAllotLen;
  var start = 0;
  var list = [];
  while (start < xbuff.length) {
    length = xbuff.readInt32(start); // use 4 byte
    if (length <= 0 || length > xbuff.length) {
      debug("Invalid length of BSONObject:::" + length);
      throw new Error("SDB_INVALIDSIZE");
    }
    objAllotLen = padLength(length, 4); // 4的整数倍
    var obj = xbuff.slice(start, start + objAllotLen);
    // TODO: bson convert endian
    start = start + objAllotLen;
    list.push(BSON.deserialize(obj.toBuffer()));
  }

  return list;
};

Message.prototype.parseBody = function (xbuff) {
  debug('ContextID is %j', xbuff.slice(0, 8).toBuffer());
  // use 8 byte
  this.ContextIDList = [xbuff.slice(0, 8).toBuffer()]; // 8 bytes
  this.Flags = xbuff.readInt32(8); // 4 bytes
  this.StartFrom = xbuff.readInt32(12); // 4 bytes
  this.NumReturned = xbuff.readInt32(16); // 4 bytes
  if (this.NumReturned > 0) {
    this.ObjectList = extractBsonObject(xbuff.slice(20));
  }
};

Message.prototype.parseLobBody = function (xbuff) {
  this.ContextIDList = [xbuff.slice(0, 8).toBuffer()]; // 8 bytes
  this.Flags = xbuff.readInt32(8); // 4 bytes
  this.StartFrom = xbuff.readInt32(12); // 4 bytes
  this.NumReturned = xbuff.readInt32(16); // 4 bytes
  this.ObjectList = null;

  if (this.Flags === 0) {
    this.LobLen = xbuff.readInt32(20); // 4 bytes
    this.LobSequence = xbuff.readInt32(24); // 4 bytes
    this.LobOffset = xbuff.slice(28, 36).toBuffer(); // 8 bytes
    this.LobBuff = xbuff.slice(36).toBuffer();
  }
};

Message.extractReply = function (xbuff) {
  var message = new Message();
  message.parseHeader(xbuff.slice(0, MESSAGE_HEADER_LENGTH));
  message.parseBody(xbuff.slice(MESSAGE_HEADER_LENGTH));
  return message;
};

Message.extractLobReply = function (xbuff) {
  var message = new Message();
  message.parseHeader(xbuff.slice(0, MESSAGE_HEADER_LENGTH));
  message.parseLobBody(xbuff.slice(MESSAGE_HEADER_LENGTH));
  return message;
};

module.exports = Message;
