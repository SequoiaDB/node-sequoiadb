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

var constants = require('./const');
var crypto = require('crypto');
var BSON = require('bson').BSONPure.BSON;
var XBuffer = require('./buffer');
var Long = require('./long');
var padLength = require('./pad').padLength;
var padBuffer = require('./pad').padBuffer;

var MESSAGE_HEADER_LENGTH = 28;
var MESSAGE_KILLCURSOR_LENGTH = 36;

exports.buildSystemInfoRequest = function () {
  var buf = new Buffer(12);
  buf.writeInt32LE(constants.MSG_SYSTEM_INFO_LEN, 0); // use 4 bytes
  buf.writeUInt32LE(constants.MSG_SYSTEM_INFO_EYECATCHER, 4); // use 4 bytes
  buf.writeIntLE(12, 8, 4); // use 4 bytes
  return buf;
};

exports.buildHeader = function (messageLength, requestID, nodeID, operationCode, isBigEndian) {
  var origin = new Buffer(MESSAGE_HEADER_LENGTH);
  origin.fill(0);
  var buf = new XBuffer(origin, isBigEndian);

  buf.writeInt32(messageLength, 0); // 4
  buf.writeInt32(operationCode, 4); // 4
  buf.writeBuffer(nodeID, 8); // nodeID.length 12
  buf.writeLong(requestID, 8 + nodeID.length); // 8

  return buf.toBuffer();
};

exports.buildAuthMessage = function (message, user, passwd, isBigEndian) {
  var opCode = message.OperationCode;
  var requestID = message.RequestID;
  var nodeID = constants.ZERO_NODEID;
  var auth = {};
  auth[constants.SDB_AUTH_USER] = user;
  auth[constants.SDB_AUTH_PASSWD] = passwd;
  var authBytes = BSON.serialize(auth, false, true, false);

  var messageLength = MESSAGE_HEADER_LENGTH + padLength(authBytes.length, 4);
  var list = [];
  list.push(exports.buildHeader(messageLength, requestID, nodeID, opCode, isBigEndian));
  list.push(padBuffer(authBytes, 4));
  return Buffer.concat(list);
};

exports.buildDisconnectRequest = function (isBigEndian) {
  var requestID = Long.ZERO;
  var nodeID = constants.ZERO_NODEID;
  var messageLength = padLength(MESSAGE_HEADER_LENGTH, 4);
  return exports.buildHeader(messageLength, requestID, nodeID, constants.Operation.OP_DISCONNECT, isBigEndian);
};

exports.buildKillCursorMessage = function (message, isBigEndian) {
  var opCode = message.OperationCode;
  var contextIDs = message.ContextIDList;
  var requestID = Long.ZERO;
  var nodeID = constants.ZERO_NODEID;
  // sizeof(long) = 8
  var lenContextIDs = 8 * contextIDs.length;
  var messageLength = MESSAGE_KILLCURSOR_LENGTH + lenContextIDs;

  var fieldList = [];
  fieldList.push(exports.buildHeader(messageLength, requestID, nodeID, opCode, isBigEndian));
  var buf = new XBuffer(8 + lenContextIDs, isBigEndian);
  buf.writeInt32(0, 0); // 4
  buf.writeInt32(1, 4); // 4
  for (var i = 0; i < contextIDs.length; i++) {
    buf.writeLong(contextIDs[i], 8 + i * 8);
  }

  fieldList.push(buf.toBuffer());
  return Buffer.concat(fieldList);
};

exports.md5 = function (input) {
  var shasum = crypto.createHash('md5');
  shasum.update(input);
  return shasum.digest('hex');
};
