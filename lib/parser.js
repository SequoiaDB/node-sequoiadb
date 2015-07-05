'use strict';

var EventEmitter = require('events');
var util = require('util');
var XBuffer = require('./buffer');
var Message = require('./message');
var debug = require('debug')('sequoiadb:parser');
var constants = require('./const');

/**
 * 连接的报文解析器
 */
var Parser = function () {
  EventEmitter.call(this);
  this.buff = null;
  this.state = "";
  this.expect = 0;
};
util.inherits(Parser, EventEmitter);

/**
 * 连接的报文解析器
 * @param {Buffer} buf 收到的网络字节
 */
Parser.prototype.execute = function (buf) {
  this.append(buf);
  this.parseResult();
};

/**
 * 解析收到的网络字节
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
        // 小端
        isBigEndian = false;
      } else if (eyeCatcher === constants.MSG_SYSTEM_INFO_EYECATCHER_REVERT) {
        // 大端
        isBigEndian = true;
      } else {
        this.emit('error', new Error("parse system info response failed"));
        return;
      }
      // 删掉用过的buff
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
    // 先读4个
    if (this.buff.length >= 4) {
      var buf = new XBuffer(this.buff.slice(0, 4), this.isBigEndian);
      this.state = this.state + "Body";
      var messageSize = buf.readInt32(0); // message size
      debug('expect message size: %s', messageSize);
      this.expect = messageSize;
      if (this.buff.length >= this.expect) {
        // 主动解析
        this.parseResult();
      }
    }
    break;
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
 * 追加Buffer到解析器中的临时字节段中
 * @param {Buffer} buf 收到的网络字节
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
