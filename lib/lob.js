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
var Message = require('./message');
var bson = require('bson');
var helper = require('./helper');
var Long = require('./long');

var Lob = function (collection) {
  this.collection = collection;
  this.conn = collection.conn;
  this.id = null;
  this.mode = -1;
  this.size = Long.ZERO;
  this.readOffset = Long.NEG_ONE;
  this.createTime = Long.ZERO;
  this.isOpen = false;
  this.contextID = Long.NEG_ONE;
};

/**
*  \memberof SDB_LOB_SEEK_SET 0
*  \brief Change the position from the beginning of lob
*/
Lob.SDB_LOB_SEEK_SET = 0;

/**
*  \memberof SDB_LOB_SEEK_CUR 1
*  \brief Change the position from the current position of lob
*/
Lob.SDB_LOB_SEEK_CUR = 1;

/**
*  \memberof SDB_LOB_SEEK_END 2
*  \brief Change the position from the end of lob
*/
Lob.SDB_LOB_SEEK_END = 2;

/**
*  \memberof SDB_LOB_CREATEONLY 0x00000001
*  \brief Open a new lob only
*/
Lob.SDB_LOB_CREATEONLY = 0x00000001;

/**
*  \memberof SDB_LOB_READ 0x00000004
*  \brief Open an existing lob to read
*/
Lob.SDB_LOB_READ = 0x00000004;

// the max lob data size to send for one message
var SDB_LOB_MAX_DATA_LENGTH = 1024 * 1024;
var SDB_LOB_DEFAULT_OFFSET = -1;
var SDB_LOB_DEFAULT_SEQ = 0;

/** \fn         Open( ObjectId id, int mode )
* \brief       Open an exist lob, or create a lob
* \param       id   the lob's id
* \param       mode available mode is SDB_LOB_CREATEONLY or SDB_LOB_READ.
*              SDB_LOB_CREATEONLY
*                  create a new lob with given id, if id is null, it will
*                  be generated in this function;
*              SDB_LOB_READ
*                  read an exist lob
* \exception SequoiaDB.Error
* \exception System.Exception
*/
Lob.prototype.open = function (id, mode, callback) {
  if (this.isOpen) {
    return callback(new Error('SDB_LOB_HAS_OPEN'));
  }

  if (Lob.SDB_LOB_CREATEONLY !== mode && Lob.SDB_LOB_READ !== mode) {
    throw new Error("SDB_INVALIDARG");
  }

  if (Lob.SDB_LOB_READ === mode && !id) {
    throw new Error("SDB_INVALIDARG");
  }
  // gen oid
  this.id = id;
  if (Lob.SDB_LOB_CREATEONLY === mode && !this.id) {
    this.id = bson.ObjectID.createPk();
  }
  // mode
  this.mode = mode;
  this.readOffset = Long.ZERO;
  var that = this;
  // open
  this._open(function (err, response) {
    if (err) {
      return callback(err);
    }
    that.isOpen = true;
    callback(null);
  });
};

/** \fn          Close()
* \brief       Close the lob
* \return void
* \exception SequoiaDB.Error
* \exception System.Exception
*/
Lob.prototype.close = function (callback) {
  var message = new Message();
  // build message
  // MsgHeader
  message.OperationCode = constants.Operation.MSG_BS_LOB_CLOSE_REQ;
  message.NodeID = constants.ZERO_NODEID;
  message.RequestID = 0;
  // the rest part of _MsgOpLOb
  message.Version = constants.DEFAULT_VERSION;
  message.W = constants.DEFAULT_W;
  message.Padding = 0;
  message.Flags = constants.DEFAULT_FLAGS;
  message.ContextIDList = [this.contextID];
  message.BsonLen = 0;

  var that = this;
  // build send msg
  var buff = helper.buildCloseLobRequest(message, this.conn.isBigEndian);
  this.conn.send(buff, message, function (err, response) {
    if (err) {
      return callback(err);
    }
    that.isOpen = false;
    callback(null);
  });
};

/** \fn          Read( byte[] b )
*  \brief       Reads up to b.length bytes of data from this
*               lob into an array of bytes.
*  \param       b   the buffer into which the data is read.
*  \return      the total number of bytes read into the buffer, or
*               <code>-1</code> if there is no more data because the end of
*               the file has been reached, or <code>0<code> if
*               <code>b.length</code> is Zero.
*  \exception SequoiaDB.Error
*  \exception System.Exception
*/
Lob.prototype.read = function (buff, callback) {
  if (!this.isOpen) {
    throw new Error("SDB_LOB_NOT_OPEN");
  }

  if (!buff) {
    throw new Error("SDB_INVALIDARG");
  }

  if (0 === buff.length) {
    return callback(null, 0);
  }
  this._read(buff, callback);
};

/** \fn          Write( byte[] b )
*  \brief       Writes b.length bytes from the specified
*               byte array to this lob.
*  \param       b   the data.
*  \exception SequoiaDB.Error
*  \exception System.Exception
*/
Lob.prototype.write = function (buff, callback) {
  var message = new Message();
  // MsgHeader
  message.OperationCode = constants.Operation.MSG_BS_LOB_WRITE_REQ;
  message.NodeID = constants.ZERO_NODEID;
  message.RequestID = 0;
  // the rest part of _MsgOpLOb
  message.Version = constants.DEFAULT_VERSION;
  message.W = constants.DEFAULT_W;
  message.Padding = 0;
  message.Flags = constants.DEFAULT_FLAGS;
  message.ContextIDList = [this.contextID];
  message.BsonLen = 0;
  // MsgLobTuple
  message.LobLen = buff.length;
  message.LobSequence = SDB_LOB_DEFAULT_SEQ;
  message.LobOffset = SDB_LOB_DEFAULT_OFFSET;

  var that = this;
  // build send msg
  var bytes = helper.buildWriteLobRequest(message, buff, this.conn.isBigEndian);
  this.conn.send(bytes, message, function (err, response) {
    if (err) {
      return callback(err);
    }
    that.size += buff.length;
    callback(null);
  });
};

/** \fn          void Seek( long size, int seekType )
*  \brief       Change the read position of the lob. The new position is
*               obtained by adding <code>size</code> to the position
*               specified by <code>seekType</code>. If <code>seekType</code>
*               is set to SDB_LOB_SEEK_SET, SDB_LOB_SEEK_CUR, or SDB_LOB_SEEK_END,
*               the offset is relative to the start of the lob, the current
*               position of lob, or the end of lob.
*  \param       size the adding size.
*  \param       seekType  SDB_LOB_SEEK_SET/SDB_LOB_SEEK_CUR/SDB_LOB_SEEK_END
*  \return void
*  \exception SequoiaDB.Error
*  \exception System.Exception
*/
Lob.prototype.seek = function (size, seekType) {
  if (!this.isOpen) {
    throw new Error("SDB_LOB_NOT_OPEN");
  }

  if (this.mode !== Lob.SDB_LOB_READ) {
    throw new Error("SDB_INVALIDARG");
  }

  if (Lob.SDB_LOB_SEEK_SET === seekType) {
    if ( size < 0 || size > this.size ) {
      throw new Error( "SDB_INVALIDARG");
    }
    this.readOffset = size;
  } else if (Lob.SDB_LOB_SEEK_CUR === seekType) {
    if ((this.size < this.readOffset + size) || (this.readOffset + size < 0)) {
      throw new Error("SDB_INVALIDARG");
    }
    this.readOffset += size;
  } else if (Lob.SDB_LOB_SEEK_END === seekType) {
    if ( size < 0 || size > this.size) {
      throw new Error( "SDB_INVALIDARG");
    }
    this.readOffset = this.size - size;
  } else {
    throw new Error("SDB_INVALIDARG");
  }
};

/** \fn          bool IsClosed()
*  \brief       Test whether lob has been closed or not
*  \return      true for lob has been closed, false for not
*/
Lob.prototype.isClosed = function () {
  return !this.isOpen;
};

/** \fn          ObjectId GetID()
*  \brief       Get the lob's id
*  \return      the lob's id
*/
Lob.prototype.getID = function () {
  return this.id;
};

/** \fn          long GetSize()
*  \brief       Get the size of lob
*  \return      the lob's size
*/
Lob.prototype.getSize = function () {
  return this.size;
};

/** \fn          long GetCreateTime()
*  \brief       get the create time of lob
*  \return The lob's create time
*  \exception SequoiaDB.Error
*  \exception System.Exception
*/
Lob.prototype.cetCreateTime = function () {
  return this.createTime;
};

Lob.prototype._open = function (callback) {
  // add info into object
  var matcher = {};
  matcher[constants.FIELD_COLLECTION] = this.collection.collectionFullName;
  matcher[constants.FIELD_LOB_OID] = this.id;
  matcher[constants.FIELD_LOB_OPEN_MODE] = this.mode;

  var message = new Message(constants.Operation.MSG_BS_LOB_OPEN_REQ);
  // build message
  message.NodeID = constants.ZERO_NODEID;
  message.RequestID = Long.ZERO;
  message.Version = constants.DEFAULT_VERSION;
  message.W = constants.DEFAULT_W;
  message.Padding = 0;
  message.Flags = constants.DEFAULT_FLAGS;
  message.ContextIDList = [constants.DEFAULT_CONTEXTID];
  message.Matcher = matcher;

  var that = this;
  // build send msg
  var buff = helper.buildOpenLobRequest(message, this.conn.isBigEndian);
  this.conn.send(buff, message, function (err, response) {
    if (err) {
      return callback(err);
    }
    var list = response.ObjectList;
    var obj = list[0];
    if (!obj || obj[constants.FIELD_LOB_SIZE] === undefined ||
      obj[constants.FIELD_LOB_CREATTIME] === undefined) {
      return callback(new Error("SDB_SYS"));
    }

    that.size = Long.fromNumber(obj[constants.FIELD_LOB_SIZE]);
    that.createTime = Long.fromNumber(obj[constants.FIELD_LOB_CREATTIME]);
    that.contextID = Long.fromNumber(response.ContextIDList[0]);
    callback(null);
  });
};

Lob.prototype._read = function (buff, callback) {
  var message = new Message(constants.Operation.MSG_BS_LOB_READ_REQ);
  // MsgHeader
  message.NodeID = constants.ZERO_NODEID;
  message.RequestID = Long.ZERO;
  // the rest part of _MsgOpLOb
  message.Version = constants.DEFAULT_VERSION;
  message.W = constants.DEFAULT_W;
  message.Padding = 0;
  message.Flags = constants.DEFAULT_FLAGS;
  message.ContextIDList = [this.contextID];
  message.BsonLen = 0;
  // MsgLobTuple
  message.LobLen = buff.length;
  message.LobSequence = SDB_LOB_DEFAULT_SEQ;
  message.LobOffset = this.readOffset;

  var that = this;

  // build send msg
  var bytes = helper.buildReadLobRequest(message, this.conn.isBigEndian);
  // send msg
  this.conn.sendForLob(bytes, message, function (err, response) {
    if (!err) {
      var lob = response.LobBuff;
      lob.copy(buff, 0);
      that.readOffset += lob.length;
      return callback(null, lob.length);
    }
    if (err.flags === constants.SDB_DMS_EOC) {
      callback(null, -1);
    } else {
      callback(err);
    }
  });
};

module.exports = Lob;
