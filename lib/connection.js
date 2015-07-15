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

var net = require('net');
var EventEmitter = require('events');
var util = require('util');
var Long = require("./long");
var helper = require('./helper');
var Parser = require('./parser');
var Message = require('./message');
var SDBError = require('./error');
var constants = require('./const');
var CollectionSpace = require('./collection_space');
var Cursor = require('./cursor');
var ReplicaGroup = require('./replica_group');
var debug = require('debug')('sequoiadb:connection');
var getNextRequstID = require('./requestid');

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
    that.onResponse(message);
    if (!that.readyState) {
      that.readyState = true;
      that.emit('ready');
    }
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
  this.conn.setNoDelay(false);
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
  this.error = new Error();
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
  debug('get data: %j', data);
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
    var delimiter = '\n    --------------------\n' ;
    err.stack += delimiter + this.error.stack.replace(/.+\n/, '');
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
  message.ContextIDList = [Long.NEG_ONE];
  var buff = helper.buildKillCursorMessage(message, this.isBigEndian);
  this.send(buff, message, function(err, response) {
    if (err) {
      callback(null, false);
    } else {
      callback(null, true);
    }
  });
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

Connection.prototype.createUser = function (username, password, callback) {
  if (!username || !password) {
    throw new Error("必须输入用户名和密码");
  }

  var message = new Message(constants.Operation.MSG_AUTH_CRTUSR_REQ);
  message.RequestID = Long.ZERO;
  var pass = helper.md5(password);
  var buff = helper.buildAuthMessage(message, username, pass, this.isBigEndian);
  this.send(buff, message, callbackWrap(callback));
};

Connection.prototype.removeUser = function (username, password, callback) {
  if (!username || !password) {
    throw new Error("必须输入用户名和密码");
  }

  var message = new Message(constants.Operation.MSG_AUTH_DELUSR_REQ);
  message.RequestID = Long.ZERO;
  var pass = helper.md5(password);
  var buff = helper.buildAuthMessage(message, username, pass, this.isBigEndian);
  this.send(buff, message, callbackWrap(callback));
};

/**
 * 删除集合空间
 * @param {String} space 集合空间名字
 */
Connection.prototype.dropCollectionSpace = function (space, callback) {
  var command = constants.ADMIN_PROMPT + constants.DROP_CMD + " " + constants.COLSPACE;
  var matcher = {};
  matcher[constants.FIELD_NAME] = space;
  this.sendAdminCommand(command, matcher, {}, {}, {}, callbackWrap(callback));
};

/**
 * 创建集合空间
 * @param {String} space 集合空间名字
 */
Connection.prototype.createCollectionSpace = function (spaceName, pageSize, callback) {
  if (typeof pageSize === 'function') {
    callback = pageSize;
    pageSize = constants.SDB_PAGESIZE_DEFAULT;
  }

  if (!spaceName ||
    pageSize !== constants.SDB_PAGESIZE_4K &&
    pageSize !== constants.SDB_PAGESIZE_8K &&
    pageSize !== constants.SDB_PAGESIZE_16K &&
    pageSize !== constants.SDB_PAGESIZE_32K &&
    pageSize !== constants.SDB_PAGESIZE_64K &&
    pageSize !== constants.SDB_PAGESIZE_DEFAULT) {
    throw new Error("Invalid args");
  }

  var matcher = {};
  matcher[constants.FIELD_NAME] = spaceName;
  matcher[constants.FIELD_PAGESIZE] = pageSize;
  var that = this;
  var command = constants.ADMIN_PROMPT + constants.CREATE_CMD + " " + constants.COLSPACE;
  this.sendAdminCommand(command, matcher, {}, {}, {}, function (err, response) {
    if (err) {
      return callback(err);
    }
    callback(null, new CollectionSpace(that, spaceName));
  });
};


Connection.prototype.getCollectionSpace = function (name, callback) {
  var that = this;
  this.isCollectionSpaceExist(name, function (err, exist) {
    if (err || !exist) {
      return callback(new Error("SDB_DMS_CS_NOTEXIST"));
    }
    callback(null, new CollectionSpace(that, name));
  });
};


Connection.prototype.isCollectionSpaceExist = function (name, callback) {
  var command = constants.ADMIN_PROMPT + constants.TEST_CMD + " " + constants.COLSPACE;
  var matcher = {};
  matcher[constants.FIELD_NAME] = name;
  this.sendAdminCommand(command, matcher, {}, {}, {}, function (err, response) {
    if (!err) {
      return callback(null, true);
    }
    if (err.flags === SDBError.errors.SDB_DMS_CS_NOTEXIST) {
      callback(null, false);
    } else {
      callback(err);
    }
  });
};

Connection.prototype.sendAdminCommand = function (command, matcher, selector, orderBy, hint, callback) {
  var message = new Message(constants.Operation.OP_QUERY);
  message.CollectionFullName = command;
  message.Version = constants.DEFAULT_VERSION;
  message.W = constants.DEFAULT_W;
  message.Padding = 0;
  message.Flags = 0;
  message.NodeID = constants.ZERO_NODEID;
  message.RequestID = getNextRequstID(); // 0
  message.SkipRowsCount = Long.ZERO; // 0
  message.ReturnRowsCount = Long.NEG_ONE; // -1
  // matcher
  message.Matcher = matcher || {};
  // selector
  message.Selector = selector || {};
  // orderBy
  message.OrderBy = orderBy || {};
  // hint
  message.Hint = hint || {};

  var buff = helper.buildQueryRequest(message, this.isBigEndian);
  this.send(buff, message, callback);
};

Connection.prototype.sendAdminCommand2 = function (command, matcher, selector, orderBy, hint, skipRows, returnRows, flag, callback) {
  var message = new Message(constants.Operation.OP_QUERY);
  message.CollectionFullName = command;
  message.Version = constants.DEFAULT_VERSION;
  message.W = constants.DEFAULT_W;
  message.Padding = 0;
  message.Flags = flag;
  message.NodeID = constants.ZERO_NODEID;
  message.RequestID = getNextRequstID(); // 0
  message.SkipRowsCount = skipRows;
  message.ReturnRowsCount = returnRows;
  // matcher
  message.Matcher = matcher || {};
  // selector
  message.Selector = selector || {};
  // orderBy
  message.OrderBy = orderBy || {};
  // hint
  message.Hint = hint || {};

  var buff = helper.buildQueryRequest(message, this.isBigEndian);
  this.send(buff, message, callback);
};

Connection.prototype.getCollectionSpaces = function (callback) {
  this.getList(constants.SDB_LIST_COLLECTIONSPACES, callback);
};

var mapping = {};
// list type
mapping[constants.SDB_LIST_CONTEXTS] = constants.CONTEXTS;
mapping[constants.SDB_LIST_CONTEXTS_CURRENT] = constants.CONTEXTS_CUR;
mapping[constants.SDB_LIST_SESSIONS] = constants.SESSIONS;
mapping[constants.SDB_LIST_SESSIONS_CURRENT] = constants.SESSIONS_CUR;
mapping[constants.SDB_LIST_COLLECTIONS] = constants.COLLECTIONS;
mapping[constants.SDB_LIST_COLLECTIONSPACES] = constants.COLSPACES;
mapping[constants.SDB_LIST_STORAGEUNITS] = constants.STOREUNITS;
mapping[constants.SDB_LIST_GROUPS] = constants.GROUPS;
mapping[constants.SDB_LIST_STOREPROCEDURES] = constants.PROCEDURES;
mapping[constants.SDB_LIST_DOMAINS] = constants.DOMAINS;
mapping[constants.SDB_LIST_TASKS] = constants.TASKS;
mapping[constants.SDB_LIST_CS_IN_DOMAIN] = constants.CS_IN_DOMAIN;
mapping[constants.SDB_LIST_CL_IN_DOMAIN] = constants.CL_IN_DOMAIN;
// snapshot type
var snapshotType = {};
snapshotType[constants.SDB_SNAP_CONTEXTS] = constants.CONTEXTS;
snapshotType[constants.SDB_SNAP_CONTEXTS_CURRENT] = constants.CONTEXTS_CUR;
snapshotType[constants.SDB_SNAP_SESSIONS] = constants.SESSIONS;
snapshotType[constants.SDB_SNAP_SESSIONS_CURRENT] = constants.SESSIONS_CUR;
snapshotType[constants.SDB_SNAP_COLLECTIONS] = constants.COLLECTIONS;
snapshotType[constants.SDB_SNAP_COLLECTIONSPACES] = constants.COLSPACES;
snapshotType[constants.SDB_SNAP_DATABASE] = constants.DATABASE;
snapshotType[constants.SDB_SNAP_SYSTEM] = constants.SYSTEM;
snapshotType[constants.SDB_SNAP_CATALOG] = constants.CATA;

Connection.prototype.getList = function (type, matcher, selector, orderBy, callback) {
  var command = constants.ADMIN_PROMPT + constants.LIST_CMD + ' ';
  if (!mapping.hasOwnProperty(type)) {
    throw new Error('未知类型：' + type);
  }
  command += mapping[type];

  if (typeof matcher === "function") {
    callback = matcher;
    matcher = {};
    selector = {};
    orderBy = {};
  }

  var that = this;
  this.sendAdminCommand(command, matcher, selector, orderBy, {}, function (err, response) {
    if (!err) {
      return callback(null, new Cursor(response, that));
    }
    if (err.flags === constants.SDB_DMS_EOC) {
      callback(null, null);
    } else {
      callback(err);
    }
  });
};

Connection.prototype.getReplicaGroups = function (callback) {
  this.getList(constants.SDB_LIST_GROUPS, callback);
};

Connection.prototype.createReplicaGroup = function (name, callback) {
  if (!name) {
    throw new Error('必须传入group名');
  }
  var command = constants.ADMIN_PROMPT + constants.CREATE_CMD + ' ' +
    constants.GROUP;

  var matcher = {};
  matcher[constants.FIELD_GROUPNAME] = name;

  var that = this;
  this.sendAdminCommand(command, matcher, {}, {}, {}, function (err, response) {
    if (err) {
      return callback(err);
    }
    that.getReplicaGroupByName(name, callback);
  });
};

Connection.prototype.removeReplicaGroup = function (group, callback) {
  if (!group) {
    throw new Error('必须传入group名');
  }
  var command = constants.ADMIN_PROMPT + constants.REMOVE_CMD + ' ' +
    constants.GROUP;

  var matcher = {};
  matcher[constants.FIELD_GROUPNAME] = group;

  this.sendAdminCommand(command, matcher, {}, {}, {}, callbackWrap(callback));
};


/** \fn ReplicaGroup GetReplicaGroup(string groupName)
 *  \brief Get the ReplicaGroup by name
 *  \param groupName The group name
 *  \return The fitted ReplicaGroup or null
 *  \exception SequoiaDB.BaseException
 *  \exception System.Exception
 */
Connection.prototype.getReplicaGroupByName = function (group, callback) {
  var matcher = {};
  matcher[constants.FIELD_GROUPNAME] = group;
  var that = this;
  this.getList(constants.SDB_LIST_GROUPS, matcher, {}, {}, function (err, cursor) {
    if (err) {
      return callback(err);
    }

    if (!cursor) {
      return callback(new Error("SDB_SYS"));
    }

    cursor.next(function (err, detail) {
      if (err) {
        return callback(err);
      }
      if (detail) {
        var groupId = detail[constants.FIELD_GROUPID];
        if (typeof groupId !== "number") {
          return callback(new Error('SDB_SYS'));
        }
        callback(null, new ReplicaGroup(that, group, groupId));
      } else {
        callback(null, null);
      }
    });
  });
};

/** \fn ReplicaGroup GetReplicaGroup(string groupName)
 *  \brief Get the ReplicaGroup by name
 *  \param groupName The group name
 *  \return The fitted ReplicaGroup or null
 *  \exception SequoiaDB.BaseException
 *  \exception System.Exception
 */
Connection.prototype.getReplicaGroupById = function (groupId, callback) {
  var matcher = {};
  matcher[constants.FIELD_GROUPID] = groupId;
  var that = this;
  this.getList(constants.SDB_LIST_GROUPS, matcher, {}, {}, function (err, cursor) {
    if (err) {
      return callback(err);
    }

    if (!cursor) {
      return callback(new Error("SDB_SYS"));
    }

    cursor.next(function (err, detail) {
      if (err) {
        return callback(err);
      }
      if (detail) {
        var group = detail[constants.FIELD_GROUPNAME];
        if (typeof group !== "string") {
          return callback(new Error('SDB_SYS'));
        }
        callback(null, new ReplicaGroup(that, group, groupId));
      } else {
        callback(null, null);
      }
    });
  });
};

Connection.prototype.activateReplicaGroup = function (name, callback) {
  this.getReplicaGroupByName(name, function (err, group) {
    if (err) {
      return callback(err);
    }

    group.start(function (err, result) {
      if (err) {
        return callback(err);
      }
      result ? callback(null, group) : callback(null, null);
    });
  });
};

module.exports = Connection;
