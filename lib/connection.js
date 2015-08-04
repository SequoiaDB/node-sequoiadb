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
var Domain = require('./domain');
var ReplicaGroup = require('./replica_group');
var debug = require('debug')('sequoiadb:connection');
var getNextRequstID = require('./requestid');
var Code = require('bson').BSONPure.Code;

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
    that.emit('error', err);
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

Connection.prototype.sendLob = function (buff, message, callback) {
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
  this.parser.state = "LobRequest";
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
Connection.prototype.createCollectionSpace = function (spaceName, options, callback) {
  var matcher = {};
  matcher[constants.FIELD_NAME] = spaceName;

  // createCollectionSpace(spaceName, callback);
  if (typeof options === 'function') {
    callback = options;
    matcher[constants.FIELD_PAGESIZE] = constants.SDB_PAGESIZE_DEFAULT;
  } else if (typeof options === 'object') {
    // createCollectionSpace(spaceName, options, callback);
    util._extend(matcher, options);
  } else {
    // createCollectionSpace(spaceName, pageSize, callback);
    matcher[constants.FIELD_PAGESIZE] = options;
  }

  var pageSize = matcher[constants.FIELD_PAGESIZE];
  if (pageSize) {
    if (pageSize !== constants.SDB_PAGESIZE_4K &&
      pageSize !== constants.SDB_PAGESIZE_8K &&
      pageSize !== constants.SDB_PAGESIZE_16K &&
      pageSize !== constants.SDB_PAGESIZE_32K &&
      pageSize !== constants.SDB_PAGESIZE_64K &&
      pageSize !== constants.SDB_PAGESIZE_DEFAULT) {
      throw new Error("Invalid args");
    }
  }

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

/**
 * @fn ArrayList<String> getCollectionSpaceNames()
 * Get all the collecion space names
 * @return A list of all collecion space names
 * @exception com.sequoiadb.exception.BaseException
 */
Connection.prototype.getCollectionSpaceNames = function (callback) {
  this.getCollectionSpaces(function (err, cursor) {
    if (err) {
      return callback(err);
    }

    if (!cursor) {
      return callback(null, []);
    }
    var list = [];

    var check = function () {
      if (cursor.hasMore) {
        cursor.next(function (err, item) {
          if (item) {
            list.push(item.Name);
          }
          check();
        });
      } else {
        callback(null, list);
      }
    };
    check();
  });
};

/**
 * @fn DBCursor listCollections()
 * Get all the collections
 * @return dbCursor of all collecions
 * @exception com.sequoiadb.exception.BaseException
 */
Connection.prototype.getCollections = function (callback) {
  this.getList(constants.SDB_LIST_COLLECTIONS, callback);
};

/**
 * @fn ArrayList<String> getCollectionSpaceNames()
 * Get all the collecion space names
 * @return A list of all collecion space names
 * @exception com.sequoiadb.exception.BaseException
 */
Connection.prototype.getCollectionNames = function (callback) {
  this.getCollections(function (err, cursor) {
    if (err) {
      return callback(err);
    }

    if (!cursor) {
      return callback(null, []);
    }

    var list = [];
    var check = function () {
      if (cursor.hasMore) {
        cursor.next(function (err, item) {
          if (item) {
            list.push(item.Name);
          }
          check();
        });
      } else {
        callback(null, list);
      }
    };
    check();
  });
};

/**
 * Get all the collecion space names
 * @return A list of all collecion space names
 */
Connection.prototype.getStorageUnits = function (callback) {
  this.getList(constants.SDB_LIST_STORAGEUNITS, function (err, cursor) {
    if (err) {
      return callback(err);
    }
    if (!cursor) {
      return callback(null, []);
    }

    var list = [];
    var check = function () {
      if (cursor.hasMore) {
        cursor.next(function (err, item) {
          if (item) {
            list.push(item.Name);
          }
          check();
        });
      } else {
        callback(null, list);
      }
    };
    check();
  });
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

/**
 * Create the Replica Catalog Group with given options
 * @param hostName The host name
 * @param port The port
 * @param dbpath The database path
 * @param configure The configure options
 */
Connection.prototype.createReplicaCataGroup = function (hostname, port, dbpath, configure, callback) {
  if (!hostname || !port || !dbpath) {
    throw new Error("SDB_INVALIDARG");
  }

  var command = constants.ADMIN_PROMPT + constants.CREATE_CMD + " " +
    constants.CATALOG + " " + constants.GROUP;
  var condition = {};
  condition[constants.FIELD_HOSTNAME] = hostname;
  condition[constants.SVCNAME] = '' + port;
  condition[constants.DBPATH] = dbpath;
  if (configure) {
    var keys = Object.keys(configure);
    for (var i = 0; i < keys.length; i++) {
      var key = keys[i];
      if (key === constants.FIELD_HOSTNAME ||
        key === constants.SVCNAME ||
        key === constants.DBPATH) {
        condition[key] = '' + configure[key];
      }
    }
  }
  this.sendAdminCommand(command, condition, {}, {}, {}, callback);
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


/**
 * Get the ReplicaGroup by name
 * @param groupName The group name
 * @return The fitted ReplicaGroup or null
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

/**
 * Get the ReplicaGroup by name
 * @param groupName The group name
 * @return The fitted ReplicaGroup or null
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

/**
 * List domains.
 * @param matcher The matching rule, return all the documents if null
 * @param selector The selective rule, return the whole document if null
 * @param orderBy The ordered rule, never sort if null
 * @return the cursor of the result.
 */
Connection.prototype.getDomains = function (matcher, selector, orderBy, hint, callback) {
  this.getList(constants.SDB_LIST_DOMAINS, matcher, selector, orderBy, callback);
};

/**
 * @fn Domain getDomain(String domainName)
 * Get the specified domain.
 * @param domainName the name of the domain
 * @return the Domain instance
 *            If the domain not exit, throw BaseException with the error type "SDB_CAT_DOMAIN_NOT_EXIST"
 */
Connection.prototype.getDomain = function (domainName, callback) {
  var that = this;
  this.isDomainExist(domainName, function (err, exist) {
    if (err) {
      return callback(err);
    }

    if (exist) {
      callback(null, new Domain(that, domainName));
    } else {
      callback(null, null);
    }
  });
};

/**
 * Verify the existence of domain.
 * @param domainName the name of domain
 * @return True if existed or False if not existed
 */
Connection.prototype.isDomainExist = function (domainName, callback) {
  if (!domainName) {
    throw new Error('SDB_INVALIDARG');
  }

  var matcher = {};
  matcher[constants.FIELD_NAME] = domainName;
  this.getList(constants.SDB_LIST_DOMAINS, matcher, null, null, function (err, cursor) {
    if (err) {
      return callback(err);
    }
    if (!cursor) {
      return callback(null, false);
    }
    cursor.current(function (err, item) {
      if (err) {
        return callback(err);
      }
      callback(null, !!item);
    });
  });
};

/**
 * Create a domain.
 * @param domainName The name of the creating domain
 * @param options The options for the domain. The options are as below:
 * <ul>
 * <li>Groups    : the list of the replica groups' names which the domain is going to contain.
   *                 eg: { "Groups": [ "group1", "group2", "group3" ] }
   *                 If this argument is not included, the domain will contain all replica groups in the cluster.
 * <li>AutoSplit    : If this option is set to be true, while creating collection(ShardingType is "hash") in this domain,
   *                    the data of this collection will be split(hash split) into all the groups in this domain automatically.
   *                    However, it won't automatically split data into those groups which were add into this domain later.
   *                    eg: { "Groups": [ "group1", "group2", "group3" ], "AutoSplit: true" }
 * </ul>
 * @return the newly created collection space object
 */
Connection.prototype.createDomain = function (domainName, options, callback) {
  if (!domainName) {
    throw new Error("SDB_INVALIDARG");
  }

  if (typeof options === 'function') {
    callback = options;
    options = null;
  }

  var that = this;
  this.isDomainExist(domainName, function (err, exist) {
    if (err) {
      return callback(err);
    }

    if (exist) {
      return callback(new Error("SDB_CAT_DOMAIN_EXIST"));
    }

    var matcher = {};
    matcher[constants.FIELD_NAME] = domainName;
    if (options) {
      matcher[constants.FIELD_OPTIONS] = options;
    }
    var command = constants.ADMIN_PROMPT + constants.CREATE_CMD + " " + constants.DOMAIN;
    that.sendAdminCommand(command, matcher, null, null, null, function (err, response) {
      if (err) {
        return callback(err);
      }
      callback(null, new Domain(that, domainName));
    });
  });
};

Connection.prototype.dropDomain = function (name, callback) {
  if (!name) {
    throw new Error("必须传入domain名");
  }
  var matcher = {};
  matcher[constants.FIELD_NAME] = name;

  var command = constants.ADMIN_PROMPT + constants.DROP_CMD + ' ' + constants.DOMAIN;
  this.sendAdminCommand(command, matcher, null, null, null, callbackWrap(callback));
};

/*
 * @param options Contains a series of backup configuration infomations.
 *         Backup the whole cluster if null. The "options" contains 5 options as below.
 *         All the elements in options are optional.
 *         eg: {"GroupName":["rgName1", "rgName2"], "Path":"/opt/sequoiadb/backup",
 *             "Name":"backupName", "Description":description, "EnsureInc":true, "OverWrite":true}
 *         <ul>
 *          <li>GroupName   : The replica groups which to be backuped
 *          <li>Path        : The backup path, if not assign, use the backup path assigned in configuration file
 *          <li>Name        : The name for the backup
 *          <li>Description : The description for the backup
 *          <li>EnsureInc   : Whether excute increment synchronization, default to be false
 *          <li>OverWrite   : Whether overwrite the old backup file, default to be false
 *         </ul>
*/
Connection.prototype.backupOffline = function (options, callback) {
  if (!options) {
    throw new Error("参数错误");
  }
  var names = Object.keys(options);
  // if (names.length === 0) {
  //   throw new Error("参数错误");
  // }

  for (var i = 0; i < names.length; i++) {
    var name = names[i];
    if (name !== constants.FIELD_GROUPNAME &&
      name !== constants.FIELD_NAME &&
      name !== constants.FIELD_PATH &&
      name !== constants.FIELD_DESP &&
      name !== constants.FIELD_ENSURE_INC &&
      name !== constants.FIELD_OVERWRITE) {
      throw new Error("参数错误：" + name);
    }
  }

  var command = constants.ADMIN_PROMPT + constants.BACKUP_OFFLINE_CMD;
  this.sendAdminCommand(command, options, {}, {}, {}, callback);
};

/**
 * List the backups.
 * @param options Contains configuration infomations for remove backups, list all the backups in the default backup path if null.
 *         The "options" contains 3 options as below. All the elements in options are optional.
 *         eg: {"GroupName":["rgName1", "rgName2"], "Path":"/opt/sequoiadb/backup", "Name":"backupName"}
 *         <ul>
 *          <li>GroupName   : Assign the backups of specifed replica groups to be list
 *          <li>Path        : Assign the backups in specifed path to be list, if not assign, use the backup path asigned in the configuration file
 *          <li>Name        : Assign the backups with specifed name to be list
 *         </ul>
 * @param matcher The matching rule, return all the documents if null
 * @param selector The selective rule, return the whole document if null
 * @param orderBy The ordered rule, never sort if null
 * @return the DBCursor of the backup or null while having no backup infonation
 */
Connection.prototype.getBackups = function (options, matcher, selector, orderBy, callback) {
  if (!options) {
    throw new Error("参数错误");
  }

  var names = Object.keys(options);
  for (var i = 0; i < names.length; i++) {
    var name = names[i];
    if (name !== constants.FIELD_GROUPNAME &&
      name !== constants.FIELD_NAME &&
      name !== constants.FIELD_PATH) {
      throw new Error("参数错误：" + name);
    }
  }

  var command = constants.ADMIN_PROMPT + constants.LIST_BACKUP_CMD;
  var that = this;
  this.sendAdminCommand(command, matcher, selector, orderBy, options, function (err, response) {
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

/**
 * Remove the backups.
 * @param options Contains configuration infomations for remove backups, remove all the backups in the default backup path if null.
 *                 The "options" contains 3 options as below. All the elements in options are optional.
 *                 eg: {"GroupName":["rgName1", "rgName2"], "Path":"/opt/sequoiadb/backup", "Name":"backupName"}
 *                 <ul>
 *                  <li>GroupName   : Assign the backups of specifed replica grouops to be remove
 *                  <li>Path        : Assign the backups in specifed path to be remove, if not assign, use the backup path asigned in the configuration file
 *                  <li>Name        : Assign the backups with specifed name to be remove
 *                 </ul>
 */
Connection.prototype.removeBackup = function (matcher, callback) {
  if (!matcher) {
    throw new Error("参数错误");
  }

  var names = Object.keys(matcher);
  for (var i = 0; i < names.length; i++) {
    var name = names[i];
    if (name !== constants.FIELD_GROUPNAME &&
      name !== constants.FIELD_NAME &&
      name !== constants.FIELD_PATH) {
      throw new Error("参数错误：" + name);
    }
  }

  var command = constants.ADMIN_PROMPT + constants.REMOVE_BACKUP_CMD;
  this.sendAdminCommand(command, matcher, {}, {}, {}, callback);
};

/**
 * Set the attributes of the session.
 * @param options The configuration options for session.The options are as below:
 *
 *      PreferedInstance : indicate which instance to respond read request in current session.
 *                         eg:{"PreferedInstance":"m"/"M"/"s"/"S"/"a"/"A"/1-7},
 *                         prefer to choose "read and write instance"/"read only instance"/"anyone instance"/instance1-insatance7,
 *                         default to be {"PreferedInstance":"A"}, means would like to choose anyone instance to respond read request such as query.
 */
Connection.prototype.setSessionAttr = function (options, callback) {
  if (!options || !options[constants.FIELD_PREFERED_INSTANCE]) {
    throw new Error("SDB_INVALIDARG");
  }
  var attr = {};
  var value = options[constants.FIELD_PREFERED_INSTANCE];
  if (typeof value === "number") {
    if (value < 1 || value > 7) {
      throw new Error("参数错误");
    }
    attr[constants.FIELD_PREFERED_INSTANCE] = value;
  } else if (typeof value === "string") {
    var val = constants.PreferInstanceType.INS_TYPE_MIN;
    if (value === "M" || value === "m") {
      val = constants.PreferInstanceType.INS_MASTER;
    } else if (value === "S" || value === "s" ||
      value === "A" || value === "a") {
      val = constants.PreferInstanceType.INS_SLAVE;
    } else {
      throw new Error("参数错误");
    }
    attr[constants.FIELD_PREFERED_INSTANCE] = val;
  } else {
    throw new Error("参数错误");
  }
  var command = constants.ADMIN_PROMPT + constants.SETSESS_ATTR;
  this.sendAdminCommand(command, attr, {}, {}, {}, callback);
};

Connection.prototype.getTasks = function (matcher, selector, orderBy, hint, callback) {
  var command = constants.ADMIN_PROMPT + constants.LIST_TASK_CMD;
  var that = this;
  this.sendAdminCommand(command, matcher, selector, orderBy, hint, function (err, response) {
    if (err) {
      return callback(err);
    }
    return callback(null, new Cursor(response, that));
  });
};

Connection.prototype.waitTasks = function (taskIds, callback) {
  if (!taskIds || taskIds.length === 0) {
    throw new Error("SDB_INVALIDARG");
  }
  var matcher = {};
  matcher[constants.FIELD_TASKID] = {
    "$in": taskIds
  };
  var command = constants.ADMIN_PROMPT + constants.WAIT_TASK_CMD;
  this.sendAdminCommand(command, matcher, {}, {}, {}, callback);
};

Connection.prototype.cancelTask = function (taskId, isAsync, callback) {
  if (taskId <= 0) {
    throw new Error("SDB_INVALIDARG");
  }
  var matcher = {};
  matcher[constants.FIELD_TASKID] = taskId;
  matcher[constants.FIELD_ASYNC] = isAsync;
  var command = constants.ADMIN_PROMPT + constants.CANCEL_TASK_CMD;
  this.sendAdminCommand(command, matcher, {}, {}, {}, callback);
};

/**
 * Set the attributes of the session.
 * @param options The configuration options for session.The options are as below:
 *
 *      PreferedInstance : indicate which instance to respond read request in current session.
 *                         eg:{"PreferedInstance":"m"/"M"/"s"/"S"/"a"/"A"/1-7},
 *                         prefer to choose "read and write instance"/"read only instance"/"anyone instance"/instance1-insatance7,
 *                         default to be {"PreferedInstance":"A"}, means would like to choose anyone instance to respond read request such as query.
 */
Connection.prototype.setSessionAttr = function (options, callback) {
  if (!options || !options[constants.FIELD_PREFERED_INSTANCE]) {
    throw new Error("SDB_INVALIDARG");
  }
  var attr = {};
  var value = options[constants.FIELD_PREFERED_INSTANCE];
  if (typeof value === "number") {
    if (value < 1 || value > 7) {
      throw new Error("参数错误");
    }
    attr[constants.FIELD_PREFERED_INSTANCE] = value;
  } else if (typeof value === "string") {
    var val = constants.PreferInstanceType.INS_TYPE_MIN;
    if (value === "M" || value === "m") {
      val = constants.PreferInstanceType.INS_MASTER;
    } else if (value === "S" || value === "s" ||
      value === "A" || value === "a") {
      val = constants.PreferInstanceType.INS_SLAVE;
    } else {
      throw new Error("参数错误");
    }
    attr[constants.FIELD_PREFERED_INSTANCE] = val;
  } else {
    throw new Error("参数错误");
  }
  var command = constants.ADMIN_PROMPT + constants.SETSESS_ATTR;
  this.sendAdminCommand(command, attr, {}, {}, {}, callbackWrap(callback));
};

Connection.prototype.transactionBegin = function (callback) {
  var message = new Message(constants.Operation.OP_TRANS_BEGIN);
  message.RequestID = getNextRequstID();
  var buff = helper.buildTransactionRequest(message, this.isBigEndian);
  this.send(buff, message, callbackWrap(callback));
};

Connection.prototype.transactionCommit = function (callback) {
  var message = new Message(constants.Operation.OP_TRANS_COMMIT);
  message.RequestID = getNextRequstID();
  var buff = helper.buildTransactionRequest(message, this.isBigEndian);
  this.send(buff, message, callbackWrap(callback));
};

Connection.prototype.transactionRollback = function (callback) {
  var message = new Message(constants.Operation.OP_TRANS_ROLLBACK);
  message.RequestID = getNextRequstID();
  var buff = helper.buildTransactionRequest(message, this.isBigEndian);
  this.send(buff, message, callbackWrap(callback));
};

/**
 * Create a store procedure.
 * @param code The code of store procedure
 * @exception com.sequoiadb.exception.BaseException
 */
Connection.prototype.createProcedure = function (code, callback) {
  if (!code) {
    throw new Error("SDB_INVALIDARG");
  }

  var matcher = {};
  matcher[constants.FIELD_FUNCTYPE] = new Code(code);
  matcher[constants.FMP_FUNC_TYPE] = constants.FMP_FUNC_TYPE_JS;
  this.sendAdminCommand2(constants.CRT_PROCEDURES_CMD, matcher, null, null, null,
                         0, 0, -1,
                         callbackWrap(this, callback));
};

/**
 * Remove a store procedure.
 * @param name The name of store procedure to be removed
 */
Connection.prototype.removeProcedure = function (name, callback) {
  if (!name) {
    throw new Error("SDB_INVALIDARG", name);
  }

  var matcher = {};
  matcher[constants.FIELD_FUNCTYPE] = name;

  this.sendAdminCommand2(constants.RM_PROCEDURES_CMD, matcher, null, null, null,
                         0, 0, -1,
                         callbackWrap(this, callback));
};

/**
 * List the store procedures.
 * @param condition The condition of list eg: {"name":"sum"}
 */
Connection.prototype.getProcedures = function (matcher, callback) {
  this.getList(constants.SDB_LIST_STOREPROCEDURES, matcher, {}, {}, callback);
};

/**
 * Eval javascript code.
 * @param code The javasript code
 * @return The result of the eval operation, including the return value type,
 *         the return data and the error message. If succeed to eval, error message is null,
 *         and we can extract the eval result from the return cursor and return type,
 *         if not, the return cursor and the return type are null, we can extract
 *         the error mssage for more detail.
 */
Connection.prototype.evalJS = function (code, callback) {
  if (!code){
    throw new Error("SDB_INVALIDARG");
  }

  var matcher = {};
  matcher[constants.FIELD_FUNCTYPE] = new Code(code);
  matcher[constants.FMP_FUNC_TYPE] = constants.FMP_FUNC_TYPE_JS;

  var that = this;
  this.sendAdminCommand2(constants.CRT_PROCEDURES_CMD, matcher, null, null, null,
                         0, 0, -1, function (err, response) {
    if (err) {
      return callback(err);
    }

    var result = {};
    var typeValue = response.NumReturned;
    result.returnType = typeValue;
    result.cursor = new Cursor(response, that);
    callback(null, result);
  });
};

/**
 * Execute sql in database.
 * @param sql the SQL command
 * @return the DBCursor of the result
 */
Connection.prototype.exec = function (sql, callback) {
  var message = new Message();
  message.RequestID = getNextRequstID();
  message.NodeID = constants.ZERO_NODEID;
  var buff = helper.buildSQLMessage(message, sql, this.isBigEndian);
  var that = this;
  this.send(buff, message, function (err, response) {
    if (response.OperationCode !== constants.Operation.MSG_BS_SQL_RES) {
      callback(new Error("SDB_UNKNOWN_MESSAGE", response.OperationCode));
      return;
    }

    if (err) {
      if (err.flags === constants.SDB_DMS_EOC) {
        callback(null, null);
      } else {
        callback(new Error(err.flags, sql));
      }
    } else {
      callback(null, new Cursor(response, that));
    }
  });
};

/**
 * Flush the options to configuration file
 * @param param
 *            The param of flush, pass {"Global":true} or {"Global":false}
 *            In cluster environment, passing {"Global":true} will flush data's and catalog's configuration file,
 *            while passing {"Global":false} will flush coord's configuration file
 *            In stand-alone environment, both them have the same behaviour
 */
Connection.prototype.flushConfigure = function (matcher, callback) {
  var command = constants.EXPORT_CONFIG_CMD;
  this.sendAdminCommand2(command, matcher, null, null, null,
                         0, 0, -1, callbackWrap(callback));
};

/**
* Execute sql in database.
* @param sql the SQL command.
* @exception com.sequoiadb.exception.BaseException
*/
Connection.prototype.execUpdate = function (sql, callback) {
  var message = new Message();
  message.RequestID = getNextRequstID();
  message.NodeID = constants.ZERO_NODEID;
  var buff = helper.buildSQLMessage(message, sql, this.isBigEndian);

  this.send(buff, message, function (err, response) {
    if (response.OperationCode !== constants.Operation.MSG_BS_SQL_RES) {
      callback(new Error("SDB_UNKNOWN_MESSAGE", response.OperationCode));
      return;
    }

    var flags = response.Flags;
    if (flags !== 0) {
      return callback(new Error(flags, sql));
    }
    callback(null);
  });
};

/**
 * Reset the snapshot.
 * @return void
 */
Connection.prototype.resetSnapshot = function (callback) {
  var command = constants.SNAP_CMD + " " + constants.RESET;
  this.sendAdminCommand2(command, null, null, null, null, 0, -1, -1, callbackWrap(callback));
};

/**
 * Get snapshot of the database.
 * @param snapType The snapshot types are as below:
 * <dl>
 * <dt>Sequoiadb.SDB_SNAP_CONTEXTS   : Get all contexts' snapshot
 * <dt>Sequoiadb.SDB_SNAP_CONTEXTS_CURRENT        : Get the current context's snapshot
 * <dt>Sequoiadb.SDB_SNAP_SESSIONS        : Get all sessions' snapshot
 * <dt>Sequoiadb.SDB_SNAP_SESSIONS_CURRENT        : Get the current session's snapshot
 * <dt>Sequoiadb.SDB_SNAP_COLLECTIONS        : Get the collections' snapshot
 * <dt>Sequoiadb.SDB_SNAP_COLLECTIONSPACES        : Get the collection spaces' snapshot
 * <dt>Sequoiadb.SDB_SNAP_DATABASE        : Get database's snapshot
 * <dt>Sequoiadb.SDB_SNAP_SYSTEM        : Get system's snapshot
 * <dt>Sequoiadb.SDB_SNAP_CATALOG        : Get catalog's snapshot
 * <dt>Sequoiadb.SDB_LIST_GROUPS        : Get replica group list ( only applicable in sharding env )
 * <dt>Sequoiadb.SDB_LIST_STOREPROCEDURES           : Get stored procedure list ( only applicable in sharding env )
 * </dl>
 * @param matcher
 *            the matching rule, match all the documents if null
 * @param selector
 *            the selective rule, return the whole document if null
 * @param orderBy
 *            the ordered rule, never sort if null
 * @return the DBCursor instance of the result
 */
Connection.prototype.getSnapshot = function (type, matcher, selector, orderBy, callback) {
  if (!snapshotType.hasOwnProperty(type)) {
    throw new Error('SDB_INVALIDARG');
  }

  var command = constants.SNAP_CMD + " " + snapshotType[type];

  var that = this;
  this.sendAdminCommand2(command, matcher, selector, orderBy, null, 0, -1, -1, function (err, response) {
    if (!err) {
      callback(null, new Cursor(response, that));
      return;
    }
    if (err.flags === constants.SDB_DMS_EOC) {
      callback(null, null);
    } else {
      callback(err);
    }
  });
};

module.exports = Connection;
