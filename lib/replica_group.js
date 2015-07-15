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
var Node = require('./node');
var util = require('util');

var ReplicaGroup = function (conn, name, groupId) {
  Object.defineProperty(this, 'conn', {
    value: conn,
    enumerable: false
  });
  this.name = name;
  this.groupId = groupId;
  this.isCatalog = (name === constants.CATALOG_GROUP);
};

/** \fn bool Stop()
 *  \brief Stop the current node
 *  \return True if succeed or False if fail
 *  \exception SequoiaDB.Error
 *  \exception System.Exception
 */
ReplicaGroup.prototype.stop = function (callback) {
  this.stopStart(false, callback);
};


/** \fn bool Start()
 *  \brief Start the current node
 *  \return True if succeed or False if fail
 *  \exception SequoiaDB.Error
 *  \exception System.Exception
 */
ReplicaGroup.prototype.start = function (callback) {
  this.stopStart(true, callback);
};

/** \fn var GetNodeNum( SDBConst.NodeStatus status)
 *  \brief Get the count of node with given status
 *  \param status The specified status as below:
 *
 *      SDB_NODE_ALL
 *      SDB_NODE_ACTIVE
 *      SDB_NODE_INACTIVE
 *      SDB_NODE_UNKNOWN
 *  \return The count of node
 *  \exception SequoiaDB.Error
 *  \exception System.Exception
 */
ReplicaGroup.prototype.getNodeCount = function (callback) {
  this.getDetail(function (err, detail) {
    if (err) {
      return callback(err);
    }
    var nodes = detail[constants.FIELD_GROUP];
    callback(null, nodes.length || 0);
  });
};

/** \fn var GetDetail()
 *  \brief Get the detail information of current group
 *  \return The detail information in var object
 *  \exception SequoiaDB.Error
 *  \exception System.Exception
 */
ReplicaGroup.prototype.getDetail = function (callback) {
  var matcher = {};
  matcher[constants.FIELD_GROUPNAME] = this.name;
  matcher[constants.FIELD_GROUPID] = this.groupId;
  this.conn.getList(constants.SDB_LIST_GROUPS, matcher, {}, {}, function (err, cursor) {
    if (err) {
      return callback(err);
    }
    if (cursor) {
      cursor.next(function (err, detail) {
        if (err) {
          return callback(err);
        }
        if (detail) {
          callback(null, detail);
        } else {
          callback(new Error('SDB_CLS_GRP_NOT_EXIST'));
        }
      });
    } else {
      callback(new Error('SDB_SYS'));
    }
  });
};

/** \fn Node CreateNode(string hostName, var port, string dbpath,
                       Dictionary<string, string> map)
 *  \brief Create the replica node
 *  \param hostName The host name of node
 *  \param port The port of node
 *  \param dbpath The database path of node
 *  \param map The other configure information of node
 *  \return The Node object
 *  \exception SequoiaDB.Error
 *  \exception System.Exception
 */
ReplicaGroup.prototype.createNode = function (hostname, port, dbpath, map, callback) {
  if (!hostname || port < 0 || port > 65535 || !dbpath) {
    throw new Error('SDB_INVALIDARG');
  }
  var command = constants.ADMIN_PROMPT + constants.CREATE_CMD + " " +
                constants.NODE;
  var matcher = {};
  matcher[constants.FIELD_GROUPNAME] = this.name;
  // TODO: 删除属性不好
  delete map[constants.FIELD_GROUPNAME];
  matcher[constants.FIELD_HOSTNAME] = hostname;
  delete map[constants.FIELD_HOSTNAME];
  matcher[constants.SVCNAME] = '' + port;
  delete map[constants.SVCNAME];
  matcher[constants.DBPATH] = dbpath;
  delete map[constants.DBPATH];
  util._extend(matcher, map);

  var that = this;
  this.conn.sendAdminCommand(command, matcher, {}, {}, {}, function (err) {
    if (err) {
      return callback(err);
    }
    that.getNode(hostname, port, callback);
  });
};

/** \fn void RemoveNode(string hostName, var port,
               var configure)
 *  \brief Remove the specified replica node
 *  \param hostName The host name of node
 *  \param port The port of node
 *  \param configure The configurations for the replica node
 *  \exception SequoiaDB.Error
 *  \exception System.Exception
 */
ReplicaGroup.prototype.removeNode = function (hostname, port, configure, callback) {
  if (!hostname || port < 0 || port > 65535) {
    throw new Error("SDB_INVALIDARG");
  }
  var command = constants.ADMIN_PROMPT + constants.REMOVE_CMD + " " +
      constants.NODE;
  var config = {};
  config[constants.FIELD_GROUPNAME] = this.name;
  config[constants.FIELD_HOSTNAME] = hostname;
  config[constants.SVCNAME] = '' + port;
  if (!configure) {
    var keys = Object.keys(configure);
    for (var i = 0; i < keys.length; i++) {
      var key = keys[i];
      if (key === constants.FIELD_GROUPNAME ||
          key === constants.FIELD_HOSTNAME ||
          key === constants.SVCNAME) {
        continue;
      }
      config[key] = configure[key];
    }
  }

  this.conn.sendAdminCommand(command, config, {}, {}, {}, function (err) {
    callback(err);
  });
};

/** \fn Node GetMaster()
 *  \brief Get the master node of current group
 *  \return The fitted node or null
 *  \exception SequoiaDB.Error
 *  \exception System.Exception
 */
ReplicaGroup.prototype.getMaster = function (callback) {
  var that = this;
  this.getDetail(function (err, detail) {
    if (err) {
      return callback(err);
    }
    var primaryNode = detail[constants.FIELD_PRIMARYNODE];
    var nodes = detail[constants.FIELD_GROUP];
    if (typeof primaryNode !== 'number' || !Array.isArray(nodes)) {
      return callback(new Error("SDB_SYS"));
    }

    for (var i = 0; i < nodes.length; i++) {
      var node = nodes[i];
      var nodeId = node[constants.FIELD_NODEID];
      if (typeof nodeId !== 'number') {
        return callback(new Error("SDB_SYS"));
      }
      if (nodeId === primaryNode) {
        var extracted = that.extractNode(node);
        return callback(null, extracted);
      }
    }
    callback(null, null);
  });
};

/** \fn Node GetSlave()
 *  \brief Get the slave node of current group
 *  \return The fitted node or null
 *  \exception SequoiaDB.Error
 *  \exception System.Exception
 */
ReplicaGroup.prototype.getSlave = function (callback) {
  var that = this;
  this.getDetail(function (err, detail) {
    if (err) {
      return callback(err);
    }
    var primaryID = detail[constants.FIELD_PRIMARYNODE];
    var nodes = detail[constants.FIELD_GROUP];
    if (typeof primaryID !== 'number' || !Array.isArray(nodes)) {
      return callback(new Error("SDB_SYS"));
    }
    var slaves = [];
    var primaryNode;
    for (var i = 0; i < nodes.length; i++) {
      var node = nodes[i];
      var nodeId = node[constants.FIELD_NODEID];
      if (typeof nodeId !== 'number') {
        return callback(new Error("SDB_SYS"));
      }
      if (nodeId !== primaryID) {
        slaves.push(node);
      } else {
        primaryNode = node;
      }
    }

    if (slaves.length > 0) {
      // 随机取一个
      var index = (new Date().getTime()) % slaves.length;
      callback(null, that.extractNode(nodes[index]));
    } else {
      callback(null, that.extractNode(primaryNode));
    }
  });
};

/** \fn Node GetNode(string nodeName)
 *  \brief Get the node by node name
 *  \param nodeName The node name
 *  \return The fitted node or null
 *  \exception SequoiaDB.Error
 *  \exception System.Exception
 */
ReplicaGroup.prototype.getNodeByName = function (nodename, callback) {
  if (!nodename || nodename.indexOf(constants.NODE_NAME_SERVICE_SEP) === -1) {
    throw new Error("SDB_INVALIDARG");
  }

  var parts = nodename.split(constants.NODE_NAME_SERVICE_SEP);
  var hostname = parts[0];
  var port = parseInt(parts[1], 10);
  if (!hostname || !port) {
    throw new Error("SDB_INVALIDARG");
  }
  this.getNode(hostname, port, callback);
};

/** \fn Node GetNode(string hostName, var port)
 *  \brief Get the node by host name and port
 *  \param hostName The host name
 *  \param port The port
 *  \return The fitted node or null
 *  \exception SequoiaDB.Error
 *  \exception System.Exception
 */
ReplicaGroup.prototype.getNode = function (hostname, port, callback) {
  var that = this;
  this.getDetail(function (err, detail) {
    if (err) {
      return callback(err);
    }
    var nodes = detail[constants.FIELD_GROUP];
    if (!Array.isArray(nodes)) {
      return callback(new Error("SDB_SYS"));
    }

    for (var i = 0; i < nodes.length; i++) {
      var node = nodes[i];
      var _hostname = node[constants.FIELD_HOSTNAME];
      if (typeof _hostname !== 'string') {
        return callback(new Error("SDB_SYS"));
      }
      if (hostname === _hostname) {
        var extracted = that.extractNode(node);
        if (extracted.port === port) {
          return callback(null, extracted);
        }
      }
    }
    callback(null, null);
  });
};

ReplicaGroup.prototype.extractNode = function (node) {
  var hostname = node[constants.FIELD_HOSTNAME];
  if (typeof hostname !== 'string') {
    throw new Error("SDB_SYS");
  }

  var nodeId = node[constants.FIELD_NODEID];
  if (typeof nodeId !== 'number') {
    throw new Error("SDB_SYS");
  }

  var svcs = node[constants.FIELD_SERVICE];
  if (!Array.isArray(svcs)) {
    throw new Error("SDB_SYS");
  }

  for (var i = 0; i < svcs.length; i++) {
    var svc = svcs[i];
    var type = svc[constants.FIELD_SERVICE_TYPE];
    if (typeof type !== 'number') {
      throw new Error("SDB_SYS");
    }
    if (type === 0) {
      var serviceName = svc[constants.FIELD_NAME];
      return new Node(this, hostname, parseInt(serviceName, 10), nodeId);
    }
  }
  return null;
};

ReplicaGroup.prototype.stopStart = function (start, callback) {
  var command = constants.ADMIN_PROMPT +
      (start ? constants.ACTIVE_CMD
             : constants.SHUTDOWN_CMD) + " " + constants.GROUP;
  var matcher = {};
  matcher[constants.FIELD_GROUPNAME] = this.name;
  matcher[constants.FIELD_GROUPID] = this.groupId;

  this.conn.sendAdminCommand(command, matcher, {}, {}, {}, function (err, response) {
    callback(null, !err);
  });
};

module.exports = ReplicaGroup;
