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

var Node = function (group, hostname, port, nodeId) {
  this.group = group;
  this.hostname = hostname;
  this.port = port;
  this.nodename = hostname + constants.NODE_NAME_SERVICE_SEP + port;
  this.nodeId = nodeId;
};

Node.prototype.stop = function (callback) {
  this.stopStart(false, callback);
};

Node.prototype.start = function (callback) {
  this.stopStart(true, callback);
};

Node.prototype.getStatus = function (callback) {
  var command = constants.ADMIN_PROMPT + constants.SNAP_CMD + " " +
                constants.DATABASE;
  var matcher = {};
  matcher[constants.FIELD_GROUPID] = this.group.GroupID;
  matcher[constants.FIELD_NODEID] = this.nodeID;
  this.group.client.sendAdminCommand(command, matcher, {}, {}, {}, function (err, response) {
    if (!err) {
      return callback(null, constants.NodeStatus.SDB_NODE_ACTIVE);
    }
    if (err.flags === constants.SDB_NET_CANNOT_CONNECT) {
      callback(null, constants.NodeStatus.SDB_NODE_INACTIVE);
    } else {
      callback(null, constants.NodeStatus.SDB_NODE_UNKNOWN);
    }
  });
};

/**
 * Connect to remote Sequoiadb database node
 * @return The Sequoiadb connection
 */
Node.prototype.connect = function (username, password) {
  username || (username = "");
  password || (password = "");
  var Client = require('./client');
  return new Client(this.port, this.hostname, {
    user: username,
    pass: password
  });
};

Node.prototype.stopStart = function (start, callback) {
  var command = constants.ADMIN_PROMPT +
    (start ? constants.STARTUP_CMD + " " + constants.NODE
           : constants.SHUTDOWN_CMD + " " + constants.NODE);
  var matcher = {};
  matcher[constants.FIELD_HOSTNAME] = this.hostname;
  matcher[constants.SVCNAME] = '' + this.port;
  this.group.client.sendAdminCommand(command, matcher, {}, {}, {}, function (err) {
    callback(null, !err);
  });
};

module.exports = Node;
