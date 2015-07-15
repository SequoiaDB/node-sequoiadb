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
