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
var SDBError = require('./error').SDBError;

var Domain = function (conn, name) {
  Object.defineProperty(this, 'conn', {
    value: conn,
    enumerable: false
  });
  this.name = name;
};

/**
 * Alter current domain.
 * @param [in] options The options user wants to alter
 *
 *      Groups:    The list of replica groups' names which the domain is going to contain.
 *                 eg: { "Groups": [ "group1", "group2", "group3" ] }, it means that domain
 *                 changes to contain "group1", "group2" and "group3".
 *                 We can add or remove groups in current domain. However, if a group has data
 *                 in it, remove it out of domain will be failing.
 *      AutoSplit: Alter current domain to have the ability of automatically split or not.
 *                 If this option is set to be true, while creating collection(ShardingType is "hash") in this domain,
 *                 the data of this collection will be split(hash split) into all the groups in this domain automatically.
 *                 However, it won't automatically split data into those groups which were add into this domain later.
 *                 eg: { "Groups": [ "group1", "group2", "group3" ], "AutoSplit: true" }
 */
Domain.prototype.alter = function (options, callback) {
  if (!options) {
    throw new SDBError("SDB_INVALIDARG");
  }

  var matcher = {};
  matcher[constants.FIELD_NAME] = this.name;
  matcher[constants.FIELD_OPTIONS] = options;
  var command = constants.ADMIN_PROMPT + constants.ALTER_CMD + " " + constants.DOMAIN;
  this.conn.sendAdminCommand(command, matcher, null, null, null, callback);
};

/**
 * List all the collection spaces in current domain.
 * @return The DBCursor of result
 */
Domain.prototype.getCollectionSpaces = function (callback) {
  this.getList(constants.SDB_LIST_CS_IN_DOMAIN, callback);
};

/**
 * List all the collections in current domain.
 * @return The DBCursor of result
 */
Domain.prototype.getCollections = function (callback) {
  this.getList(constants.SDB_LIST_CL_IN_DOMAIN, callback);
};

Domain.prototype.getList = function (type, callback) {
  var matcher = {};
  matcher[constants.FIELD_DOMAIN] = this.name;
  var selector = {};
  selector[constants.FIELD_NAME] = null;
  this.conn.getList(type, matcher, selector, null, callback);
};

module.exports = Domain;
