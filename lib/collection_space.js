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

var util = require('util');
var constants = require('./const');
var SDBError = require('./error');
var Collection = require('./collection');

var CollectionSpace = function (conn, name) {
  Object.defineProperty(this, 'conn', {
    value: conn,
    enumerable: false
  });
  this.name = name;
};

CollectionSpace.prototype.getCollection = function (name, callback) {
  var that = this;
  this.isCollectionExist(name, function (err, has) {
    if (err) {
      return callback(err);
    }
    if (has) {
      callback(null, new Collection(that, name));
    } else {
      callback(null, null);
    }
  });
};

CollectionSpace.prototype.isCollectionExist = function (name, callback) {
  var command = constants.ADMIN_PROMPT + constants.TEST_CMD + " " + constants.COLLECTION;
  var matcher = {};
  matcher[constants.FIELD_NAME] = this.name + "." + name;

  this.conn.sendAdminCommand(command, matcher, {}, {}, {}, function (err, response) {
    if (!err) {
      return callback(null, true);
    }

    if (err.flags === SDBError.errors.SDB_DMS_NOTEXIST) {
      callback(null, false);
    } else {
      callback(err);
    }
  });
};

/**
 * Create the named collection in current collection space
 * @param collectionName The collection name
 * @return The DBCollection handle
 */
CollectionSpace.prototype.createCollection = function (name, options, callback) {
  var command = constants.ADMIN_PROMPT + constants.CREATE_CMD + " " + constants.COLLECTION;
  var matcher = {};
  matcher[constants.FIELD_NAME] = this.name + "." + name;
  if (typeof options === "function") {
    callback = options;
  } else {
    util._extend(matcher, options);
  }

  var that = this;
  this.conn.sendAdminCommand(command, matcher, {}, {}, {}, function (err, response) {
    if (err) {
      return callback(err);
    }
    callback(null, new Collection(that, name));
  });
};

CollectionSpace.prototype.dropCollection = function (name, callback) {
  var command = constants.ADMIN_PROMPT + constants.DROP_CMD + " " + constants.COLLECTION;

  var matcher = {};
  matcher[constants.FIELD_NAME] = this.name + "." + name;

  this.conn.sendAdminCommand(command, matcher, {}, {}, {}, function (err, response) {
    callback(err);
  });
};

module.exports = CollectionSpace;
