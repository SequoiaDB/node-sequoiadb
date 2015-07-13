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

var Cursor = require('./cursor');
var constants = require('./const');
var Message = require('./message');
var Query = require('./query');
var bson = require('bson');
var helper = require('./helper');
var Long = require("./long");
var getNextRequstID = require('./requestid');
var debug = require('debug')('sequoiadb:collection');

var Collection = function (space, name) {
  this.collSpace = space;
  this.name = name;
  this.collectionFullName = space.name + "." + name;
  Object.defineProperty(this, 'conn', {
    value: space.conn,
    enumerable: false
  });
};


var callbackWrap = function (callback) {
  return function (err, response) {
    callback(err);
  };
};

var callbackWrapForCursor = function (collection, callback) {
  return function (err, response) {
    if (!err) {
      return callback(null, new Cursor(response, collection));
    }
    if (err.flags === constants.SDB_DMS_EOC) {
      callback(null, null);
    } else {
      callback(err);
    }
  };
};

/** \fn ObjectId Insert(BsonDocument insertor)
*  \brief Insert a document into current collection
*  \param insertor The Bson document of insertor, can't be null
*  \return ObjectId
*  \exception SequoiaDB.BaseException
*  \exception System.Exception
*/
Collection.prototype.insert = function (insertor, callback) {
  if (!insertor) {
    throw new Error("SDB_INVALIDARG");
  }
  var message = new Message(constants.Operation.OP_INSERT);
  message.Version = constants.DEFAULT_VERSION;
  message.W = constants.DEFAULT_W;
  message.Padding = 0;
  message.CollectionFullName = this.collectionFullName;
  message.NodeID = constants.ZERO_NODEID;
  message.RequestID = Long.ZERO;
  message.Insertor = insertor;

  var oid = insertor[constants.OID];
  if (!oid) {
    oid = bson.ObjectId.createPk();
    insertor[constants.OID] = oid;
  }
  debug('message is %j', message);
  var buff = helper.buildInsertRequest(message, this.conn.isBigEndian);
  this.conn.send(buff, message, function (err, response) {
    if (err) {
      return callback(err);
    }
    callback(null, oid);
  });
};

/** \fn void Delete(BsonDocument matcher)
*  \brief Delete the matching document of current collection
*  \param matcher The matching condition
*  \exception SequoiaDB.BaseException
*  \exception System.Exception
*/
Collection.prototype.delete = function (matcher, hint, callback) {
  if (typeof matcher === "function") {
    callback = matcher;
    matcher = {};
    hint = {};
  } else if (typeof hint === "function") {
    callback = hint;
    hint = {};
  }

  var message = new Message();

  message.OperationCode = constants.Operation.OP_DELETE;
  message.Version = constants.DEFAULT_VERSION;
  message.W = constants.DEFAULT_W;
  message.Padding = 0;
  message.Flags = 0;
  message.NodeID = constants.ZERO_NODEID;
  message.CollectionFullName = this.collectionFullName;
  message.RequestID = Long.ZERO;
  message.Matcher = matcher;
  message.Hint = hint;

  var buff = helper.buildDeleteRequest(message, this.conn.isBigEndian);
  this.conn.send(buff, message, callbackWrap(callback));
};

/** \fn DBCursor Query(BsonDocument query, BsonDocument selector, BsonDocument orderBy, BsonDocument hint,
 *                     long skipRows, long returnRows, int flag)
 *  \brief Find documents of current collection
 *  \param query The matching condition
 *  \paramselector The selective rule
 *  \param orderBy The ordered rule
 *  \param hint One of the indexs in current collection, using default index to query if not provided
 *           eg:{"":"ageIndex"}
 *  \param skipRows Skip the first numToSkip documents, default is 0
 *  \param returnRows Only return numToReturn documents, default is -1 for returning all results
 *  \param flag the flag is used to choose the way to query, the optional options are as below:
 *
 *      DBQuery.FLG_QUERY_FORCE_HINT(0x00000080)      : Force to use specified hint to query, if database have no index assigned by the hint, fail to query
 *      DBQuery.FLG_QUERY_PARALLED(0x00000100)        : Enable paralled sub query
 *      DBQuery.FLG_QUERY_WITH_RETURNDATA(0x00000200) : In general, query won't return data until cursor get from database,
 *                                                      when add this flag, return data in query response, it will be more high-performance
 *
 *  \return The DBCursor of matching documents or null
 *  \exception SequoiaDB.BaseException
 *  \exception System.Exception
 */
Collection.prototype.query = function (matcher, selector, orderBy, hint,
                                       skipRows, returnRows, flag, callback) {

  // query(callback);
  if (typeof matcher === 'function') {
    callback = matcher;
    matcher = {};
    selector = {};
    orderBy = {};
    hint = {};
    skipRows = Long.ZERO;
    returnRows = Long.NEG_ONE;
    flag = 0;
  } else if (typeof selector === 'function') {
    // query(query, callback)
    callback = selector;
    matcher = selector.Matcher;
    selector = selector.Selector;
    orderBy = selector.OrderBy;
    hint = selector.Hint;
    skipRows = selector.SkipRowsCount;
    returnRows = selector.ReturnRowsCount;
    flag = selector.Flag;
  } else if (typeof skipRows === 'function') {
    // query(matcher, selector, orderBy, hint, callback)
    callback = skipRows;
    skipRows = Long.ZERO;
    returnRows = Long.NEG_ONE;
    flag = 0;
  }
  if (Long.equals(returnRows, Long.ZERO)) {
    returnRows = Long.NEG_ONE;
  }

  if (Long.equals(returnRows, Long.ONE)) {
    flag = flag | Query.FLG_QUERY_WITH_RETURNDATA;
  }

  var command = this.collectionFullName;
  this.conn.sendAdminCommand2(command, matcher, selector, orderBy, hint,
                              skipRows, returnRows, flag,
                              callbackWrapForCursor(this, callback));
};

Collection.prototype.update = function (flag, matcher, modifier, hint, callback) {
  if (typeof matcher === 'function') {
    // update(query, callback);
    callback = matcher;
    matcher = flag.Matcher;
    modifier = flag.Modifier;
    hint = flag.Hint;
    flag = 0;
  } else if (typeof hint === 'function') {
    // update(matcher, modifier, hint, callback)
    callback = hint;
    hint = modifier;
    modifier = matcher;
    matcher = flag;
    flag = 0;
  }

  if (!modifier) {
    throw new Error("SDB_INVALIDARG");
  }

  var message = new Message(constants.Operation.OP_UPDATE);
  message.Version = constants.DEFAULT_VERSION;
  message.W = constants.DEFAULT_W;
  message.Padding = 0;
  message.Flags = flag;
  message.NodeID = constants.ZERO_NODEID;
  message.CollectionFullName = this.collectionFullName;
  message.RequestID = getNextRequstID();
  message.Matcher = matcher || {};
  message.Modifier = modifier;
  message.Hint = hint || {};

  var buff = helper.buildUpdateRequest(message, this.conn.isBigEndian);
  this.conn.send(buff, message, callbackWrap(callback));
};

module.exports = Collection;
