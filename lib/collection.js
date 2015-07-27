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
var util = require('util');
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
    var query = matcher;
    matcher = query.Matcher;
    selector = query.Selector;
    orderBy = query.OrderBy;
    hint = query.Hint;
    skipRows = query.SkipRowsCount;
    returnRows = query.ReturnRowsCount;
    flag = query.Flag;
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

/** \fn void Upsert(BsonDocument matcher, BsonDocument modifier, BsonDocument hint)
*  \brief Update the document of current collection, insert if no matching
*  \param matcher The matching condition
*  \param modifier The updating rule
*  \param hint Hint
*  \exception SequoiaDB.BaseException
*  \exception System.Exception
*  \note It won't work to upsert the "ShardingKey" field, but the other fields take effect
*/
Collection.prototype.upsert = function (matcher, modifier, hint, callback) {
  this.update(constants.FLG_UPDATE_UPSERT, matcher, modifier, hint, callback);
};

/** \fn DBCursor GetIndexes()
 *  \brief Get all the indexes of current collection
 *  \return A cursor of all indexes or null
 *  \exception SequoiaDB.BaseException
 *  \exception System.Exception
 */
Collection.prototype.getIndexes = function (callback) {
  var command = constants.ADMIN_PROMPT + constants.GET_INXES;
  var hint = {};
  hint[constants.FIELD_COLLECTION] = this.collectionFullName;
  this.conn.sendAdminCommand2(command, {}, {}, {}, hint, Long.NEG_ONE, Long.NEG_ONE, 0, callbackWrapForCursor(this, callback));
};

/** \fn DBCursor GetIndex(string name)
 *  \brief Get the named index of current collection
 *  \param name The index name
 *  \return A index, if not exist then return null
 *  \exception SequoiaDB.BaseException
 *  \exception System.Exception
 */
Collection.prototype.getIndex = function (name, callback) {
  if (typeof name === 'function') {
    callback = name;
    return this.getIndexes(callback);
  }

  var command = constants.ADMIN_PROMPT + constants.GET_INXES;
  var hint = {};
  hint[constants.FIELD_COLLECTION] = this.collectionFullName;

  var matcher = {};
  matcher[constants.IXM_INDEXDEF + "." + constants.IXM_NAME] = name;

  this.conn.sendAdminCommand2(command, matcher, {}, {}, hint, Long.NEG_ONE, Long.NEG_ONE, 0, callbackWrapForCursor(this, callback));
};

/** \fn void CreateIndex(string name, BsonDocument key, bool isUnique, bool isEnforced)
 *  \brief Create a index with name and key
 *  \param name The index name
 *  \param key The index key
 *  \param isUnique Whether the index elements are unique or not
 *  \param isEnforced Whether the index is enforced unique.
 *                    This element is meaningful when isUnique is group to true.
 *  \exception SequoiaDB.BaseException
 *  \exception System.Exception
 */
Collection.prototype.createIndex = function (name, key, isUnique, isEnforced, callback) {
  var command = constants.ADMIN_PROMPT + constants.CREATE_INX;

  var index = {};
  index[constants.IXM_KEY] = key;
  index[constants.IXM_NAME] = name;
  index[constants.IXM_UNIQUE] = isUnique;
  index[constants.IXM_ENFORCED] = isEnforced;

  var matcher = {};
  matcher[constants.FIELD_COLLECTION] = this.collectionFullName;
  matcher[constants.FIELD_INDEX] = index;

  this.conn.sendAdminCommand2(command, matcher, {}, {}, {}, Long.NEG_ONE, Long.NEG_ONE, 0, callbackWrap(callback));
};

/** \fn void DropIndex(string name)
 *  \brief Remove the named index of current collection
 *  \param name The index name
 *  \exception SequoiaDB.BaseException
 *  \exception System.Exception
 */
Collection.prototype.dropIndex = function (name, callback) {
  var command = constants.ADMIN_PROMPT + constants.DROP_INX;
  var matcher = {};
  var index = {};
  index[""] = name;
  matcher[constants.FIELD_COLLECTION] = this.collectionFullName;
  matcher[constants.FIELD_INDEX] = index;
  this.conn.sendAdminCommand2(command, matcher, {}, {}, {}, Long.NEG_ONE, Long.NEG_ONE, 0, callbackWrap(callback));
};

/** \fn void Split(string sourceGroupName, string destGroupName,
                   BsonDocument splitCondition, BsonDocument splitEndCondition)
 *  \brief Split the collection from one group into another group by range
 *  \param sourceGroupName The source group
 *  \param destGroupName The destination group
 *  \param splitCondition The split condition
 *  \param splitEndCondition The split end condition or null
 *    eg:If we create a collection with the option {ShardingKey:{"age":1},ShardingType:"Hash",Partition:2^10},
 *    we can fill {age:30} as the splitCondition, and fill {age:60} as the splitEndCondition. when split,
 *    the targe group will get the records whose age's hash value are in [30,60). If splitEndCondition is null,
 *    they are in [30,max).
 *  \exception SequoiaDB.BaseException
 *  \exception System.Exception
 */
Collection.prototype.split = function (source, dest, splitCondition, splitEndCondition, callback) {
  if (!source || !dest || !splitCondition) {
    throw new Error("SDB_INVALIDARG");
  }
  var command = constants.ADMIN_PROMPT + constants.SPLIT_CMD;
  var matcher = {};
  matcher[constants.FIELD_NAME] = this.collectionFullName;
  matcher[constants.FIELD_SOURCE] = source;
  matcher[constants.FIELD_TARGET] = dest;
  matcher[constants.FIELD_SPLITQUERY] = splitCondition;
  if (typeof splitEndCondition === 'function') {
    callback = splitEndCondition;
  } else {
    matcher[constants.FIELD_SPLITENDQUERY] = splitEndCondition;
  }

  this.conn.sendAdminCommand2(command, matcher, {}, {}, {}, Long.NEG_ONE, Long.NEG_ONE, 0, callbackWrap(callback));
};

/** \fn void Split(string sourceGroupName, string destGroupName, double percent)
 *  \brief Split the collection from one group into another group by percent
 *  \param sourceGroupName The source group
 *  \param destGroupName The destination group
 *  \param percent percent The split percent, Range:(0.0, 100.0]
 *  \exception SequoiaDB.BaseException
 *  \exception System.Exception
 */
Collection.prototype.splitByPercent = function (source, dest, percent, callback) {
  if (!source || !dest || percent < 0.0 || percent > 100.0) {
    throw new Error("SDB_INVALIDARG");
  }
  var command = constants.ADMIN_PROMPT + constants.SPLIT_CMD;
  var matcher = {};
  matcher[constants.FIELD_NAME] = this.collectionFullName;
  matcher[constants.FIELD_SOURCE] = source;
  matcher[constants.FIELD_TARGET] = dest;
  matcher[constants.FIELD_SPLITPERCENT] = percent;

  this.conn.sendAdminCommand2(command, matcher, {}, {}, {}, -1, -1, 0, callbackWrap(callback));
};

/** \fn long SplitAsync(String sourceGroupName,
 *                      String destGroupName,
 *                      BsonDocument splitCondition,
 *                      BsonDocument splitEndCondition)
 *  \brief Split the specified collection from source group to target group by range asynchronously.
 *  \param sourceGroupName the source group name
 *  \param destGroupName the destination group name
 *  \param splitCondition
 *            the split condition
 *  \param splitEndCondition
 *            the split end condition or null
 *            eg:If we create a collection with the option {ShardingKey:{"age":1},ShardingType:"Hash",Partition:2^10},
 *         we can fill {age:30} as the splitCondition, and fill {age:60} as the splitEndCondition. when split,
 *         the targe group will get the records whose age's hash values are in [30,60). If splitEndCondition is null,
 *         they are in [30,max).
 *  \return return the task id, we can use the return id to manage the sharding which is run backgroup.
 *  \exception SequoiaDB.BaseException
 *  \exception System.Exception
 *  \see listTask, cancelTask
 */
Collection.prototype.splitAsync = function (source, dest, splitCondition, splitEndCondition, callback) {
  // check argument
  if (!source || !dest || !splitCondition) {
    throw new Error("SDB_INVALIDARG");
  }
  // build a bson to send
  var matcher = {};
  matcher[constants.FIELD_NAME] = this.collectionFullName;
  matcher[constants.FIELD_SOURCE] = source;
  matcher[constants.FIELD_TARGET] = dest;
  matcher[constants.FIELD_SPLITQUERY] = splitCondition;
  matcher[constants.FIELD_SPLITENDQUERY] = splitEndCondition;
  matcher[constants.FIELD_ASYNC] = true;

  var command = constants.ADMIN_PROMPT + constants.SPLIT_CMD;
  var that = this;
  this.conn.sendAdminCommand2(command, matcher, {}, {}, {}, 0, -1, 0, function (err, response) {
    if (err) {
      return callback(err);
    }
    var cursor = new Cursor(response, that);
    cursor.next(function (err, result) {
      if (err) {
        return callback(err);
      }
      if (!result) {
        return callback(new Error("SDB_CAT_TASK_NOTFOUND"));
      }
      var taskid = result[constants.FIELD_TASKID];
      if (!taskid) {
        return callback(new Error("SDB_CAT_TASK_NOTFOUND"));
      }
      callback(null, taskid);
    });
  });
};

/** \fn long SplitAsync(String sourceGroupName,
 *                      String destGroupName,
 *                      double percent)
 *  \brief Split the specified collection from source group to target group by percent asynchronously.
 *  \param sourceGroupName the source group name
 *  \param destGroupName the destination group name
 *  \param percent
 *            the split percent, Range:(0,100]
 *  \return return the task id, we can use the return id to manage the sharding which is run backgroup.
 *  \exception SequoiaDB.BaseException
 *  \exception System.Exception
 *  \see listTask, cancelTask
 */
Collection.prototype.splitByPercentAsync = function (source, dest, percent, callback) {
  // check argument
  if (!source || !dest || (percent <= 0.0 || percent > 100.0)) {
    throw new Error("SDB_INVALIDARG");
  }
  // build a bson to send
  var matcher = {};
  matcher[constants.FIELD_NAME] = this.collectionFullName;
  matcher[constants.FIELD_SOURCE] = source;
  matcher[constants.FIELD_TARGET] = dest;
  matcher[constants.FIELD_SPLITPERCENT] = percent;
  matcher[constants.FIELD_ASYNC] = true;

  var command = constants.ADMIN_PROMPT + constants.SPLIT_CMD;
  var that = this;
  this.conn.sendAdminCommand2(command, matcher, {}, {}, {}, 0, -1, 0, function (err, response) {
    if (err) {
      return callback(err);
    }
    var cursor = new Cursor(response, that);
    cursor.next(function (err, result) {
      if (err) {
        return callback(err);
      }
      if (!result) {
        return callback(new Error("SDB_CAT_TASK_NOTFOUND"));
      }
      var taskid = result[constants.FIELD_TASKID];
      if (!taskid) {
        return callback(new Error("SDB_CAT_TASK_NOTFOUND"));
      }
      callback(null, taskid);
    });
  });
};


/** \fn void BulkInsert(List<BsonDocument> insertor, int flag)
*  \brief Insert a bulk of bson objects into current collection
*  \param insertor The Bson document of insertor list, can't be null
*  \param flag FLG_INSERT_CONTONDUP or 0
*  \exception SequoiaDB.BaseException
*  \exception System.Exception
*/
Collection.prototype.bulkInsert = function (insertors, flag, callback) {
  if (!insertors || !insertors.length) {
    throw new Error("SDB_INVALIDARG");
  }
  var message = new Message(constants.Operation.OP_INSERT);
  message.Version = constants.DEFAULT_VERSION;
  message.W = constants.DEFAULT_W;
  message.Padding = 0;
  message.CollectionFullName = this.collectionFullName;
  message.NodeID = constants.ZERO_NODEID;
  message.RequestID = Long.ZERO;
  if (flag !== 0 || flag !== constants.FLG_INSERT_CONTONDUP) {
    message.Flags = flag;
  }
  message.Insertor = insertors[0];

  var buff = helper.buildInsertRequest(message, this.conn.isBigEndian);

  for (var i = 1; i < insertors.length; i++) {
    buff = helper.appendInsertMessage(buff, insertors[i], this.conn.isBigEndian);
  }
  this.conn.send(buff, message, callbackWrap(callback));
};

/** \fn DBCursor Explain(BsonDocument query, BsonDocument selector, BsonDocument orderBy, BsonDocument hint,
 *                       long skipRows, long returnRows, int flag, BsonDocument options)
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
 *  \param [in] options The rules of query explain, the options are as below:
 *
 *      Run     : Whether execute query explain or not, true for excuting query explain then get
 *                the data and time information; false for not excuting query explain but get the
 *                query explain information only. e.g. {Run:true}
 *  \return The DBCursor of matching documents or null
 *  \exception SequoiaDB.BaseException
 *  \exception System.Exception
 */
Collection.prototype.explain = function (matcher, selector, orderBy, hint,
                      skipRows, returnRows, flag, options, callback) {
  var newHint = {};
  if (hint) {
    newHint[constants.FIELD_HINT] = hint;
  }
  if (options) {
    newHint[constants.FIELD_OPTIONS] = options;
  }

  this.query(matcher, selector, orderBy, newHint, skipRows, returnRows, flag | Query.FLG_QUERY_EXPLAIN, callback);
};

/** \fn long GetCount(BsonDocument condition)
 *  \brief Get the count of matching documents in current collection
 *  \param condition The matching rule
 *  \return The count of matching documents
 *  \exception SequoiaDB.BaseException
 *  \exception System.Exception
 */
Collection.prototype.count = function (matcher, callback) {
  if (typeof matcher === "function") {
    callback = matcher;
    matcher = {};
  }

  var hint = {};
  hint[constants.FIELD_COLLECTION] = this.collectionFullName;
  var command = constants.ADMIN_PROMPT + constants.GET_COUNT;
  var that = this;
  this.conn.sendAdminCommand2(command, matcher, {}, {}, hint, 0, -1, 0, function (err, response) {
    if (err) {
      return callback(err);
    }
    that.getMoreCommand(response, function (err, response) {
      if (err) {
        return callback(err);
      }
      callback(null, response[0][constants.FIELD_TOTAL]);
    });
  });
};

/** \fn DBCursor Aggregate(List<BsonDocument> obj)
 *  \brief Execute aggregate operation in specified collection
 *  \param insertor The array of bson objects, can't be null
 *  \return The DBCursor of result or null
 *  \exception SequoiaDB.BaseException
 *  \exception System.Exception
 */
Collection.prototype.aggregate = function (list, callback) {
  if (!list || list.length === 0) {
    throw new Error("SDB_INVALIDARG");
  }
  var message = new Message(constants.Operation.OP_AGGREGATE);
  message.Version = constants.DEFAULT_VERSION;
  message.W = constants.DEFAULT_W;
  message.Padding = 0;
  message.CollectionFullName = this.collectionFullName;
  message.NodeID = constants.ZERO_NODEID;
  message.RequestID = getNextRequstID();
  message.Flags = 0;
  message.Insertor = list[0];

  var buff = helper.buildAggrRequest(message, this.conn.isBigEndian);

  for (var i = 1; i < list.length; i++) {
    buff = helper.appendAggrMessage(buff, list[i], this.conn.isBigEndian);
  }

  this.conn.send(buff, message, callbackWrap(callback));
};

/** \fn DBCursor GetQueryMeta(BsonDocument query, BsonDocument orderBy, BsonDocument hint,
 *                            long skipRows, long returnRows)
 *  \brief Get the index blocks' or data blocks' infomations for concurrent query
 *  \param query The matching condition, return the whole range of index blocks if not provided
 *           eg:{"age":{"$gt":25},"age":{"$lt":75}}
 *  \param orderBy The ordered rule, result set is unordered if not provided
 *  \param hint hint One of the indexs in current collection, using default index to query if not provided
 *           eg:{"":"ageIndex"}
 *  \param skipRows Skip the first numToSkip documents, default is 0
 *  \param returnRows Only return numToReturn documents, default is -1 for returning all results
 *  \return The DBCursor of matching infomations or null
 *  \exception SequoiaDB.BaseException
 *  \exception System.Exception
 */
Collection.prototype.getQueryMeta = function (matcher, orderBy, hint,
                            skipRows, returnRows, callback) {
    matcher || (matcher = {});
    orderBy || (orderBy = {});
    hint || (hint = {});
    returnRows || (returnRows = -1);
    var selector = {};
    selector[constants.FIELD_COLLECTION] = this.collectionFullName;

    var command = constants.ADMIN_PROMPT + constants.GET_QUERYMETA;

    this.conn.sendAdminCommand2(command, matcher, hint, orderBy,
                          selector, skipRows, returnRows, 0,
                          callbackWrapForCursor(this, callback));
};

/** \fn void AttachCollection (string subClFullName, BsonDocument options)
 * \brief Attach the specified collection.
 * \param subClFullName The name of the subcollection
 * \param options The low boudary and up boudary
 *       eg: {"LowBound":{a:1},"UpBound":{a:100}}
 * \retval void
 * \exception SequoiaDB.BaseException
 * \exception System.Exception
 */
Collection.prototype.attachCollection = function (subClFullName, options, callback) {
  // check argument
  if (!subClFullName || subClFullName.length > constants.COLLECTION_MAX_SZ ||
    !options || Object.keys(options).length === 0) {
    throw new Error("SDB_INVALIDARG");
  }
  // build a bson to send
  var matcher = {};
  matcher[constants.FIELD_NAME] = this.collectionFullName;
  matcher[constants.FIELD_SUBCLNAME] = subClFullName;
  util._extend(matcher, options);
  // build commond
  var command = constants.ADMIN_PROMPT + constants.LINK_CL;

  this.conn.sendAdminCommand2(command, matcher, {}, {}, {}, 0, -1, 0, callbackWrap(callback));
};

/** \fn void DetachCollection(string subClFullName)
 * \brief Detach the specified collection.
 * \param subClFullName The name of the subcollection
 * \retval void
 * \exception SequoiaDB.BaseException
 * \exception System.Exception
 */
Collection.prototype.detachCollection = function (subClFullName, callback) {
  // check argument
  if (!subClFullName || subClFullName.length > constants.COLLECTION_MAX_SZ) {
    throw new Error("SDB_INVALIDARG");
  }
  // build a bson to send
  var matcher = {};
  matcher[constants.FIELD_NAME] = this.collectionFullName;
  matcher[constants.FIELD_SUBCLNAME] = subClFullName;
  // build command
  var command = constants.ADMIN_PROMPT + constants.UNLINK_CL;

  // run command
  this.conn.sendAdminCommand2(command, matcher, {}, {}, {}, 0, -1, 0, callbackWrap(callback));
};

/** \fn void Alter(BsonDocument options)
 * \brief Alter the attributes of current collection
 * \param options The options for altering current collection:
 *
 *     ReplSize     : Assign how many replica nodes need to be synchronized when a write request(insert, update, etc) is executed
 *     ShardingKey  : Assign the sharding key
 *     ShardingType : Assign the sharding type
 *     Partition    : When the ShardingType is "hash", need to assign Partition, it's the bucket number for hash, the range is [2^3,2^20]
 *                    e.g. {RepliSize:0, ShardingKey:{a:1}, ShardingType:"hash", Partition:1024}
 * \note Can't alter attributes about split in partition collection; After altering a collection to
 *       be a partition collection, need to split this collection manually
 * \exception SequoiaDB.BaseException
 * \exception System.Exception
 */
Collection.prototype.alter = function (options, callback) {
  // check argument
  if (!options) {
    throw new Error("SDB_INVALIDARG");
  }
  // build a bson to send
  var matcher = {};
  matcher[constants.FIELD_NAME] = this.collectionFullName;
  matcher[constants.FIELD_OPTIONS] = options;
  // build command
  var command = constants.ADMIN_PROMPT + constants.ALTER_COLLECTION;
  // run command
  this.conn.sendAdminCommand2(command, matcher, {}, {}, {}, 0, -1, 0, callbackWrap(callback));
};

Collection.prototype.getMoreCommand = function (response, callback) {
  var requestID = response.RequestID;
  var contextIDs = response.ContextIDList;
  var fullList = [];
  var hasMore = true;
  var that = this;
  var getMore = function (check) {
    var message = new Message(constants.Operation.OP_GETMORE);
    message.NodeID = constants.ZERO_NODEID;
    message.ContextIDList = contextIDs;
    message.RequestID = requestID;
    message.NumReturned = -1;

    var buff = helper.buildGetMoreRequest(message, that.conn.isBigEndian);
    that.conn.send(buff, message, function (err, response) {
      if (!err) {
        requestID = response.RequestID;
        var objects = response.ObjectList;
        fullList = fullList.concat(objects);
      } else if (err.flags === constants.SDB_DMS_EOC) {
        hasMore = false;
      } else {
        return callback(err);
      }
      check();
    });
  };

  var check = function () {
    if (hasMore) {
      getMore(check);
    } else {
      callback(null, fullList);
    }
  };
  getMore(check);
};

module.exports = Collection;
