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

var Long = require("./long");
var errors = require('./error').errors;

exports.OP_ERRNOFIELD = "errno";
exports.OP_MAXNAMELENGTH = 255;

exports.COLLECTION_SPACE_MAX_SIZE = 127;
exports.COLLECTION_MAX_SZ = 127;
exports.MAX_CS_SIZE = 127;

exports.MSG_SYSTEM_INFO_LEN = ~0;
exports.MSG_SYSTEM_INFO_EYECATCHER = 0xFFFEFDFC;
exports.MSG_SYSTEM_INFO_EYECATCHER_REVERT = 0xFCFDFEFF;

exports.UNKNOWN_TYPE = "UNKNOWN";
exports.UNKONWN_DESC = "Unknown Error";
exports.UNKNOWN_CODE = 1;

exports.SYS_PREFIX = "SYS";
exports.CATALOG_GROUP = exports.SYS_PREFIX + "CatalogGroup";
exports.CATALOG_GROUPID = 1;
exports.NODE_NAME_SERVICE_SEP = ":";

exports.FMP_FUNC_TYPE_JS = 0;

exports.ADMIN_PROMPT = "$";
exports.LIST_CMD = "list";
exports.TEST_CMD = "test";
exports.ADD_CMD = "add";
exports.ALTER_CMD = "alter";
exports.CREATE_CMD = "create";
exports.REMOVE_CMD = "remove";
exports.RENAME_CMD = "rename";
exports.SPLIT_CMD = "split";
exports.DROP_CMD = "drop";
exports.STARTUP_CMD = "startup";
exports.SHUTDOWN_CMD = "shutdown";
exports.ACTIVE_CMD = "active";
exports.SNAP_CMD = "snapshot";
exports.COLSPACES = "collectionspaces";
exports.COLSPACE = "collectionspace";
exports.COLLECTIONS = "collections";
exports.GROUPS = "groups";
exports.GROUP = "group";
exports.CATALOG = "catalog";
exports.NODE = "node";
exports.CONTEXTS = "contexts";
exports.CONTEXTS_CUR = "contexts current";
exports.SESSIONS = "sessions";
exports.SESSIONS_CUR = "sessions current";
exports.STOREUNITS = "storageunits";
exports.PROCEDURES = "procedures";
exports.DOMAIN = "domain";
exports.DOMAINS = "domains";
exports.TASKS = "tasks";
exports.CS_IN_DOMAIN = "collectionspaces in domain";
exports.CL_IN_DOMAIN = "collections in domain";

exports.COLLECTION = "collection";
exports.CREATE_INX = "create index";
exports.DROP_INX = "drop index";
exports.GET_INXES = "get indexes";
exports.GET_COUNT = "get count";
exports.DATABASE = "database";
exports.SYSTEM = "system";
exports.CATA = "catalog";
exports.RESET = "reset";
exports.GET_QUERYMETA = "get querymeta";
exports.LINK_CL = "link collection";
exports.UNLINK_CL = "unlink collection";
exports.SETSESS_ATTR = "set session attribute";
exports.LIST_TASK_CMD = "list tasks";
exports.WAIT_TASK_CMD = "wait task";
exports.CANCEL_TASK_CMD = "cancel task";
exports.BACKUP_OFFLINE_CMD = "backup offline";
exports.LIST_BACKUP_CMD = "list backups";
exports.REMOVE_BACKUP_CMD = "remove backup";
exports.CRT_PROCEDURES_CMD = "create procedure";
exports.RM_PROCEDURES_CMD = "remove procedure";
exports.EVAL_CMD = "eval";
exports.ALTER_COLLECTION = "alter collection";
exports.LIST_LOBS_CMD = "list lobs";


exports.OID = "_id";
exports.CLIENT_RECORD_ID_INDEX = "$id";
exports.SVCNAME = "svcname";
exports.DBPATH = "dbpath";

exports.FIELD_SOURCE = "Source";
exports.FIELD_TARGET = "Target";
exports.FIELD_SPLITQUERY = "SplitQuery";
exports.FIELD_SPLITENDQUERY = "SplitEndQuery";
exports.FIELD_SPLITPERCENT = "SplitPercent";
exports.FIELD_NAME = "Name";
exports.FIELD_OLDNAME = "OldName";
exports.FIELD_NEWNAME = "NewName";
exports.FIELD_PAGESIZE = "PageSize";
exports.FIELD_INDEX = "Index";
exports.FIELD_TOTAL = "Total";
exports.FIELD_HINT = "Hint";
exports.FIELD_COLLECTION = "Collection";
exports.FIELD_COLLECTIONSPACE = "CollectionSpace";
exports.FIELD_GROUP = "Group";
exports.FIELD_GROUPS = "Groups";
exports.FIELD_GROUPNAME = "GroupName";
exports.FIELD_HOSTNAME = "HostName";
exports.FIELD_SERVICE = "Service";
exports.FIELD_NODEID = "NodeID";
exports.FIELD_GROUPID = "GroupID";
exports.FIELD_SERVICE_TYPE = "Type";
exports.FIELD_STATUS = "Status";
exports.FIELD_PRIMARYNODE = "PrimaryNode";
exports.FIELD_SHARDINGKEY = "ShardingKey";
exports.FIELD_SUBCLNAME = "SubCLName";
exports.FIELD_PREFERED_INSTANCE = "PreferedInstance";
exports.FIELD_ASYNC = "Async";
exports.FIELD_TASKTYPE = "TaskType";
exports.FIELD_TASKID = "TaskID";
exports.FIELD_PATH = "Path";
exports.FIELD_DESP = "Description";
exports.FIELD_ENSURE_INC = "EnsureInc";
exports.FIELD_OVERWRITE = "OverWrite";
exports.FIELD_OPTIONS = "Options";
exports.FIELD_DOMAIN = "Domain";
exports.FIELD_LOB_OID = "Oid";
exports.FIELD_LOB_OPEN_MODE = "Mode";
exports.FIELD_LOB_SIZE = "Size";
exports.FIELD_LOB_CREATTIME = "CreateTime";
exports.FIELD_FUNCTYPE = "func";
exports.FMP_FUNC_TYPE = "funcType";

exports.IXM_NAME = "name";
exports.IXM_KEY = "key";
exports.IXM_UNIQUE = "unique";
exports.IXM_ENFORCED = "enforced";
exports.IXM_INDEXDEF = "IndexDef";

exports.SDB_AUTH_USER = "User";
exports.SDB_AUTH_PASSWD = "Passwd";

exports.FLG_UPDATE_UPSERT = 0x00000001;
exports.FLG_REPLY_CONTEXTSORNOTFOUND = 0x00000001;
exports.FLG_REPLY_SHARDCONFSTALE = 0x00000004;

exports.SDB_DMS_EOC = errors.SDB_DMS_EOC;

exports.ZERO_NODEID = new Buffer(12).fill(0);
exports.DEFAULT_VERSION    = 1;
exports.DEFAULT_W        = 0;
exports.DEFAULT_FLAGS      = 0;
exports.DEFAULT_CONTEXTID = Long.NEG_ONE;

exports.Operation = {
  OP_MSG                    : 1000,
  OP_UPDATE                 : 2001,
  OP_INSERT                 : 2002,
  OP_SQL                    : 2003,
  OP_QUERY                  : 2004,
  OP_GETMORE                : 2005,
  OP_DELETE                 : 2006,
  OP_KILL_CONTEXT           : 2007,
  OP_DISCONNECT             : 2008,

  OP_KILL_ALL_CONTEXTS      : 2009,
  OP_TRANS_BEGIN            : 2010,
  OP_TRANS_COMMIT           : 2011,
  OP_TRANS_ROLLBACK         : 2012,

  OP_AGGREGATE              : 2019,

  MSG_AUTH_VERIFY_REQ       : 7000,
  MSG_AUTH_CRTUSR_REQ       : 7001,
  MSG_AUTH_DELUSR_REQ       : 7002,

  MSG_BS_LOB_OPEN_REQ       : 8001,
  MSG_BS_LOB_WRITE_REQ      : 8002,
  MSG_BS_LOB_READ_REQ       : 8003,
  MSG_BS_LOB_REMOVE_REQ     : 8004,
  MSG_BS_LOB_UPDATE_REQ     : 8005,
  MSG_BS_LOB_CLOSE_REQ      : 8006
};

exports.PreferInstanceType = {
  INS_TYPE_MIN: 0,
  INS_NODE_1: 1,
  INS_NODE_2: 2,
  INS_NODE_3: 3,
  INS_NODE_4: 4,
  INS_NODE_5: 5,
  INS_NODE_6: 6,
  INS_NODE_7: 7,
  INS_MASTER: 8,
  INS_SLAVE: 9,
  INS_ANYONE: 10,
  INS_TYPE_MAX: 11
};

/** \memberof FLG_INSERT_CONTONDUP 0x00000001
 *  The flags represent whether bulk insert continue when hitting index key duplicate error
 */
exports.FLG_INSERT_CONTONDUP = 0x00000001;

exports.SDB_PAGESIZE_4K = 4096;
exports.SDB_PAGESIZE_8K = 8192;
exports.SDB_PAGESIZE_16K = 16384;
exports.SDB_PAGESIZE_32K = 32768;
exports.SDB_PAGESIZE_64K = 65536;
exports.SDB_PAGESIZE_DEFAULT = 0;

exports.SDB_SNAP_CONTEXTS         = 0;
exports.SDB_SNAP_CONTEXTS_CURRENT = 1;
exports.SDB_SNAP_SESSIONS         = 2;
exports.SDB_SNAP_SESSIONS_CURRENT = 3;
exports.SDB_SNAP_COLLECTIONS      = 4;
exports.SDB_SNAP_COLLECTIONSPACES = 5;
exports.SDB_SNAP_DATABASE         = 6;
exports.SDB_SNAP_SYSTEM           = 7;
exports.SDB_SNAP_CATALOG          = 8;

exports.SDB_LIST_CONTEXTS         = 0;
exports.SDB_LIST_CONTEXTS_CURRENT = 1;
exports.SDB_LIST_SESSIONS         = 2;
exports.SDB_LIST_SESSIONS_CURRENT = 3;
exports.SDB_LIST_COLLECTIONS      = 4;
exports.SDB_LIST_COLLECTIONSPACES = 5;
exports.SDB_LIST_STORAGEUNITS     = 6;
exports.SDB_LIST_GROUPS           = 7;
exports.SDB_LIST_STOREPROCEDURES  = 8;
exports.SDB_LIST_DOMAINS          = 9;
exports.SDB_LIST_TASKS            = 10;
exports.SDB_LIST_CS_IN_DOMAIN     = 11;
exports.SDB_LIST_CL_IN_DOMAIN     = 12;

exports.NodeStatus = {
  SDB_NODE_ALL: 0,
  SDB_NODE_ACTIVE: 1,
  SDB_NODE_INACTIVE: 2,
  SDB_NODE_UNKNOWN: 3
};

exports.SptReturnType = {
  TYPE_VOID: 0,
  TYPE_STR: 1,
  TYPE_NUMBER: 2,
  TYPE_OBJ: 3,
  TYPE_BOOL: 4,
  TYPE_RECORDSET: 5,
  TYPE_CS: 6,
  TYPE_CL: 7,
  TYPE_RG: 8,
  TYPE_RN: 9
};
