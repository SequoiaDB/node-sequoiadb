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

var Long = require('./long');

var Query = function (matcher, selector, orderBy, hint) {
  this.Matcher = matcher || {};
  this.Selector = selector || {};
  this.OrderBy = orderBy || {};
  this.Hint = hint || {};
  this.Modifier = null;
  this.Flag = 0;
  this.SkipRowsCount = Long.ZERO; // 0
  this.ReturnRowsCount = Long.NEG_ONE; // -1
};

/** \memberof FLG_QUERY_STRINGOUT 0x00000001
 *  \brief Normally, query return bson stream,
 *         when this flag is added, query return binary data stream
 */
Query.FLG_QUERY_STRINGOUT = 0x00000001;
/** \memberof FLG_INSERT_CONTONDUP 0x00000080
 *  \brief Force to use specified hint to query,
 *         if database have no index assigned by the hint, fail to query
 */
Query.FLG_QUERY_FORCE_HINT = 0x00000080;

/** \memberof FLG_QUERY_PARALLED 0x00000100
 *  \brief Enable paralled sub query
 */
Query.FLG_QUERY_PARALLED = 0x00000100;

/** \memberof FLG_QUERY_WITH_RETURNDATA 0x00000200
   *  \brief Return data in query response
   */
Query.FLG_QUERY_WITH_RETURNDATA = 0x00000200;

/** \memberof FLG_QUERY_EXPLAIN 0x00000400
 *  \brief Explain query
 */
Query.FLG_QUERY_EXPLAIN = 0x00000400;

module.exports = Query;
