/**
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

var assert = require('assert');
var bson = require("bson");
var BSONObject = new bson.BSONPure.BSON();

var getStrLength = function (buff, begin) {
  var length = 0;
  while (buff[begin] !== 0) {
    ++length;
    ++begin;
  }
  return length;
};

var reverse = function (buff, begin, length) {
  var i = begin;
  var j = begin + length - 1;
  var tmp;
  while (i < j) {
    tmp = buff[i];
    buff[i] = buff[j];
    buff[j] = tmp;
    ++i;
    --j;
  }
};

var BSON = {
  EOO: 0,
  NUMBER: 1,
  STRING: 2,
  OBJECT: 3,
  ARRAY: 4,
  BINARY: 5,
  UNDEFINED: 6,
  OID: 7,
  BOOLEAN: 8,
  DATE: 9,
  NULL: 10,
  REGEX: 11,
  REF: 12,
  CODE: 13,
  SYMBOL: 14,
  CODE_W_SCOPE: 15,
  NUMBER_INT: 16,
  TIMESTAMP: 17,
  NUMBER_LONG: 18,
  MINKEY: -1,
  MAXKEY: 127
};

var bsonEndianConvert = function (doc, offset, length) {
  offset = offset || 0;
  var start = offset;
  length = length || doc.length;
  // revert the doc length, it's an int(4 bytes)
  reverse(doc, offset, 4);
  offset += 4;
  var type, len;
  while (offset < doc.length) {
    type = doc[offset]; // read the type
    ++offset;
    if (type === BSON.EOO) {
      break;
    }
    // skip key string
    offset += getStrLength(doc, offset) + 1;
    switch (type) {
    case BSON.NUMBER:
      reverse(doc, offset, 8); // double, 8 byte
      offset += 8;
      break;
    case BSON.STRING:
    case BSON.CODE:
    case BSON.SYMBOL: { // len + str
      len = doc.readInt32LE(offset);
      reverse(doc, offset, 4);
      offset += len + 4;
      break;
    }
    case BSON.OBJECT:
    case BSON.ARRAY: {
      len = doc.readInt32LE(offset);
      bsonEndianConvert(doc, offset, len);
      offset += len;
      break;
    }
    case BSON.BINARY: {
      len = doc.readInt32LE(offset);
      reverse(doc, offset, 4);
      offset += len + 5;
      break;
    }
    case BSON.UNDEFINED:
    case BSON.NULL:
    case BSON.MAXKEY:
    case BSON.MINKEY:
      break;
    case BSON.OID:
      offset += 12;
      break;
    case BSON.BOOLEAN:
      offset += 1;
      break;
    case BSON.DATE:
      reverse(doc, offset, 8);
      offset += 8;
      break;
    case BSON.REGEX:
      offset += getStrLength(doc, offset) + 1;
      offset += getStrLength(doc, offset) + 1;
      break;
    case BSON.REF: {
      offset += 12;
      len = doc.readInt32LE(offset);
      reverse(doc, offset, 4);
      offset += len + 4;
      offset += 12;
      break;
    }
    case BSON.CODE_W_SCOPE: {
      reverse(doc, offset, 4);
      offset += 4;
      len = doc.readInt32LE(offset);
      reverse(doc, offset, 4);
      offset += len + 4;
      len = doc.readInt32LE(offset);
      bsonEndianConvert(doc, offset, len);
      offset += len;
      break;
    }
    case BSON.NUMBER_INT:
      reverse(doc, offset, 4);
      offset += 4;
      break;
    case BSON.TIMESTAMP:
      reverse(doc, offset, 4);
      offset += 4;
      reverse(doc, offset, 4);
      offset += 4;
      break;
    case BSON.NUMBER_LONG:
      reverse(doc, offset, 8);
      offset += 8;
      break;
    }
  }

  assert.equal(offset - start, length);
};

var bsonEndianConvertb2l = function (doc, offset) {
  offset = offset || 0;
  var start = offset;
  var length = doc.readInt32BE(offset);
  // revert the doc length, it's an int(4 bytes)
  reverse(doc, offset, 4);
  offset += 4;
  var type, len;
  while (offset < doc.length) {
    type = doc[offset]; // read the type
    ++offset;
    if (type === BSON.EOO) {
      break;
    }
    // skip key string
    offset += getStrLength(doc, offset) + 1;
    switch (type) {
    case BSON.NUMBER:
      reverse(doc, offset, 8); // double, 8 byte
      offset += 8;
      break;
    case BSON.STRING:
    case BSON.CODE:
    case BSON.SYMBOL: { // len + str
      len = doc.readInt32BE(offset);
      reverse(doc, offset, 4);
      offset += len + 4;
      break;
    }
    case BSON.OBJECT:
    case BSON.ARRAY: {
      len = doc.readInt32BE(offset);
      bsonEndianConvertb2l(doc, offset);
      offset += len;
      break;
    }
    case BSON.BINARY: {
      len = doc.readInt32BE(offset);
      reverse(doc, offset, 4);
      offset += len + 5;
      break;
    }
    case BSON.UNDEFINED:
    case BSON.NULL:
    case BSON.MAXKEY:
    case BSON.MINKEY:
      break;
    case BSON.OID:
      offset += 12;
      break;
    case BSON.BOOLEAN:
      offset += 1;
      break;
    case BSON.DATE:
      reverse(doc, offset, 8);
      offset += 8;
      break;
    case BSON.REGEX:
      offset += getStrLength(doc, offset) + 1;
      offset += getStrLength(doc, offset) + 1;
      break;
    case BSON.REF: {
      offset += 12;
      len = doc.readInt32BE(offset);
      reverse(doc, offset, 4);
      offset += len + 4;
      offset += 12;
      break;
    }
    case BSON.CODE_W_SCOPE: {
      reverse(doc, offset, 4);
      offset += 4;
      len = doc.readInt32BE(offset);
      reverse(doc, offset, 4);
      offset += len + 4;
      len = doc.readInt32BE(offset);
      bsonEndianConvertb2l(doc, offset);
      offset += len;
      break;
    }
    case BSON.NUMBER_INT:
      reverse(doc, offset, 4);
      offset += 4;
      break;
    case BSON.TIMESTAMP:
      reverse(doc, offset, 4);
      offset += 4;
      reverse(doc, offset, 4);
      offset += 4;
      break;
    case BSON.NUMBER_LONG:
      reverse(doc, offset, 8);
      offset += 8;
      break;
    }
  }

  assert.equal(offset - start, length);
};

/**
 * About bson spec, you can find it here(http://bsonspec.org/spec.html)
 */
exports.serialize = function (doc, isBigEndian) {
  var buff = BSONObject.serialize(doc, false, true, false);
  if (isBigEndian) {
    bsonEndianConvert(buff);
  }
  return buff;
};

exports.deserialize = function (buff, isBigEndian) {
  if (isBigEndian) {
    bsonEndianConvertb2l(buff);
  }

  return BSONObject.deserialize(buff);
};
