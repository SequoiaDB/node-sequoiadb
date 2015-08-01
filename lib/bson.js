'use strict';

var assert = require('assert');
var bson = require("bson");
var BSONObject = new bson.BSONPure.BSON();

var getStrLength = function (array, begin) {
  var length = 0;
  while (array[begin] !== 0) {
    ++length;
    ++begin;
  }
  return length;
};

var arrayReverse = function (array, begin, length) {
  var i = begin;
  var j = begin + length - 1;
  var tmp;
  while (i < j) {
    tmp = array[i];
    array[i] = array[j];
    array[j] = tmp;
    ++i;
    --j;
  }
};

var getBsonLength = function (inBytes, offset, endianConvert) {
  return inBytes.readInt32LE(offset);
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

var bsonEndianConvert = function (inBytes) {
  var offset = 0;
  // 将doc的length位置进行翻转
  arrayReverse(inBytes, offset, 4);
  offset += 4;
  var type;
  while (offset < inBytes.length) {
    type = inBytes[offset];
    ++offset;
    if (type === BSON.EOO) {
      break;
    }
    offset += getStrLength(inBytes, offset) + 1;
    switch (type) {
    case BSON.NUMBER:
      arrayReverse(inBytes, offset, 8);
      offset += 8;
      break;
    case BSON.STRING:
    case BSON.CODE:
    case BSON.SYMBOL: {
      var length = inBytes.readInt32LE(offset);
      arrayReverse(inBytes, offset, 4);
      offset += length + 4;
      break;
    }
    case BSON.OBJECT:
    case BSON.ARRAY: {
      var length = getBsonLength(inBytes, offset);
      bsonEndianConvert(inBytes, offset, length);
      offset += length;
      break;
    }
    case BSON.BINARY: {
      var length = inBytes.readInt32LE(offset);
      arrayReverse(inBytes, offset, 4);
      offset += length + 5;
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
      arrayReverse(inBytes, offset, 8);
      offset += 8;
      break;
    case BSON.REGEX:
      offset += getStrLength(inBytes, offset) + 1;
      offset += getStrLength(inBytes, offset) + 1;
      break;
    case BSON.REF: {
      offset += 12;
      var length = inBytes.readInt32LE(offset);
      arrayReverse(inBytes, offset, 4);
      offset += length + 4;
      offset += 12;
      break;
    }
    case BSON.CODE_W_SCOPE: {
      arrayReverse(inBytes, offset, 4);
      offset += 4;
      var length = inBytes.readInt32LE(offset);
      arrayReverse(inBytes, offset, 4);
      offset += length + 4;
      var objLength = getBsonLength(inBytes, offset, l2r);
      bsonEndianConvert(inBytes, offset, objLength, l2r);
      offset += objLength;
      break;
    }
    case BSON.NUMBER_INT:
      arrayReverse(inBytes, offset, 4);
      offset += 4;
      break;
    case BSON.TIMESTAMP:
      arrayReverse(inBytes, offset, 4);
      offset += 4;
      arrayReverse(inBytes, offset, 4);
      offset += 4;
      break;
    case BSON.NUMBER_LONG:
      arrayReverse(inBytes, offset, 8);
      offset += 8;
      break;
    }
  }

  assert.equal(offset, inBytes.length);
};

module.exports = function (doc, isBigEndian) {
  var buff = BSONObject.serialize(doc, false, true, false);
  if (isBigEndian) {
    bsonEndianConvert(buff);
  }
  return buff;
};
