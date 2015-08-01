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

var Long = function (high, low) {
  this.high = high; // Int32
  this.low = low; // Int32
};

/**
 * Returns the bitwise NOT of this Long.
 * @returns {!Long}
 * @expose
 */
Long.prototype.not = function() {
  return new Long(~this.high, ~this.low);
};

Long.prototype.add = function(addend) {
  // Divide each number into 4 chunks of 16 bits, and then sum the chunks.

  var a48 = this.high >>> 16;
  var a32 = this.high & 0xFFFF;
  var a16 = this.low >>> 16;
  var a00 = this.low & 0xFFFF;

  var b48 = addend.high >>> 16;
  var b32 = addend.high & 0xFFFF;
  var b16 = addend.low >>> 16;
  var b00 = addend.low & 0xFFFF;

  var c48 = 0, c32 = 0, c16 = 0, c00 = 0;
  c00 += a00 + b00;
  c16 += c00 >>> 16;
  c00 &= 0xFFFF;
  c16 += a16 + b16;
  c32 += c16 >>> 16;
  c16 &= 0xFFFF;
  c32 += a32 + b32;
  c48 += c32 >>> 16;
  c32 &= 0xFFFF;
  c48 += a48 + b48;
  c48 &= 0xFFFF;
  return new Long((c48 << 16) | c32, (c16 << 16) | c00);
};

Long.prototype.toBEBuffer = function () {
  var buf = new Buffer(8);
  Long.writeLongBE(buf, this, 0);
  return buf;
};

Long.prototype.toLEBuffer = function () {
  var buf = new Buffer(8);
  Long.writeLongLE(buf, this, 0);
  return buf;
};

Long.ZERO = new Long(0x0, 0x0); // 0
Long.NEG_ONE = new Long(0xffffffff, 0xffffffff); // -1
Long.ONE = new Long(0x0, 0x1); // 1

Long.equals = function (long1, long2) {
  return long1.high === long2.high && long1.low === long2.low;
};

Long.readLongBE = function (buff, offset) {
  // 8个字节，大端
  var low = buff.readInt32BE(offset + 4);
  var high = buff.readInt32BE(offset);
  return new Long(high, low);
};

Long.readLongLE = function (buff, offset) {
  // 8个字节，大小端
  var low = buff.readInt32LE(offset);
  var high = buff.readInt32LE(offset + 4);
  return new Long(high, low);
};

Long.writeLongBE = function (buff, value, offset) {
  // 8个字节，大端。高位写在左边
  buff.writeUInt32BE(value.high, offset);
  buff.writeUInt32BE(value.low, offset + 4);
};

Long.writeLongLE = function (buff, value, offset) {
  // 8个字节，小端。高位写在右边
  buff.writeUInt32LE(value.low, offset);
  buff.writeUInt32LE(value.high, offset + 4);
};

/**
 * @type {number}
 * @const
 * @inner
 */
var TWO_PWR_16_DBL = 1 << 16;

/**
 * @type {number}
 * @const
 * @inner
 */
var TWO_PWR_32_DBL = TWO_PWR_16_DBL * TWO_PWR_16_DBL;
var TWO_PWR_64_DBL = TWO_PWR_32_DBL * TWO_PWR_32_DBL;
var TWO_PWR_63_DBL = TWO_PWR_64_DBL / 2;

Long.fromNumber = function (value) {
  if (isNaN(value) || !isFinite(value)) {
    return Long.ZERO;
  }

  if (value <= -TWO_PWR_63_DBL) {
    return Long.MIN_VALUE;
  }

  if (value + 1 >= TWO_PWR_63_DBL) {
    return Long.MAX_VALUE;
  }

  if (value < 0) {
    return Long.fromNumber(-value).not().add(Long.ONE);
  }

  return new Long((value / TWO_PWR_32_DBL) | 0, (value % TWO_PWR_32_DBL) | 0);
};

module.exports = Long;
