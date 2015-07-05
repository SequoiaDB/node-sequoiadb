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

Long.prototype.toBEBuffer = function () {
  var buf = new Buffer(8);
  exports.writeLongBE(buf, this);
};

Long.prototype.toLEBuffer = function () {
  var buf = new Buffer(8);
  exports.writeLongLE(buf, this);
};

Long.ZERO = new Long(0x0, 0x0); // 0
Long.NEG_ONE = new Long(0xffffffff, 0xffffffff); // -1
Long.ONE = new Long(0x0, 0x1); // 1

Long.equals = function (long1, long2) {
  return Buffer.compare(long1.high, long2.high) === 0 &&
    Buffer.compare(long1.low, long2.low) === 0;
};

Long.readLongBE = function (buff) {
  // 8个字节，大端
  var low = buff.readInt32BE(4);
  var high = buff.readInt32BE(0);
  return new Long(high, low);
};

Long.readLongLE = function (buff) {
  // 8个字节，大小端
  var low = buff.readInt32LE(0);
  var high = buff.readInt32LE(4);
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

module.exports = Long;
