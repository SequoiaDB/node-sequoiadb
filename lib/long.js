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
Long.NEG_ONE = new Long(0xffffff, 0xffffff); // -1
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
  buff.writeInt32BE(value.high, offset);
  buff.writeInt32BE(value.low, offset + 4);
};

Long.writeLongLE = function (buff, value, offset) {
  // 8个字节，小端。高位写在右边
  buff.writeInt32LE(value.low, offset);
  buff.writeInt32LE(value.high, offset + 4);
};

module.exports = Long;
