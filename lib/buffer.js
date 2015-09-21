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

var Long = require('long');

/**
 * XBuffer is base on Buffer with endianness information.
 * When write or read from XBuffer, no more care the BE/LE.
 *
 * Examples:
 * ```js
 * var xbuff = new XBuffer(new Buffer(10), true);
 * xbuff.writeUInt32(10, 0);
 * xbuff.readUInt32(0); // => 10
 * ```
 * @param {Buffer} buff origin buffer
 * @param {Boolean} isBigEndian endianness. big-endian is true, little-endian is false
 */
var XBuffer = function (buff, isBigEndian) {
  if (Buffer.isBuffer(buff)) {
    this.buff = buff;
  } else if (typeof buff === 'number') {
    this.buff = new Buffer(buff);
  } else {
    var str = typeof buff === "string" ? buff : JSON.stringify(buff);
    this.buff = new Buffer(str);
  }
  this.isBigEndian = isBigEndian;

  Object.defineProperty(this, 'length', {
    get: function () {
      return this.buff.length;
    }
  });
};

['UInt', 'Int'].forEach(function (unit) {
  var write = 'write' + unit;
  /**
   * Write an (UInt/Int) value into Xbuffer.
   * The value will be saved by endianness automatically.
   *
   * Examples:
   * ```js
   * xbuff.writeUInt(10, 0, 8); // write 10(UInt8) 1 byte
   * xbuff.writeUInt(10, 0, 16); // write 10(UInt16) 2 bytes
   * xbuff.writeUInt(10, 0, 24); // write 10(UInt24) 3 bytes
   * xbuff.writeUInt(10, 0, 32); // write 10(UInt32) 4 bytes
   * ```
   * @param {Number} value the value
   * @param {Number} offset the start offset in XBuffer
   * @param {Number} byteLength used byte length, support 8/16/24/32
   * @name write(U)Int
   */
  XBuffer.prototype[write] = function (value, offset, byteLength) {
    if (offset === undefined) {
      throw new Error("Must pass the offset");
    }
    this.isBigEndian ? this.buff[write + 'BE'](value, offset, byteLength)
                     : this.buff[write + 'LE'](value, offset, byteLength);
  };

  var read = 'read' + unit;
  /**
   * Read an (UInt/Int) value from XBuffer.
   * The value will be read by endianness automatically.
   *
   * Examples:
   * ```js
   * xbuff.readUInt(0, 8); // read (UInt8) 1 byte
   * xbuff.readUInt(0, 16); // read (UInt16) 2 bytes
   * xbuff.readUInt(0, 24); // read (UInt24) 3 bytes
   * xbuff.readUInt(0, 32); // read (UInt32) 4 bytes
   * ```
   * @param {Number} offset the start offset in XBuffer
   * @param {Number} byteLength used byte length, suport 8/16/24/32
   * @name read(U)Int
   */
  XBuffer.prototype[read] = function (offset, byteLength) {
    if (offset === undefined) {
      throw new Error("Must pass the offset");
    }
    return this.isBigEndian ? this.buff[read + 'BE'](offset, byteLength)
                            : this.buff[read + 'LE'](offset, byteLength);
  };
});

['UInt8', 'Int8'].forEach(function (unit) {
  var write = 'write' + unit;
  /**
   * Write an (UInt8/Int8) value into XBuffer.
   * The value will be saved by endianness automatically.
   *
   * Examples:
   * ```js
   * xbuff.writeUInt8(10, 0); // write 10(UInt8) 1 byte at 0
   * ```
   * @param {Number} value the (UInt8/Int8) value
   * @param {Number} offset the start offset
   * @name write(U)Int8
   */
  XBuffer.prototype[write] = function (value, offset) {
    if (offset === undefined) {
      throw new Error("Must pass the offset");
    }
    this.buff[write](value, offset);
  };

  var read = 'read' + unit;
  /**
   * Read an (UInt8/Int8) from XBuffer.
   * The value will be read by endianness automatically.
   *
   * Examples:
   * ```js
   * xbuff.readUInt8(0); // read 10(UInt8) 1 byte from 0
   * ```
   * @param {Number} offset the start offset
   * @name read(U)Int8
   */
  XBuffer.prototype[read] = function (offset) {
    if (offset === undefined) {
      throw new Error("Must pass the offset");
    }
    return this.buff[read](offset);
  };
});

['UInt16', 'Int16', 'UInt32', 'Int32', 'Float', 'Double'].forEach(function (unit) {
  var write = 'write' + unit;
  /**
   * Write an (UInt16/Int16/UInt32/Int32/Float/Double) value into XBuffer.
   * The value will be saved by endianness automatically.
   *
   * Examples:
   * ```js
   * xbuff.writeUInt16(10, 0); // write 10(UInt16) at 0, use 2 byte
   * xbuff.writeInt16(10, 0); // write 10(Int16) at 0, use 2 byte
   * xbuff.writeUInt32(10, 0); // write 10(UInt32) at 0, use 4 byte
   * xbuff.writeInt32(10, 0); // write 10(Int32) at 0, use 4 byte
   * xbuff.writeFloat(10, 0); // write 10(Float) at 0, use 4 byte
   * xbuff.writeDouble(10, 0); // write 10(Double) at 0, use 8 byte
   * ```
   * @param {Number} value the (UInt16/Int16/UInt32/Int32/Float/Double) value
   * @param {Number} offset the start offset
   * @name writeXXX
   */
  XBuffer.prototype[write] = function (value, offset) {
    if (offset === undefined) {
      throw new Error("Must pass the offset");
    }
    this.isBigEndian ? this.buff[write + 'BE'](value, offset)
                     : this.buff[write + 'LE'](value, offset);
  };

  var read = 'read' + unit;
  /**
   * Read an (UInt16/Int16/UInt32/Int32/Float/Double) value from XBuffer.
   * The value will be read by endianness automatically.
   *
   * Examples:
   * ```js
   * xbuff.readUInt16(10, 0); // read (UInt16) at 0, 2 byte
   * xbuff.readInt16(10, 0); // read (Int16) at 0, 2 byte
   * xbuff.readUInt32(10, 0); // read (UInt32) at 0, 4 byte
   * xbuff.readInt32(10, 0); // read (Int32) at 0, 4 byte
   * xbuff.readFloat(10, 0); // read (Float) at 0, 4 byte
   * xbuff.readDouble(10, 0); // read (Double) at 0, 8 byte
   * ```
   * @param {Number} offset the start offset
   * @name readXXX
   */
  XBuffer.prototype[read] = function (offset) {
    if (offset === undefined) {
      throw new Error("Must pass the offset");
    }
    return this.isBigEndian ? this.buff[read + 'BE'](offset)
                            : this.buff[read + 'LE'](offset);
  };
});

/**
 * Write an origin buffer into XBuffer.
 *
 * Examples:
 * ```js
 * xbuff.writeBuffer(new Buffer(10), 0); // write buffer at 0
 * ```
 * @param {Buffer} buff origin buffer
 * @param {Number} offset the start offset
 */
XBuffer.prototype.writeBuffer = function (buff, offset) {
  if (offset === undefined) {
    throw new Error("Must pass the offset");
  }
  buff.copy(this.buff, offset);
};

/**
 * Write a Long value into XBuffer, use 8 bytes.
 *
 * Examples:
 * ```js
 * xbuff.writeLong(longValue, 0); // write Long value at 0, use 8 bytes
 * ```
 * @param {Buffer} value the Long value
 * @param {Number} offset the start offset
 */
XBuffer.prototype.writeLong = function (value, offset) {
  if (offset === undefined) {
    throw new Error("Must pass the offset");
  }

  if (!Long.isLong(value)) {
    throw new Error("Must pass the Long type as value");
  }

  if (this.isBigEndian) {
    this.buff.writeInt32BE(value.getHighBits(), offset);
    this.buff.writeInt32BE(value.getLowBits(), offset + 4);
  } else {
    this.buff.writeInt32LE(value.getLowBits(), offset);
    this.buff.writeInt32LE(value.getHighBits(), offset + 4);
  }
};

/**
 * Read a Long value from XBuffer
 * The Long value will be read by endianness automatically.
 *
 * Examples:
 * ```js
 * xbuff.readLong(0); // read Long value at 0
 * ```
 * @param {Number} offset the start offset
 */
XBuffer.prototype.readLong = function (offset) {
  if (offset === undefined) {
    throw new Error("Must pass the offset");
  }
  var low, high;
  if (this.isBigEndian) {
    high = this.buff.readInt32BE(offset);
    low = this.buff.readInt32BE(offset + 4);
  } else {
    low = this.buff.readInt32LE(offset);
    high = this.buff.readInt32BE(offset + 4);
  }
  return new Long(low, high);
};

/**
 * Export the origin Buffer
 *
 * Examples:
 * ```js
 * xbuff.toBuffer();
 * ```
 * @return {Buffer} Get an origin Buffer
 */
XBuffer.prototype.toBuffer = function () {
  return this.buff;
};

/**
 * Slice new XBuffer from old XBuffer
 *
 * Examples:
 * ```js
 * xbuff.slice(0, 10); // slice from 0 to 9
 * ```
 * @param {Number} start the start position
 * @param {Number} end the end position
 * @return {XBuffer} the new XBuffer
 */
XBuffer.prototype.slice = function (start, end) {
  return new XBuffer(this.buff.slice(start, end), this.isBigEndian);
};

module.exports = XBuffer;
