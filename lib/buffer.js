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

/**
 * 基于默认Buffer封装的XBuffer类，该类携带有大小端信息。
 * 读取和写入时可以不再关注大小端信息。
 *
 * Examples:
 * ```js
 * var xbuff = new XBuffer(new Buffer(10), true);
 * xbuff.writeUInt32(10, 0);
 * xbuff.readUInt32(0); // => 10
 * ```
 * @param {Buffer} buff 原始Buffer对象
 * @param {Boolean} isBigEndian 大小端信息。大端为true，小端为false
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
   * 写入一个带（UInt/Int）类型的值到XBuffer中。自动按XBuffer的端信息进行写入
   *
   * Examples:
   * ```js
   * xbuff.writeUInt(10, 0, 8); // write 10(UInt8) 1 byte
   * xbuff.writeUInt(10, 0, 16); // write 10(UInt16) 2 bytes
   * xbuff.writeUInt(10, 0, 24); // write 10(UInt24) 3 bytes
   * xbuff.writeUInt(10, 0, 32); // write 10(UInt32) 4 bytes
   * ```
   * @param {Number} value 值
   * @param {Number} offset 写入到XBuffer中的起始偏移位置
   * @param {Number} byteLength 所用字节长度，支持8/16/24/32
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
   * 从XBuffer中读出一个带（UInt/Int）类型的值。自动按XBuffer的端信息进行读取
   *
   * Examples:
   * ```js
   * xbuff.readUInt(0, 8); // read (UInt8) 1 byte
   * xbuff.readUInt(0, 16); // read (UInt16) 2 bytes
   * xbuff.readUInt(0, 24); // read (UInt24) 3 bytes
   * xbuff.readUInt(0, 32); // read (UInt32) 4 bytes
   * ```
   * @param {Number} offset 读取的起始偏移位置
   * @param {Number} byteLength 所读取字节长度，支持8/16/24/32
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
   * 写入一个（UInt8/Int8）类型的值到XBuffer中。自动按XBuffer的端信息进行写入
   *
   * Examples:
   * ```js
   * xbuff.writeUInt8(10, 0); // write 10(UInt8) 1 byte at 0
   * ```
   * @param {Number} value 值
   * @param {Number} offset 写入到XBuffer中的起始偏移位置
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
   * 从XBuffer中读出一个（UInt8/Int8）类型的值。自动按XBuffer的端信息进行读取
   *
   * Examples:
   * ```js
   * xbuff.readUInt8(0); // read 10(UInt8) 1 byte from 0
   * ```
   * @param {Number} offset 读取的起始偏移位置
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
   * 写入一个带类型的值到XBuffer中。自动按XBuffer的端信息进行写入
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
   * @param {Number} value 值
   * @param {Number} offset 写入到XBuffer中的起始偏移位置
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
   * 从XBuffer中读出一个（UInt16/Int16/UInt32/Int32/Float/Double）类型的值。自动按XBuffer的端信息进行读取
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
   * @param {Number} offset XBuffer中的读取的起始偏移位置
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
 * 将一个原始Buffer写入到XBuffer中。
 *
 * Examples:
 * ```js
 * xbuff.writeBuffer(new Buffer(10), 0); // write buffer at 0
 * ```
 * @param {Buffer} buff 原始Buffer
 * @param {Number} offset 写入到XBuffer中的起始位置
 */
XBuffer.prototype.writeBuffer = function (buff, offset) {
  if (offset === undefined) {
    throw new Error("Must pass the offset");
  }
  buff.copy(this.buff, offset);
};

/**
 * 将一个Long类型写入到XBuffer中。
 *
 * Examples:
 * ```js
 * xbuff.writeLong(longValue, 0); // write Long value at 0, use 8 bytes
 * ```
 * @param {Buffer} buff 原始Buffer
 * @param {Number} offset 写入到XBuffer中的起始位置
 */
XBuffer.prototype.writeLong = function (value, offset) {
  if (offset === undefined) {
    throw new Error("Must pass the offset");
  }
  if (this.isBigEndian) {
    Long.writeLongBE(this.buff, value, offset);
  } else {
    Long.writeLongLE(this.buff, value, offset);
  }
};

/**
 * 读出一个Long类型的值
 *
 * Examples:
 * ```js
 * xbuff.readLong(0); // read Long value at 0
 * ```
 * @param {Number} start 从XBuffer中进行读取的起始位置
 */
XBuffer.prototype.readLong = function (offset) {
  if (offset === undefined) {
    throw new Error("Must pass the offset");
  }
  if (this.isBigEndian) {
    return Long.readLongBE(this.buff, offset);
  } else {
    return Long.readLongLE(this.buff, offset);
  }
};

/**
 * 导出原始Buffer
 *
 * Examples:
 * ```js
 * xbuff.toBuffer();
 * ```
 * @return {Buffer} 原始Buffer
 */
XBuffer.prototype.toBuffer = function () {
  return this.buff;
};

/**
 * 从旧的XBuffer中选取出新的XBuffer
 *
 * Examples:
 * ```js
 * xbuff.slice(0, 10); // slice from 0 to 9
 * ```
 * @param {Number} start 起始位置
 * @param {Number} end 结束位置
 * @return {XBuffer} 新的XBuffer
 */
XBuffer.prototype.slice = function (start, end) {
  return new XBuffer(this.buff.slice(start, end), this.isBigEndian);
};

module.exports = XBuffer;
