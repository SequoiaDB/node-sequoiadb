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

/**
 * 按对齐位数计算出对齐后的长度
 *
 * Examples:
 * ```js
 * pad(2, 4); // 4
 * pad(3, 4); // 4
 * pad(4, 4); // 4
 * pad(5, 4); // 8
 * ```
 * @param {Number} length 原始长度
 * @param {Number} multipler 对齐位数
 * @return {Number} 对齐之后的长度
 */
exports.padLength = function (length, multipler) {
  if (multipler === 0) {
    return length;
  }
  var mod = length % multipler;
  return mod === 0 ? length : length + multipler - mod;
};

/**
 * 按对齐位数对一个Buffer进行对齐
 *
 * Examples:
 * ```js
 * pad(new Buffer([1]), 4); // [1, 0, 0, 0]
 * ```
 * @param {Buffer} buff 原始Buffer
 * @param {Number} multipler 对齐位数
 * @return {Buffer} 对齐之后的Buffer
 */
exports.padBuffer = function (buff, multipler) {
  if (multipler === 0) {
    return buff;
  }

  var length = buff.length;
  var mod = length % multipler;
  if (mod === 0) {
    return buff;
  }

  var newLength = length + multipler - mod;
  var paded = new Buffer(newLength);
  buff.copy(paded, 0);
  paded.fill(0, buff.length, newLength);
  return paded;
};
