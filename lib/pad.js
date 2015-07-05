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

exports.padLength = function (length, multipler) {
  if (multipler === 0) {
    return length;
  }
  var mod = length % multipler;
  return mod === 0 ? length : length + multipler - mod;
};

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
