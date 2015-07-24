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

var expect = require('expect.js');
var pad = require('../lib/pad');

describe('/lib/pad.js', function () {
  it('padLength should ok', function () {
    expect(pad.padLength(1, 0)).to.be(1);
    expect(pad.padLength(1, 4)).to.be(4);
    expect(pad.padLength(2, 4)).to.be(4);
    expect(pad.padLength(3, 4)).to.be(4);
    expect(pad.padLength(4, 4)).to.be(4);
  });

  it('padBuffer should ok', function () {
    var buff = new Buffer([1, 2, 3, 4]);
    expect(pad.padBuffer(buff, 0)).to.eql(new Buffer([1, 2, 3, 4]));
    expect(pad.padBuffer(buff.slice(0, 1), 4)).to.eql(new Buffer([1, 0, 0, 0]));
    expect(pad.padBuffer(buff.slice(0, 2), 4)).to.eql(new Buffer([1, 2, 0, 0]));
    expect(pad.padBuffer(buff.slice(0, 3), 4)).to.eql(new Buffer([1, 2, 3, 0]));
    expect(pad.padBuffer(buff.slice(0, 4), 4)).to.eql(new Buffer([1, 2, 3, 4]));
  });
});
