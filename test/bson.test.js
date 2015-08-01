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
var serialize = require('../lib/bson');

describe('/lib/bson.js', function () {
  it('convert should ok', function () {
    var auth = {"User": "", "Passwd": ""};
    var buff = serialize(auth, false);
    expect(buff).to.eql(new Buffer([
      0x1d, 0x00, 0x00, 0x00, // length
      0x02, // e_name string
      0x55, 0x73, 0x65, 0x72, 0x00, // User
      0x01, 0x00, 0x00, 0x00, // length
      0x00, // ("" + "\x00")
      0x02, // e_name string
      0x50, 0x61, 0x73, 0x73, 0x77, 0x64, 0x00, // Passwd
      0x01, 0x00, 0x00, 0x00, // length
      0x00, // ("" + "\x00")
      0x00 // "\x00"
    ]));

    var buff = serialize(auth, true);
    expect(buff).to.eql(new Buffer([
      0x00, 0x00, 0x00, 0x1d, // length
      0x02, // e_name string
      0x55, 0x73, 0x65, 0x72, 0x00, // User
      0x00, 0x00, 0x00, 0x01, // length
      0x00, // ("" + "\x00")
      0x02, // e_name string
      0x50, 0x61, 0x73, 0x73, 0x77, 0x64, 0x00, // Passwd
      0x00, 0x00, 0x00, 0x01, // length
      0x00, // ("" + "\x00")
      0x00 // "\x00"
    ]));
  });
});
