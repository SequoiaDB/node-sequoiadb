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
var common = require('./common');

describe('Connection', function () {
  var conn = common.createConnection();

  before(function (done) {
    this.timeout(8000);
    conn.ready(done);
  });

  after(function (done) {
    conn.disconnect(done);
  });

  it('isValid should ok', function (done) {
    conn.isValid(function (err, valid) {
      expect(err).not.to.be.ok();
      expect(valid).to.be(true);
      done();
    });
  });

  it('setSessionAttr should ok', function (done) {
    var conf = {"PreferedInstance": "m"};
    conn.setSessionAttr(conf, function (err) {
      expect(err).not.to.be.ok();
      done();
    });
  });
});
