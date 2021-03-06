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

describe('Domain', function () {
  var client = common.createClient();

  before(function (done) {
    this.timeout(8000);
    client.ready(done);
  });

  after(function (done) {
    client.disconnect(done);
  });

  it('getDomains should ok', function (done) {
    client.getDomains(null, null, null, null, function (err, cursor) {
      expect(err).not.to.be.ok();
      expect(cursor).to.be.ok();
      done();
    });
  });

  it('isDomainExist should ok', function (done) {
    client.isDomainExist('inexist', function (err, exist) {
      expect(err).not.to.be.ok();
      expect(exist).to.be(false);
      done();
    });
  });

  it('getDomain should ok', function (done) {
    client.getDomain('inexist', function (err, domain) {
      expect(err).not.to.be.ok();
      expect(domain).to.be(null);
      done();
    });
  });

  it('createDomain should ok', function (done) {
    client.createDomain('mydomain', function (err, domain) {
      expect(err).not.to.be.ok();
      expect(domain).to.be.ok();
      expect(domain.name).to.be('mydomain');
      done();
    });
  });

  it('getDomain should ok with exist', function (done) {
    client.getDomain('mydomain', function (err, domain) {
      expect(err).not.to.be.ok();
      expect(domain.name).to.be('mydomain');
      done();
    });
  });

  it('dropDomain should ok', function (done) {
    client.dropDomain('mydomain', function (err, domain) {
      expect(err).not.to.be.ok();
      done();
    });
  });
});
