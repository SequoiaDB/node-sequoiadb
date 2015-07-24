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

var expect = require('expect.js');
var common = require('./common');

describe('Domain Actions', function () {
  var conn = common.createConnection();
  var domainName = 'domain_name';
  var domain;

  before(function (done) {
    this.timeout(8000);
    conn.ready(function () {
      conn.createDomain(domainName, function (err, _domain) {
        expect(err).not.to.be.ok();
        expect(_domain).to.be.ok();
        expect(_domain.name).to.be(domainName);
        domain = _domain;
        done();
      });
    });
  });

  after(function (done) {
    conn.dropDomain(domainName, function (err, domain) {
      expect(err).not.to.be.ok();
      conn.disconnect(done);
    });
  });

  it('getCollectionSpaces should ok', function (done) {
    domain.getCollectionSpaces(function (err, cursor) {
      expect(err).not.to.be.ok();
      expect(cursor).to.be.ok();
      cursor.current(function (err, item) {
        expect(err).not.to.be.ok();
        expect(item).to.be(null);
        done();
      });
    });
  });

  it('getCollections should ok', function (done) {
    domain.getCollections(function (err, cursor) {
      expect(err).not.to.be.ok();
      expect(cursor).to.be.ok();
      cursor.current(function (err, item) {
        expect(err).not.to.be.ok();
        expect(item).to.be(null);
        done();
      });
    });
  });

  it('alter should ok', function (done) {
    var options = {
      "Groups": [ "group1", "group2", "group3" ],
      "AutoSplit": true
    };
    domain.alter(options, function (err) {
      expect(err).not.to.be.ok();
      done();
    });
  });
});
