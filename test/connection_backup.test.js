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

describe('Connection Backup', function () {
  var conn = common.createConnection();

  before(function (done) {
    this.timeout(8000);
    conn.ready(done);
  });

  after(function (done) {
    conn.disconnect(done);
  });

  it('getBackups should ok', function (done) {
    var options = {
      // "Path": "/opt/sequoiadb/backup"
    };
    conn.getBackups(options, null, null, null, function (err, cursor) {
      expect(err).not.to.be.ok();
      cursor.current(function (err, item) {
        expect(err).to.not.be.ok();
        expect(item).to.be(null);
        done();
      });
    });
  });

  it('backupOffline should ok', function (done) {
    var options = {
      // "GroupName": ["rgName1", "rgName2"],
      // "Path": "/opt/sequoiadb/backup",
      // "Name": "backupName",
      // "Description": "description",
      // "EnsureInc": true,
      // "OverWrite": true
    };
    conn.backupOffline(options, function (err) {
      expect(err).not.to.be.ok();
      done();
    });
  });

  var _item;
  it('getBackups should ok with items', function (done) {
    var options = {};
    conn.getBackups(options, null, null, null, function (err, cursor) {
      expect(err).not.to.be.ok();
      cursor.current(function (err, item) {
        expect(err).to.not.be.ok();
        expect(item).to.be.ok();
        _item = item;
        done();
      });
    });
  });

  it('removeBackup should ok with items', function (done) {
    var options = {};
    conn.removeBackup(options, function (err, cursor) {
      expect(err).not.to.be.ok();
      done();
    });
  });

  it('getBackups should ok with zero', function (done) {
    var options = {
      // "Path": "/opt/sequoiadb/backup"
    };
    conn.getBackups(options, null, null, null, function (err, cursor) {
      expect(err).not.to.be.ok();
      cursor.current(function (err, item) {
        expect(err).to.not.be.ok();
        expect(item).to.be(null);
        done();
      });
    });
  });
});
