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
var constants = require('../lib/const');

describe('Collection Snapshot', function () {
  var conn = common.createConnection();

  before(function (done) {
    this.timeout(8000);
    conn.ready(done);
  });

  after(function (done) {
    conn.disconnect(done);
  });

  it("getSnapshot(SDB_SNAP_CONTEXTS) should ok", function (done) {
    conn.getSnapshot(constants.SDB_SNAP_CONTEXTS, null, null, null, function (err, cursor) {
      expect(err).not.to.be.ok();
      cursor.current(function (err, item) {
        expect(err).not.to.be.ok();
        expect(item).to.be.ok();
        done();
      });
    });
  });

  it("getSnapshot(SDB_SNAP_CONTEXTS_CURRENT) should ok", function (done) {
    conn.getSnapshot(constants.SDB_SNAP_CONTEXTS_CURRENT, null, null, null, function (err, cursor) {
      expect(err).not.to.be.ok();
      cursor.current(function (err, item) {
        expect(err).not.to.be.ok();
        expect(item).to.be.ok();
        done();
      });
    });
  });

  it("getSnapshot(SDB_SNAP_SESSIONS) should ok", function (done) {
    conn.getSnapshot(constants.SDB_SNAP_SESSIONS, null, null, null, function (err, cursor) {
      expect(err).not.to.be.ok();
      cursor.current(function (err, item) {
        expect(err).not.to.be.ok();
        expect(item).to.be.ok();
        done();
      });
    });
  });

  it("getSnapshot(SDB_SNAP_SESSIONS_CURRENT) should ok", function (done) {
    conn.getSnapshot(constants.SDB_SNAP_SESSIONS_CURRENT, null, null, null, function (err, cursor) {
      expect(err).not.to.be.ok();
      cursor.current(function (err, item) {
        expect(err).not.to.be.ok();
        expect(item).to.be.ok();
        done();
      });
    });
  });

  it("getSnapshot(SDB_SNAP_COLLECTIONS) should ok", function (done) {
    conn.getSnapshot(constants.SDB_SNAP_COLLECTIONS, null, null, null, function (err, cursor) {
      expect(err).not.to.be.ok();
      cursor.current(function (err, item) {
        expect(err).not.to.be.ok();
        expect(item).to.be.ok();
        done();
      });
    });
  });

  it("getSnapshot(SDB_SNAP_COLLECTIONSPACES) should ok", function (done) {
    conn.getSnapshot(constants.SDB_SNAP_COLLECTIONSPACES, null, null, null, function (err, cursor) {
      expect(err).not.to.be.ok();
      cursor.current(function (err, item) {
        expect(err).not.to.be.ok();
        expect(item).to.be.ok();
        done();
      });
    });
  });

  it("getSnapshot(SDB_SNAP_DATABASE) should ok", function (done) {
    conn.getSnapshot(constants.SDB_SNAP_DATABASE, null, null, null, function (err, cursor) {
      expect(err).not.to.be.ok();
      cursor.current(function (err, item) {
        expect(err).not.to.be.ok();
        expect(item).to.be.ok();
        done();
      });
    });
  });

  it("getSnapshot(SDB_SNAP_SYSTEM) should ok", function (done) {
    conn.getSnapshot(constants.SDB_SNAP_SYSTEM, null, null, null, function (err, cursor) {
      expect(err).not.to.be.ok();
      cursor.current(function (err, item) {
        expect(err).not.to.be.ok();
        expect(item).to.be.ok();
        done();
      });
    });
  });

  it("resetSnapshot should ok", function (done) {
    conn.resetSnapshot(function (err) {
      expect(err).not.to.be.ok();
      done();
    });
  });

  it('flushConfigure should ok', function (done) {
    var matcher = {"Global":false};
    conn.flushConfigure(matcher, function (err) {
      expect(err).not.to.be.ok();
      done();
    });
  });

});
