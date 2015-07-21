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
var Cursor = require('../lib/cursor');
var CollectionSpace = require('../lib/collection_space');

describe('CollectionSpace', function () {
  var conn = common.createConnection();

  before(function (done) {
    conn.ready(done);
  });

  after(function (done) {
    conn.disconnect(done);
  });

  var spaceName = 'spaceName' + Math.floor(Math.random() * 100);
  it('createCollectionSpace should ok', function (done) {
    conn.createCollectionSpace(spaceName, function (err, space) {
      expect(err).not.to.be.ok();
      expect(space).to.be.a(CollectionSpace);
      expect(space.name).to.be(spaceName);
      done();
    });
  });

  it('isCollectionSpaceExist should ok', function (done) {
    conn.isCollectionSpaceExist(spaceName, function (err, exist) {
      expect(err).not.to.be.ok();
      expect(exist).to.be(true);
      done();
    });
  });

  it('getCollectionSpace should ok', function (done) {
    conn.getCollectionSpace(spaceName, function (err, space) {
      expect(err).not.to.be.ok();
      expect(space).to.be.a(CollectionSpace);
      expect(space.name).to.be(spaceName);
      done();
    });
  });

  it('getCollectionSpace inexist should ok', function (done) {
    conn.getCollectionSpace('inexist', function (err) {
      expect(err).to.be.ok();
      expect(err.message).to.be('SDB_DMS_CS_NOTEXIST');
      done();
    });
  });

  it('dropCollectionSpace should ok', function (done) {
    conn.dropCollectionSpace(spaceName, function (err, space) {
      expect(err).not.to.be.ok();
      done();
    });
  });

  it('isCollectionSpaceExist should ok', function (done) {
    conn.isCollectionSpaceExist(spaceName, function (err, exist) {
      expect(err).not.to.be.ok();
      expect(exist).to.be(false);
      done();
    });
  });

  it('should ok', function (done) {
    conn.getCollectionSpaces(function (err, cursor) {
      expect(err).not.to.be.ok();
      expect(cursor).to.be.a(Cursor);
      done();
    });
  });
});
