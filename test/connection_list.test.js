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

describe('Connection List', function () {
  var conn = common.createConnection();
  var spaceName = 'spaceName' + Math.floor(Math.random() * 100);
  var collectionName = 'test_coll';

  before(function (done) {
    this.timeout(8000);
    conn.ready(function () {
      conn.createCollectionSpace(spaceName, function (err, space) {
        expect(err).not.to.be.ok();
        expect(space.name).to.be(spaceName);
        space.createCollection(collectionName, function (err, collection) {
          expect(err).not.to.be.ok();
          done();
        });
      });
    });
  });

  after(function (done) {
    conn.dropCollectionSpace(spaceName, function (err) {
      expect(err).not.to.be.ok();
      conn.disconnect(done);
    });
  });

  it('getCollectionSpaces should ok', function (done) {
    conn.getCollectionSpaces(function (err, cursor) {
      expect(err).to.not.be.ok();
      cursor.current(function (err, item) {
        expect(err).to.not.be.ok();
        expect(item).to.be.ok();
        done();
      });
    });
  });

  it('getCollectionSpaceNames should ok', function (done) {
    conn.getCollectionSpaceNames(function (err, names) {
      expect(err).to.not.be.ok();
      expect(names.length).to.be.above(0);
      done();
    });
  });

  it('getCollections should ok', function (done) {
    conn.getCollections(function (err, cursor) {
      expect(err).to.not.be.ok();
      cursor.current(function (err, item) {
        expect(err).to.not.be.ok();
        expect(item.Name).to.be.ok();
        done();
      });
    });
  });

  it('getCollectionNames should ok', function (done) {
    conn.getCollectionNames(function (err, names) {
      expect(err).to.not.be.ok();
      expect(names.length).to.be.above(0);
      done();
    });
  });

  it('getStorageUnits should ok', function (done) {
    conn.getStorageUnits(function (err, names) {
      expect(err).to.not.be.ok();
      expect(names.length).to.be(0);
      done();
    });
  });
});
