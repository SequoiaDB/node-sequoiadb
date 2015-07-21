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
var CollectionSpace = require('../lib/collection_space');

describe('Collection DDL', function () {
  var Collection = require('../lib/collection');
  var conn = common.createConnection();
  var collectionSpace;
  var spaceName = 'spacename' + Math.floor(Math.random() * 100);
  before(function (done) {
    this.timeout(8000);
    conn.ready(function () {
      conn.createCollectionSpace(spaceName, function (err, space) {
        expect(err).not.to.be.ok();
        expect(space).to.be.a(CollectionSpace);
        expect(space.name).to.be(spaceName);
        collectionSpace = space;
        done();
      });
    });
  });

  after(function (done) {
    conn.dropCollectionSpace(spaceName, function (err) {
      expect(err).not.to.be.ok();
      collectionSpace = null;
      conn.disconnect(done);
    });
  });

  var collectionName = "collection";

  it('isCollectionExist should ok', function (done) {
    collectionSpace.isCollectionExist(collectionName, function (err, exist) {
      expect(err).not.to.be.ok();
      expect(exist).to.be(false);
      done();
    });
  });

  it('getCollection for inexist should ok', function (done) {
    collectionSpace.getCollection('inexist', function (err, collection) {
      expect(err).not.to.be.ok();
      expect(collection).to.be(null);
      done();
    });
  });

  it('createCollection should ok', function (done) {
    collectionSpace.createCollection(collectionName, function (err, collection) {
      expect(err).not.to.be.ok();
      expect(collection).to.be.a(Collection);
      collectionSpace.isCollectionExist(collectionName, function (err, exist) {
        expect(err).not.to.be.ok();
        expect(exist).to.be(true);
        done();
      });
    });
  });

  it('getCollection should ok', function (done) {
    collectionSpace.getCollection(collectionName, function (err, collection) {
      expect(err).not.to.be.ok();
      expect(collection).to.be.a(Collection);
      done();
    });
  });

  it('dropCollection should ok', function (done) {
    collectionSpace.dropCollection(collectionName, function (err) {
      expect(err).not.to.be.ok();
      collectionSpace.isCollectionExist(collectionName, function (err, exist) {
        expect(err).not.to.be.ok();
        expect(exist).to.be(false);
        done();
      });
    });
  });
});
