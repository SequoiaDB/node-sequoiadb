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
var Collection = require('../lib/collection');
var CollectionSpace = require('../lib/collection_space');

describe('Collection index', function () {
  var client = common.createClient();
  var collection;

  var spaceName = 'foo6';
  var collectionName = "bar5";

  before(function (done) {
    this.timeout(8000);
    client.ready(function () {
      var createCollection = function (space) {
        space.createCollection(collectionName, function (err, _collection) {
          expect(err).not.to.be.ok();
          expect(_collection).to.be.a(Collection);
          collection = _collection;
          done();
        });
      };
      client.createCollectionSpace(spaceName, function (err, space) {
        if (err) {
          client.getCollectionSpace(spaceName, function (err, _space) {
            expect(err).not.to.be.ok();
            createCollection(_space);
          });
        } else {
          expect(space).to.be.a(CollectionSpace);
          expect(space.name).to.be(spaceName);
          createCollection(space);
        }
      });
    });
  });

  after(function (done) {
    client.dropCollectionSpace(spaceName, function (err) {
      expect(err).not.to.be.ok();
      client.disconnect(done);
    });
  });

  it("set read from master first", function(done){
    var option = {"PreferedInstance":"M"};
    client.setSessionAttr(option, function (err) {
      expect(err).not.to.be.ok();
      done();
    });
  });

  it('createIndex should ok', function (done) {
    var key = {
      "Last Name": 1,
      "First Name": 1
    };
    collection.createIndex("index name", key, false, false, function (err) {
      expect(err).not.to.be.ok();
      done();
    });
  });

  it('getIndex should ok', function (done) {
    collection.getIndex("index name", function (err, cursor) {
      expect(err).not.to.be.ok();
      cursor.current(function (err, index) {
        expect(index.IndexDef.name).to.be('index name');
        done();
      });
    });
  });

  it('getIndex without name should ok', function (done) {
    collection.getIndex(function (err, cursor) {
      expect(err).not.to.be.ok();
      cursor.next(function (err, index) {
        expect(index.IndexDef.name).to.be('$id');
        cursor.next(function (err, index) {
          expect(index.IndexDef.name).to.be('index name');
          done();
        });
      });
    });
  });

  it('getIndexes should ok', function (done) {
    collection.getIndexes(function (err, cursor) {
      expect(err).not.to.be.ok();
      cursor.next(function (err, index) {
        expect(index.IndexDef.name).to.be('$id');
        cursor.next(function (err, index) {
          expect(index.IndexDef.name).to.be('index name');
          done();
        });
      });
    });
  });

  it('dropIndex should ok', function (done) {
    collection.dropIndex("index name", function (err) {
      expect(err).not.to.be.ok();
      collection.getIndexes(function (err, cursor) {
        expect(err).not.to.be.ok();
        cursor.next(function (err, index) {
          expect(index.IndexDef.name).to.be('$id');
          cursor.next(function (err, index) {
            expect(index).to.be(null);
            done();
          });
        });
      });
    });
  });
});
