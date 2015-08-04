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
var Query = require('../lib/query');
var Long = require('../lib/long');

describe('Collection DML', function () {
  var conn = common.createConnection();
  var collection;
  var _space;

  var spaceName = 'foo6';
  var collectionName = "bar6";

  before(function (done) {
    this.timeout(8000);
    conn.ready(function () {
      var createCollection = function (space) {
        _space = space;
        space.createCollection(collectionName, function (err, _collection) {
          expect(err).not.to.be.ok();
          expect(_collection).to.be.a(Collection);
          collection = _collection;
          done();
        });
      };
      conn.createCollectionSpace(spaceName, function (err, space) {
        if (err) {
          conn.getCollectionSpace(spaceName, function (err, _space) {
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
    conn.dropCollectionSpace(spaceName, function (err) {
      expect(err).not.to.be.ok();
      conn.disconnect();
      done();
    });
  });

  it('query should ok', function (done) {
    collection.query(function (err, cursor) {
      expect(err).not.to.be.ok();
      expect(cursor).to.be.ok();
      cursor.current(function (err, item) {
        expect(err).not.to.be.ok();
        expect(item).to.be(null);
        done();
      });
    });
  });

  it('insert should ok', function (done) {
    collection.insert({"name":"sequoiadb"}, function (err) {
      expect(err).not.to.be.ok();
      done();
    });
  });

  it('query should ok with one item', function (done) {
    collection.query(function (err, cursor) {
      expect(err).not.to.be.ok();
      expect(cursor).to.be.ok();
      cursor.current(function (err, item) {
        expect(err).not.to.be.ok();
        expect(item.name).to.be("sequoiadb");
        done();
      });
    });
  });

  describe('explain', function () {
    var indexName = "QueryExpalinIndex";
    before('createIndex', function (done) {
      collection.createIndex(indexName, {"age": 1}, false, false, function (err) {
        expect(err).not.to.be.ok();
        done();
      });
    });

    it('explain should ok with one item', function (done) {
      // matcher, selector, orderBy, hint,
      // skipRows, returnRows, flag, options, callback
      var matcher = {
        "age": {
          "$gt": 50
        }
      };
      var selector = { "age": "" };
      var orderBy = { "age": -1 };
      var hint = { "": indexName };
      var options = { "Run": true };
      collection.explain(matcher, selector, orderBy, hint, Long.fromNumber(47), Long.fromNumber(3), 0, options, function (err, cursor) {
        expect(err).not.to.be.ok();
        expect(cursor).to.be.ok();
        cursor.next(function (err, item) {
          expect(err).not.to.be.ok();
          expect(item.IndexRead).to.be(1);
          expect(item.DataRead).to.be(0);
          expect(item.IndexName).to.be('QueryExpalinIndex');
          done();
        });
      });
    });
  });

  it('update should ok', function (done) {
    var query = new Query();
    query.Matcher = {name: "sequoiadb"};
    query.Modifier = {'$set': {age: 25}};
    collection.update(query, function (err) {
      expect(err).not.to.be.ok();
      done();
    });
  });

  it('update(matcher, modifier, hint) should ok', function (done) {
    collection.update({name: "sequoiadb"}, {'$set': {age: 26}}, {}, function (err) {
      expect(err).not.to.be.ok();
      done();
    });
  });

  it('delete should ok', function (done) {
    collection.delete({name: "sequoiadb"}, function (err) {
      expect(err).not.to.be.ok();
      done();
    });
  });

  it('delete all should ok', function (done) {
    collection.delete(function (err) {
      expect(err).not.to.be.ok();
      done();
    });
  });

  it('query should ok with none', function (done) {
    collection.query({}, {}, {}, {}, function (err, cursor) {
      expect(err).not.to.be.ok();
      expect(cursor).to.be.ok();
      cursor.current(function (err, item) {
        expect(err).not.to.be.ok();
        expect(item).to.be(null);
        done();
      });
    });
  });

  it('query should ok with Query', function (done) {
    var query = new Query();
    collection.query(query, function (err, cursor) {
      expect(err).not.to.be.ok();
      expect(cursor).to.be.ok();
      cursor.current(function (err, item) {
        expect(err).not.to.be.ok();
        expect(item).to.be(null);
        done();
      });
    });
  });

  it('upsert(matcher, modifier, hint) should ok', function (done) {
    collection.upsert({name: "sequoiadb"}, {'$set': {age: 26}}, {}, function (err) {
      expect(err).not.to.be.ok();
      done();
    });
  });

  it('bulkInsert should ok', function (done) {
    var insertors = [
      {name: "hi"},
      {name: "jack"}
    ];
    collection.bulkInsert(insertors, 0, function (err) {
      expect(err).not.to.be.ok();
      done();
    });
  });

  xit('aggregate should ok', function (done) {
    var insertors = [
      {name: "hi"},
      {name: "jack"}
    ];
    collection.aggregate(insertors, function (err) {
      expect(err).not.to.be.ok();
      done();
    });
  });

  it('getQueryMeta should ok', function (done) {
    var matcher = {};
    var orderBy = {"Indexblocks": 1};
    var hint = {"": "ageIndex"};
    collection.getQueryMeta(matcher, orderBy, hint, 0, -1, function (err, cursor) {
      expect(err).not.to.be.ok();
      expect(cursor).to.be.ok();
      cursor.current(function (err, item) {
        expect(err).not.to.be.ok();
        expect(item).to.be(null);
        done();
      });
    });
  });

  it('count should ok', function (done) {
    collection.count(function (err, count) {
      expect(err).not.to.be.ok();
      expect(count).to.be(0);
      done();
    });
  });

  describe('xxtach collection', function () {
    var mcl, scl;
    before('create sub collection', function (done) {
      var options = {
        "IsMainCL": true,
        "ShardingKey": {"id": 1},
        "ReplSize": 0
      };
      _space.createCollection('mcl', options, function (err, _collection) {
        expect(err).not.to.be.ok();
        expect(_collection).to.be.ok();
        mcl = _collection;
        done();
      });
    });

    it('createSubCollection', function (done) {
      var options = {
        "ReplSize": 0
      };
      _space.createCollection('scl', options, function (err, _collection) {
        expect(err).not.to.be.ok();
        expect(_collection).to.be.ok();
        scl = _collection;
        done();
      });
    });

    it('attachCollection should ok', function (done) {
      var options = {
        "LowBound": {id: 0},
        "UpBound": {id: 10}
      };
      mcl.attachCollection(scl.collectionFullName, options, function (err) {
        expect(err).not.to.be.ok();
        done();
      });
    });

    it('detachCollection should ok', function (done) {
      mcl.detachCollection(scl.collectionFullName, function (err) {
        expect(err).not.to.be.ok();
        done();
      });
    });
  });

  it('alter should ok', function (done) {
    var options = {
      "ReplSize": 0,
      "ShardingKey": {"a": 1},
      "ShardingType": "hash",
      "Partition": 4096
    };
    collection.alter(options, function (err) {
      expect(err).not.to.be.ok();
      done();
    });
  });
});
