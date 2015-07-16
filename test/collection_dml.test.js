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
var Collection = require('../lib/collection');
var CollectionSpace = require('../lib/collection_space');
var Query = require('../lib/query');

describe('Collection DML', function () {
  var conn = common.createConnection();
  var collection;

  var spaceName = 'foo5';
  var collectionName = "bar5";

  before(function (done) {
    conn.ready(function () {
      var createCollection = function (space) {
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
});
