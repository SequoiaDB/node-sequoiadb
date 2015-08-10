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

describe('Collection split', function () {
  var conn = common.createConnection();
  var collection;
  var _space;

  var spaceName = 'split';
  var collectionName = "split";

  before(function (done) {
    this.timeout(8000);
    conn.ready(function () {
      var createCollection = function (space) {
        _space = space;
        var options = {ShardingKey: {"age": 1}, ShardingType: "hash", Partition: 4096};
        space.createCollection(collectionName, options, function (err, _collection) {
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

  xit('split should ok', function (done) {
    var splitCondition = {age: 30};
    var splitEndCondition = {age: 60};
    collection.split('source', 'dest', splitCondition, splitEndCondition, function (err, cursor) {
      expect(err).not.to.be.ok();
      done();
    });
  });

  xit('splitByPercent should ok', function (done) {
    collection.splitByPercent('source', 'dest', 50, function (err, cursor) {
      expect(err).not.to.be.ok();
      done();
    });
  });

  xit('splitAsync should ok', function (done) {
    var splitCondition = {age: 30};
    var splitEndCondition = {age: 60};
    collection.splitAsync('source', 'dest', splitCondition, splitEndCondition, function (err, cursor) {
      expect(err).not.to.be.ok();
      done();
    });
  });

  xit('splitByPercentAsync should ok', function (done) {
    collection.splitByPercentAsync('source', 'dest', 50, function (err, cursor) {
      expect(err).not.to.be.ok();
      done();
    });
  });
});
