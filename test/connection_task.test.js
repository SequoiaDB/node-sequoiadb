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
var Node = require('../lib/node');

describe('Connection Task', function () {
  var conn = common.createConnection();
  var _collection;
  var _space;

  var srcGroup;
  var dstGroup;

  var spaceName = 'foox';
  var collectionName = "barx";

  before(function (done) {
    conn.ready(function () {
      done();
    });
  });

  after(function (done) {
    conn.disconnect(done);
  });

  var task;
  it('getTasks should ok', function (done) {
    conn.getTasks({}, {}, {}, {}, function (err, cursor) {
      expect(err).to.not.be.ok();
      cursor.current(function (err, item) {
        expect(err).to.not.be.ok();
        // expect(item).to.be.ok();
        task = item;
        done();
      });
    });
  });

  it('create collection space should ok', function(done){
    conn.createCollectionSpace(spaceName, function (err, space) {
      expect(space).not.to.be(null);
      expect(space.name).to.be(spaceName);
      _space = space;
      done();
    });
  });

  it('create source group should ok', function(done){
    conn.createReplicaGroup("source", function(err, group){
      expect(err).not.to.be.ok();
      expect(group).not.to.be(null);
      srcGroup = group;
      done();
    });
  });

  it('source group create node should ok', function (done) {
    this.timeout(8000);
    var host = '123.56.143.17';
    var port = 22000;
    var dbpath = '/opt/sequoiadb/database/data/22000';
    srcGroup.createNode(host, port, dbpath, {}, function(err, _){
      expect(err).not.to.be.ok();
      expect(_).to.be.a(Node);

      conn.activateReplicaGroup('source', function (err, _) {
        expect(err).not.to.be.ok();
        done();
      });
    });
  });

  it('create collection on source group should ok', function(done){
    var options = {ShardingKey: {"age": 1}, ShardingType: "hash", Partition: 4096, Group:"source"};
    _space.createCollection(collectionName, options, function (err, collection) {
      expect(err).not.to.be.ok();
      expect(collection).to.be.a(Collection);
      _collection = collection;
      done();
    });
  });

  it('create dest group should ok', function(done){
    conn.createReplicaGroup("dest", function(err, group){
      expect(err).not.to.be.ok();
      expect(group).not.to.be(null);
      dstGroup = group;
      done();
    });
  });

  it('create node for dest group should ok', function (done) {
    this.timeout(8000);
    var host = '123.56.143.17';
    var port = 22010;
    var dbpath = '/opt/sequoiadb/database/data/22010';
    dstGroup.createNode(host, port, dbpath, {}, function(err, _){
      expect(err).not.to.be.ok();
      expect(_).to.be.a(Node);
      done();
    });
  });

  it('wait for 10s', function(done) {
    this.timeout(11000);
    setTimeout(function () {
      done();
    }, 10000);
  });

  it('activate dest group should ok', function (done) {
    this.timeout(15000);
    conn.activateReplicaGroup('dest', function (err, _) {
      expect(err).not.to.be.ok();
      done();
    });
  });

  var taskID;
  it('get task id should ok', function (done) {
    this.timeout(8000);
    var splitCondition = {age: 10};
    var splitEndCondition = {age: 30};
    _collection.splitAsync('source', 'dest', splitCondition, splitEndCondition, function (err, id) {
      expect(err).not.to.be.ok();
      taskID = id;
      done();
    });
  });

  xit('waitTasks should ok', function (done) {
    this.timeout(8000);
    var taskIds = [taskID];
    conn.waitTasks(taskIds, {}, {}, {}, function (err) {
      expect(err).to.not.be.ok();
      done();
    });
  });

  it('cancelTask should ok', function (done) {
    this.timeout(10000);
    conn.cancelTask(taskID, true, function (err) {
      expect(err).to.not.be.ok();
      done();
    });
  });

  it('wait for 10s', function(done) {
    this.timeout(11000);
    setTimeout(function () {
      done();
    }, 10000);
  });

  it('drop collection space should ok', function(done){
    conn.dropCollectionSpace(spaceName, function(err){
      expect(err).not.to.be.ok();
      done();
    });
  });

  it('remove source group should ok', function(done){
    this.timeout(10000);
    conn.removeReplicaGroup('source', function(err, _){
      expect(err).not.to.be.ok();
      done();
    });
  });

  it('remove dest group should ok', function(done){
    this.timeout(10000);
    conn.removeReplicaGroup('dest', function(err, _){
      expect(err).not.to.be.ok();
      done();
    });
  });
});
