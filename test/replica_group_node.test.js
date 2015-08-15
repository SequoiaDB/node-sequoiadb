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

describe('Replica Group Node', function () {
  var conn = common.createConnection();

  var groupname = 'for_node';
  var group;
  var node;
  before(function (done) {
    this.timeout(8000);
    conn.ready(function () {
      conn.createReplicaGroup(groupname, function (err, _group) {
        expect(err).not.to.be.ok();
        group = _group;
        done();
      });
    });
  });

  after(function (done) {
    conn.removeReplicaGroup(groupname, function (err) {
      expect(err).not.to.be.ok();
      conn.disconnect(done);
    });
  });

  it('getDetail should ok', function (done) {
    group.getDetail(function (err, detail) {
      expect(err).not.to.be.ok();
      expect(detail.GroupName).to.be(groupname);
      expect(detail.Group.length).to.be(0);
      done();
    });
  });

  it('getNodeCount should ok', function (done) {
    group.getNodeCount(function (err, count) {
      expect(err).not.to.be.ok();
      expect(count).to.be(0);
      done();
    });
  });

  it('createNode should ok', function (done) {
    var host = '123.56.143.17';
    var port = 11880;
    var dbpath = '/opt/sequoiadb/database/data/11880';
    group.createNode(host, port, dbpath, {}, function (err, _node) {
      expect(err).not.to.be.ok();
      node = _node;
      expect(_node.nodename).to.be('123.56.143.17:11880');
      done();
    });
  });

  it('getNodeByName should ok', function (done) {
    var name = '123.56.143.17:11880';
    group.getNodeByName(name, function (err, node) {
      expect(err).not.to.be.ok();
      expect(node.nodename).to.be('123.56.143.17:11880');
      done();
    });
  });

  it('node.getStatus should ok', function (done) {
    node.getStatus(function (err, status) {
      expect(err).not.to.be.ok();
      expect(status).to.be(constants.NodeStatus.SDB_NODE_ACTIVE);
      done();
    });
  });

  it('node.start should ok', function (done) {
    this.timeout(8000);
    node.start(function (err, status) {
      expect(err).not.to.be.ok();
      expect(status).to.be(true);
      done();
    });
  });

  it('node.connect should ok', function (done) {
    this.timeout(8000);
    var conn = node.connect("", "");
    conn.on('error', done);
    conn.ready(function () {
      conn.disconnect(done);
    });
  });

  it('node.stop should ok', function (done) {
    this.timeout(8000);
    node.stop(function (err, status) {
      expect(err).not.to.be.ok();
      expect(status).to.be(true);
      done();
    });
  });
  it('removeNode should ok', function (done) {
    var host = '123.56.143.17';
    group.removeNode(host, 11880, {}, function (err) {
      expect(err).to.be.ok();
      done();
    });
  });

  it('getNodeCount should be 1', function (done) {
    group.getNodeCount(function (err, count) {
      expect(err).not.to.be.ok();
      expect(count).to.be(1);
      done();
    });
  });

  it('start Group should ok', function (done) {
    this.timeout(8000);
    group.start(function (err, ok) {
      expect(err).not.to.be.ok();
      expect(ok).to.be(true);
      done();
    });
  });

  it('stop Group should ok', function (done) {
    this.timeout(8000);
    group.stop(function (err, ok) {
      expect(err).not.to.be.ok();
      expect(ok).to.be(true);
      done();
    });
  });

  it('getMaster should ok', function (done) {
    group.getMaster(function (err, node) {
      expect(err).not.to.be.ok();
      expect(node).to.be.ok();
      expect(node.nodename).to.be('123.56.143.17:11880');
      expect(node.group.name).to.be('for_node');
      done();
    });
  });

  it('getSlave should ok', function (done) {
    group.getSlave(function (err, node) {
      expect(err).not.to.be.ok();
      expect(node).to.be.ok();
      expect(node.nodename).to.be('123.56.143.17:11880');
      expect(node.group.name).to.be('for_node');
      done();
    });
  });
});
