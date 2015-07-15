var expect = require('expect.js');
var common = require('./common');

describe('Replica Group Node', function () {
  var conn = common.createConnection();

  var groupname = 'for_node';
  var group;
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
    group.createNode('1426595184.dbaas.sequoialab.net', 12161, '/opt/sequoiadb/database/data/11830', {}, function (err, node) {
      expect(err).not.to.be.ok();
      expect(node.nodename).to.be('1426595184.dbaas.sequoialab.net:12161');
      done();
    });
  });

  it('getNodeByName should ok', function (done) {
    group.getNodeByName('1426595184.dbaas.sequoialab.net:12161', function (err, node) {
      expect(err).not.to.be.ok();
      expect(node.nodename).to.be('1426595184.dbaas.sequoialab.net:12161');
      done();
    });
  });

  it('removeNode should ok', function (done) {
    group.removeNode('1426595184.dbaas.sequoialab.net', 12161, {}, function (err) {
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

  it('start should ok', function (done) {
    this.timeout(8000);
    group.start(function (err, ok) {
      expect(err).not.to.be.ok();
      expect(ok).to.be(false);
      done();
    });
  });

  it('stop should ok', function (done) {
    group.stop(function (err, ok) {
      expect(err).not.to.be.ok();
      expect(ok).to.be(true);
      done();
    });
  });

  xit('getMaster should ok', function (done) {
    group.getMaster(function (err, node) {
      expect(err).not.to.be.ok();
      expect(node).to.be(null);
      done();
    });
  });

  xit('getSlave should ok', function (done) {
    group.getSlave(function (err, node) {
      expect(err).not.to.be.ok();
      expect(node).to.be(null);
      done();
    });
  });
});
