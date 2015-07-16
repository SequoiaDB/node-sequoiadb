var expect = require('expect.js');
var common = require('./common');

describe('Collection DML', function () {
  var conn = common.createConnection();

  before(function (done) {
    this.timeout(8000);
    conn.ready(done);
  });

  after(function (done) {
    conn.disconnect(done);
  });

  it('getReplicaGroups should ok', function (done) {
    conn.getReplicaGroups(function (err, cursor) {
      expect(err).not.to.be.ok();
      expect(cursor).to.be.ok();
      cursor.current(function (err, item) {
        expect(err).not.to.be.ok();
        expect(item.Group.length).to.be(1);
        expect(item.GroupID).to.be(1);
        expect(item.GroupName).to.be('SYSCatalogGroup');
        done();
      });
    });
  });

  it('getReplicaGroupById should ok', function (done) {
    conn.getReplicaGroupById(1, function (err, group) {
      expect(err).not.to.be.ok();
      expect(group).to.be.ok();
      expect(group.isCatalog).to.be(true);
      expect(group.groupId).to.be(1);
      expect(group.name).to.be('SYSCatalogGroup');
      done();
    });
  });

  it('getReplicaGroupByName should ok', function (done) {
    conn.getReplicaGroupByName('SYSCatalogGroup', function (err, group) {
      expect(err).not.to.be.ok();
      expect(group).to.be.ok();
      expect(group.isCatalog).to.be(true);
      expect(group.groupId).to.be(1);
      expect(group.name).to.be('SYSCatalogGroup');
      done();
    });
  });

  it('createReplicaGroup should ok', function (done) {
    conn.createReplicaGroup('group5', function (err, group) {
      expect(err).not.to.be.ok();
      expect(group).to.be.ok();
      expect(group.isCatalog).to.be(false);
      expect(group.name).to.be('group5');
      done();
    });
  });

  it('createReplicaCataGroup should ok', function (done) {
    this.timeout(8000);
    var host = '1426595184.dbaas.sequoialab.net';
    var port = 12167;
    var dbpath = '/opt/sequoiadb/database/data/11832';
    conn.createReplicaCataGroup(host, port, dbpath, null, function (err) {
      expect(err).to.be.ok();
      expect(err.message).to.be("Unable to create new catalog when there's already one exists");
      done();
    });
  });

  it('activateReplicaGroup should ok', function (done) {
    conn.activateReplicaGroup('group5', function (err, group) {
      expect(err).not.to.be.ok();
      // expect(group).to.be.ok();
      done();
    });
  });

  it('removeReplicaGroup should ok', function (done) {
    conn.removeReplicaGroup('group5', function (err, group) {
      expect(err).not.to.be.ok();
      done();
    });
  });
});