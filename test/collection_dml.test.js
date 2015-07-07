var expect = require('expect.js');
var common = require('./common');
var Collection = require('../lib/collection');
var CollectionSpace = require('../lib/collection_space');

xdescribe('Collection DML', function () {
  var conn = common.createConnection();
  var collection;

  var spaceName = 'foo3';
  var collectionName = "bar3";

  before(function (done) {
    conn.ready(function () {
      conn.createCollectionSpace(spaceName, function (err, space) {
        expect(err).not.to.be.ok();
        expect(space).to.be.a(CollectionSpace);
        expect(space.name).to.be(spaceName);
        space.createCollection(collectionName, function (err, _collection) {
          expect(err).not.to.be.ok();
          expect(_collection).to.be.a(Collection);
          collection = _collection;
          done();
        });
      });
    });
  });

  after(function (done) {
    conn.dropCollectionSpace(spaceName, function (err) {
      expect(err).not.to.be.ok();
      done();
      conn.disconnect();
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
});

// db.foo.bar.find();
// {
//   "_id": {
//     "$oid": "53a82aa2c4b970091e000000"
//   },
//   "name": "sequoiadb"
// }
// Return 1 row(s).
