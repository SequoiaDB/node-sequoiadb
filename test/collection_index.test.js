var expect = require('expect.js');
var common = require('./common');
var Collection = require('../lib/collection');
var CollectionSpace = require('../lib/collection_space');

describe('Collection index', function () {
  var conn = common.createConnection();
  var collection;

  var spaceName = 'foo6';
  var collectionName = "bar5";

  before(function (done) {
    this.timeout(8000);
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
