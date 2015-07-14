var expect = require('expect.js');
var common = require('./common');
var Collection = require('../lib/collection');
var CollectionSpace = require('../lib/collection_space');

describe('Collection Lob', function () {
  var conn = common.createConnection();
  var collection;

  var spaceName = 'foo7';
  var collectionName = "bar8";

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

  it('getLobs should ok', function (done) {
    collection.getLobs(function (err, cursor) {
      expect(err).not.to.be.ok();
      expect(cursor).to.be.ok();
      cursor.current(function (err, item) {
        expect(err).not.to.be.ok();
        expect(item).to.be(null);
        done();
      });
    });
  });

  var lob;

  it('createLob should ok', function (done) {
    lob = collection.createLob(function (err) {
      expect(err).not.to.be.ok();
      done();
    });
  });


});
