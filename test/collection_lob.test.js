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
var CollectionSpace = require('../lib/collection_space');
var Lob = require('../lib/lob');
var Long = require('long');

describe('Collection Lob', function () {
  var client = common.createClient();
  var collection;

  var spaceName = 'foo7';
  var collectionName = "bar_" + Math.floor(Math.random() * 10);

  before(function (done) {
    this.timeout(8000);
    client.ready(function () {
      var createCollection = function (space) {
        space.createCollection(collectionName, function (err, _collection) {
          expect(err).not.to.be.ok();
          expect(_collection).to.be.ok();
          collection = _collection;
          done();
        });
      };
      client.createCollectionSpace(spaceName, function (err, space) {
        if (err) {
          client.getCollectionSpace(spaceName, function (err, _space) {
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
    client.dropCollectionSpace(spaceName, function (err) {
      expect(err).not.to.be.ok();
      client.disconnect(done);
    });
  });

  it('getLobs should ok with empty list', function (done) {
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

  it('Lob.write should ok', function (done) {
    lob.write(new Buffer("0123456789abcdef"), function (err) {
      expect(err).not.to.be.ok();
      expect(Long.fromNumber(16).equals(lob.size)).to.be.ok();
      done();
    });
  });

  it('Lob.write(bigbuff) should ok', function (done) {
    this.timeout(25000);
    var bigsize = 1024 * 1024 + 1;
    lob.write(new Buffer(bigsize), function (err) {
      expect(err).not.to.be.ok();
      expect(Long.fromNumber(bigsize + 16).equals(lob.size)).to.be.ok();
      done();
    });
  });

  it('Lob.close should ok', function (done) {
    lob.close(function (err) {
      expect(err).not.to.be.ok();
      done();
    });
  });

  it('Lob.isClosed should ok', function () {
    expect(lob.isClosed()).to.be(true);
  });

  var currentLob;
  it("set read from master first", function(done){
    var option = {"PreferedInstance":"M"};
    client.setSessionAttr(option, function (err) {
      expect(err).not.to.be.ok();
      done();
    });
  });

  it('Lob.open should ok', function (done) {
    currentLob = collection.openLob(lob.id, function (err) {
      expect(err).not.to.be.ok();
      expect(currentLob.getID()).to.be(currentLob.id);
      expect(currentLob.getSize()).to.be(currentLob.size);
      expect(currentLob.getCreateTime()).to.be(currentLob.createTime);
      done();
    });
  });

  it('Lob.read should ok', function (done) {
    currentLob.read(16, function (err, buff) {
      expect(err).not.to.be.ok();
      expect(Long.fromNumber(16).equals(currentLob.readOffset)).to.be.ok();
      expect(buff).to.eql(new Buffer("0123456789abcdef"));
      done();
    });
  });

  it('Lob.seek should ok', function () {
    currentLob.seek(1, Lob.SDB_LOB_SEEK_SET);
    expect(Long.isLong(currentLob.readOffset)).to.be.ok();
    expect(Long.fromNumber(1).equals(currentLob.readOffset)).to.be.ok();
    currentLob.seek(1, Lob.SDB_LOB_SEEK_CUR);
    expect(Long.isLong(currentLob.readOffset)).to.be.ok();
    expect(Long.fromNumber(2).equals(currentLob.readOffset)).to.be.ok();
    currentLob.seek(1, Lob.SDB_LOB_SEEK_END);
    expect(Long.isLong(currentLob.readOffset)).to.be.ok();
    var totalSize = Long.fromNumber(1024 * 1024 + 17 - 1);
    expect(totalSize.equals(currentLob.readOffset)).to.be.ok();
  });

  it('Lob.close should ok', function (done) {
    currentLob.close(function (err) {
      expect(err).not.to.be.ok();
      done();
    });
  });

  it("set read from master first", function(done){
    var option = {"PreferedInstance":"M"};
    client.setSessionAttr(option, function (err) {
      expect(err).not.to.be.ok();
      done();
    });
  });

 it('getLobs should ok with item', function (done) {
    collection.getLobs(function (err, cursor) {
      expect(err).not.to.be.ok();
      expect(cursor).to.be.ok();
      cursor.current(function (err, item) {
        expect(err).not.to.be.ok();
        expect(item).to.be.ok();
        done();
      });
    });
  });

  it('removeLob should ok', function (done) {
    collection.removeLob(lob.id, function (err) {
      expect(err).not.to.be.ok();
      done();
    });
  });

  it('getLobs should ok with empty', function (done) {
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
});
