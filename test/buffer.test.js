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
var XBuffer = require('../lib/buffer');
var Long = require('long');

describe('/lib/buffer.js', function () {
  it('new XBuffer(buff, isBigEndian) should ok', function () {
    var buff = new Buffer(10);
    var xbuff = new XBuffer(buff, true);
    expect(xbuff.length).to.be(10);
    expect(xbuff.buff).to.eql(buff);
  });

  it('new XBuffer(length, isBigEndian) should ok', function () {
    var xbuff = new XBuffer(10, true);
    expect(xbuff.length).to.be(10);
  });

  it('new XBuffer(string, isBigEndian) should ok', function () {
    var xbuff = new XBuffer('1', true);
    expect(xbuff.buff).to.eql(new Buffer('1'));
  });

  it('new XBuffer(obj, isBigEndian) should ok', function () {
    var xbuff = new XBuffer({}, true);
    expect(xbuff.buff).to.eql(new Buffer('{}'));
  });

  it('toBuffer should ok', function () {
    var buff = new Buffer(10);
    var xbuff = new XBuffer(buff, true);
    expect(xbuff.buff).to.eql(buff);
  });

  it('writeUInt32/readUInt32 should ok', function () {
    var xbuff = new XBuffer(new Buffer(10), true);
    xbuff.writeUInt32(10, 0);
    expect(xbuff.readUInt32(0)).to.be(10);
    expect(function () {
      xbuff.writeUInt32(0x00);
    }).to.throwError(/Must pass the offset/);
    expect(function () {
      xbuff.readUInt32();
    }).to.throwError(/Must pass the offset/);
  });

  it('isBigEndian should ok', function () {
    var buff = new Buffer(4);
    buff.writeUInt32LE(0xFFFEFDFC, 0);
    var lbuff = new XBuffer(buff, false);
    expect(lbuff.readUInt32(0)).to.be(0xFFFEFDFC);
    var bbuff = new XBuffer(buff, true);
    expect(bbuff.readUInt32(0)).to.be(0xFCFDFEFF);
  });

  it('writeUInt32 should ok', function () {
    var bbuff = new XBuffer(4, true);
    bbuff.writeUInt32(0xFCFDFEFF, 0);
    var lbuff = new XBuffer(4, false);
    lbuff.writeUInt32(0xFFFEFDFC, 0);
    expect(bbuff.toBuffer()).to.eql(lbuff.toBuffer());
  });

  it('writeUInt/readUInt should ok', function () {
    var bbuff = new XBuffer(4, true);
    bbuff.writeUInt(0xFCFDFEFF, 0, 4);
    expect(bbuff.readUInt(0, 4)).to.be(0xFCFDFEFF);
    var lbuff = new XBuffer(4, false);
    lbuff.writeUInt(0xFFFEFDFC, 0, 4);
    expect(lbuff.readUInt(0, 4)).to.be(0xFFFEFDFC);
    expect(bbuff.toBuffer()).to.eql(lbuff.toBuffer());
    expect(function () {
      bbuff.writeUInt(0x00);
    }).to.throwError(/Must pass the offset/);
    expect(function () {
      bbuff.readUInt();
    }).to.throwError(/Must pass the offset/);
  });

  it('writeUInt8/readUInt8 should ok', function () {
    var bbuff = new XBuffer(1, true);
    bbuff.writeUInt8(1, 0);
    expect(bbuff.readUInt8(0)).to.be(1);
    var lbuff = new XBuffer(1, true);
    lbuff.writeUInt8(1, 0);
    expect(lbuff.readUInt8(0)).to.be(1);
    expect(function () {
      bbuff.writeUInt8(0x00);
    }).to.throwError(/Must pass the offset/);
    expect(function () {
      bbuff.readUInt8();
    }).to.throwError(/Must pass the offset/);
  });

  it('slice should ok', function () {
    var buff = new XBuffer(4, true);
    var buf = buff.slice(1);
    expect(buf).to.be.a(XBuffer);
    expect(buf.length).to.be(3);
  });

  it('writeBuffer should ok', function () {
    var buff = new XBuffer(4, true);
    buff.writeBuffer(new Buffer([1, 2, 3, 4]), 0);
    expect(buff.buff).to.eql(new Buffer([1, 2, 3, 4]));
    expect(function () {
      buff.writeBuffer(new Buffer([1, 2, 3, 4]));
    }).to.throwError(/Must pass the offset/);
  });

  it('writeLong should ok', function () {
    var bbuff = new XBuffer(8, true);
    bbuff.writeLong(new Long(0x1, 0x0), 0);
    expect(bbuff.buff).to.eql(new Buffer([0, 0, 0, 0, 0, 0, 0, 1]));
    var lbuff = new XBuffer(8, false);
    lbuff.writeLong(new Long(0x1, 0x0), 0);
    expect(lbuff.buff).to.eql(new Buffer([1, 0, 0, 0, 0, 0, 0, 0]));
    expect(function () {
      lbuff.writeLong('hehe');
    }).to.throwError(/Must pass the offset/);
    expect(function () {
      lbuff.writeLong({}, 0);
    }).to.throwError(/Must pass the Long type as value/);
  });

  it('readLong should ok', function () {
    var bbuff = new XBuffer(new Buffer([0, 0, 0, 0, 0, 0, 0, 1]), true);
    var bval = bbuff.readLong(0);
    expect(bval.high).to.be(0);
    expect(bval.getLowBits()).to.be(1);
    var lbuff = new XBuffer(new Buffer([1, 0, 0, 0, 0, 0, 0, 0]), false);
    var lval = lbuff.readLong(0);
    expect(lval.high).to.be(0);
    expect(lval.low).to.be(1);
    expect(function () {
      lbuff.readLong();
    }).to.throwError(/Must pass the offset/);
  });
});
