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
var bson = require("bson");
var serialize = require('../lib/bson');

describe('/lib/bson.js', function () {
  it('convert String should ok', function () {
    var auth = {"User": "", "Passwd": ""};
    var buff = serialize(auth, false);
    expect(buff).to.eql(new Buffer([
      0x1d, 0x00, 0x00, 0x00, // length
      0x02, // e_name string
      0x55, 0x73, 0x65, 0x72, 0x00, // User
      0x01, 0x00, 0x00, 0x00, // length
      0x00, // ("" + "\x00")
      0x02, // e_name string
      0x50, 0x61, 0x73, 0x73, 0x77, 0x64, 0x00, // Passwd
      0x01, 0x00, 0x00, 0x00, // length
      0x00, // ("" + "\x00")
      0x00 // "\x00"
    ]));

    var buff = serialize(auth, true);
    expect(buff).to.eql(new Buffer([
      0x00, 0x00, 0x00, 0x1d, // length
      0x02, // e_name string
      0x55, 0x73, 0x65, 0x72, 0x00, // User
      0x00, 0x00, 0x00, 0x01, // length
      0x00, // ("" + "\x00")
      0x02, // e_name string
      0x50, 0x61, 0x73, 0x73, 0x77, 0x64, 0x00, // Passwd
      0x00, 0x00, 0x00, 0x01, // length
      0x00, // ("" + "\x00")
      0x00 // "\x00"
    ]));
  });

  it('convert Boolean should ok', function () {
    var doc = {"ok": true};
    var buff = serialize(doc, false);
    expect(buff).to.eql(new Buffer([
      0x0a, 0x00, 0x00, 0x00, // length
      0x08, // e_name boolean
      0x6f, 0x6b, 0x00, // ok
      0x01, // true
      0x00 // "\x00"
    ]));

    var buff = serialize(doc, true);
    expect(buff).to.eql(new Buffer([
      0x00, 0x00, 0x00, 0x0a, // length
      0x08, // e_name boolean
      0x6f, 0x6b, 0x00, // ok
      0x01, // true
      0x00 // "\x00"
    ]));
  });

  it('convert Number should ok', function () {
    var doc = {"ok": 1};
    var buff = serialize(doc, false);
    expect(buff).to.eql(new Buffer([
      0x0d, 0x00, 0x00, 0x00, // length
      0x10, // e_name int32
      0x6f, 0x6b, 0x00, // ok
      0x01, 0x00, 0x00, 0x00, // (1)
      0x00 // "\x00"
    ]));

    var buff = serialize(doc, true);
    expect(buff).to.eql(new Buffer([
      0x00, 0x00, 0x00, 0x0d, // length
      0x10, // e_name int32
      0x6f, 0x6b, 0x00, // ok
      0x00, 0x00, 0x00, 0x01, // (1)
      0x00 // "\x00"
    ]));
  });

  it('convert Double should ok', function () {
    var doc = {"ok": 1.1};
    var buff = serialize(doc, false);
    expect(buff).to.eql(new Buffer([
      0x11, 0x00, 0x00, 0x00, // length
      0x01, // e_name double
      0x6f, 0x6b, 0x00, // ok
      0x9a, 0x99, 0x99, 0x99, 0x99, 0x99, 0xf1, 0x3f, // (1.1)
      0x00 // "\x00"
    ]));

    var buff = serialize(doc, true);
    expect(buff).to.eql(new Buffer([
      0x00, 0x00, 0x00, 0x11, // length
      0x01, // e_name double
      0x6f, 0x6b, 0x00, // ok
      0x3f, 0xf1, 0x99, 0x99, 0x99, 0x99, 0x99, 0x9a, // (1.1)
      0x00 // "\x00"
    ]));
  });

  it('convert Long should ok', function () {
    var Long = bson.BSONPure.Long;
    var doc = {"ok": Long.fromNumber(100)};
    var buff = serialize(doc, false);
    expect(buff).to.eql(new Buffer([
      0x11, 0x00, 0x00, 0x00, // length
      0x12, // e_name long
      0x6f, 0x6b, 0x00, // ok
      0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // (100)
      0x00 // "\x00"
    ]));

    var buff = serialize(doc, true);
    expect(buff).to.eql(new Buffer([
      0x00, 0x00, 0x00, 0x11, // length
      0x12, // e_name long
      0x6f, 0x6b, 0x00, // ok
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64, // (100)
      0x00 // "\x00"
    ]));
  });

  it('convert Regex should ok', function () {
    var doc = {"ok": /a/i};
    var buff = serialize(doc, false);
    expect(buff).to.eql(new Buffer([
      0x0d, 0x00, 0x00, 0x00, // length
      0x0b, // e_name regex
      0x6f, 0x6b, 0x00, // ok
      0x61, 0x00, // "a"
      0x69, 0x00, // "i"
      0x00 // "\x00"
    ]));

    var buff = serialize(doc, true);
    expect(buff).to.eql(new Buffer([
      0x00, 0x00, 0x00, 0x0d, // length
      0x0b, // e_name regex
      0x6f, 0x6b, 0x00, // ok
      0x61, 0x00, // "a"
      0x69, 0x00, // "i"
      0x00 // "\x00"
    ]));
  });

  it('convert Date should ok', function () {
    var day = new Date(2014, 9, 18);
    var doc = {"ok": day};
    var buff = serialize(doc, false);
    expect(buff).to.eql(new Buffer([
      0x11, 0x00, 0x00, 0x00, // length
      0x09, // e_name Date
      0x6f, 0x6b, 0x00, // ok
      0x00, 0x48, 0xd6, 0x1e, 0x49, 0x01, 0x00, 0x00, // date
      0x00 // "\x00"
    ]));

    var buff = serialize(doc, true);
    expect(buff).to.eql(new Buffer([
      0x00, 0x00, 0x00, 0x11, // length
      0x09, // e_name Date
      0x6f, 0x6b, 0x00, // ok
      0x00, 0x00, 0x01, 0x49, 0x1e, 0xd6, 0x48, 0x00, // date
      0x00 // "\x00"
    ]));
  });

  it('convert Timestamp should ok', function () {
    var Timestamp = bson.BSONPure.Timestamp;
    var doc = {"ok": Timestamp.fromNumber(100)};
    var buff = serialize(doc, false);
    expect(buff).to.eql(new Buffer([
      0x11, 0x00, 0x00, 0x00, // length
      0x11, // e_name Date
      0x6f, 0x6b, 0x00, // ok
      0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // timestamp
      0x00 // "\x00"
    ]));

    var buff = serialize(doc, true);
    expect(buff).to.eql(new Buffer([
      0x00, 0x00, 0x00, 0x11, // length
      0x11, // e_name Date
      0x6f, 0x6b, 0x00, // ok
      0x00, 0x00, 0x00, 0x64, 0x00, 0x00, 0x00, 0x00, // timestamp
      0x00 // "\x00"
    ]));
  });

  it('convert Binary should ok', function () {
    var day = new Buffer('12345678');
    var doc = {"ok": day};
    var buff = serialize(doc, false);
    expect(buff).to.eql(new Buffer([
      0x16, 0x00, 0x00, 0x00, // length
      0x05, // e_name Binary
      0x6f, 0x6b, 0x00, // ok
      0x08, 0x00, 0x00, 0x00, // len(buffer)
      0x00, // type
      0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, // buffer
      0x00 // "\x00"
    ]));

    var buff = serialize(doc, true);
    expect(buff).to.eql(new Buffer([
      0x00, 0x00, 0x00, 0x16, // length
      0x05, // e_name Binary
      0x6f, 0x6b, 0x00, // ok
      0x00, 0x00, 0x00, 0x08, // len(buffer)
      0x00, // type
      0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, // buffer
      0x00 // "\x00"
    ]));
  });

  it('convert Null should ok', function () {
    var doc = {"ok": null};
    var buff = serialize(doc, false);
    expect(buff).to.eql(new Buffer([
      0x09, 0x00, 0x00, 0x00, // length
      0x0a, // e_name Null
      0x6f, 0x6b, 0x00, // ok
      0x00 // "\x00"
    ]));

    var buff = serialize(doc, true);
    expect(buff).to.eql(new Buffer([
      0x00, 0x00, 0x00, 0x09, // length
      0x0a, // e_name Null
      0x6f, 0x6b, 0x00, // ok
      0x00 // "\x00"
    ]));
  });

  it('convert Undefined should ok', function () {
    var doc = {"ok": undefined};
    var buff = serialize(doc, false);
    expect(buff).to.eql(new Buffer([
      0x09, 0x00, 0x00, 0x00, // length
      0x0a, // e_name undefined
      0x6f, 0x6b, 0x00, // ok
      0x00 // "\x00"
    ]));

    var buff = serialize(doc, true);
    expect(buff).to.eql(new Buffer([
      0x00, 0x00, 0x00, 0x09, // length
      0x0a, // e_name undefined
      0x6f, 0x6b, 0x00, // ok
      0x00 // "\x00"
    ]));
  });

  it('convert ObjectID should ok', function () {
    var ObjectID = bson.BSONPure.ObjectID;
    var doc = {"ok": ObjectID.createFromHexString('0123456789abcdef01234567')};
    var buff = serialize(doc, false);
    expect(buff).to.eql(new Buffer([
      0x15, 0x00, 0x00, 0x00, // length
      0x07, // e_name ObjectID
      0x6f, 0x6b, 0x00, // ok
      0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, // buffer
      0x01, 0x23, 0x45, 0x67, // buffer
      0x00 // "\x00"
    ]));

    var buff = serialize(doc, true);
    expect(buff).to.eql(new Buffer([
      0x00, 0x00, 0x00, 0x15, // length
      0x07, // e_name ObjectID
      0x6f, 0x6b, 0x00, // ok
      0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, // buffer
      0x01, 0x23, 0x45, 0x67, // buffer
      0x00 // "\x00"
    ]));
  });

  it('convert Array should ok', function () {
    var doc = {"ok": ["a", "b"]};
    var buff = serialize(doc, false);
    expect(buff).to.eql(new Buffer([
      0x20, 0x00, 0x00, 0x00, // length
      0x04, // e_name Array
      0x6f, 0x6b, 0x00, // ok
        0x17, 0x00, 0x00, 0x00, // length
        0x02, // string
        0x30, 0x00, // 0
        0x02, 0x00, 0x00, 0x00, // len('a')
        0x61, 0x00,
        0x02, // string
        0x31, 0x00, // 1
        0x02, 0x00, 0x00, 0x00, // len('b')
        0x62, 0x00,
        0x00, // 0x00
      0x00 // "\x00"
    ]));

    var buff = serialize(doc, true);
    expect(buff).to.eql(new Buffer([
      0x00, 0x00, 0x00, 0x20, // length
      0x04, // e_name Array
      0x6f, 0x6b, 0x00, // ok
        0x00, 0x00, 0x00, 0x17, // length
        0x02, // string
        0x30, 0x00, // 0
        0x00, 0x00, 0x00, 0x02, // len('a')
        0x61, 0x00,
        0x02, // string
        0x31, 0x00, // 1
        0x00, 0x00, 0x00, 0x02, // len('b')
        0x62, 0x00,
        0x00, // 0x00
      0x00 // "\x00"
    ]));
  });

  it('convert Object should ok', function () {
    var doc = {"ok": {"a": "b"}};
    var buff = serialize(doc, false);
    expect(buff).to.eql(new Buffer([
      0x17, 0x00, 0x00, 0x00, // length
      0x03, // e_name Array
      0x6f, 0x6b, 0x00, // ok
        0x0e, 0x00, 0x00, 0x00, // length
        0x02, // string
        0x61, 0x00, // "a"
        0x02, 0x00, 0x00, 0x00, // len('a')
        0x62, 0x00,
        0x00, // 0x00
      0x00 // "\x00"
    ]));

    var buff = serialize(doc, true);
    expect(buff).to.eql(new Buffer([
      0x00, 0x00, 0x00, 0x17, // length
      0x03, // e_name Array
      0x6f, 0x6b, 0x00, // ok
        0x00, 0x00, 0x00, 0x0e, // length
        0x02, // string
        0x61, 0x00, // "a"
        0x00, 0x00, 0x00, 0x02, // len('a')
        0x62, 0x00,
        0x00, // 0x00
      0x00 // "\x00"
    ]));
  });

  it('convert Object(in Object) should ok', function () {
    var doc = { '$set': { age: 25 } };
    var buff = serialize(doc, false);
    expect(buff).to.eql(new Buffer([
      0x19, 0x00, 0x00, 0x00, // length
      0x03, // e_name Document
      0x24, 0x73, 0x65, 0x74, 0x00, // $set
        0x0e, 0x00, 0x00, 0x00, // length
        0x10, // int32
        0x61, 0x67, 0x65, 0x00, // age
        0x19, 0x00, 0x00, 0x00, // 25
        0x00, // 0x00
      0x00 // "\x00"
    ]));

    var buff = serialize(doc, true);
    expect(buff).to.eql(new Buffer([
      0x00, 0x00, 0x00, 0x19, // length
      0x03, // e_name Document
      0x24, 0x73, 0x65, 0x74, 0x00, // $set
        0x00, 0x00, 0x00, 0x0e, // length
        0x10, // int32
        0x61, 0x67, 0x65, 0x00, // age
        0x00, 0x00, 0x00, 0x19, // 25
        0x00, // 0x00
      0x00 // "\x00"
    ]));
  });

  xit('convert Ref should ok', function () {
    var DBRef = bson.BSONPure.DBRef;
    var ObjectID = bson.BSONPure.ObjectID;
    var oid = ObjectID.createFromHexString('0123456789abcdef01234567');
    var doc = {"ok": new DBRef('namespace', oid)};
    var buff = serialize(doc, false);
    expect(buff).to.eql(new Buffer([
      0x33, 0x00, 0x00, 0x00, // length
      0x03, // e_name Ref
      0x6f, 0x6b, 0x00, // ok
      0x2a, 0x00, 0x00, 0x00, // length
      0x02, // string
      0x61, 0x01, 0x02, 0x03, 0x04, 0x06, 0x61, 0x01, 0x02, 0x00, // "a"
      0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, // buffer
      0x01, 0x23, 0x45, 0x67, // buffer
      0x00, // 0x00
      0x00 // "\x00"
    ]));

    var buff = serialize(doc, true);
    expect(buff).to.eql(new Buffer([
      0x00, 0x00, 0x00, 0x17, // length
      0x03, // e_name Array
      0x6f, 0x6b, 0x00, // ok
        0x00, 0x00, 0x00, 0x0e, // length
        0x02, // string
        0x61, 0x00, // "a"
        0x00, 0x00, 0x00, 0x02, // len('a')
        0x62, 0x00,
        0x00, // 0x00
      0x00 // "\x00"
    ]));
  });
});
