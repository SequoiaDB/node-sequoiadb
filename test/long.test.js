var expect = require('expect.js');
var Long = require('../lib/long');

describe('/lib/long.js', function () {
  it('new Long(high, low) should ok', function () {
    var value = new Long(0, 0);
    expect(value.high).to.be(0);
    expect(value.low).to.be(0);
  });

  it('const should ok', function () {
    var zero = Long.ZERO;
    expect(zero.high).to.be(0);
    expect(zero.low).to.be(0);
    var one = Long.ONE;
    expect(one.high).to.be(0);
    expect(one.low).to.be(1);
  });

  it('toBuffer should ok', function () {
    var one = Long.ONE;
    var be = one.toBEBuffer();
    expect(be).to.eql(new Buffer([0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1]));
    var le = one.toLEBuffer();
    expect(le).to.eql(new Buffer([0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0]));
  });

  it('equals should ok', function () {
    expect(Long.equals(Long.ONE, new Long(0x0, 0x1))).to.be(true);
  });

  it('readLong from buffer should ok', function () {
    var buf = new Buffer([0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8]);
    var bvalue = Long.readLongBE(buf, 0);
    expect(Long.equals(bvalue, new Long(0x01020304, 0x05060708))).to.be(true);
    var lvalue = Long.readLongLE(buf, 0);
    expect(Long.equals(lvalue, new Long(0x08070605, 0x04030201))).to.be(true);
  });

  it('Long.fromNumber should ok', function () {
    var val = Long.fromNumber(1);
    expect(Long.equals(val, new Long(0, 1))).to.be(true);
    // TODO: for neg-number
    // var negone = Long.fromNumber(-1);
    // expect(Long.equals(negone, new Long(0xffffffff, 0xffffffff))).to.be(true);
  });
});
