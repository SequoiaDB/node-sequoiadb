var expect = require('expect.js');
var common = require('./common');

describe('Connection', function () {
  var conn = common.createConnection();

  before(function (done) {
    conn.ready(done);
  });

  after(function (done) {
    conn.disconnect(done);
  });

  it('isValid should ok', function (done) {
    conn.isValid(function (err) {
      expect(err).not.to.be.ok();
      done();
    });
  });
});
