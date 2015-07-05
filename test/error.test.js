var expect = require('expect.js');
var error = require('../lib/error');

describe('/lib/error.js', function () {
  it('should ok', function () {
    var message = error.getErrorMessage(-1);
    expect(message).to.be('IO Exception');
  });
});
