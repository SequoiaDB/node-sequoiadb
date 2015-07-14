var expect = require('expect.js');
var Query = require('../lib/query');

describe('/lib/query.js', function () {
  it('should ok', function () {
    var query = new Query();
    expect(query.Flag).to.be(0);
  });
});
