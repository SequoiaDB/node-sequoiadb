'use strict';

var Connection = require('../lib/connection');

exports.createConnection = function () {
  return new Connection(12480, "1426595184.dbaas.sequoialab.net", {
    user: "",
    pass: ""
  });
};
