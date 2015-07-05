'use strict';

exports.padLength = function (length, multipler) {
  if (multipler === 0) {
    return length;
  }
  var mod = length % multipler;
  return mod === 0 ? length : length + multipler - mod;
};

exports.padBuffer = function (buff, multipler) {
  if (multipler === 0) {
    return buff;
  }

  var length = buff.length;
  var mod = length % multipler;
  if (mod === 0) {
    return buff;
  }

  var newLength = length + multipler - mod;
  var paded = new Buffer(newLength);
  buff.copy(paded, 0);
  paded.fill(0, buff.length, newLength);
  return paded;
};
