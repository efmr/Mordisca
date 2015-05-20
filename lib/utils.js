var _ = require('lodash'),
  mongo = require('mongodb'),
  flatten = require('flat'),
  unflatten = flatten.unflatten;

var EJSON = require('./extended-json');

function parse(doc) {
  return Array.isArray(doc) ? _.map(doc, function(n) {
    return parse(n);
  }) : EJSON.deflate(unflatten(doc));
}

function reddify(doc, hash) {
  var out = Array.isArray(doc) ?
    _.map(doc, reddify) :
    flatten(EJSON.inflate(doc));
  if (!out['_id.$oid']) {
    hash = hash || mongo.ObjectID();
    out['_id.$oid'] = hash.toString();
  }
  return out;
}

module.exports = {
  parse: parse,
  reddify: reddify
};
