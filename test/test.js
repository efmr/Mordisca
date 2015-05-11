var util = require('util');

var mongo = require('mongodb');
var redis = require('redis');
var _ = require('lodash');

var EJSON = require('../lib/extended-json');
var mordisca = require('../lib/mordisca');
var flatten = require('flat');
var unflatten = require('flat').unflatten;

var doc = {
  _id: mongo.ObjectID(),
  last_seen_at: new Date(),
  display_name: undefined,
  tes2t: 101.131310000000001,
  nested: {
    test: 101.131310000000001,
    another: 10.24e-2,
    time: new Date(),
    arrayTest: [1231, 13e-2, {
      a: new Date(),
      b: mongo.ObjectID()
    }],
    function: function() {
      return 'a';
    },
    nestedAgain: {
      _id: mongo.ObjectID()
    }
  }
};

console.log(_.keys(EJSON));
console.log(_.keys(mongo));
console.log(_.keys(redis));
console.log(_.keys(mordisca));
console.log('Doc', doc);

// > Doc { _id: 53c2ab5e4291b17b666d742a, last_seen_at: Sun Jul 13 2014 11:53:02 GMT-0400 (EDT), display_name: undefined }

console.log('JSON', JSON.stringify(doc));
// > JSON {"_id":"53c2ab5e4291b17b666d742a","last_seen_at":"2014-07-13T15:53:02.008Z"}

console.log('EJSON', EJSON.stringify(doc));
// > EJSON {"_id":{"$oid":"53c2ab5e4291b17b666d742a"},"last_seen_at":{"$date":1405266782008},"display_name":{"$undefined":true}}

console.log('inflated', EJSON.inflate(doc));

console.log('deflated', EJSON.deflate(doc));

var flattenDoc = flatten(EJSON.inflate(doc));
console.log('flat', flattenDoc);
var unflatten = unflatten(flattenDoc);
var deflated = EJSON.deflate(unflatten);
console.log('unflat', util.inspect(unflatten, {depth: null}));
console.log('unflat>deflated', util.inspect(deflated, {depth: null}));
console.log(Object.prototype.toString.call(deflated.nested.arrayTest[2].b));
console.log(Object.prototype.toString.call(12013));
