var mongo = require('mongodb');
var redis = require('redis');
var EJSON = require('../lib/extended-json');
var _ = require('lodash')
var mordisca = require('../lib/mordisca')
var doc = {
  _id: mongo.ObjectID(),
  last_seen_at: new Date(),
  display_name: undefined,
  nested: {
    test: 101.131310000000001,
    another: 10.24e-2,
    time: new Date(),
    function: function() { return 'a'},
    nested_again: {
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
