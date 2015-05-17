var util = require('util');

var mongo = require('mongodb');
var redis = require('redis');
var _ = require('lodash');

var EJSON = require('../lib/extended-json');
var mordisca = require('../lib/mordisca');
var flatten = require('flat');
var unflatten = require('flat')
  .unflatten;

var doc = {
  lastSeen: new Date(),
  displayName: undefined,
  test: 101.131310000000001,
  b: mongo.ObjectID(),
  nested: {
    test: 101.131310000000001,
    b: mongo.ObjectID(),
    another: 10.24e-2,
    time: new Date(),
    arrayTest: [1231, 13e-2, {
      a: new Date(),
      b: mongo.ObjectID()
    }],
    nestedAgain: {
      _id: mongo.ObjectID()
    }
  }
};

var mrClient = new mordisca.Client()
  .connect();
var mr;
mrClient.once('connect', function(mordisca) {
  mr = mordisca;
  mr.collection('mordisca', function(err, mrCol) {
    if(err) return console.log(err);

    mrCol.createIndex('nested', function(err, reply){
      mrCol.insert([doc, doc, doc],
      function(err, replies) {
        if(err) console.log(err.stack);
        mrCol.cache.expire(mrCol.redisPrefix+'5558ff1e74893c194a1b17bf', 2);
        setTimeout(function(){
          console.log(mrCol)
        }, 3000)
      });
    });
  });
});

mrClient.on('error', function(err) {
  console.log('err', err);
});
