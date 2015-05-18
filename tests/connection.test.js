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
  b: new Date(),
  nested: {
    test: 101.131310000000001,
    b: new Date(),
    another: 10.24e-2,
    time: new Date(),
    arrayTest: [1231, 13e-2]
  }
};

var mrClient = new mordisca.Client()
  .connect();
var mr;
mrClient.once('connect', function(mordisca) {
  mr = mordisca;
  mr.collection('mordisca', function(err, mrCol) {
    if (err) return console.log(err);

    mrCol.createIndex('b', function(err, reply) {

      console.log(mrCol)
      mrCol.memcache([doc, doc, doc],
        function(err, replies) {
          if (err) console.log(err.stack);
          //mrCol.cache.expire(mrCol.redisPrefix+'5558ff1e74893c194a1b17bf', 2);
          setTimeout(function() {
            mrCol.find(['5559b935f52b8a5c09ab5f62',
              '5554a8cda21baee40a2674db',
              '5554a89b6203c8100c727bea',
              '5554a86dd045dd540ff98c4d',
              '5554a826ca94316c0dba022e',
              '5554b5e604dfc7fc10e7ec11'
            ], function(err, docs) {
              console.log('response', err, docs);
            });
          }, 3000);
        });
    });
  });
});

mrClient.on('error', function(err) {
  console.log('err', err);
});
