var util = require('util');
var events = require('events');

var asyn = require('async');
var _ = require('lodash');
var mongo = require('mongodb');
var redis = require('redis');
var CacheLayer = require('./cachelayer');
var dRedisOpts = {

};
var dMongoOpts = {

};


function Client() {
  var self = this,
    redisClient,
    redisListener,
    redisSaver,
    redisArg0 = 6379,
    redisArg1 = '127.0.0.1',
    redisArg2,
    mongoUrl = 'mongodb://localhost',
    mongoOptions;

  self.redisConfig = function(arg0, arg1, arg2) {
    redisArg0 = arg0;
    redisArg1 = arg2 ? arg1 : _.defaults(arg1, dRedisOpts);
    redisArg2 = arg2 ? _.defaults(arg2, dRedisOpts) : arg2;
    return self;
  };

  self.mongoConfig = function(url, options) {
    mongoUrl = url;
    mongoOptions = _.defaults(options, dMongoOpts);
    return self;
  };

  self.connect = function() {
    var mongoSettings = [mongoUrl, mongoOptions];
    var redisSettings = [redisArg0, redisArg1, redisArg2];
    redisClient = redisClient ||
      redis.createClient.apply(null, redisSettings);
    redisListener = redisListener ||
      redis.createClient.apply(null, redisSettings);
    redisSaver = redisSaver ||
      redis.createClient.apply(null, redisSettings);
    redisListener.on('error', function(err) {
      self.emit('error', err);
    });
    redisClient.on('error', function(err) {
      self.emit('error', err);
    });
    redisClient.on('connect', function() {
      var m = redisClient.multi();
      m.config('set', 'notify-keyspace-events', 'KEA');
      m.config('set', 'maxmemory-samples', 5);
      m.config('set', 'maxmemory', 0);
      m.config('set', 'maxmemory-policy', 'allkeys-lru');
      m.exec(function(err) {
        if(err) {
          return self.emit('error', err);
        }
        mongo.MongoClient.connect.apply(null, [mongoSettings[0],
          mongoSettings[1],
          function(err, db) {
            if (err) {
              self.emit('error', err);
            } else {
              self.emit('connect', new CacheLayer(self, db, mongoSettings,
                redisClient, redisSettings, redisListener, redisSaver));
            }
          }
        ]);
      });
    });
    return self;
  };

  events.EventEmitter.call(self);
}

util.inherits(Client, events.EventEmitter);

module.exports = Client;
