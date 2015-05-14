var util = require('util');
var events = require('events');

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
    redisClient = redisClient ||
      redis.createClient(redisArg0, redisArg1, redisArg2);
    redisClient.on('error', function(err) {
      self.emit('error', err);
    });
    redisClient.on('connect', function() {
      mongo.MongoClient.connect(mongoUrl, mongoOptions,
        function(err, db) {
          if (err) {
            self.emit('error', err);
          } else {
            self.emit('connect', new CacheLayer(db, redisClient));
          }
        }
      );
    });
    return self;
  };

  events.EventEmitter.call(self);
}

util.inherits(Client, events.EventEmitter);

module.exports = Client;
