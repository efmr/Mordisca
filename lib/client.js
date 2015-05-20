var util = require('util');
var events = require('events');

var _ = require('lodash');
var mongo = require('mongodb');
var redis = require('redis');
var CacheLayer = require('./cachelayer');
var dRedisOpts = {
  'overwriteConfig': true,
  //notify me about everything
  'config_notify-keyspace-events': 'AKE',
  //eviction sampling
  'config_maxmemory-samples': 5,
  //no max memory
  'config_maxmemory': 0,
  //evict only volatile keys that are least recently used
  'config_maxmemory-policy': 'volatile-lru',
  //disable aof
  'config_appendonly': 'no',
  //disable RDB
  'config_save': '',
  //disable background save error block
  'config_stop-writes-on-bgsave-error': 'no',
  //no activerehashing (faster redis, more memory wasted)
  'config_activerehashing': 'no',
  //responsiveness of redis to bg tasks (higher cpu usage, even idle)
  'config_hz': 100
};
var dMongoOpts = {

};
//TODO: configs mongo and redis (client options)
//TODO: check mongo journaling (disable)
//TODO: 2015-04-23T08:35:59.516+0000 I CONTROL  [initandlisten]
/*
CONTROL  [initandlisten] ** WARNING: /sys/kernel/mm/tr
ansparent_hugepage/enabled is 'always'.
We suggest setting it to 'never'
CONTROL  [initandlisten] ** WARNING: /sys/kernel/mm/tr
ansparent_hugepage/defrag is 'always'.
We suggest setting
it to 'never'
*/
function Client() {
  var self = this,
    redisClient,
    redisListener,
    redisSaver,
    redisArg0,
    redisArg1,
    redisArg2,
    mongoUrl = 'mongodb://localhost',
    mongoOptions;

  self.redisConfig = function(arg0, arg1, arg2) {
    redisArg0 = typeof arg0 === 'object' ?
      _.defaults(arg0, dRedisOpts) : arg0;
    redisArg1 = typeof arg1 === 'object' ?
      _.defaults(arg1, dRedisOpts) : arg1;
    redisArg2 = typeof arg2 === 'object' ?
      _.defaults(arg2, dRedisOpts) : arg2;

    return self;
  };

  self.mongoConfig = function(url, options) {
    mongoUrl = url;
    mongoOptions = _.defaults(options, dMongoOpts);
    return self;
  };

  self.connect = function() {
    var mongoSettings = [mongoUrl, mongoOptions];
    var redisSettings = [];

    if(redisArg0) {
      redisSettings.push(redisArg0);
    }
    if(redisArg1) {
      redisSettings.push(redisArg1);
    }
    if(redisArg2) {
      redisSettings.push(redisArg2);
    }
    //TODO: opts for each client?
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
      var m = redisClient.multi(),
        redisOpts =
        typeof redisArg0 === 'object' ? redisArg0 :
        typeof redisArg1 === 'object' ? redisArg1 :
        typeof redisArg2 === 'object' ? redisArg2 : dRedisOpts;

      if(redisOpts.overwriteConfig){
        _.forEach(redisOpts, function(val, key) {
          if (/^config_\w+/.test(key)) {
            var setting = key.replace('config_', '');
            m.config('set', setting, val);
          }
        });
      }
      m.exec(function(err) {
        if (err) {
          return self.emit('error', err);
        }
        mongo.MongoClient.connect.apply(null, [mongoSettings[0],
          mongoSettings[1],
          function(err, db) {
            if (err) {
              self.emit('error', err);
            } else {
              //TODO: recovery mode (optional) also flushall (optional)
              self.emit('connect', new CacheLayer(self, db,
                mongoSettings,
                redisClient, redisSettings, redisListener,
                redisSaver));
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
