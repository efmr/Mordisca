var util = require('util');
var events = require('events');

var _ = require('lodash');
var mongo = require('mongodb');
var redis = require('redis');
var CacheLayer = require('./cachelayer');
var dRedisOpts = {
  'overwriteConfig': false,
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
  var self = this;
  events.EventEmitter.call(self);
}

util.inherits(Client, events.EventEmitter);

Client.prototype.createCacheClient = function(cb) {
  var self = this;
  var newRedisClient = redis.createClient.apply(null, self.redisSettings);
  var called;
  newRedisClient.once('error', function(err) {
    if (!called) {
      called = true;
      if (typeof cb === 'function') {
        cb(err);
      }
    } else if (called && err) {
      throw err;
    }
  });
  newRedisClient.once('connect', function() {
    if (!called && typeof cb === 'function') {
      called = true;
      cb(null, newRedisClient);
    }
  });
  return self;
};

Client.prototype.redisConfig = function() {
  var self = this;
  self.redisSettings = [];

  if (arguments.length > 3) {
    throw new Error('invalid number of arguments');
  }
  _.forEach(arguments, function(arg) {
    if (typeof arg === 'object') {
      self.redisOptions = _.defaults(arg, dRedisOpts);
      self.redisSettings.push(self.redisOptions);
    } else {
      self.redisSettings.push(arg);
    }
  });
  if (!self.redisOptions) {
    self.redisOptions = dRedisOpts;
    self.redisSettings.push(dRedisOpts);
  }
  return self;
};

Client.prototype.mongoConfig = function(url, options) {
  var self = this;
  self.mongoSettings = [url || 'mongodb://localhost',
    options ? _.defaults(options, dMongoOpts) : dMongoOpts
  ];
  return self;
};

Client.prototype.connect = function(hallow) {
  var self = this;
  self.redisClient = redis.createClient.apply(null, self.redisSettings);
  self.redisClient.on('error', function(err) {
    self.emit('error', err);
  });
  self.redisClient.once('connect', function() {
    var m = self.redisClient.multi();
    if (self.redisOptions && self.redisOptions.overwriteConfig) {
      _.forEach(self.redisOptions, function(val, key) {
        if (/^config_\w+/.test(key)) {
          m.config('set', key.replace('config_', ''), val);
        }
      });
    }
    m.exec(function(err) {
      if (err) {
        return self.emit('error', err);
      }

      if (hallow) {
        self.db = {
          s: {
            databaseName: hallow
          }
        };
        self.emit('connect',
          new CacheLayer(self, self.db, self.redisClient));
      } else {
        mongo.MongoClient.connect.apply(null, [self.mongoSettings[0],
          self.mongoSettings[1],
          function(err, db) {
            if (err) {
              self.emit('error', err);
            } else {
              self.db = db;
              self.emit('connect',
                new CacheLayer(self, self.db, self.redisClient)
              );
            }
          }
        ]);
      }
    });
  });
  return self;
};

module.exports = Client;
