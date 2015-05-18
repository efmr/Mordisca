var _ = require('lodash');

var Collection = require('./collection');
var dOpts = require('./utils')
  .dOpts;
var dOpts = {};

function CacheLayer(client, db, mongoSettings,
  redis, redisSettings, redisListener, redisSaver) {
  var self = this;

  self.name = db.s.databaseName;
  self.collections = {};
  self.database = db;
  self.database.settings = mongoSettings;
  self.cache = redis;
  self.cacheListener = redisListener;
  self.cacheSaver = redisSaver;
  self.cache.settings = redisSettings;
  self.client = client;

  self.collection = function(name, options, callback) {
    callback = callback || options;
    var opts = callback !== options && options ?
      _.defaults(options, dOpts) : dOpts;

    db.collection(name, opts, function(err, col) {
      if (err) {
        if (typeof callback === 'function') {
          return callback(err);
        }
      }
      var mrCollection = new Collection(self, col, opts);
      self.collections[name] = mrCollection;
      if (typeof callback === 'function') {
        callback(null, mrCollection);
      }
    });
    return self;
  };

}

module.exports = CacheLayer;
