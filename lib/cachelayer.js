var _ = require('lodash');

var Collection = require('./collection');
var dOpts = require('./utils').dOpts;
var dOpts = {};

function CacheLayer(client, db, mongoSettings,
  redis, redisSettings, redisListener) {
  var self = this;

  self.name = db.s.databaseName;
  self.collections = {};
  self.database = db;
  self.database.settings = mongoSettings;
  self.cache = redis;
  self.cacheListener = redisListener;
  self.cache.settings = redisSettings;
  self.client = client;

  self.collection = function(name, options, callback) {
    callback = callback || options;
    var opts = callback !== options ? _.defaults(options, dOpts) : dOpts;

    db.collection(name, opts, function(err, col) {
      if (err && typeof callback === 'function') {
        callback(err);
      } else {
        var mrCollection = new Collection(self, col);
        callback(null, mrCollection);
        self.collections[name] = mrCollection;
      }
    });
    return self;
  };

}

module.exports = CacheLayer;
