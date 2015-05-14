var _ = require('lodash');

var Collection = require('./collection');
var dOpts = require('./utils').dOpts;
var dOpts = {};

function CacheLayer(db, redis) {
  var self = this;

  self.collections = {};
  self.database = db;
  self.cache = redis;

  self.collection = function(name, options, callback) {
    callback = callback || options;
    var opts = callback !== options ? _.defaults(options, dOpts) : dOpts;

    db.collection(name, opts, function(err, col) {
      if (err && typeof callback === 'function') {
        callback(err);
      } else {
        var mrCollection = new Collection(db, redis, col);
        callback(null, mrCollection);
        self.collections[name] = mrCollection;
      }
    });
    return self;
  };

}

module.exports = CacheLayer;
