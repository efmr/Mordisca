var _ = require('lodash');
var async = require('async');

var Collection = require('./collection');

var dOpts = {
  //TODO: collection default options
};

function CacheLayer(client, db, redisClient) {
  var self = this;

  self.name = db.s.databaseName;
  self.collections = {};
  self.database = db;
  self.cacheClient = redisClient;
  self.client = client;
  //TODO: recovery mode (optional) also flushall (optional)
  self.collection = function(name, options, callback) {
    callback = callback ||
      (typeof options === 'function' ? options : undefined);
    var opts = callback !== options && options ? _.defaults(options, dOpts) :
      dOpts,
      listener, col, c = [function(cb) {
        if (db.collection) {
          db.collection(name, opts, cb);
        } else {
          cb(null, {
            s: {
              name: name
            }
          });
        }
      }];

    c.push(function(cb) {
      self.client.createCacheClient(cb);
    });

    async.parallel(c, function(err, replies) {
      if (err) {
        if (typeof callback === 'function') {
          return callback(err);
        }
      }

      col = replies[0];
      listener = replies[1];

      var doNext = function(err) {
        if (err) {
          if (typeof callback === 'function') {
            return callback(err);
          }
        }
        var mrCollection = new Collection(self, col, listener, opts);
        self.collections[name] = mrCollection;
        if (typeof callback === 'function') {
          callback(null, mrCollection);
        }
      };

      if (col.createIndex) {
        col.createIndex({
          _mr: 1
        }, {
          sparse: true,
        }, doNext);
      } else {
        doNext();
      }
    });
    return self;
  };

}

module.exports = CacheLayer;
