var redis = require('redis');
//var _ = require('lodash');

var u = require('./utils');

//var dOpts = {};

function snapshotFeed(redisClient, redisListener, options, callback) {
  var self = this;
  callback = callback || options;
  //var opts = callback !== options && options ?
  //  _.defaults(options, dOpts) : dOpts,
  var getting = {};

  self.redisClient = redisClient ||
    new redis.createClient(6379, '127.0.0.1');
  self.redisListener = redisListener ||
    new redis.createClient(6379, '127.0.0.1');

  self.subscribe = function(dbName, colName, ssName) {
    var key = dbName + ':' + colName + ':_snapshot:' + ssName;
    self.redisListener.subscribe('__keyspace@0__:' + key);
    return self;
  };
  self.redisListener.on('message', function(ch) {
    if (getting[ch]) {
      return;
    }
    getting[ch] = true;
    var key = ch.replace('__keyspace@0__:', '');
    self.redisClient.hgetall(key,
      function(err, doc) {
        callback(err,
          u.parse(doc),
          key.replace('_snapshot:', '')
        );
        getting[ch] = false;
      }
    );
  });
}

module.exports = snapshotFeed;
