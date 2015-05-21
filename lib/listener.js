var redis = require('redis');
var _ = require('lodash');

var u = require('./utils');

var dOpts = {};

function Listener(redisClient, redisListener, options, callback) {
  var self = this;
  callback = callback || options;
  var opts = callback !== options && options ?
    _.defaults(options, dOpts) : dOpts,
  var getting = {};

  self.redisClient = redisClient ||
    new redis.createClient(opts);
  self.redisListener = redisListener ||
    new redis.createClient(opts);

  self.subscribe = function(dbName, colName, type, name) {
    var key;
    if(!arguments.length){
      return self;
    } else if(arguments.length === 1) {
      key = dbName +
        !colName ? '' :
        !type ? '' :':'+colName+ ':_' + type +
        !name ? '' :
        name === 'Set' ? name : ':' + name;
    } else if {
     //TODO: key = dbName + ':' + colName + ':_'+type+':' + ssName;
    }
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

module.exports = Listener;
