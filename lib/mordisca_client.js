var util = require('util');
var events = require('events');

var async = require('async');
var mongo = require('mongodb');
var redis = require('redis');

function MordiscaClient() {
  var _this = this,
    _redisArg0, _redisArg1, _redisArg2, _mongoUrl, _mongoOptions;

  _this.redisConfig = function redisConfig(arg0, arg1, arg2) {
    _redisArg0 = arg0;
    _redisArg1 = arg1;
    _redisArg2 = arg2;
  };

  _this.mongoConfig = function mongoConfig(url, options) {
    _mongoUrl = url;
    _mongoOptions = options;
  };

  _this.connect = function connect() {
    _this.redisClient = redis.createClient(_redisArg0, _redisArg1,
      _redisArg2);
    async.parallel({
        redis: async.apply(_this.redisClient.on, 'connect'),
        mongo: async.apply(mongo.connect(_mongoUrl, _mongoOptions)),
      },
      function bothConnected(err, results) {
        _this.database = results[mongo];
        _this.emit('connect', results[mongo]);
      }
    );
  };

  events.EventEmitter.call(_this);
}

util.inherits(MordiscaClient, events.EventEmitter);

module.exports = MordiscaClient;
