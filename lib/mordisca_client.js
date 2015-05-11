var util = require("util");
var events = require('events');

var async = require('async');
var mongo = require('mongodb');
var redis = require('redis');

function MordiscaClient() {
  var _this = this;
  var _redisArg0, _redisArg1, _redisArg2, _mongoUrl, _mongoOptions;

  _this.redisClient;
  _this.database;

  _this.redisConfig = function redisConfig(arg0, arg1, arg2) {
    _redisArg0 = arg0;
    _redisArg1 = arg1;
    _redisArg2 = arg2;
  }

  _this.mongoConfig = function mongoConfig(url, options) {
    _this.mongoUrl = url;
    _this.mongoOptions = options;
  }

  events.EventEmitter.call(_this);
}

util.inherits(MordiscaClient, events.EventEmitter);

MordiscaClient.prototype.redisConfig = function redisConfig(arg0, arg1, arg2) {
  this.redisArg0 = arg0;
  this.redisArg1 = arg1;
  this.redisArg2 = arg2;
}
MordiscaClient.prototype.mongoConfig = function mongoConfig(url, options) {
  this.mongoUrl = url;
  this.mongoOptions = options;
}

MordiscaClient.prototype.connect = function connect(options) {
  var _this = this;
  _this.redisClient = redis.createClient(redisArg0, redisArg1, redisArg2);
  async.parallel({
      redis: async.apply(_this.redisClient.on, 'connect'),
      mongo: async.apply(mongo.connect(mongoUrl, mongoOptions)),
    },
    function(err, results) {
      _this.database = results[mongo];
      _this.emit('connect', results[mongo]);
    }
  );
}

module.exports = MordiscaClient;
