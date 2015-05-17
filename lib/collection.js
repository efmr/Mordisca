var _ = require('lodash');
var async = require('async');
var mongo = require('mongodb');

var u = require('./utils');

var dOpts = {
  atomic: true,
  async: 'series',
  invert: false
};

function cache(self, docs, options, callback) {
  callback = callback || options;
  var objs = Array.isArray(docs) ? docs : [docs],
    opts = callback !== options ? _.defaults(options, dOpts) : dOpts,
    redisClient = opts.atomic ? self.cache.multi() : self.cache,
    hashes = [],
    method = '',
    cb = function(err, replies) {
      if (typeof callback === 'function') {
        callback(err, replies, hashes, docs);
      }
    };

  if (opts.async === 'series') {
    method = 'Series';
  }
  async['each' + method](objs,
    function(doc, seqCallback) {
      var hash = doc._id ? doc._id : mongo.ObjectID();
      hash = hash.toString();
      hashes.push(hash);
      var indexs = _.keys(self.index);
      _.forEach(indexs, function(index) {
        if (index === '_id' || _.has(doc, index)) {
          var obj = JSON.stringify(u.reddify(
            index !== '_id' ? {value: _.get(doc, index)} : {}, hash
          ));
          redisClient.sadd(self.redisPrefix + index, obj);
        }
      });
      redisClient.hmset(self.redisPrefix + hash,
        u.reddify(doc, hash),
        opts.atomic ? undefined : seqCallback);
      if (opts.atomic) {
        seqCallback();
      }
    }, opts.atomic ? function() {
      redisClient.exec(cb);
    } : cb
  );
}

function find(self, query, options, callback) {
  callback = callback || options;
  var hashs = Array.isArray(query) ? query : [query],
    opts = callback !== options ? _.defaults(options, dOpts) : dOpts,
    redisClient = opts.atomic ? self.cache.multi() : self.cache,
    method = '',
    cb = function(err, replies) {
      if (typeof callback === 'function') {
        callback(err, err ? undefined : u.parse(replies, opts));
      }
    };
  if (opts.async === 'series') {
    method = 'Series';
  }
  async['each' + method](hashs,
    function(hash, seqCallback) {
      hash = typeof hash !== 'string' ? hash.toString() : hash;
      if (opts.atomic) {
        redisClient.hgetall(hash);
        seqCallback();
      } else {
        redisClient.hgetall(hash, seqCallback);
      }
    }, opts.atomic ? function() {
      redisClient.exec(cb);
    } : cb
  );
}

function insert(self, docs, options, callback) {
  callback = callback || options;
  var objs = Array.isArray(docs) ? docs : [docs],
    opts = callback !== options ? _.defaults(options, dOpts) : dOpts,
    method = dOpts.async,
    mObjs, doSave;

  mObjs = _.map(objs, function(doc) {
    var obj = _.cloneDeep(doc);
    obj._id = mongo.ObjectID();
    return obj;
  });

  doSave = [
    async.apply(cache, self, mObjs, opts),
    function(cb) {
      self.dbCollection.insert(mObjs, opts, cb);
    }
  ];
  if (opts.invert) {
    doSave = [doSave[1], doSave[0]];
  }
  if (opts.async === 'parallel') {
    method = opts.async;
  }
  async[method](doSave, callback);
}

function dump(self, options, callback) {

}

function createIndex(self, index, options, callback) {
  callback = callback || options;
  var indexs = Array.isArray(index) ? index : [index];
  // opts = callback !== options ? _.defaults(options, dOpts) : dOpts;

  _.forEach(indexs, function(i) {
    self.cacheListener.subscribe('__keyspace@0__:' + self.redisPrefix + i);
    self.syncIndex(i, callback);
  });
}

function syncIndex(self, index, options, callback) {
  callback = callback || options;
  var indexs = Array.isArray(index) ? index : [index],
    redisClient = self.cache.multi();

  _.forEach(indexs, function(i) {
    redisClient.smembers(self.redisPrefix + i, function(err, reply) {
      if (!err) {
        self.index[i] = reply;
      }
    });
  });
  redisClient.exec(callback);
}

function Collection(cacheLayer, col) {
  var self = this;
  self.name = col.s.name;
  self.dbName = cacheLayer.database.s.databaseName;
  self.dbCollection = col;
  self.database = cacheLayer.database;
  self.cache = cacheLayer.cache;
  self.cacheListener = cacheLayer.cacheListener;
  self.cacheLayer = cacheLayer;
  self.client = cacheLayer.client;
  self.redisPrefix = self.dbName + ':' + self.name + ':';
  self.index = {
    _id: []
  };
  self.createIndex('_id');
  self.cacheListener.subscribe('__keyevent@0__:evicted');
  self.cacheListener.subscribe('__keyevent@0__:expired');
  self.cacheListener.on('message', function(ch, msg) {
    if (/__keyevent@0__:((\bevicted\b)|(\bexpired\b))/.test(ch)) {
      var hash = msg.split(':')[2];
      var redisClient = self.cache.multi();
      _.forEach(self.index, function(val, i) {
        redisClient.srem(self.redisPrefix + i, hash);
        _.pull(val, hash);
      });
      redisClient.exec(function(err) {
        if (err) {
          self.client.emit('error', err);
        }
      });
      return;
    }
    if (/__keyspace@0__:/.test(ch)) {
      var index = ch.split(':')[3];
      self.syncIndex(index, function(err) {
        if (err) {
          self.client.emit('error', err);
        }
      });
    }
  });
}

Collection.prototype.cache = function(docs, options, callback) {
  cache(this, docs, options, callback);
};

Collection.prototype.find = function(query, options, callback) {
  find(this, query, options, callback);
};

Collection.prototype.insert = function(docs, options, callback) {
  insert(this, docs, options, callback);
};

Collection.prototype.dump = function(options, callback) {
  dump(this, options, callback);
};

Collection.prototype.createIndex = function(index, options, callback) {
  createIndex(this, index, options, callback);
};

Collection.prototype.syncIndex = function(index, options, callback) {
  syncIndex(this, index, options, callback);
};

module.exports = Collection;
