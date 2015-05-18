var _ = require('lodash');
var async = require('async');
var mongo = require('mongodb');

var u = require('./utils');
var EJSON = require('./extended-json');

var dOpts = {
  atomic: true,
  async: 'series',
  invert: false
};

function set(self, redisClient, docs, mode, options, callback) {
  callback = callback || options;
  var objs = Array.isArray(docs) ? docs : [docs],
    opts = callback !== options ? _.defaults(options, dOpts) : dOpts,
    hashes = [],
    method = '',
    cb = function(err, replies) {
      if (typeof callback === 'function') {
        callback(err, replies, hashes, docs);
      }
    };
  redisClient = redisClient || opts.atomic ? self.cache.multi() : self.cache;

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
          redisClient.hset(self.redisPrefix + index,
            self.redisPrefix + hash,
            index === '_id' ? '{"$oid": "' + hash + '"}' :
            EJSON.stringify(_.get(doc, index)));
        }
      });
      if (mode === 'cache') {
        redisClient.lpush(self.redisPrefix + '__cache',
          self.redisPrefix + hash);
      }
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

function get(self, redisClient, hashes, options, callback) {
  callback = callback || options;
  var hashs = Array.isArray(hashes) ? hashes : [hashes],
    opts = callback !== options ? _.defaults(options, dOpts) : dOpts,
    method = '',
    cb = function(err, replies) {
      if (typeof callback === 'function') {
        callback(err, err ? undefined : u.parse(replies, opts));
      }
    };
  redisClient = redisClient || opts.atomic ? self.cache.multi() : self.cache;
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
    async.apply(set, self, undefined, mObjs, 'set', opts),
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

function saveCache(self, options, callback) {
  callback = callback || options;
  var opts = callback !== options ? _.defaults(options, dOpts) : dOpts,
    cb = function(err, replies) {
      if (typeof callback === 'function') {
        callback(err, replies);
      }
    };
  self.cacheSaver.brpoplpush(self.redisPrefix + '__cache',
    self.redisPrefix + '__save',
    10,
    function(err, hash) {
      if (err || hash === null) {
        return cb(err, hash);
      }
      self.cacheSaver.hgetall(hash, function(err, obj) {
        if (err) {
          return cb(err);
        }
        var doc = u.parse(obj);
        self.dbCollection.insert(doc, opts, function(err) {
          if (err) {
            return cb(err);
          }
          self.cacheSaver.lrem(self.redisPrefix + '__save', -1, hash, cb);
        });
      });
    }
  );
}

function createIndex(self, index, options, callback) {
  callback = callback || options;
  var indexs = Array.isArray(index) ? index : [index],
    opts = callback !== options ? _.defaults(options, dOpts) : dOpts;

  _.forEach(indexs, function(i) {
    self.cacheListener.subscribe('__keyspace@0__:' + self.redisPrefix + i);
    self.syncIndex(i, opts, callback);
  });
}

function syncIndex(self, index, options, callback) {
  callback = callback || options;
  var indexs = Array.isArray(index) ? index : [index],
    opts = callback !== options ? _.defaults(options, dOpts) : dOpts,
    redisClient = self.cache.multi();

  _.forEach(indexs, function(i) {
    redisClient.hgetall(self.redisPrefix + i, function(err, reply) {
      if (!err) {
        var obj = {};
        _.forEach(reply, function(val, key) {
          obj[key] = u.parse(val, opts);
        });
        self.index[i] = obj;
      }
    });
  });
  redisClient.exec(function(err, replies) {
    if (typeof callback === 'function') {
      callback(err, replies);
    }
  });
}

function find(self, query, options, callback) {
  callback = callback || options;
  var queries = Array.isArray(query) ? query : [query],
    opts = callback !== options ? _.defaults(options, dOpts) : dOpts,
    hashesCached = [],
    hashes = [],
    doFind = [];
  _.forEach(queries, function(hash) {
    if (_.has(self.index, '_id.' + self.redisPrefix + hash)) {
      hashesCached.push(self.redisPrefix + hash);
    } else {
      hashes.push(mongo.ObjectID(hash));
    }
  });
  if (hashesCached.length) {
    doFind.push(async.apply(get, self, undefined, hashesCached, opts));
  }
  if (hashes.length) {
    doFind.push(function(cb) {
      self.dbCollection.find({
          _id: {
            $in: hashes
          }
        })
        .toArray(function(err, docs) {
          if (err) {
            return cb(err);
          }
          set(self, undefined, docs, 'set', opts);
          cb(err, docs);
        });
    });
  }
  async.parallel(doFind, function(err, replies){
    var response = replies;
    if(doFind.length === 2 && replies.length === 2) {
      response = replies[0].concat(replies[1]);
    }
    if(typeof callback === 'function') {
      callback(err, response);
    }
  });
}

function Collection(cacheLayer, col) {
  var self = this,
    saving = false;
  self.name = col.s.name;
  self.dbName = cacheLayer.database.s.databaseName;
  self.dbCollection = col;
  self.database = cacheLayer.database;
  self.cache = cacheLayer.cache;
  self.cacheSaver = cacheLayer.cacheSaver;
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
  self.cacheListener.subscribe('__keyspace@0__:' + self.redisPrefix + '__cache');
  self.cacheListener.on('message', function(ch, msg) {
    if (/__keyevent@0__:((\bevicted\b)|(\bexpired\b))/.test(ch)) {
      var hash = msg.split(':')[2];
      var redisClient = self.cache.multi();
      _.forEach(self.index, function(val, i) {
        redisClient.hdel(self.redisPrefix + i, hash);
        delete val[hash];
      });
      redisClient.exec(function(err) {
        if (err) {
          self.client.emit('error', err);
        }
      });
      return;
    }
    if (ch === '__keyspace@0__:' + self.redisPrefix + '__cache') {
      if (saving) {
        return;
      }
      saving = true;
      setTimeout(function() {
        self.saveCache(function cb(err, reply) {
          if (err) {
            saving = false;
            return self.client.emit('error', err);
          }
          if (reply !== null) {
            process.nextTick(function() {
              self.saveCache(cb);
            });
          } else {
            saving = false;
          }
        });
      }, 1000);
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

Collection.prototype.memcache = function(docs, options, callback) {
  set(this, undefined, docs, 'cache', options, callback);
};

Collection.prototype.find = function(query, options, callback) {
  find(this, query, options, callback);
};

Collection.prototype.insert = function(docs, options, callback) {
  insert(this, docs, options, callback);
};

Collection.prototype.createIndex = function(index, options, callback) {
  createIndex(this, index, options, callback);
};

Collection.prototype.syncIndex = function(index, options, callback) {
  syncIndex(this, index, options, callback);
};

Collection.prototype.saveCache = function(options, callback) {
  saveCache(this, options, callback);
};

module.exports = Collection;
