var _ = require('lodash');
var async = require('async');
var mongo = require('mongodb');

var u = require('./utils');

var dOpts = {
  atomic: true,
  async: 'series',
  invert: false,
  autosave: true,
  indexId: true,
  saveBatchsize: 1000,
  saveWait: true,
  saveWaitTimeout: 1,
  saveBlock: true,
  syncTimeout: 100,
  syncBlock: false,
  createSets: false,
  snapshot: true,
  snapshotAutosave: 0,
  // expire cached docs in 6h
  cacheExpire: 21600
};

function set(self, redisMulti, docs, mode, options, callback) {
  callback = callback || options;
  var objs = Array.isArray(docs) ? docs : [docs],
    opts = callback !== options && options ? _.defaults(options, dOpts) :
    dOpts,
    hashes = [],
    cb = function(err, replies) {
      if (typeof callback === 'function') {
        callback(err, replies, hashes, docs);
      }
    };
  redisMulti = redisMulti || self.cache.multi();

  _.forEach(objs, function(doc) {
    var hash = doc._id ? doc._id : mongo.ObjectID();
    hash = hash.toString();
    hashes.push(hash);
    var indexs = _.keys(self.index);
    _.forEach(indexs, function(index) {
      if (index === '_id' || _.has(doc, index)) {
        var val = index === '_id' ? hash : _.get(doc, index);
        if (typeof val !== 'string') {
          throw new Error('Only Index String values');
        }
        redisMulti.hset(self.redisPrefix + '_index:' + index,
          self.redisPrefix + hash, val);
        redisMulti.sadd(self.redisPrefix + '_indexSet',
          index);
      }
    });

    var redDoc = u.reddify(doc, hash);
    if (mode === 'cache') {
      redisMulti.lpush(self.redisPrefix + '_cache',
        self.redisPrefix + hash);
    }
    if (opts.snapshot) {
      redisMulti.hmset(
        self.redisPrefix + '_snapshot:' +
        (doc._snapshot || 'snapshot'),
        redDoc
      );
      redisMulti.sadd(self.redisPrefix + '_snapshotSet', (doc._snapshot ||
        'snapshot'));
    }

    if (opts.createSets) {
      var sets = [];
      _.forEach(_.keys(doc), function(key) {
        if (/^_(\w+)Set$/.test(key) && typeof doc[key] === 'string') {
          sets.push(key);
        }
      });
      if (sets.length) {
        _.forEach(sets, function(setType) {
          redisMulti.sadd(self.redisPrefix + setType,
            doc[setType]);

          var event = '__keyspace@0__:' + self.redisPrefix + setType;
          self.cacheListener.subscribe(event);
        });
      }
    }

    redisMulti.hmset(self.redisPrefix + hash,
      redDoc
    );
    redisMulti.expire(self.redisPrefix + hash, opts.cacheExpire);
  });
  redisMulti.exec(cb);
}

function get(self, redisClient, hashes, options, callback) {
  callback = callback || options;
  var hashs = Array.isArray(hashes) ? hashes : [hashes],
    opts = callback !== options ? _.defaults(options, dOpts) : dOpts,
    method = '',
    cb = function(err, replies) {
      if (typeof callback === 'function') {
        callback(err, err ? undefined : u.parse(replies));
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
        redisClient.expire(self.redisPrefix + hash, opts.cacheExpire);
        redisClient.hgetall(hash);
        seqCallback();
      } else {
        redisClient.expire(self.redisPrefix + hash, opts.cacheExpire);
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
    obj._id = obj._id || mongo.ObjectID();
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

function takeChunk(self, hashes, data, opts, cb) {
  var m = self.cacheSaver.multi();
  if (hashes.length) {
    m.hgetall(hashes[hashes.length - 1]);
  }

  if (opts.saveWait) {
    m.brpoplpush(self.redisPrefix + '_cache',
      self.redisPrefix + '_save', opts.saveWaitTimeout);
  } else {
    m.rpoplpush(self.redisPrefix + '_cache',
      self.redisPrefix + '_save', opts.saveWaitTimeout);
  }
  m.exec(function(err, replies) {
    var lastDoc, hash;
    if (hashes.length) {
      lastDoc = replies[0];
      hash = replies[1];
      data.push(lastDoc);
    } else {
      hash = replies[0];
    }
    if (err || (hash === null && !hashes.length)) {
      return cb(err, hashes, data, hash);
    }
    if (hash !== null && hashes.length < opts.saveBatchsize - 1) {
      hashes.push(hash);
      return takeChunk(self, hashes, data, opts, cb);
    }
    if (hash !== null) {
      hashes.push(hash);
    }
    cb(err, hashes, data, hash);
  });
}

function saveCache(self, options, callback) {
  callback = callback || options;
  var opts = callback !== options ? _.defaults(options, dOpts) : dOpts,
    hashes = [],
    data = [],
    m;
  takeChunk(self, hashes, data, opts, function(err, hashes, data, state) {
    if (err || (!hashes.length && state === null)) {
      if (typeof callback === 'function') {
        callback(err, state);
      }
      return;
    }
    var doNext;
    if (state !== null) {
      doNext = function(cb) {
        self.cacheSaver.hgetall(hashes[hashes.length - 1], cb);
      };
    } else {
      doNext = function(cb) {
        cb();
      };
    }
    doNext(function(err, reply) {
      if (err) {
        if (typeof callback === 'function') {
          callback(err);
        }
        return;
      }
      if (reply) {
        data.push(reply);
      }
      var docs = [];
      _.forEach(data, function(doc) {
        if (doc) {
          docs.push(u.parse(doc));
        }
        //TODO: if not doc means hash returns null, do something?
      });
      self.dbCollection.insert(docs, opts, function(err) {
        if (err) {
          if (typeof callback === 'function') {
            callback(err);
          }
          return;
        }
        //TODO: do something with insert replies
        m = self.cacheSaver.multi();
        _.forEach(hashes, function(hash) {
          m.lrem(self.redisPrefix + '_save', -1,
            hash);
        });
        m.exec(function(err) {
          if (err) {
            self.client.emit('error', err);
            return;
          }
          //TODO: do something with lrem replies
        });
        if (typeof callback === 'function') {
          return callback(null, state);
        }
      });
    });
  });
}

function createIndex(self, index, options, callback) {
  callback = callback || options;
  var indexs = Array.isArray(index) ? index : [index],
    opts = callback !== options ? _.defaults(options, dOpts) : dOpts;
  _.forEach(indexs, function(i) {
    self.cacheListener.subscribe('__keyspace@0__:' + self.redisPrefix +
      '_index:' + i);
    self.index[i] = {};
    self.syncIndex(i, opts, callback);
  });
}

function syncIndex(self, index, options, callback) {
  callback = callback || options;
  var indexs = Array.isArray(index) ? index : [index],
    //opts = callback !== options ? _.defaults(options, dOpts) : dOpts,
    m = self.cache.multi();

  _.forEach(indexs, function(i) {
    m.hgetall(self.redisPrefix + '_index:' + i,
      function(err, reply) {
        if (err) {
          self.client.emit('error', err);
          return;
        }
        if (reply) {
          self.index[i] = reply;
        } else {
          self.index[i] = {};
        }
      }
    );
  });
  m.exec(function(err, replies) {
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

  //TODO: improve queries
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
          //TODO: consider set of sets files in mongo and snapshots
          cb(err, docs);
        });
    });
  }
  async.parallel(doFind, function(err, replies) {
    var response = replies;
    if (doFind.length === 2 && replies.length === 2) {
      response = replies[0].concat(replies[1]);
    }
    if (typeof callback === 'function') {
      callback(err, response);
    }
  });
}

function syncSet(self, setType, options, callback) {
  callback = callback || options;
  //var setTypes = Array.isArray(setType) ? setType : [setType],
  //  opts = callback !== options ? _.defaults(options, dOpts) : dOpts,

  self.cache.smembers(self.redisPrefix + '_' + setType + 'Set',
    function(err, mem) {
      if (err) {
        if (typeof callback === 'function') {
          callback(err);
        }
        return;
      }
      self.set[setType] = mem;
      if (typeof callback === 'function') {
        callback(err, mem);
      }
    }
  );
}

function memcacheSet(self, members, setType, options, callback) {
  var obj = {};
  obj._set = setType;
  obj.members = members;
  obj.d = new Date()
    .getTime();
  set(self, undefined, obj, 'cache', _.defaults({
    snapshot: false
  }, options), callback);
}

function Collection(cacheLayer, col, options) {
  var self = this,
    saving = false,
    syncing = {},
    opts = options ? _.defaults(options, dOpts) : dOpts;
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
  self.index = {};
  self.set = {};

  if (opts.indexId) {
    self.createIndex('_id');
  }
  self.cacheListener.subscribe('__keyevent@0__:evicted');
  self.cacheListener.subscribe('__keyevent@0__:expired');

  if (opts.autosave) {
    self.cacheListener.subscribe('__keyspace@0__:' + self.redisPrefix +
      '_cache');
  }

  self.cacheListener.subscribe('__keyspace@0__:' + self.redisPrefix +
    '_indexSet');
  self.cacheListener.subscribe('__keyspace@0__:' + self.redisPrefix +
    '_snapshotSet');

  self.cacheListener.on('message', function(ch, msg) {
    if (/__keyevent@0__:((\bevicted\b)|(\bexpired\b))/.test(ch)) {
      var hash = msg.split(':')[3];
      var m = self.cache.multi();
      _.forEach(self.index, function(val, i) {
        m.hdel(self.redisPrefix + i, hash);
        delete val[hash];
      });
      m.exec(function(err) {
        if (err) {
          self.client.emit('error', err);
        }
      });
      return;
    }
    if (ch === '__keyspace@0__:' + self.redisPrefix + '_cache') {
      if (saving) {
        return;
      }
      saving = true;
      self.saveCache(opts, function cb(err, reply) {
        if (err) {
          saving = false;
          self.client.emit('error', err);
          return;
        }
        if (reply !== null) {
          self.saveCache(opts, cb);
        } else {
          saving = false;
        }
      });
      return;
    }
    if (/__keyspace@0__:/.test(ch)) {
      if(saving && opts.saveBlockSync) {
        return;
      }
      if (/_index:/.test(ch)) {
        var index = ch.split(':')[4];
        if (syncing['index_' + index]) {
          return;
        }
        syncing['index_' + index] = true;
        var doSyncIndex = function() {
          self.syncIndex(index, opts, function(err) {
            if (err) {
              self.client.emit('error', err);
            }
            syncing['index_' + index] = false;
          });
        };
        if (opts.syncTimeout) {
          setTimeout(doSyncIndex, opts.syncTimeout);
        } else if(opts.syncBlock) {
          doSyncIndex();
        } else {
          process.nextTick(doSyncIndex);
        }
        return;
      }
      if (/_(\w+)Set$/.test(ch)) {
        var setType = ch.slice(ch.lastIndexOf('_') + 1)
          .replace(/Set$/, '');
        if (syncing['set_' + setType]) {
          return;
        }
        syncing['set' + setType] = true;
        var doSyncSet = function() {
          self.syncSet(setType, opts, function(err, mem) {
            if (err) {
              self.client.emit('error', err);
              syncing['set_' + setType] = false;
              return;
            }
            memcacheSet(self, mem, setType, opts, function(err) {
              if (err) {
                self.client.emit('error', err);
              }
              syncing['set_' + setType] = false;
            });
          });
        };
        if (opts.syncTimeout) {
          setTimeout(doSyncSet, opts.syncTimeout);
        } else if(opts.syncBlock) {
          doSyncSet();
        } else {
          process.nextTick(doSyncSet);
        }
        return;
      }
    }
  });
}

Collection.prototype.memcache = function(docs, options, callback) {
  callback = callback || options;
  var self = this,
    opts = callback !== options && options ? _.defaults(options, dOpts) :
    dOpts;

  if (!opts.snapshot) {
    set(self, undefined, docs, 'cache', opts, callback);
    return;
  }

  var objs = Array.isArray(docs) ? docs : [docs],
    mObjs;

  mObjs = _.map(objs, function(doc) {
    var obj = _.cloneDeep(doc);
    obj._id = obj._id || mongo.ObjectID();
    obj._snapshot = obj._snapshot || 'snapshot';
    return obj;
  });

  set(self, undefined, mObjs, 'cache', opts, callback);
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

Collection.prototype.syncSet = function(setType, options, callback) {
  syncSet(this, setType, options, callback);
};

//TODO: sive of _cache, size of _save, rescue lost save
//TODO: recovery from mongo
//TODO: cache stuff found in mongo
//TODO: snapshot autosave
module.exports = Collection;
