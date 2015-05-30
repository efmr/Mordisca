var async = require('async');
var mongo = require('mongodb');
var unflatten = require('flat').unflatten;
var _ = require('lodash').runInContext();
_.mixin({
  'defaultsDeep': require('merge-defaults')
});

var u = require('./utils');

//TODO: consider dels/expires of things that are in a queue
//CHECKFOR ':' when adding things to redis keys
//TODO: do the same for _value as _prop
//

var mrStructures = {
    index: {
      get: 'hgetall',
      set: 'hset',
      event: {
        set: 'hset',
        del: 'hdel'
      },
      actions: {
        set: true,
        queue: true
      },
    },
    set: {
      get: 'smembers',
      set: 'sadd',
      event: {
        set: 'sadd',
        del: 'srem'
      }
    },
    snapshot: {
      get: 'hgetall',
      set: 'hmset',
      event: {
        set: 'hset',
        del: 'hdel'
      }
    },
    queue: {
      get: 'rpoplpush',
      set: 'lpush',
      event: {
        set: 'lpush',
        del: 'rpop'
      }
    }
  },
  dSettings = {
    forget: 604800000, //7days forget everything in collection from _mordisca
    saveCache: {
      block: true,
      recursive: true,
      batchsize: 1000,
      wait: true,
      waitTimeout: 1, //TODO: seconds?
      method: 'insert', //update, replace
      upsert: true
    },
    event: {
      evicted: {
        purge: {
          cache: true,
          mem: true,
          db: false
        }
      },
      expired: {
        purge: {
          cache: true,
          mem: true,
          db: false
        }
      }
    },
    keyspace: {

    }
    index: {
      prop: {},
      event: {
        msg: {}
        upkeep: true,
        skip: 10,
        timeout: 1000
      },
      keep: true,
      coerce: true,
      noWarning: false
    },
    set: {
      prop: {},
      event: {
        prop: {},
        keep: true,
        timeout: 1000,
        queue: {
          saveCache: true
        }
      },
      coerce: true,
      noWarning: false
    },
    snapshot: {
      prop: {
        snapshot: true
      },
      event: {
        prop: {},
        keep: true,
        skip: 10,
        timeout: 1000,
        queue: {
          saveCache: true
        }
      },
      coerce: true,
      emitWarning: true
    },
    queue: {
      prop: {},
      event: {
        prop: {
          saveCache: true
        }

      },
      keep: true,
      saveCache: true,
    },
    object: {
      object: {
        prop: {}, //similar to snapshot but doesn't update dels then sets
      }
      snapshot: {
        prop: {
          snapshot: true,
        }
      }
      queue: {
        saveCache: true
      },
      expire: 21600, // 6h
    },
  },
  dCollOpts = {
    maxListeners: 100,
  },
  dOpts = {
    atomic: true,
    async: 'series',
    invert: false
  };


var dSetSettings = {
  object: {
    object: {
      prop: {}, //similar to snapshot but doesn't update dels then sets
      value: {},
      overwrite: false
    }
    snapshot: {
      prop: {
        snapshot: true,
      }
    }
    queue: {
      saveCache: true
    },
    expire: 21600, // 6h
  }
};



//TODO: sive of _cache, size of _save, rescue lost save
//TODO: recovery from mongo (recover sets, snapshot)
//TODO: expires in mongo too consider expires of sets, indexs, snapshots
//TODO: cache stuff found in mongo
//TODO: snapshot autosave
//NOTE: mongo expiration (just use direct mongo client)

function set(self, redisMulti, mode, docs, options, callback) {
  //TODO: deal with _mordisca
  callback = callback || options;
  var objs = Array.isArray(docs) ? docs : [docs];
  var opts = callback !== options && options ?
    _.defaultsDeep(options, dOpts) : dOpts,
  var hashes = [];
  var ss = self.settings;
    cb = function(err, replies) {
      if (typeof callback === 'function') {
        callback(err, replies, hashes, docs);
      }
    };

  _.forEach(objs, function(doc) {
    var hash = doc._id ? doc._id : mongo.ObjectID();
    hash = hash.toString();
    var mr = doc._mr && mrStructures[doc._mr.split(':')[0]] ?
      doc._mr.split(':')[0] : 'object';
    hashes.push(hash);
    var redDoc = mr === 'set' || mr === 'queue' ? doc._data :
      u.reddify(doc, hash, mr);

    //look into settings of this structure (mr)
    _.forEach(ss[mr], function(setting, action) {
      //self, ss, doc, hash, mr, what, action,

      if(mrStructures[action]){
        //create structure from structure
        if(typeof setting === 'object') {
          _.forEach(setting, function(def, fromWhat){
             //define new structure from property
            if(fromWhat === 'prop'){
              _.forEach(def, function(define, prop){
                if(!define) return;
                var propValue = prop === '_id' ? hash :
                  action === 'snapshot' && prop === 'snapshot' ? prop :
                  _.get(doc, prop)

                var definition = typeof define === object ?
                  _.defaults(define, ) : _.defaults()

                }

              })


            }


          })
        }
      }


        }
          var propValue = prop === '_id' ? hash :
            prop === '_mr'


        }
        var val = prop === '_id' ? hash :
          structure === 'snapshot' && prop === 'snapshot' ? prop :
          _.get(doc, prop);
        if (typeof val !== 'string') {
          //TODO: test coercity
          if (opts[structure].coerce && val.toString) {
            val = val.toString();
          } else {
            if (opts[structure].emitWarning) {
              var warning = 'Cannot build ' + structure +
                ' from property ' + prop;
              self.client.emit('warn', new Error(warning));
            }
            return;
          }
        }
        redisMulti[methodSet[structure]](
          self.redisPrefix + structure + ':' + val,
          self.redisPrefix + hash, val
        );

      });
    });


    var indexs = _.keys(self.index);
    _.forEach(indexs, function(index) {
      if (index === '_id' || _.has(doc, index)) {
        var val = index === '_id' ? hash : _.get(doc, index);
        if (typeof val === 'string') {


        }
        //TODO: do something with non indexed docs?
      }
    });

    if (opts.snapshot) {
      redisMulti.hmset(
        self.redisPrefix + 'snapshot:' +
        (doc._snapshot || 'snapshot'),
        redDoc
      );
      redisMulti.sadd(self.redisPrefix + 'set:_snapshot', (doc._snapshot ||
        'snapshot'));
    }

    if (opts.createSets) {
      var sets = [];
      _.forEach(_.keys(doc), function(key) {
        if (/^_set:(\w+)$/.test(key) && typeof doc[key] === 'string') {
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
    if (queueSave) {
      redisMulti.lpush(self.redisPrefix + 'queue:save',
        self.redisPrefix + hash);
    }
    redisMulti.hmset(self.redisPrefix + hash,
      redDoc
    );
    redisMulti.expire(self.redisPrefix + hash, opts.cacheExpire);
  });
  redisMulti.exec(cb);
}

//TOOD: review get doc (differntfrom) get snapshot/index/set
//FIXME: probably broken find function
function get(self, redisClient, hashes, options, callback) {
  callback = callback || options;
  var hashs = Array.isArray(hashes) ? hashes : [hashes],
    opts = callback !== options ? _.defaultsDeep(options, dOpts) : dOpts,
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
    opts = callback !== options ? _.defaultsDeep(options, dOpts) : dOpts,
    method = dOpts.async,
    mObjs, doSave;

  mObjs = _.map(objs, function(doc) {
    var obj = _.cloneDeep(doc);
    obj._id = obj._id || mongo.ObjectID();
    return obj;
  });

  doSave = [
    async.apply(set, self, undefined, mObjs, false, opts),
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
    //TODO: figureout method from hash
    m.hgetall(hashes[hashes.length - 1]);
  }

  if (opts.saveWait) {
    m.brpoplpush(self.redisPrefix + ':queue:save',
      self.redisPrefix + ':dequeue:save', opts.saveWaitTimeout);
  } else {
    m.rpoplpush(self.redisPrefix + ':queue:save',
      self.redisPrefix + ':dequeue:save');
  }
  //TODO: test if savewaittimeoutis working and nonblocking
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

function dequeueSave(self, options, callback) {
  callback = callback || options;
  var opts = callback !== options ? _.defaultsDeep(options, dOpts) : dOpts,
    hashes = [],
    data = [];
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
        //TODO: figureout method from hash
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
        var m = self.cacheSaver.multi();
        _.forEach(hashes, function(hash) {
          m.lrem(self.redisPrefix + ':dequeue:save', -1,
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

function create(self, structure, prop, options, callback) {
  callback = callback || options;
  var props = Array.isArray(prop) ? prop : [prop],
    opts = callback !== options ? _.defaultsDeep(options, dOpts) : dOpts;
  _.forEach(props, function(i) {
    self.cacheListener.subscribe('__keyspace@0__:' + self.redisPrefix +
      structure + ':' + i);
    self[structure][i] = {};
    self.syncIndex(i, opts, callback);
  });
}

function sync(self, structure, prop, options, callback) {
    callback = callback || options;
    var props = Array.isArray(prop) ? prop : [prop],
      //opts = callback !== options ? _.defaultsDeep(options, dOpts) : dOpts,
      method = 'hgetall',
      m = self.cache.multi();
    //TODO: fix methodGet
    if (structure === 'set') {
      method = 'smembers';
    }
    _.forEach(props, function(i) {
      m[method](self.redisPrefix + structure + ':' + i,
        function(err, reply) {
          if (err) {
            return;
          }
          if (reply) {
            //TODO: keep?
            self[structure][i] = reply;
          }
          //TODO: whatif null? self[structure][i] = {}; logger.warn
        }
      );
      //TODO: save?
      //TODO: consider other structures and skips
      if (structure === 'set') {
        m.lpush(self.redisPrefix + ':queue:save',
          self.redisPrefix + structure + ':' + i);
      }
    });
    m.exec(function(err, replies) {
      if (typeof callback === 'function') {
        callback(err, replies);
      }
    });
  }
  //FIXME: probably broken find function
function find(self, query, options, callback) {

  callback = callback || options;
  var queries = Array.isArray(query) ? query : [query],
    opts = callback !== options ? _.defaultsDeep(options, dOpts) : dOpts,
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
          set(self, undefined, docs, false, opts);
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

function Collection(cacheLayer, col, settings) {
  var self = this,
    autosaving = false,
    syncing = {},
    skiping = {};

  self.name = col.s.name;
  self.dbName = cacheLayer.database.s.databaseName;
  self.dbCollection = col;
  self.database = cacheLayer.database;
  self.cache = cacheLayer.cache;
  //TODO: better way to handle clients and their settings
  self.cacheSaver = cacheLayer.cacheSaver;
  self.cacheListener = cacheLayer.cacheListener;
  self.cacheLayer = cacheLayer;
  self.client = cacheLayer.client;
  self.redisPrefix = self.dbName + ':' + self.name + ':';
  self.settings = settings ?
    _.defaultsDeep(unflatten(settings), dSettings) : dSettings;

  //TODO: create index _mordisca with forgetting

  _.forEach(structures, function(structure) {
    self[structure] = {};
    syncing[structure] = {};
    skiping[structure] = {};
  });

  self.cacheListener.subscribe('__keyevent@0__:evicted');
  self.cacheListener.subscribe('__keyevent@0__:expired');

  if (opts.autosave) {
    self.cacheListener.subscribe('__keyspace@0__:' + self.redisPrefix +
      ':queue:save');
    self.queue.
  }

  if (opts.autosaveBlock && opts.autosaveBlockMax) {
    self.client.setMaxListeners(opts.autosaveBlockMax);
  }

  self.cacheListener.on('message', function(ch, msg) {
    var info = ch.split(':');

    if (info[0] === '__keyevent@0__') {
      if (info[1] === 'evicted' || info[1] === 'expired') {
        var msgInfo = msg.split(':');
        if (self.redisPrefix !== msgInfo[0] + ':' + msgInfo[1]) {
          return;
        }
        var hash = msgInfo[2];
        var m = self.cache.multi();
        _.forEach(self.index, function(val, i) {
          m.hdel(self.redisPrefix + i, hash);
          if (val[hash]) {
            delete val[hash];
          }
        });
        m.exec(function(err) {
          if (err) {
            self.client.emit('error', err);
          }
        });
        return;
        //TODO: deal with expired structures
        //TODO: do something with missing hash deletes? what for non indexed
        //TODO: option listen to evicted and expired
      }
    } else if (info[0] === '__keyspace@0__') {
      if (info[3] === 'queue' && info[4] === 'save') {
        if (autosaving || !opts.autosave) {
          return;
        }

      }

    }
    //TODO: finish simplification
    //if(keyevent)
    //else keyspace
    //if queue
    //else sync

    if (ch === '__keyspace@0__:' + self.redisPrefix + ':queue:save') {

      autosaving = true;
      self.dequeueSave(opts, function cb(err, reply) {
        if (err) {
          autosaving = false;
          self.client.emit('error', err);
          self.client.emit('autosave');
          //TODO: test if autosave is emitting
          return;
        }
        if (reply !== null && opts.autosaveUntilEmpty) {
          self.dequeueSave(opts, cb);
        } else {
          autosaving = false;
          self.client.emit('autosave');
        }
      });
      //return; TODO: all structures sync
    }
    if (/__keyspace@0__:/.test(ch)) {
      var doSync = function(structure, prop) {
        if (syncing[structure + '_' + prop]) {
          return;
        }
        syncing[structure + '_' + prop] = true;
        var doSyncAfterAutosave = function() {
          var doSyncAfterTimeout = function() {
            syncing[structure + '_' + prop] = false;
            self['sync' + structure](prop, opts, function(err) {
              if (err) {
                self.client.emit('error', err);
                return;
              }
            });
          };
          if (opts.syncTimeout) {
            setTimeout(doSyncAfterTimeout, opts.syncTimeout);
          } else {
            doSyncAfterTimeout();
          }
        };
        if (autosaving && opts.autosaveBlock) {
          self.client.once('autosave', function() {
            doSyncAfterAutosave();
          });
          return;
        }
        doSyncAfterAutosave();
      };
      //FIXME: different test (generic) exclude, cache and save
      if (/_(\w+):/.test(ch)) {
        doSync(ch.split(':')[3], ch.split(':')[4]);
      }
    }
  });
}

Collection.prototype.memcache = function(docs, options, callback) {
  set(this, undefined, docs, true, options, callback);
};

Collection.prototype.find = function(query, options, callback) {
  find(this, query, options, callback);
};

Collection.prototype.insert = function(docs, options, callback) {
  insert(this, docs, options, callback);
};

Collection.prototype.create = function(structure, prop, options, callback) {
  create(this, structure, prop, options, callback);
};

Collection.prototype.sync = function(structure, prop, options, callback) {
  sync(this, structure, prop, options, callback);
};

Collection.prototype.saveCache = function(options, callback) {
  saveCache(this, options, callback);
};

module.exports = Collection;
