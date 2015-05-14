var _ = require('lodash');
var async = require('async');
var mongo = require('mongodb');

var u = require('./utils');

var dOpts = {
  atomic: true,
  async: 'series',
  invert: false
};

function cache(redis, docs, options, callback) {
  callback = callback || options;
  var objs = Array.isArray(docs) ? docs : [docs],
    opts = callback !== options ? _.defaults(options, dOpts) : dOpts,
    redisClient = opts.atomic ? redis.multi() : redis,
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
      redisClient.hmset(hash,
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

function find(redis, query, options, callback) {
  callback = callback || options;
  var hashs = Array.isArray(query) ? query : [query],
    opts = callback !== options ? _.defaults(options, dOpts) : dOpts,
    redisClient = opts.atomic ? redis.multi() : redis,
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

function save(redis, collection, docs, options, callback) {
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
    async.apply(cache, redis, mObjs, opts),
    function(cb) {
      collection.insert(mObjs, opts, cb);
    }
  ];
  if(opts.invert) {
    doSave = [doSave[1], doSave[0]];
  }
  if(opts.async === 'parallel') {
    method = opts.async;
  }
  async[method](doSave, callback);
}

function Collection(db, redis, col) {
  var self = this;

  self.dbCollection = col;
  self.database = db;
  self.cache = redis;
  self.index = {
    _id: []
  };

  self.cache = function(docs, options, callback) {
    cache(self.cache, docs, options, callback);
    return self;
  };
  self.find = function(query, options, callback) {
    find(self.cache, query, options, callback);
    return self;
  };
  self.save = function(docs, options, callback) {
    save(self.cache, self.dbCollection, docs, options, callback);
    return self;
  };
}

module.exports = Collection;
