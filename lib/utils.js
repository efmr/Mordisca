var _ = require('lodash'),
  mongo = require('mongodb'),
  flatten = require('flat'),
  unflatten = flatten.unflatten;

var EJSON = require('extended-json');

var def = {
  //source of derived comes first: derived structures = _.drop(def)
  object: {
    get: 'hgetall',
    pget: 'hmget',
    set: 'hmset',
  },
  index: {
    get: 'hgetall',
    pget: 'hmget',
    set: 'hmset',
  },
  vindex: {
    get: 'hgetall',
    pget: 'hmget',
    set: 'hmset'
  },
  set: {
    get: 'smembers',
    pget: 'sismember',
    set: 'sadd',
  },
  snapshot: {
    get: 'hgetall',
    pget: 'hmget',
    set: 'hmset',
  },
  lastshot: {
    get: 'hgetall',
    pget: 'hmget',
    set: 'hmset',
  },
  list: {
    get: 'lrange',
    pget: 'lrange',
    set: 'rpush',
  },
  queue: {
    get: 'lrange',
    pget: 'lrange',
    set: 'lpush',
  },
};

function parse(doc) {
  return Array.isArray(doc) ? _.map(doc, function(n) {
      return parse(n);
    }) : typeof doc === 'string' ? EJSON.parse(doc) :
    EJSON.deflate(unflatten(doc));
}

function reddify(doc, id) {
  var out = Array.isArray(doc) ?
    _.map(doc, reddify) :
    flatten(EJSON.inflate(doc));
  if (!out['_id.$oid']) {
    id = id ? mongo.ObjectID(id) : mongo.ObjectID();
    out['_id.$oid'] = id.toString();
  }
  return out;
}

function stringify(doc, id) {
  var out;
  if (Array.isArray(doc)) {
    out = _.map(doc, stringify);
  } else {
    if (!doc._id || (doc._id && !(doc._id instanceof mongo.ObjectID))) {
      doc._id = id || mongo.ObjectID();
    }
    out = EJSON.stringify(doc);
  }
  return out;
}

function pubCmd(cmds, structure, id, mrid, doc, opts, callback) {
  var c, ch = '__publish__:' + _.initial(mrid.split(':')).join(':') +
    ':' + opts.event;
  c = ['publish', ch].concat(
    structure === 'set' ||
    structure === 'queue' ||
    structure === 'list' ||
    structure === 'index' ||
    structure === 'vindex' ? [EJSON.stringify(doc)] : [
      stringify(structure === 'snapshot' ||
        structure === 'lastshot' ? doc._mr :
        doc, id)
    ]
  );
  if (callback) {
    c = c.concat(callback);
  }
  cmds.push(c);
}

function setCmd(cmds, structure, id, mrid, doc, opts, callback) {
  var c;

  if (structure === 'lastshot' || (opts && opts.replace)) {
    cmds.push(['del', mrid]);
  }

  c = [def[structure].set, mrid].concat(
    structure === 'set' ||
    structure === 'queue' ||
    structure === 'list' ? doc._mr :
    structure === 'index' ||
    structure === 'vindex' ? [doc._mr] : [
      reddify(structure === 'snapshot' ||
        structure === 'lastshot' ? doc._mr :
        doc, id)
    ]
  );
  if (callback) {
    c = c.concat(callback);
  }
  cmds.push(c);

  if (opts && opts.expire) {
    cmds.push(['pexpire', mrid, opts.expire]);
  } else if (opts && opts.expireat) {
    cmds.push(['pexpireat', mrid, opts.expireat]);
  }
}

//TODO: deal with p _id.$oid or other types
function getCmd(cmds, structure, id, mrid, p, opts, callback) {
  var cb = typeof callback !== 'function' ? undefined :
    opts && opts.raw ? callback :
    function(err, replies) {
      callback(err, err ? undefined :
        (!replies || replies[0] === null) ? null :
        structure === 'object' ? parse(replies) : {
          _mrid: mrid,
          _mr: (structure === 'set' && p && replies) ? [p] : (
            structure === 'snapshot' ||
            structure === 'lastshot' ? (p ?
              parse(_.zipObject(_.isArray(p) ? p : [p], replies)) :
              parse(replies)) : (structure === 'vindex' ||
              structure === 'index' ? _.zipObject(_.isArray(p) ? p : [p],
                replies) :
              replies))
        }
      );
    },
    c;

  if (opts && opts.refresh) {
    cmds.push(['pexpire', mrid, opts.refresh]);
  }

  if (p) {
    c = [def[structure].pget, mrid]
      .concat(
        _.isArray(p) ? p :
        structure === 'queue' || structure === 'list' ? [p, p] : [p]
      );
  } else {
    c = [def[structure].get, mrid];
    if (structure === 'queue' || structure === 'list') {
      c = c.concat([0, -1]);
    }
  }

  if (cb) {
    c = c.concat(cb);
  }

  cmds.push(c);
}

var dqOpts = {
  maxBatchSize: 1000
};

function mget(cmds, mrid, p, opts, callback) {
  var info = mrid.split(':');
  getCmd(cmds, info[2], info[3], mrid, p, opts, callback);
}

//TODO: do it in lua script
function takeOrder(cacheClient, q, dq, mrids, data, opts, cb) {
  var c = [],
    len = mrids.length,
    dlen = data.length,
    stop = false;

  if (dlen) {
    mget(c, mrids[len - 1], null, null, function(err, reply) {
      if (!err) {
        if (data[0] === true) {
          data[0] = reply;
        } else {
          data.push(reply);
        }
      }
    });
  }
  if ((len && len < opts.maxBatchSize) || !len) {
    c.push(['rpoplpush', q, dq, function(err, mrid) {
      if (!err) {
        if (!_.isNull(mrid)) {
          mrids.push(mrid);
          if (!dlen) {
            data[0] = true;
          }
        } else {
          stop = true;
        }
      }
    }]);
  } else if (len) {
    stop = true;
  }
  cacheClient.multi(c).exec(function(err) {
    if (err || stop) {
      cb(err);
    } else {
      takeOrder(cacheClient, q, dq, mrids, data, opts, cb);
    }
  });
}

function dequeue(cacheClient, q, dq, mrids, data, options, callback) {
  callback = callback || (typeof options === 'function' ? options : undefined);
  var opts = callback !== options && options ?
    _.defaults(options, dqOpts) : dqOpts;
  takeOrder(cacheClient, q, dq, mrids, data, opts, function(err) {
    if (typeof callback === 'function') {
      callback(err);
    }
  });
}

function purgeDequeue(cacheClient, dq, mrids, cb) {
  var m = cacheClient.multi();
  _.forEach(mrids, function(mrid) {
    m.lrem(dq, -1, mrid);
  });
  m.exec(cb);
}


module.exports = {
  parse: parse,
  reddify: reddify,
  stringify: stringify,
  pubCmd: pubCmd,
  setCmd: setCmd,
  getCmd: getCmd,
  dequeue: dequeue,
  purgeDequeue: purgeDequeue,
  definitions: def
};
exports.done = true;
