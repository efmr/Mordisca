var _ = require('lodash');
var u = require('./utils');

var dOpts = {
  batchSize: 1000,
  untilEmpty: true,
  timeout: 1000
};

function Saver(col, options) {
  var self = this,
    opts = options ? _.defaults(options, dOpts) : dOpts,
    mrids = [],
    _mrids = [],
    data = [],
    docs = [],
    added = false,
    active = false,
    toActive = false,
    offset = 0,
    client = col.cacheClient,
    dq = col.cachePrefix + 'queue:save',
    q = dq + 'Cache',
    batchSize = opts.batchSize,
    untilEmpty = opts.untilEmpty,
    timeout = opts.timeout;

  var save = function(next, event) {
    var toMode = typeof event === 'number';
    if (active) {
      return next();
    } else {
      active = true;
    }
    var maxBatchSize = toMode ? event : batchSize;
    offset = mrids.length - 1;
    offset = offset < 0 ? 0 : offset;
    _mrids = _.clone(mrids);
    u.dequeue(client, q, dq, mrids, data, {
      maxBatchSize: maxBatchSize
    }, function cb(err) {
      if (err) {
        col.client.emit('error', err);
        mrids = [];
        data = [];
        docs = [];
        active = false;
        return next();
      }
      var empty = [],
        full = [];
      _.forEach(data, function(doc, i) {
        if (doc) {
          full.push(mrids[offset + i]);
          docs.push(doc);
        } else {
          empty.push(mrids[offset + i]);
        }
      });
      var len = docs.length;
      data = [];
      if (len < maxBatchSize) {
        var doTimeout;
        //TODO: review timeout (not activating)
        if (len && (!toActive || toActive && toMode)) {
          doTimeout = function() {
            toActive = true;
            setTimeout(function() {
              save(function(again) {
                if (!again) {
                  toActive = false;
                }
              }, len);
            }, timeout);
          };
        }
        var doNext;
        if (len && toActive && toMode) {
          doNext = function() {
            if (doTimeout) {
              doTimeout();
            }
            active = false;
            next(true);
          };
        } else {
          doNext = function() {
            if (doTimeout) {
              doTimeout();
            }
            active = false;
            next();
          };
        }
        if (empty.length) {
          mrids = _mrids.concat(full);
          u.purgeDequeue(client, dq, empty, function(err) {
            if (err) {
              col.client.emit('error', err);
            }
            doNext();
          });
        } else {
          doNext();
        }
      } else if (len) {
        var bulk = col.dbCollection.initializeUnorderedBulkOp({
          useLegacyOps: true
        });
        _.forEach(docs, function(doc) {
          if (doc._mr) {
            bulk.find({
              _id: doc._id
            }).upsert().updateOne(doc);
          } else {
            bulk.insert(doc);
          }
        });
        bulk.execute(function(err, r) {
          if (err) {
            col.client.emit('error', err.toJSON());
            active = false;
            return next();
          }
          var doPurge;
          if (len === (r.nInserted + r.nUpserted + r.nMatched)) {
            doPurge = function(cb) {
              u.purgeDequeue(client, dq, mrids,
                function(err) {
                  if (err) {
                    col.client.emit('error', err);
                  }
                  cb();
                }
              );
            };
          } else {
            doPurge = function(cb) {
              col.client.emit('error', new Error(
                'save missmatch acknowledgment ' +
                q));
              cb();
            };
          }
          doPurge(function() {
            mrids = [];
            docs = [];
            if (len >= maxBatchSize && untilEmpty) {
              active = false;
              save(next);
            } else {
              active = false;
              next();
            }
          });
        });
      } else {
        mrids = [];
        docs = [];
        active = false;
        next();
      }
    });
  };
  self.start = function() {
    save(function() {
      col.subscribe(q);
      if (!added) {
        added = true;
        col.addListener(
          function filter(event, id, structure, mrid) {
            return (event === 'lpush' && mrid === q);
          }, save, q
        );
      }
    });
    return self;
  };
  self.stop = function() {
    col.unsubscribe(q);
    return self;
  };
}

module.exports = Saver;
