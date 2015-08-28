var Structure = require('./structure').Structure;

var _ = require('lodash');
var u = require('./utils');

function Transaction(cacheClient, cachePrefix, cmds, collection) {
  var self = this;
  self.collection = collection;
  self.cacheClient = cacheClient;
  self.cachePrefix = cachePrefix;
  self.cmds = cmds || [];
}

Transaction.prototype.build = function(docs) {
  var self = this;
  var objs = Array.isArray(docs) ? docs : [docs];
  return new Structure(self, objs);
};

Transaction.prototype.get = function(mrid, p, options, callback) {
  var self = this,
    info = mrid.split(':');
  callback = callback || (typeof options === 'function' ? options : undefined);
  var opts = callback !== options && options ? options : undefined;
  u.getCmd(self.cmds, info[2], info[3], mrid, p, opts, callback);
  return self;
};

Transaction.prototype.exec = function(callback) {
  var self = this;
  self.cacheClient.multi(self.cmds).exec(callback);
  self.cmds = [];
  return self;
};

Transaction.prototype.retrive = function(mrids, p, options, callback) {
  var self = this;

  if (!self.collection || self.collection.dbCollection.hallow) {
    throw new Error('hallow collection: no db to retrive');
  }

  callback = callback || (typeof options === 'function' ? options :
    function() {});
  var opts = callback !== options && options ? options : undefined;

  var len = mrids.length;
  var out = new Array(mrids.length);
  var mIndex = [];
  var nfIndex = [];
  var missing = [];
  var count = 0;
  var error = [];

  _.forEach(mrids, function(mrid, i) {
    self.get(mrid, p, opts, function(err, reply) {
      count++;
      if (err) {
        error.push(err);
        if (count !== len) {
          return;
        }
      }
      if (count === len && error.length) {
        return callback(error);
      }
      if ((reply && !reply._mr) ||
        (reply && reply._mr && !_.isEmpty(reply._mr))) {
        out[i] = reply;
      } else {
        missing.push(mrid);
        mIndex.push(i);
      }

      if (count === len && missing.length) {
        self.collection.find(missing, function(err, docs) {
          if (err) {
            return callback(err, out, mIndex, mIndex);
          }
          _.forEach(docs, function(doc, i) {
            if (doc) {
              out[mIndex[i]] = doc;
            } else {
              nfIndex.push(mIndex[i]);
            }
          });
          if (!docs) {
            nfIndex = mIndex;
          }
          return callback(null, out, mIndex, nfIndex);

        });
      } else if (count === len) {
        return callback(null, out, mIndex, nfIndex);
      }
    });
  });
  return self;
};

module.exports = Transaction;
