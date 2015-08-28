var util = require('util');

var _ = require('lodash');

var Transaction = require('./transaction');
var Subscriber = require('./subscriber');
var Saver = require('./saver');

var ObjectID = require('mongodb').ObjectID;

function Collection(cacheLayer, col, listener, opts) {
  var self = this;
  self.name = col.s.name;
  self.dbName = cacheLayer.database.s.databaseName;
  self.dbCollection = col;
  self.database = cacheLayer.database;
  self.cacheLayer = cacheLayer;
  self.cacheClient = cacheLayer.cacheClient;
  self.cacheListener = listener;
  self.cachePrefix = self.dbName + ':' + self.name + ':';
  self.client = cacheLayer.client;
  self.settings = opts; //TODO: not being used

  Subscriber.call(self, self.cacheListener);
}

util.inherits(Collection, Subscriber);

Collection.prototype.transaction = function(cachePrefix, cmds) {
  var self = this;
  return new Transaction(self.cacheClient, cachePrefix || self.cachePrefix,
    cmds || [], self);
};
Collection.prototype.autosave = function(options) {
  var self = this;
  self._saver = new Saver(self, options).start();
  return self;
};

Collection.prototype.find = function(mrids, options, callback) {
  var self = this;
  if (self.dbCollection.hallow) {
    throw new Error('hallow collection: no db to find');
  }

  callback = callback || (typeof options === 'function' ? options :
    function() {});
  //var opts = callback !== options && options ? options : undefined;

  var objects = [];
  var structures = [];
  var matchIndex = {};
  var find = [];
  var query;
  var method = 'aggregate';
  var args;
  mrids = _.isArray(mrids) ? mrids : [mrids];
  var out = new Array(mrids.length);

  _.forEach(mrids, function(mrid, i) {
    if (typeof mrid !== 'string') {
      return;
    }
    var info = mrid.split(':');
    if (info[2] === 'object') {
      objects.push(new ObjectID(info[3]));
      matchIndex[info[3]] = i;
    } else {
      structures.push(mrid);
      matchIndex[mrid] = i;
    }
  });

  if (objects.length) {
    find.push({
      _mrid: {
        $in: structures
      }
    });
  }

  if (objects.length) {
    find.push({
      _id: {
        $in: objects
      }
    });
  }

  if (find.length > 1) {
    query = {
      $match: {
        $or: find
      }
    };
  } else if (structures.length) {
    query = {
      $match: find[0]
    };
  } else {
    method = 'find';
    args = find[0];
  }

  if (!args) {
    args = [query, {
      $sort: {
        _id: -1
      }
    }, {
      $group: {
        _id: {
          $ifNull: ['$_mrid', '$_id']
        },
        last: {
          $first: '$$ROOT'
        }
      }
    }];
  }

  self.dbCollection[method](args, {
    cursor: {
      batchSize: 1
    },
    allowDiskUse: true
  }).toArray(function(err, docs) {
    if (err) {
      return callback(err);
    }

    _.forEach(docs, function(doc) {
      if (doc.last._mrid) {
        out[matchIndex[doc.last._mrid]] = doc.last;
      } else if (doc.last) {
        out[matchIndex[doc.last._id.toString()]] = doc.last;
      }
    });
    return callback(null, out);
  });

};

module.exports = Collection;
