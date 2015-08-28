var u = require('./utils');
var _ = require('lodash');
var mongo = require('mongodb');

var dOpts = {
  expire: false,
  refresh: false,
  replace: false,
  event: 'publish',
  idFrom: false,
  idEquals: false
};

var listStructures = _.keys(u.definitions);
var derivedStructures = _.drop(listStructures);

module.exports.list = listStructures;
module.exports.derived = derivedStructures;

function deriveStructure(self, structure, prop, opts, cb) {
  //self[structure+info] = [col.dbName, col.name, structure, id];

  //TODO: build from others beside object
  _.forEach(self.objectData, function(doc, i) {
    var val = prop === '_id' ? self.objectInfo[i][3] :
      prop === '_mrid' ? self.objectInfo[i].join(':') :
      structure === 'queue' ? self.objectInfo[i].join(':') :
      (structure === 'snapshot' && prop === 'snapshot') ||
      (structure === 'lastshot' && prop === 'lastshot') ? prop :
      _.get(doc, prop);

    if (val && typeof val !== 'string' &&
      typeof val.toString === 'function') {
      val = val.toString();
    } else if (val && typeof val !== 'string') {
      throw new Error('Invalid property value');
    }

    if (val) {
      var obj;
      if (structure === 'index') {
        obj = {};
        if (typeof opts.indexKey === 'string') {
          obj[_.get(doc, opts.indexKey)] = val;
        } else {
          obj[self.objectInfo[i].join(':')] = val;
        }
      } else if (structure === 'vindex') {
        obj = {};
        if (typeof opts.indexValue === 'string') {
          obj[val] = _.get(doc, opts.indexValue);
        } else {
          obj[val] = self.objectInfo[i].join(':');
        }
      }
      u.setCmd(self.cmds, structure, self.objectInfo[i][3],
        self.cachePrefix + structure + ':' +
        (structure === 'snapshot' ||
          structure === 'lastshot' ||
          structure === 'object' ? val : (
            typeof opts.idFrom === 'string' ? (opts.idFrom ===
              '_id' ?
              self.objectInfo[i][3] : _.get(doc, opts.idFrom)) : (
              typeof opts.idEquals === 'string' ? opts.idEquals : prop))),
        structure === 'set' ||
        structure === 'queue' ||
        structure === 'list' ? {
          _mr: [val]
        } : structure === 'index' || structure === 'vindex' ? {
          _mr: obj
        } : structure === 'snapshot' || structure === 'lastshot' ? {
          _mr: doc
        } : doc, opts, cb
      );
    }
  });
  if (structure === 'queue') {
    _.forEach(derivedStructures, function(struct) {
      _.forEach(self[struct + 'Info'], function(info) {
        u.setCmd(self.cmds, structure, undefined,
          self.cachePrefix + structure + ':' + prop, {
            _mr: [info.join(':')]
          }, opts, cb
        );
      });
    });
  }
}

function protoWarp(self, options, callback, buildCmd) {
  callback = callback || (typeof options === 'function' ? options : undefined);
  var cb, count, max = 0,
    replies,
    opts = callback !== options && options ?
    _.defaults(options, dOpts) : dOpts;

  if (typeof callback === 'function') {
    count = 0;

    _.forEach(listStructures, function(def, structure) {
      max = max + (self[structure + 'Data'] ? self[structure + 'Data'].length :
        0);
    });
    replies = [];

    cb = function(err, reply) {
      count++;
      if (!err) {
        replies.push(reply);
      }
      if (count === max) {
        callback(err, replies.length ? replies : undefined);
        count = 0;
        replies = [];
      }
    };
  }
  _.forEach(listStructures, function(structure) {
    _.forEach(self[structure + 'Data'], function(doc, i) {
      buildCmd(self.cmds, structure, self[structure + 'Info'][i][3],
        self[structure + 'Info'][i].join(':'), doc, opts, cb);
    });
  });
}



function Structure(transaction, objs) {
  var self = this;
  self.cmds = transaction.cmds;
  self.cachePrefix = transaction.cachePrefix;

  _.forEach(listStructures, function(structure) {
    self[structure + 'Data'] = [];
    self[structure + 'Info'] = [];
  });

  _.forEach(objs, function(doc) {
    var id, structure, info;
    if (doc._mrid) {
      //TODO: consider dbName and ColName different from col
      var mrid = doc._mrid;
      info = mrid.split(':');
      structure = info[2];
    } else {
      id = doc._id || mongo.ObjectID();
      id = id.toString();
      structure = 'object';
      var dbInfo = self.cachePrefix.split(':');
      info = [dbInfo[0], dbInfo[1], structure, id];
    }
    self[structure + 'Data'].push(doc);
    self[structure + 'Info'].push(info);
    //TODO: validation of structures?
  });
}

_.forEach(derivedStructures, function(structure) {
  Structure.prototype[structure] = function(prop, options, callback) {
    var self = this;
    callback = callback ||
      (typeof options === 'function' ? options : undefined);
    var opts = callback !== options && options ?
      _.defaults(options, dOpts) : dOpts;
    deriveStructure(self, structure, prop, opts, callback);
    return self;
  };
});

Structure.prototype.cache = function(options, callback) {
  var self = this;
  protoWarp(self, options, callback, u.setCmd);
  return self;
};

Structure.prototype.publish = function(options, callback) {
  var self = this;
  protoWarp(self, options, callback, u.pubCmd);
  return self;
};


module.exports.Structure = Structure;
