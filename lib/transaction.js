var Structure = require('./structure').Structure;

var u = require('./utils');

function Transaction(cacheClient, cachePrefix, cmds) {
  var self = this;
  self.cacheClient = cacheClient;
  self.cachePrefix = cachePrefix;
  self.cmds = cmds || [];
}

Transaction.prototype.build = function(docs){
  var self = this;
  var objs = Array.isArray(docs) ? docs : [docs];
  return new Structure(self, objs);
};

Transaction.prototype.get = function(mrid, p, options, callback){
  var self = this, info = mrid.split(':');
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

module.exports = Transaction;
