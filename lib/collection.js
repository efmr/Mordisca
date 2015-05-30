var util = require('util');

var Transaction = require('./transaction');
var Listener = require('./listener');
var Saver = require('./saver');

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

  Listener.call(self, self.cacheListener);
}

util.inherits(Collection, Listener);

Collection.prototype.transaction = function(cachePrefix, cmds) {
  var self = this;
  return new Transaction(self.cacheClient, cachePrefix || self.cachePrefix,
    cmds || []);
};
Collection.prototype.autosave = function(options) {
  var self = this;
  self._saver = new Saver(self, options).start();
  return self;
};
module.exports = Collection;
