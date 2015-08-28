var _ = require('lodash');

var dOpts = {
  skipWhileBusy: true,
  skipCount: false,
  timeout: false,
  blockOthers: false
};

function Listener(cacheListener) {
  var self = this;
  self._busy = {};
  self._skip = {};
  self._block = false;
  self._listener = cacheListener;

  self.onEvent = function() {};

  self._listener.on('message', function(ch, msg) {
    var info = ch.split(':'),
      mrid;
    if (info[0] === '__keyevent@0__') {
      var mInfo = msg.split(':');
      self.onEvent(info[1], mInfo[3], mInfo[2], msg);
    } else if (info[0] === '__keyspace@0__') {
      mrid = ch.slice(15);
      self.onEvent(msg, info[4], info[3], mrid);
    }
  });
}

Listener.prototype.addListener = function(filter, exec, execid, options) {

  var self = this,
    original = self.onEvent,
    opts = options ? _.defaults(options, dOpts) : dOpts;

  if (opts.skipCount) {
    self._skip[execid] = 0;
  }
  if (opts.blockOthers) {
    self._listener.setMaxListeners(opts.blockOthers);
  }
  self.onEvent = function(event, id, structure, mrid) {

    original.apply(this, [event, id, structure, mrid]);

    if (filter(event, id, structure, mrid)) {
      var doExec = function() {
        exec(function() {
          self._busy[execid] = false;
          if (opts.blockOthers) {
            self._listener.emit('unblock');
            self._block = false;
          }
        }, event, id, structure, mrid);
      };

      var doExecAfterUnblock = function() {
        if (opts.blockOthers) {
          self._block = true;
        }
        if (opts.timeout) {
          setTimeout(doExec, opts.timeout);
        } else {
          doExec();
        }
      };

      if (!self._busy[execid] || (self._busy[execid] && !opts.skipWhileBusy)) {
        if (opts.skipCount && self._skip < opts.skipCount) {
          self._skip++;
        } else {
          if (opts.skipCount && self._skip >= opts.skipCount) {
            self._skip = 0;
          }
          self._busy[execid] = true;
          if (self._block) {
            self._listener.once('unblock', doExecAfterUnblock);
          } else {
            doExecAfterUnblock();
          }
        }
      }
    }
  };
};
_.forEach(['subscribe', 'unsubscribe'], function(m){
  Listener.prototype[m] = function(arg) {
    var self = this,
      key;
    if (arg && typeof arg === 'string' && arg.indexOf(':')) {
      key = '__keyspace@0__:' + arg;
    } else if (arg && typeof arg === 'string') {
      key = '__keyevent@0__:' + arg;
    } else {
      return self;
    }
    self._listener[m](key);
    return self;
  };
});

module.exports = Listener;
