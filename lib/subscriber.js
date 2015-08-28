var _ = require('lodash');

var u = require('./utils');

var dOpts = {
  skipWhileBusy: true,
  skipCount: false,
  timeout: false,
  blockOthers: false
};

var onMessage = function(self, pattern, ch, msg) {
  var info = ch.split(':'),
    mrid;
  if (info[0] === '__keyevent@0__') {
    var mInfo = msg.split(':');
    self.onEvent(info[1], mInfo[3], mInfo[2], msg, 'keyevent', undefined,
      pattern);
  } else if (info[0] === '__keyspace@0__') {
    mrid = ch.slice(15);
    self.onEvent(msg, info[4], info[3], mrid, 'keyspace', undefined, pattern);
  } else if (info[0] === '__publish__') {
    mrid = info.slice(1, 5).join(':');
    self.onEvent(info.slice(4).join(':'), info[4],
      info[3], mrid, 'publish', u.parse(msg), pattern);
  } else {
    self.onEvent(ch, null, null, null, null, msg, pattern);
  }
  //TODO: else
};

function Subscriber(cacheListener) {
  var self = this;
  self._busy = {};
  self._skip = {};
  self._block = false;
  self._listener = cacheListener;
  self._exec = {};

  self.onEvent = function() {};

  self._messageListener = onMessage.bind(undefined, self, undefined);
  self._pmessageListener = onMessage.bind(undefined, self);
  self._listener.on('message', self._messageListener);
  self._listener.on('pmessage', self._pmessageListener);
}

Subscriber.prototype.removeListeners = function() {
  var self = this;
  self._listener.removeListener('message', self._messageListener);
  self._listener.removeListener('pmessage', self._pmessageListener);
  self._exec = {};
  return self;
};

Subscriber.prototype.stopListener = function(execid) {
  var self = this;
  if (!execid || !self._exec[execid]) {
    return self;
  } else {
    self._exec[execid] = false;
  }
  return self;
};

Subscriber.prototype.addListener = function(filter, exec, execid, options) {

  var self = this,
    original = self.onEvent,
    opts = options ? _.defaults(options, dOpts) : dOpts;

  if (self._exec[execid]) {
    return self;
  } else {
    self._exec[execid] = true;
  }
  var doExec = function(exec, event, id, structure, mrid, type, data, pattern) {
      exec(function() {
        self._busy[execid] = false;
        if (opts.blockOthers) {
          self._listener.emit('unblock');
          self._block = false;
        }
      }, event, id, structure, mrid, type, data, pattern);
    },
    doExecAfterUnblock = function(exec, event, id, structure, mrid, type,
      data, pattern) {
      if (opts.blockOthers) {
        self._block = true;
      }
      if (opts.timeout) {
        setTimeout(function() {
          doExec(exec, event, id, structure, mrid, type, data, pattern);
        }, opts.timeout);
      } else {
        doExec(exec, event, id, structure, mrid, type, data, pattern);
      }
    };

  if (opts.skipCount) {
    self._skip[execid] = 0;
  }
  if (opts.blockOthers) {
    self._listener.setMaxListeners(opts.blockOthers);
  }
  self.onEvent = function(event, id, structure, mrid, type, data, pattern) {

    original.apply(this, [event, id, structure, mrid, type, data, pattern]);

    if (self._exec[execid] && filter(event, id, structure, mrid, type, data,
        pattern)) {
      if (!self._busy[execid] || (self._busy[execid] && !opts.skipWhileBusy)) {
        if (opts.skipCount && self._skip < opts.skipCount) {
          self._skip++;
        } else {
          if (opts.skipCount && self._skip >= opts.skipCount) {
            self._skip = 0;
          }
          self._busy[execid] = true;
          if (self._block) {
            self._listener.once('unblock', function() {
              doExecAfterUnblock(exec, event, id, structure, mrid, type,
                data,
                pattern);
            });
          } else {
            doExecAfterUnblock(exec, event, id, structure, mrid, type, data,
              pattern);
          }
        }
      }
    }
  };
  return self;
};

_.forEach(['subscribe', 'psubscribe', 'unsubscribe', 'punsubscribe'],
  function(m) {
    Subscriber.prototype[m] = function(arg, type) {
      var self = this,
        key;
      if (arg && typeof arg === 'string') {
        if (type === 'keyspace') {
          key = '__keyspace@0__:' + arg;
        } else if (type === 'keyevent') {
          key = '__keyevent@0__:' + arg;
        } else if (type === 'publish') {
          key = '__publish__:' + arg;
        } else {
          key = arg;
        }
      } else {
        return self;
      }
      self._listener[m](key);
      return self;
    };
  });

module.exports = Subscriber;
