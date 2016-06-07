var _         = require('lodash');
var async     = require('async');
var redis     = require('redis');
var redisLock = require('node-redis-lock-diamond');

function RedisStore(opts) {
  var self = this;
  opts = opts || {};
  if (!opts.url) throw new Error('The "url" options is required to create a redis store.');
  self.opts = opts;
  self.lockTTL = 20; // 20 seconds
  self.lockRetries = 5; // 5 retries
  self.lockRetryDelay = 20; // 20 milliseconds
}

module.exports = RedisStore;

RedisStore.prototype.connect = function (cb) {
  var self = this;
  self._client = redis.createClient({
    url: self.opts.url,
  });
}

RedisStore.prototype.close = function (cb) {
  self._client.quit();
  setImmediate(cb);
}

RedisStore.prototype.setUpdateTime = function (uuid, updateTime, cb) {
  var self = this;
  self._client.set(uuid + 'updateTime', updateTime.toISOString(), cb);
}

RedisStore.prototype.getUpdateTime = function (uuid, cb) {
  var self = this;
  self._client.get(uuid + 'updateTime', function (err, dateString) {
    if (err) return cb(err);
    cb(null, new Date(dateString));
  });
}

RedisStore.prototype.setChunkExpiry = function (uuid, expiryTimeout, cb) {
  var self = this;
  self._client.set(uuid + 'expiryTimeout', '' + expiryTimeout, cb);
}

RedisStore.prototype.getChunkExpiry = function (uuid, cb) {
  var self = this;
  self._client.get(uuid + 'expiryTimeout', function (err, timeoutString) {
    if (err) return cb(err);
    cb(null, parseInt(dateString));
  });
}

RedisStore.prototype.setPiecePath = function (uuid, pieceNum, path, cb) {
  var self = this;
  var lock = new redisLock({namespace: ''}, self._client);
  lock.acquire(uuid, self.lockTTL, self.lockRetries, self.lockRetryDelay, function (err) {
    if (err) return cb(err)
    self._client.get(uuid + 'pieces' + pieceNum + 'path', function (err, origPath) {
      var after = function () {
        self._client.set(uuid + 'pieces' + pieceNum + 'path', path, function (err) {
          lock.release(uuid, () => {});
          cb(err);
        });
      }
      if (origPath) return after();
      self._client.get(uuid + 'numPieces', function (err, val) {
        if (err) {
          lock.release(uuid, () => {});
          return cb(err);
        }
        var newVal = (val || 0) + 1;
        self._client.set(uuid + 'numPieces', newVal, function (err) {
          if (err) {
          lock.release(uuid, () => {});
            return cb(err);
          }
          self._client.get(uuid + 'pieceList', function (err, val) {
            if (err) {
              lock.release(uuid, () => {});
              return cb(err);
            }
            val = val || '[]';
            var list = JSON.parse(val);
            if (list.indexOf(pieceNum) <= -1) list.push(pieceNum);
            self._client.set(uuid + 'pieceList', JSON.stringify(list), function (err) {
              if (err) {
                lock.release(uuid, () => {});
                return cb(err);
              }
              after();
            });
          });
        });
      });
    });
  });
}

RedisStore.prototype.getAllPieces = function (uuid, cb) {
  var self = this;
  var pieces = {};

  var lock = new redisLock({namespace: ''}, self._client);
  lock.acquire(uuid + 'pieces', self.lockTTL, self.lockRetries, self.lockRetryDelay, function (err) {
    if (err) return cb(err);
    self._client.get(uuid + 'pieceList', function (err, val) {
      if (err) {
        lock.release(uuid + 'pieces', () => {});
        return cb(err);
      }
      var pieceList = JSON.parse(val || '[]');
      async.each(pieceList, function (piece, asyncCB) {
        self._client.get(uuid + 'pieces' + piece + 'path', function (err, path) {
          if (err) return asyncCB(err);
          pieces[piece] = {
            path: path
          }
          asyncCB();
        });
      }, function (err) {
        lock.release(uuid + 'pieces', () => {});
        cb(err, pieces);
      });
    });
  });
}

RedisStore.prototype.expireTransfer = function (uuid, cb) {
  var self = this;
  var lock = new redisLock({namespace: ''}, self._client);
  lock.acquire(uuid, self.lockTTL, self.lockRetries, self.lockRetryDelay, function (err) {
    if (err) return cb(err);
    self.getAllPieces(uuid, function (err, pieces) {
      if (err) {
        lock.release(uuid, () => {});
        return cb(err);
      }
      var pathsToRemove = [];
      Object.keys(pieces).forEach(function (pieceNum) {
        pathsToRemove.push(uuid + 'pieces' + pieceNum + 'path');
      });
      pathsToRemove.push(uuid + 'updateTime');
      pathsToRemove.push(uuid + 'expiryTimeout');
      pathsToRemove.push(uuid + 'numPieces');
      pathsToRemove.push(uuid + 'pieceList');

      async.each(pathsToRemove, function (key, asyncCB) {
        self._client.del(key, asyncCB);
      }, function (err) {
        lock.release(uuid, () => {});
        cb(err);
      });
    });
  });
}

RedisStore.prototype.getNumPieces = function (uuid, cb) {
  var self = this;
  self._client.get(uuid + 'numPieces', function (err, val) {
    if (err) return cb(err);
    cb(null, val || 0);
  });
}
