var _ = require('lodash');

function MemoryStore(opts) {
  var self = this;
  self.info = {};
}

module.exports = MemoryStore;

MemoryStore.prototype.connect = function (cb) { return cb() }

MemoryStore.prototype.close = function (cb) { return cb() }

MemoryStore.prototype.setUpdateTime = function (uuid, updateTime, cb) {
  var self = this;
  _.set(self.info, [uuid, 'updateTime'], updateTime);
  setImmediate(cb);
}

MemoryStore.prototype.getUpdateTime = function (uuid, cb) {
  var self = this;
  var time = _.get(self.info, [uuid, 'updateTime']);
  setImmediate(function () {
    if (!time) return cb('nonexistent_uuid');
    cb(null, time);
  });
}

MemoryStore.prototype.setChunkExpiry = function (uuid, expiryTimeout, cb) {
  var self = this;
  _.set(self.info, [uuid, 'expiryTimeout'], expiryTimeout);
  setImmediate(cb);
}

MemoryStore.prototype.getChunkExpiry = function (uuid, cb) {
  var self = this;
  var timeout = _.get(self.info, [uuid, 'expiryTimeout']);
  setImmediate(function () {
    if (!timeout) return cb('nonexistent_uuid');
    cb(null, timeout);
  });
}

MemoryStore.prototype.setPiecePath = function (uuid, pieceNum, path, cb) {
  var self = this;
  if (!_.get(self.info, [uuid, 'pieces', pieceNum, 'path'], false)) _.set(self.info, [uuid, 'numPieces'], _.get(self.info, [uuid, 'numPieces'], 0) + 1);
  _.set(self.info, [uuid, 'pieces', pieceNum, 'path'], path);
  setImmediate(cb);
}

MemoryStore.prototype.getAllPieces = function (uuid, cb) {
  var self = this;
  var pieces = _.get(self.info, [uuid, 'pieces']);
  setImmediate(function () {
    if (!pieces) return cb('nonexistent_uuid');
    cb(null, pieces);
  });
}

MemoryStore.prototype.expireTransfer = function (uuid, cb) {
  var self = this;
  delete self.info[uuid];
  setImmediate(cb);
}

MemoryStore.prototype.getNumPieces = function (uuid, cb) {
  var self = this;
  var numPieces =  _.get(self.info, [uuid, 'numPieces'], 0);
  setImmediate(function () {
    cb(null, numPieces);
  });
}
