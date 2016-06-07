var async  = require('async');
var concat = require('concat-files');
var fs     = require('fs-extra');
var multer = require('multer');
var path   = require('path');
var uuid   = require('node-uuid');
var mkdirp = require('mkdirp');

var optionBuilder = require('./options');

var isInTest = typeof global.it === 'function';

// TODO: Make a readme!


var stores = {
  memory: '../stores/memory',
  redis:  '../stores/redis'
}

var middlewareOptionsDefaults = {
  tempDir: '/tmp',
  chunkExpiry: 0,
  maxFileSize: 1024 * 1024 * 256,
  flakiness: 0,
  simulatedChunkExpiry: false,
}

var middlewareMandatoryOptions = [
  'filePath',
];

function TransferMiddleware (opts) {
  var self = this;
  opts = opts || {};
  // Initialize Storage
  self.use(opts.store || 'memory');
  if (!self._store) {
    throw new Error('TransferMiddleware cannot continue without a valid store.')
  }

  self._flakeCount = 0;
  var options = optionBuilder(opts, middlewareOptionsDefaults, middlewareMandatoryOptions);

  var storage = multer.diskStorage({
    destination: function (req, file, cb) {
      cb(null, options.tempDir);
    },
    filename: function (req, file, cb) {
      var fileName = uuid.v1() + path.extname(file.originalname);
      req.uploadedFileName = fileName;
      cb(null, fileName);
    }
  });

  self._uploader = multer({
    limits: {
      fileSize: options.maxFileSize
    },
    storage: storage,
  });

  self._options = options;
}

module.exports = TransferMiddleware;


TransferMiddleware.prototype.use = function (store) {
  var self = this;
  if (typeof store === 'string' && stores[store]) {
    try {
      var Store;
      if (store === 'memory') {
        Store = require('../stores/memory');
      } else if (store === 'redis') {
        Store = require('../stores/redis');
      }
      self._store = new Store();
    } catch (e) { throw e }
  } else if (typeof store === 'object' && typeof store.type === 'string' && stores[store.type]) {
    try {
      var Store;
      if (store.type === 'memory') {
        Store = require('../stores/memory');
      } else if (store.type === 'redis') {
        Store = require('../stores/redis');
      }
      self._store = new Store(store);
    } catch (e) { throw e }
  } else if (
    typeof store === 'object' &&
    store.connect &&
    store.close &&
    store.setUpdateTime &&
    store.getUpdateTime &&
    store.setChunkExpiry &&
    store.getChunkExpiry &&
    store.setPiecePath &&
    store.getAllPieces &&
    store.expireTransfer &&
    store.getNumPieces) self._store = store;
  else {
    throw new Error('unknown_store');
  }

  self._connected = false;
  self._pending_calls = [];
  self._connectToStore();
}


TransferMiddleware.prototype._connectToStore = function () {
  var self = this;
  self._store.connect(function (err) {
    if (err) throw new Error(`Failed to connect to store! Err was: ${err}`);
    self._connected = true;
  });
}

TransferMiddleware.prototype._checkForExpiry = function (uploadUUID, cb) {
  var self = this;
  self._store.getNumPieces(uploadUUID, function (err, numPieces) {
    if (numPieces === 0) return cb(null);
    if (err) return cb(err);
    self._store.getUpdateTime(uploadUUID, function (err, updateTime) {
      if (err) return cb(err);
      self._store.getChunkExpiry(uploadUUID, function (err, chunkExpiry) {
        if (err) return cb(err);
        if ((new Date() - updateTime) >= chunkExpiry) {
          self._store.getAllPieces(uploadUUID, function (err, pieces) {
            if (err) return cb(err);
            var paths = [];
            Object.keys(pieces).forEach(function (pieceKey) {
              paths.push(pieces[pieceKey].path);
            });
            async.each(paths, function (path, asyncCB) {
              fs.remove(path, function (err) {
                // Regardless of whether there was an error, we should continue.
                asyncCB();
              });
            }, function () {
              self._store.expireTransfer(uploadUUID, cb);
            });
          });
        } else {
          cb(null);
        }
      });
    });
  });
}

TransferMiddleware.prototype.getMiddlewareFunction = function () {
  var self = this;
  return function (req, res, next) {
    var fileName = decodeURIComponent(req.headers.chunkinfofilename);
    var numParts = Number(req.headers.chunkinfonumparts);
    var partNum = Number(req.headers.chunkinfopartnum);
    var uploadUUID = req.headers.chunkinfouploaduuid;

    if (numParts === 0) {
      return self._options.filePath(req, fileName, function (err, desiredPath) {
        if (err) return next(err);
        req.filePath = desiredPath;
        mkdirp(path.dirname(desiredPath), function (err) {
          if (err) return next(err);
          fs.writeFile(desiredPath, '', next);
        });
      });
    }

    self._uploader.single('file')(req, res, function (err) {
      if (req.file.size * (numParts - 1) > self._options.maxFileSize) {
        console.error('Max file size exceeded');
        return res.status(400).json({ message: 'file_size_exceeded' });
      }
      self._flakeCount += self._options.flakiness;
      if (self._flakeCount >= 1) {
        self._flakeCount -= 1;
        return res.status(400).json({ message: 'Induced Failure from flakiness option'});
      }
      if (err) return next(err);
      self._store.setPiecePath(uploadUUID, partNum, req.file.path, function (err) {
        if (err) return next(err);
        self._store.setUpdateTime(uploadUUID, new Date(), function (err) {
          if (err) return next(err);
          self._store.setChunkExpiry(uploadUUID, self._options.chunkExpiry, function (err) {
            if (err) return next(err);
            var reassemblePieces = function () {
              self._store.getAllPieces(uploadUUID, function (err, pieces) {
                if (err) return next(err);
                if (Object.keys(pieces).length === numParts) {
                  var filePieces = [];
                  for (var i = 0; i < numParts; i++) {
                    filePieces.push(pieces[i].path);
                  }
                  self._options.filePath(req, fileName, function (err, desiredPath) {
                    if (err) return next(err);
                    req.filePath = desiredPath;
                    concat(filePieces, desiredPath, function () {
                      async.series(filePieces.map(piece => fs.unlink.bind(null, piece)), function (err) {
                        // If there was an issue with removing these files, that doesn't necessarily
                        // mean that we need to be worried, so let's keep going.
                        return next();
                      });
                    });
                  });
                } else {
                  return res.status(206).json({storedPieces: Object.keys(pieces)});
                }
              });
            }
            if (self._options.chunkExpiry) {
              setTimeout(function () {
                self._checkForExpiry(uploadUUID, () => {});
              }, self._options.chunkExpiry + 10);
            }

            if (self._options.simulatedChunkExpiry) {
              // Just simulate the expiration of the chunks.
              self._options.simulatedChunkExpiry = false;
              return self._store.expireTransfer(uploadUUID, reassemblePieces);
            }
            reassemblePieces();
          });
        });
      });
    });
  }
};
