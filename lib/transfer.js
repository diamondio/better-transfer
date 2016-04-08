var async = require('async');
var fs = require('fs-extra');
var multer = require('multer');
var concat = require('concat-files');
var Queue = require('better-queue');
var request = require('request');
var path = require('path');
var uuid = require('node-uuid');
var extend = require('extend');

var incorporateDefaults = function (userOptions, defaultOptions, mandatoryOptions) {
  var hasMandatoryOptions = true;
  mandatoryOptions.forEach(function (opt) {
    if (!userOptions[opt]) {
      console.error(`Method call must have the "${opt}" option defined!`);
      hasMandatoryOptions = false;
    }
  });

  if (!hasMandatoryOptions) throw new Error('missing_options');
  return extend({}, defaultOptions, userOptions);
}

var isInTest = typeof global.it === 'function';

// TODO: Make a readme!

var storage = multer.diskStorage({
  destination: function (req, file, cb) {
    cb(null, '/tmp');
  },
  filename: function (req, file, cb) {
    var fileName = uuid.v1() + path.extname(file.originalname);
    req.uploadedFileName = fileName;
    cb(null, fileName);
  }
});

var pieceMap = {}

var middlewareOptionsDefaults = {
  chunkExpiry: 0,
  maxFileSize: 1024 * 1024 * 256,
  flakiness: 0,
  simulatedChunkExpiry: false,
}

var middlewareMandatoryOptions = [
  'filePath',
];

var checkForExpiry = function (uploadUUID) {
  if (pieceMap[uploadUUID]) {
    if ((new Date() - pieceMap[uploadUUID].updateTime) >= pieceMap[uploadUUID].chunkExpiry) {
      // If the upload has expired, then we need to remove all of the chunks.
      var pieces = [];
      Object.keys(pieceMap[uploadUUID].pieces).forEach(function (pieceKey) {
        pieces.push(pieceMap[uploadUUID].pieces[pieceKey]);
      });
      async.parallel(pieces.map(piece => fs.unlink.bind(null, piece.path)), () => {});
      pieceMap[uploadUUID].pieces = {};
    }
  }
}

exports.getPieceMap = function () {
  return pieceMap;
}

exports.middleware = function (options) {
  var flakeCount = 0;
  options = incorporateDefaults(options, middlewareOptionsDefaults, middlewareMandatoryOptions);

  var uploader = multer({
    limits: {
      fileSize: options.maxFileSize
    },
    storage: storage,
  });

  /* options include: 
  chunkExpiry: amountOfTimeBeforeChunksAreDiscarded,
  maxFileSize: maxFileSize
  filePath:    function (req, file, cb), cb of form cb(err, desiredPath)

  After this middleware, the req.filePath key will be set to the path of the file.
  */

  return function (req, res, next) {
    var fileName = req.headers.chunkinfofilename;
    var numParts = Number(req.headers.chunkinfonumparts);
    var partNum = Number(req.headers.chunkinfopartnum);
    var uploadUUID = req.headers.chunkinfouploaduuid;

    uploader.single('file')(req, res, function (err) {
      if (req.file.size * (numParts - 1) > options.maxFileSize) {
        console.error('Max file size exceeded');
        return res.status(400).json({ message: 'file_size_exceeded' });
      }
      flakeCount += options.flakiness;
      if (flakeCount >= 1) {
        flakeCount -= 1;
        return res.status(400).json({ message: 'Induced Failure from flakiness option'});
      }
      if (err) return next(err);
      if (!pieceMap[uploadUUID]) pieceMap[uploadUUID] = {pieces: {}};
      pieceMap[uploadUUID].pieces[partNum] = {path: req.file.path};
      pieceMap[uploadUUID].updateTime = new Date();
      pieceMap[uploadUUID].chunkExpiry = options.chunkExpiry;
      if (options.simulatedChunkExpiry) {
        // Just simulate the expiration of one chunk.
        options.simulatedChunkExpiry = false;
        fs.unlinkSync(req.file.path);
        delete pieceMap[uploadUUID].pieces[partNum];
      }
      if (options.chunkExpiry) setTimeout(checkForExpiry.bind(null, uploadUUID), options.chunkExpiry + 10);
      if (Object.keys(pieceMap[uploadUUID].pieces).length === numParts) {
        var filePieces = [];
        for (var i = 0; i < numParts; i++) {
          filePieces.push(pieceMap[uploadUUID].pieces[i].path);
        }
        options.filePath(req, fileName, function (err, desiredPath) {
          if (err) return next(err);
          req.filePath = desiredPath;
          concat(filePieces, desiredPath, function () {
            async.parallel(filePieces.map(piece => fs.unlink.bind(null, piece)), function (err) {
              // If there was an issue with removing these files, that doesn't necessarily
              // mean that we need to be worried, so let's keep going.
              return next();
            });
          });
        });
      } else {
        return res.status(206).json({storedPieces: Object.keys(pieceMap[uploadUUID].pieces)});
      }
    });
  }
}


var uploadQueues = [];


//TODO: Update this?
/* options include:
  url:theURL,
  chunkSize: someSize,
  headers: extraHeaderThings,
  formData: someFormData,
  filePath: pathToTheFile,
  progress: function (progress [0.0 - 1.0]),
  numParallel: number of paralell uploads to do at once

  Testing Options:
  failAfter: numChunks -- induces failures in the upload queue after this many chunks have uploaded

*/

var uploadOptionDefaults = {
  chunkSize: 1024 * 1024 * 2,
  headers: {},
  progress: () => {},
  numParallel: 2,
  maxRetries: 5,
  failAfter: -1,
  flakiness: 0,
};

var uploadMandatoryOptions = [
  'url',
  'filePath',
];

exports.upload = function (options, cb) {
  options = incorporateDefaults(options, uploadOptionDefaults, uploadMandatoryOptions);
  var uploadSucceeded = false;
  var uploadedPieces = {};
  var jobMap = {};
  var flakeCount = 0;
  var has_called_cb = false;
  var responseBody = {};

  var uploadUUID = uuid.v4();

  var queueOptions = {
    concurrent: options.numParallel,
    maxRetries: options.maxRetries,
    name: 'Uploader',
  }
  var uploadQueue = new Queue(function (job, queueCB) {
    flakeCount += options.flakiness;
    if (flakeCount >= 1) {
      flakeCount -= 1;
      return queueCB('Induced Failure from flakiness option');
    }
    if ((options.failAfter >= 0) && (Object.keys(uploadedPieces).length >= options.failAfter)) return queueCB('Induced Failure from failAfter option');
    filestream = fs.createReadStream(options.filePath, {start: job.start, end: job.end});
    options.headers.chunkinfofilename = path.basename(options.filePath);
    options.headers.chunkinfonumparts = job.numParts;
    options.headers.chunkinfopartnum = job.partNum;
    options.headers.chunkinfouploaduuid = uploadUUID;
    request.post({
      url: options.url,
      headers: options.headers,
      json: true,
      formData: {
        file: filestream,
      }
    }, function (err, res, body) {
      if (err) return queueCB(err);
      if (res.statusCode >= 400) return queueCB(`Status code ${res.statusCode}`);
      if (res.statusCode === 200) uploadSucceeded = true;
      uploadedPieces[job.partNum] = true;

      if (res.statusCode === 206) {
        // Now we need to take stock of the pieces that the server has, and the pieces that we believe we've sent.
        // There may be a mismatch if some chunks expired during a prolonged outage.

        Object.keys(uploadedPieces).forEach(function (uploadPiece) {
          if (body.storedPieces.indexOf(uploadPiece) <= -1) {
            delete uploadedPieces[uploadPiece];
            uploadQueue.push(jobMap[uploadPiece]);
          }
        });
      }
      responseBody = body;
      options.progress(Object.keys(uploadedPieces).length / numPieces);
      queueCB(null);
    });
  }, queueOptions);

  uploadQueue.on('drain', function () {
    uploadQueue.destroy();
    if (!uploadSucceeded) { 
      if (!has_called_cb) {
        has_called_cb = true;
        return cb('upload_failed', responseBody, uploadUUID);
      }
    }
    if (!has_called_cb) {
      has_called_cb = true;
      cb(null, responseBody, uploadUUID);
    }
  });

  uploadQueue.on('task_failed', function (err) {
    if (!isInTest) console.error('A task has errored!', err);
  });

  var numPieces = null;

  fs.stat(options.filePath, function (err, stats) {
    var size = stats.size;
    numPieces = Math.ceil(size / options.chunkSize);
    for (var i = 0; i < numPieces; i++) {
      if (i === numPieces - 1) {
        jobMap[i] = {
          numParts: numPieces,
          partNum:  i,
          start: options.chunkSize * i,
          end: size - 1,
        };
      } else {
        jobMap[i] = {
          numParts: numPieces,
          partNum:  i,
          start: options.chunkSize * i,
          end: options.chunkSize * (i + 1) - 1,
        };
      }
      uploadQueue.push(jobMap[i]);
    }
  });

  return {
    cancel: function () {
      uploadQueue.destroy();
      if (!has_called_cb) {
        has_called_cb = true;
        cb('upload_canceled', null, uploadUUID);
      }
    }
  }
}
