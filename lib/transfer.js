var async = require('async');
var fs = require('fs-extra');
var multer = require('multer');
var concat = require('concat-files');
var Queue = require('better-queue');
var request = require('request');
var path = require('path');
var uuid = require('node-uuid');
var extend = require('extend');

require('longjohn');

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


/* TODO:

  Tests for all of these features

  - chunk expiry
    - check that chunks expire and get deleted
    - check that, in the event of deletion, the uploader can still recover
  - max file size
    - check that if a file exceed the max file size, an error occurs
    - check that even if the chunks are small, the file size is still respected
  - resumability on failed uploads
    - check that if the uploads are flaky, the upload still eventually succeeds
  - retry logic that can handle internet outages
    - inject internet issue failures, wait for a while, then resume and verify that things are okay
  - progress
    - verify that progress updates happen in order I guess?
  - readme?
*/
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
}

var middlewareMandatoryOptions = [
  'filePath',
];

exports.middleware = function (options) {
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

// TODO: do a check to see if the maxFileSize is going to be exceeded, and fail out after the first chunk.
// TODO: make sure we eventually timeout and discard the chunks for stale files.

  return function (req, res, next) {
    var fileName = req.headers.chunkinfo_filename;
    var numParts = Number(req.headers.chunkinfo_numparts);
    var partNum = Number(req.headers.chunkinfo_partnum);
    var uploadUUID = req.headers.chunkinfo_uploaduuid;

    uploader.single('file')(req, res, function (err) {
      if (err) return next(err);
      if (!pieceMap[uploadUUID]) pieceMap[uploadUUID] = {};
      pieceMap[uploadUUID][partNum] = {path: req.file.path};
      if (Object.keys(pieceMap[uploadUUID]).length === numParts) {
        var filePieces = [];
        for (var i = 0; i < numParts; i++) {
          filePieces.push(pieceMap[uploadUUID][i].path);
        }
        options.filePath(req, fileName, function (err, desiredPath) {
          if (err) return next(err);
          req.filePath = desiredPath;
          concat(filePieces, desiredPath, function () {
            async.parallel(filePieces.map(piece => fs.unlink.bind(null, piece)), function (err) {
              // If there was an issue with removing these files, that doesn't necessarily
              // mean that we need to be worried, so let's keep going.
              return next();
            })
          });
        });
      } else {
        return res.status(204).json({ message: 'awaiting_more_pieces'});
      }
    });
  }
}


var uploadQueues = [];


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
  formData: {},
  progress: () => {},
  numParallel: 2,
  failAfter: -1
};

var uploadMandatoryOptions = [
  'url',
  'filePath',
];

exports.upload = function (options, cb) {
  options = incorporateDefaults(options, uploadOptionDefaults, uploadMandatoryOptions);

  var uploadUUID = uuid.v4();

  var queueOptions = {
    concurrent: options.numParallel,
    name: 'Uploader',
  }
  var uploadQueue = new Queue(function (job, queueCB) {
    filestream = fs.createReadStream(options.filePath, {start: job.start, end: job.end});
    options.headers.chunkinfo_filename = path.basename(options.filePath);
    options.headers.chunkinfo_numparts = job.numParts;
    options.headers.chunkinfo_partnum = job.partNum;
    options.headers.chunkinfo_uploaduuid = uploadUUID;
    request.post({
      url: options.url,
      headers: options.headers,
      json: true,
      formData: {
        file: filestream,
      }
    }, function (err, res, body) {
      queueCB(err);
    });
  }, queueOptions);

  uploadQueue.on('drain', function () {
    uploadQueue.destroy();
    cb(null);
  });

  uploadQueue.on('task_failed', function (err) {
    console.error('A task has errored!', err);
  });

  fs.stat(options.filePath, function (err, stats) {
    var size = stats.size;
    var numPieces = Math.ceil(size / options.chunkSize);
    for (var i = 0; i < numPieces; i++) {
      if (i === numPieces - 1) {
        uploadQueue.push({
          numParts: numPieces,
          partNum:  i,
          start: options.chunkSize * i,
          end: size - 1,
        });
      } else {
        uploadQueue.push({
          numParts: numPieces,
          partNum:  i,
          start: options.chunkSize * i,
          end: options.chunkSize * (i + 1) - 1,
        });
      }
    }
  });
}
