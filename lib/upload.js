var fs      = require('fs-extra');
var path    = require('path');
var Queue   = require('better-queue');
var request = require('request');
var uuid    = require('node-uuid');

var optionBuilder = require('./options');

var isInTest = typeof global.it === 'function';

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

module.exports = function (options, cb) {
  options = optionBuilder(options, uploadOptionDefaults, uploadMandatoryOptions);
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
    options.headers.chunkinfofilename = encodeURIComponent(path.basename(options.filePath));
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
      if (res.statusCode >= 400)  {
        if (body.message && body.message === 'file_size_exceeded') {
          cb('file_size_exceeded');
          uploadQueue.destroy();
        }
        return queueCB(`Status code ${res.statusCode}`);
      }
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

  uploadQueue.on('task_failed', function (err, something) {
    if (!isInTest) console.error('A task has errored!', err);
  });

  var numPieces = null;

  fs.stat(options.filePath, function (err, stats) {
    if (err) return cb(err);

    var size = stats.size;

    if (size === 0) {
      jobMap[0] =  {
        numParts: 0,
        partNum:  0,
        start: 0,
        end: 0,
      };
      return uploadQueue.push(jobMap[0]);
    }

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