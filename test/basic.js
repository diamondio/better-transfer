var assert = require('assert');
var async = require('async');
var fs = require('fs-extra');
var express = require('express');
var path = require('path');
var transfer = require('../lib/transfer');
var bodyParser = require('body-parser');
var uuid = require('node-uuid');

require('longjohn');

var checkFilesEqual = function (file1, file2, cb) {
  var s1 = '';
  var s2 = '';

  var pruneSame = function () {
    if (s1.startsWith(s2)) {
      s1 = s1.substring(s2.length);
      s2 = '';
    } else if (s2.startsWith(s1)) {
      s2 = s2.substring(s1.length);
      s1 = '';
    }
  }

  var f1_closed = false;
  var f2_closed = false;

  fs.createReadStream(file1)
  .on('data', function (chunk) {
    s1 += chunk;
    pruneSame();
  })
  .on('end', function () {
    f1_closed = true;
    if (f2_closed) {
      return cb(s2 === s1);
    }
  });  

  fs.createReadStream(file2)
  .on('data', function (chunk) {
    s2 += chunk;
    pruneSame();
  })
  .on('end', function () {
    f2_closed = true;
    if (f1_closed) {
      return cb(s2 === s1);
    }
  });
}


describe('Basic Upload Cases', function() {
  var server = null;

  beforeEach(function (done){
    server = null;
    done();
  });

  afterEach(function (done) {
    if (server) server.close();
    done();
  });

  it('upload one empty file', function (done) {
    var app = express();
    app.use(bodyParser.json());
    var testfile = uuid.v4();

    app.post('/upload', transfer.middleware({chunkExpiry: 0, maxFileSize: 1000, filePath: (req, filename, cb) => cb(null, `/tmp/` + testfile)}), function (req, res) {
      return res.status(200).json({'message': 'ok'});
    });

    server = app.listen(3101, function () {
      transfer.upload({url: 'http://localhost:3101/upload', filePath: './test/resources/emptyFile.txt'}, function (err) {
        assert.ok(!err);
        checkFilesEqual('./test/resources/emptyFile.txt', '/tmp/' + testfile, function (equal) {
          assert.ok(equal);
          done();
        });
      });
    });
  });

  it('upload one small file', function (done) {
    var app = express();
    app.use(bodyParser.json());
    var testfile = uuid.v4();

    app.post('/upload', transfer.middleware({chunkExpiry: 0, maxFileSize: 1000, filePath: (req, filename, cb) => cb(null, `/tmp/` + testfile)}), function (req, res) {
      return res.status(200).json({'message': 'ok'});
    });

    server = app.listen(3101, function () {
      transfer.upload({url: 'http://localhost:3101/upload', filePath: './test/resources/testfile'}, function (err) {
        assert.ok(!err);
        checkFilesEqual('./test/resources/testfile', '/tmp/' + testfile, function (equal) {
          assert.ok(equal);
          done();
        });
      });
    });
  });

  it('upload several small files', function (done) {
    var app = express();
    app.use(bodyParser.json());

    app.post('/upload', transfer.middleware({chunkExpiry: 0, filePath: (req, filename, cb) => cb(null, `/tmp/` + path.basename(filename))}), function (req, res) {
      return res.status(200).json({'message': 'ok'});
    });

    server = app.listen(3101, function () {

      var uploadAndCheck = function (testfile, cb) {
        transfer.upload({url: 'http://localhost:3101/upload', filePath: testfile}, function (err) {
          assert.ok(!err);
          checkFilesEqual(testfile, '/tmp/' + path.basename(testfile), function (equal) {
            assert.ok(equal);
            cb();
          });
        });
      }
      async.parallel([
        uploadAndCheck.bind(null, './test/resources/testfile'),
        uploadAndCheck.bind(null, './test/resources/testfile2'),
        uploadAndCheck.bind(null, './test/resources/testfile3'),
      ], function (err) {
        assert.ok(!err);
        done();
      });
    });
  });

  it('upload one small file in many pieces', function (done) {
    var app = express();
    app.use(bodyParser.json());
    var testfile = uuid.v4();

    app.post('/upload', transfer.middleware({chunkExpiry: 0, maxFileSize: 1000, filePath: (req, filename, cb) => cb(null, `/tmp/` + testfile)}), function (req, res) {
      return res.status(200).json({'message': 'ok'});
    });

    server = app.listen(3101, function () {
      transfer.upload({url: 'http://localhost:3101/upload', filePath: './test/resources/testfile', chunkSize: 2}, function (err) {
        assert.ok(!err);
        checkFilesEqual('./test/resources/testfile', '/tmp/' + testfile, function (equal) {
          assert.ok(equal);
          done();
        });
      });
    });
  });

  it('upload one small file with smallest possible chunk size', function (done) {
    var app = express();
    app.use(bodyParser.json());
    var testfile = uuid.v4();

    app.post('/upload', transfer.middleware({maxFileSize: 1000, filePath: (req, filename, cb) => cb(null, `/tmp/` + testfile)}), function (req, res) {
      return res.status(200).json({'message': 'ok'});
    });

    server = app.listen(3101, function () {
      transfer.upload({url: 'http://localhost:3101/upload', filePath: './test/resources/testfile', chunkSize: 1}, function (err) {
        assert.ok(!err);
        checkFilesEqual('./test/resources/testfile', '/tmp/' + testfile, function (equal) {
          assert.ok(equal);
          done();
        });
      });
    });
  });

  it('failAfter options should work', function (done) {
    var app = express();
    app.use(bodyParser.json());
    var testfile = uuid.v4();

    app.post('/upload', transfer.middleware({maxFileSize: 1000, filePath: (req, filename, cb) => cb(null, `/tmp/` + testfile)}), function (req, res) {
      return res.status(200).json({'message': 'ok'});
    });

    server = app.listen(3101, function () {
      transfer.upload({url: 'http://localhost:3101/upload', filePath: './test/resources/testfile', chunkSize: 1, failAfter: 3}, function (err) {
        // make sure we hit an error
        assert.ok(err);
        done();
      });
    });
  });

  it('check for chunk expiry', function (done) {
    var app = express();
    app.use(bodyParser.json());
    var testfile = uuid.v4();

    // Chunks now expire in 10 ms
    app.post('/upload', transfer.middleware({chunkExpiry: 10, maxFileSize: 1000, filePath: (req, filename, cb) => cb(null, `/tmp/` + testfile)}), function (req, res) {
      return res.status(200).json({'message': 'ok'});
      next();
    });

    server = app.listen(3101, function () {
      // Because the upload will fail after 3 chunks get uploaded, those chunks will be orphaned on the server, and the server should be able to clean them up
      transfer.upload({url: 'http://localhost:3101/upload', filePath: './test/resources/testfile', chunkSize: 1, failAfter: 1}, function (err, body, uploadUUID) {
        setTimeout(function () {
          var pieceMap = transfer.getPieceMap();
          Object.keys(pieceMap[uploadUUID].pieces).forEach(function (chunkNum) {
            var exists = false;
            try {
              stats = fs.statSync(pieceMap[uploadUUID].pieces[chunkNum].path);
              //If we make it here, it means the file exists, so we need to fail.
              exists = true;
            }
            catch (e) {
            }
            if (exists) {
              assert.ok(false, `${pieceMap[uploadUUID].pieces[chunkNum].path} exists, but it should have been deleted`);
            }
          });
          done();
        }, 60);
      });
    });
  });

  it('flakey upload interface', function (done) {
    var app = express();
    app.use(bodyParser.json());
    var testfile = uuid.v4();

    app.post('/upload', transfer.middleware({filePath: (req, filename, cb) => cb(null, `/tmp/` + testfile)}), function (req, res) {
      return res.status(200).json({'message': 'ok'});
    });

    server = app.listen(3101, function () {
      // flake out on 30% of the transfers
      transfer.upload({flakiness: 0.3, url: 'http://localhost:3101/upload', filePath: './test/resources/testfile', chunkSize: 2}, function (err) {
        assert.ok(!err);
        checkFilesEqual('./test/resources/testfile', '/tmp/' + testfile, function (equal) {
          assert.ok(equal);
          done();
        });
      });
    });
  });


  it('flakey server interface', function (done) {
    var app = express();
    app.use(bodyParser.json());
    var testfile = uuid.v4();

    app.post('/upload', transfer.middleware({flakiness: 0.3, filePath: (req, filename, cb) => cb(null, `/tmp/` + testfile)}), function (req, res) {
      return res.status(200).json({'message': 'ok'});
    });

    server = app.listen(3101, function () {
      // flake out on 30% of the transfers
      transfer.upload({url: 'http://localhost:3101/upload', filePath: './test/resources/testfile', chunkSize: 2}, function (err) {
        assert.ok(!err);
        checkFilesEqual('./test/resources/testfile', '/tmp/' + testfile, function (equal) {
          assert.ok(equal);
          done();
        });
      });
    });
  });

  it('flakey upload and server interface', function (done) {
    var app = express();
    app.use(bodyParser.json());
    var testfile = uuid.v4();

    app.post('/upload', transfer.middleware({flakiness: 0.3, filePath: (req, filename, cb) => cb(null, `/tmp/` + testfile)}), function (req, res) {
      return res.status(200).json({'message': 'ok'});
    });

    server = app.listen(3101, function () {
      // flake out on 30% of the transfers
      transfer.upload({flakiness: 0.3, url: 'http://localhost:3101/upload', filePath: './test/resources/testfile', chunkSize: 2}, function (err) {
        assert.ok(!err);
        checkFilesEqual('./test/resources/testfile', '/tmp/' + testfile, function (equal) {
          assert.ok(equal);
          done();
        });
      });
    });
  });

  it('erroneous chunk expiration', function (done) {
    var app = express();
    app.use(bodyParser.json());
    var testfile = uuid.v4();

    app.post('/upload', transfer.middleware({simulatedChunkExpiry: true, filePath: (req, filename, cb) => cb(null, `/tmp/` + testfile)}), function (req, res) {
      return res.status(200).json({'message': 'ok'});
    });

    server = app.listen(3101, function () {
      // flake out on 30% of the transfers
      transfer.upload({url: 'http://localhost:3101/upload', filePath: './test/resources/testfile', chunkSize: 2}, function (err) {
        assert.ok(!err);
        checkFilesEqual('./test/resources/testfile', '/tmp/' + testfile, function (equal) {
          assert.ok(equal);
          done();
        });
      });
    });
  });

  it('erroneous chunk expiration plus server and upload flakiness', function (done) {
    var app = express();
    app.use(bodyParser.json());
    var testfile = uuid.v4();

    app.post('/upload', transfer.middleware({simulatedChunkExpiry: true, flakiness: 0.3, filePath: (req, filename, cb) => cb(null, `/tmp/` + testfile)}), function (req, res) {
      return res.status(200).json({'message': 'ok'});
    });

    server = app.listen(3101, function () {
      // flake out on 30% of the transfers
      transfer.upload({flakiness: 0.3, url: 'http://localhost:3101/upload', filePath: './test/resources/testfile', chunkSize: 2}, function (err) {
        assert.ok(!err);
        checkFilesEqual('./test/resources/testfile', '/tmp/' + testfile, function (equal) {
          assert.ok(equal);
          done();
        });
      });
    });
  });

  it('check max file size works', function (done) {
    var app = express();
    app.use(bodyParser.json());
    var testfile = uuid.v4();
    //Squelch errors for this test:
    var oldConsoleError = console.error;
    console.error = () => {};

    app.post('/upload', transfer.middleware({maxFileSize: 5, filePath: (req, filename, cb) => cb(null, `/tmp/` + testfile)}), function (req, res) {
      return res.status(200).json({'message': 'ok'});
    });

    server = app.listen(3101, function () {
      transfer.upload({url: 'http://localhost:3101/upload', filePath: './test/resources/testfile', chunkSize: 3}, function (err) {
        assert.ok(err);
        console.error = oldConsoleError;
        done();
      });
    });
  });

  it('check progress', function (done) {
    var app = express();
    app.use(bodyParser.json());
    var testfile = uuid.v4();
    //Squelch errors for this test:
    var oldConsoleError = console.error;
    console.error = () => {};

    app.post('/upload', transfer.middleware({filePath: (req, filename, cb) => cb(null, `/tmp/` + testfile)}), function (req, res) {
      return res.status(200).json({'message': 'ok'});
    });
    var currentProgress = 0;
    server = app.listen(3101, function () {
      transfer.upload({url: 'http://localhost:3101/upload', filePath: './test/resources/testfile', chunkSize: 3, progress: function (progress) {
        assert.ok(progress > currentProgress);
        currentProgress = progress;
      }}, function (err) {
        assert.ok(currentProgress === 1);
        done();
      });
    });
  });

  it('check cancel', function (done) {
    var app = express();
    app.use(bodyParser.json());
    var testfile = uuid.v4();
    //Squelch errors for this test:
    var oldConsoleError = console.error;
    console.error = () => {};

    app.post('/upload', transfer.middleware({filePath: (req, filename, cb) => cb(null, `/tmp/` + testfile)}), function (req, res) {
      return res.status(200).json({'message': 'ok'});
    });
    var currentProgress = 0;
    server = app.listen(3101, function () {
      var uploader = transfer.upload({url: 'http://localhost:3101/upload', filePath: './test/resources/testfile', chunkSize: 1, progress: function (progress) {
        currentProgress = progress;
      }}, function (err) {
        assert.ok(err === 'upload_canceled');
        setTimeout(function () {
          assert(currentProgress < 1);
          done();
        }, 80);
      });
      uploader.cancel();
    });
  });
});
