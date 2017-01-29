'use strict';

var exec = require('child_process').exec
  , spawn = require('child_process').spawn
  , path = require('path')
  , domain = require('domain')
  , fs = require('fs')
  , async = require('async')
  , knox = require('knox')
  , d = domain.create();

/**
 * log
 *
 * Logs a message to the console with a tag.
 *
 * @param message  the message to log
 * @param tag      (optional) the tag to log with.
 */
function log(message, tag) {
  var util = require('util')
    , color = require('cli-color')
    , tags, currentTag;

  tag = tag || 'info';

  tags = {
    error: color.red.bold,
    warn: color.yellow,
    info: color.cyanBright
  };

  currentTag = tags[tag] || function(str) { return str; };
  util.log((currentTag("[" + tag + "] ") + message).replace(/(\n|\r|\r\n)$/, ''));
}

/**
 * getArchiveName
 *
 * Returns the archive name in database_YYYY_MM_DD.tar.gz format.
 *
 * @param databaseName   The name of the database
 */
function getArchiveName(databaseName) {
  var date = new Date()
    , datestring;

  datestring = [
    databaseName,
    date.getFullYear(),
    date.getMonth() + 1,
    date.getDate(),
    date.getTime()
  ];

  return datestring.join('_') + '.tar.gz';
}

/**
 * getLastArchiveName
 *
 * Returns the last archive name used in database_YYYY_MM_DD.tar.gz format.
 *
 * @param databaseName   The name of the database
 */
function getLastArchiveName(options, databaseName, callback) {
  var s3Client = knox.createClient(options)
    , newest;

  s3Client.list({}, function(err, data) {
    if(err ) {
      log(err, 'error');
    } else {
      for(var d of data.Contents) {
        if( d.Key.indexOf(databaseName) != -1) {
          newest = newest || d;
          if( d.LastModified > newest.LastModified) {
            newest = d;
          }
        }
      }

      if (newest) {
        log('Successfully read last archive name ' + newest.Key)
        callback(null, newest.Key);
      } else {
        log('No Archives match the database name ' + databaseName)
        callback(1);
      }
    }
  });
}

/* removeRF
 *
 * Remove a file or directory. (Recursive, forced)
 *
 * @param target       path to the file or directory
 * @param callback     callback(error)
 */
function removeRF(target, callback) {
  var fs = require('fs');

  callback = callback || function() { };

  fs.exists(target, function(exists) {
    if (!exists) {
      return callback(null);
    }
    log("Removing " + target, 'info');
    exec( 'rm -rf ' + target, callback);
  });
}

/**
 * mongoDump
 *
 * Calls mongodump on a specified database.
 *
 * @param options    MongoDB connection options [host, port, username, password, db]
 * @param directory  Directory to dump the database to
 * @param callback   callback(err)
 */
function mongoDump(options, directory, callback) {
  var mongodump
    , mongoOptions;

  callback = callback || function() { };

  mongoOptions= [
    '-h', options.host + ':' + options.port,
    '-d', options.db,
    '-o', directory
  ];

  if(options.username && options.password) {
    mongoOptions.push('-u');
    mongoOptions.push(options.username);

    mongoOptions.push('-p');
    mongoOptions.push(options.password);
  }

  log('Starting mongodump of ' + options.db, 'info');
  mongodump = spawn('mongodump', mongoOptions);

  mongodump.stdout.on('data', function (data) {
    log(data);
  });

  mongodump.stderr.on('data', function (data) {
    log(data, 'error');
  });

  mongodump.on('exit', function (code) {
    if(code === 0) {
      log('mongodump executed successfully', 'info');
      callback(null);
    } else {
      callback(new Error("Mongodump exited with code " + code));
    }
  });
}

/**
 * mongoRestore
 *
 * Calls mongorestore on a specified database.
 *
 * @param options    MongoDB connection options [host, port, username, password, db]
 * @param directory  Directory to dump the database to
 * @param callback   callback(err)
 */
function mongoRestore(options, backup, callback) {
  var mongorestore
    , mongoOptions;

  callback = callback || function() { };

  mongoOptions= [
    '-h', options.host + ':' + options.port,
    '-d', options.db,
    backup
  ];

  if(options.username && options.password) {
    mongoOptions.push('-u');
    mongoOptions.push(options.username);

    mongoOptions.push('-p');
    mongoOptions.push(options.password);
  }

  log('Starting mongorestore of ' + options.db, 'info');
  mongorestore = spawn('mongorestore', mongoOptions);

  mongorestore.stdout.on('data', function (data) {
    log(data);
  });

  mongorestore.stderr.on('data', function (data) {
    log(data, 'error');
  });

  mongorestore.on('exit', function (code) {
    if(code === 0) {
      log('mongorestore executed successfully', 'info');
      callback(null);
    } else {
      callback(new Error("Mongorestore exited with code " + code));
    }
  });
}

/**
 * compressDirectory
 *
 * Compressed the directory so we can upload it to S3.
 *
 * @param directory  current working directory
 * @param input     path to input file or directory
 * @param output     path to output archive
 * @param callback   callback(err)
 */
function compressDirectory(directory, input, output, callback) {
  var tar
    , tarOptions;

  callback = callback || function() { };

  tarOptions = [
    '-zcf',
    output,
    input
  ];

  log('Starting compression of ' + input + ' into ' + output, 'info');
  tar = spawn('tar', tarOptions, { cwd: directory });

  tar.stderr.on('data', function (data) {
    log(data, 'error');
  });

  tar.on('exit', function (code) {
    if(code === 0) {
      log('successfully compress directory', 'info');
      callback(null);
    } else {
      callback(new Error("Tar exited with code " + code));
    }
  });
}

/**
 * decompressDirectory
 *
 * DeCompresses the tar that we downloaded from S3.
 *
 * @param directory  current working directory
 * @param input     path to input file or directory
 * @param output     path to output archive
 * @param callback   callback(err)
 */
function decompressDirectory(directory, input, output, callback) {
  var tar
    , tarOptions;

  callback = callback || function() { };

  tarOptions = [
    '-xzvf',
    output,
    input
  ];

  log('Starting decompression of ' + input + ' into ' + output, 'info');
  tar = spawn('tar', tarOptions, { cwd: directory });

  tar.stderr.on('data', function (data) {
    log(data, 'error');
  });

  tar.on('exit', function (code) {
    if(code === 0) {
      log('successfully decompressed tar', 'info');
      callback(null);
    } else {
      callback(new Error("Tar exited with code " + code));
    }
  });
}

/**
 * sendToS3
 *
 * Sends a file or directory to S3.
 *
 * @param options   s3 options [key, secret, bucket]
 * @param directory directory containing file or directory to upload
 * @param target    file or directory to upload
 * @param callback  callback(err)
 */
function sendToS3(options, directory, target, callback) {
  var sourceFile = path.join(directory, target)
    , s3client
    , destination = options.destination || '/'
    , headers = {};

  callback = callback || function() { };

  // Deleting destination because it's not an explicitly named knox option
  delete options.destination;
  s3client = knox.createClient(options);

  if (options.encrypt)
    headers = {"x-amz-server-side-encryption": "AES256"}

  log('Attemping to upload ' + target + ' to the ' + options.bucket + ' s3 bucket');
  s3client.putFile(sourceFile, path.join(destination, target), headers, function(err, res){
    if(err) {
      return callback(err);
    }

    res.setEncoding('utf8');

    res.on('data', function(chunk){
      if(res.statusCode !== 200) {
        log(chunk, 'error');
      } else {
        log(chunk);
      }
    });

    res.on('end', function(chunk) {
      if (res.statusCode !== 200) {
        return callback(new Error('Expected a 200 response from S3, got ' + res.statusCode));
      }
      log('Successfully uploaded to s3');
      return callback();
    });
  });
}

/**
 * retrieveFromS3
 *
 * Retrieves a file or directory from S3.
 *
 * @param options   s3 options [key, secret, bucket]
 * @param directory directory containing to download contents to
 * @param target    file or directory name of downloaded content
 * @param callback  callback(err)
 */
function retrieveFromS3(options, directory, target, callback) {
  var knox = require('knox')
    , sourceFile = path.join(options.destination || '/', target)
    , destination = directory
    , s3client
    , headers = {};

  callback = callback || function() { };

  // Deleting destination because it's not an explicitly named knox option
  delete options.destination;
  s3client = knox.createClient(options);

  log('Attemping to download ' + target + ' from the ' + options.bucket + ' s3 bucket');
  var file = fs.createWriteStream(path.join(destination, target));

  s3client.getFile(sourceFile, headers, function(err, res){
    if(err) {
      return callback(err);
    }

    res.on('data', function(chunk){
      if(res.statusCode !== 200) {
        log(chunk, 'error');
      } else {
        log(chunk);
        file.write(chunk);
      }
    });

    res.on('end', function(chunk) {
      file.end();
      if (res.statusCode !== 200) {
        return callback(new Error('Expected a 200 response from S3, got ' + res.statusCode));
      }
      log('Successfully downloaded from s3');
      return callback();
    });
  });
}

/**
 * deleteFromS3
 *
 * Deletes a file or directory from S3.
 *
 * @param options   s3 options [key, secret, bucket]
 * @param directory directory containing to download contents to
 * @param target    file or directory name of downloaded content
 * @param callback  callback(err)
 */
function deleteFromS3(options, target, callback) {
  var knox = require('knox')
    , sourceFile = path.join(options.destination || '/', target)
    , s3client
    , headers = {};

  callback = callback || function() { };

  // Deleting destination because it's not an explicitly named knox option
  delete options.destination;
  s3client = knox.createClient(options);

  log('Attemping to delete ' + target + ' from the ' + options.bucket + ' s3 bucket');

  s3client.deleteFile(sourceFile, headers, function(err, res){
    if(err) {
      return callback(err);
    }

    res.setEncoding('utf8');

    res.on('data', function(chunk){
      if(res.statusCode !== 204) {
        log(chunk, 'error');
      } else {
        log(chunk);
      }
    });

    res.on('end', function(chunk) {
      if (res.statusCode !== 204) {
        return callback(new Error('Expected a 204 response from S3, got ' + res.statusCode));
      }
      log('Successfully deleted ' + target + ' from s3');
      return callback();
    });
  });
}

/**
 * sync
 *
 * Performs a mongodump on a specified database, gzips the data,
 * and uploads it to s3.
 *
 * @param mongodbConfig   mongodb config [host, port, username, password, db]
 * @param s3Config        s3 config [key, secret, bucket]
 * @param callback        callback(err)
 */
function sync(mongodbConfig, s3Config, callback) {
  var tmpDir = path.join(require('os').tmpDir(), 'mongodb_s3_backup')
    , backupDir = path.join(tmpDir, mongodbConfig.db)
    , archiveName = getArchiveName(mongodbConfig.db)
    , tmpDirCleanupFns;

  callback = callback || function() { };

  tmpDirCleanupFns = [
    async.apply(removeRF, backupDir),
    async.apply(removeRF, path.join(tmpDir, archiveName))
  ];

  async.series(tmpDirCleanupFns.concat([
    async.apply(mongoDump, mongodbConfig, tmpDir),
    async.apply(compressDirectory, tmpDir, mongodbConfig.db, archiveName),
    d.bind(async.apply(sendToS3, s3Config, tmpDir, archiveName)) // this function sometimes throws EPIPE errors
  ]), function(err) {
    if(err) {
      log(err, 'error');
    } else {
      log('Successfully backed up ' + mongodbConfig.db);
    }
    // cleanup folders
    async.series(tmpDirCleanupFns, function() {
      return callback(err);
    });
  });

  // this cleans up folders in case of EPIPE error from AWS connection
  d.on('error', function(err) {
      d.exit()
      async.series(tmpDirCleanupFns, function() {
        throw(err);
      });
  });

}


/**
 * cleanupOldArchives
 *
 * Removes old archives 
 *
 * @param options S3 Config options 
 * @param databaseName The name of the database
 * @param total The total number of archives that should be kept 
 */
function cleanupOldArchives(options, databaseName, total,  callback) {
  var s3Client = knox.createClient(options)
    , newest;

  s3Client.list({}, function(err, data) {
    if(err) {
      log(err, 'error');
    } else {
      data.Contents.sort(function(a,b) {
        return b.LastModified - a.LastModified;
      })
      var list = data.Contents.filter(function(d) {
        return d.Key.indexOf(databaseName) != -1;
      });

      if (list.length > total) {
        var deleteList = list.slice(total)
        var deleteCmds = [];

        for(var item of deleteList) {
          deleteCmds.push(async.apply(deleteFromS3, options, item.Key))
        }

        async.series(deleteCmds, function(err) {
          if(err) {
            log(err, 'error');
          } else {
            log('Successfully cleaned up old archives');
          }
          callback(err);
        });
      } else {
        log('No Archives need to be removed')
        callback();
      }
    }
  });
}

/**
 * restore
 *
 * Performs a mongorestore on a specified database after unzipping the data,
 * and downloading it from s3.
 *
 * @param mongodbConfig   mongodb config [host, port, username, password, db]
 * @param s3Config        s3 config [key, secret, bucket]
 * @param callback        callback(err)
 */
function restore(mongodbConfig, s3Config, callback) {
  var tmpDir = path.join(require('os').tmpDir(), 'mongodb_s3_backup')
    , backupDir = path.join(tmpDir, mongodbConfig.db)
    , tmpDirCleanupFns;

  callback = callback || function() { };

  getLastArchiveName(s3Config, mongodbConfig.db, function(err, archiveName) {
    if( err || !archiveName) {
      return callback(err)
    }

    tmpDirCleanupFns = [
      async.apply(removeRF, backupDir),
      async.apply(removeRF, path.join(tmpDir, archiveName))
    ];

    async.series(tmpDirCleanupFns.concat([
      d.bind(async.apply(retrieveFromS3, s3Config, tmpDir, archiveName)), // this function sometimes throws EPIPE errors
      async.apply(decompressDirectory, tmpDir, mongodbConfig.db, archiveName),
      async.apply(mongoRestore, mongodbConfig, path.join(tmpDir, mongodbConfig.db))
    ]), function(err) {
      if(err) {
        log(err, 'error');
      } else {
        log('Successfully restored ' + mongodbConfig.db);
      }
      // cleanup folders
      async.series(tmpDirCleanupFns, function() {
        return callback(err);
      });
    });
  });

  // this cleans up folders in case of EPIPE error from AWS connection
  d.on('error', function(err) {
      d.exit()
      async.series(tmpDirCleanupFns, function() {
        throw(err);
      });
  });
}

module.exports = { clean: cleanupOldArchives, sync: sync, restore: restore, log: log };
