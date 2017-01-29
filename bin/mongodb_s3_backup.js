#!/usr/bin/env node

/* Dependencies */

var cli = require('cli')
  , path = require('path')
  , util = require('util')
  , async = require('async')
  , backup = require('../')
  , cronJob = require('cron').CronJob
  , pkg = require('../package.json')
  , crontab = "0 0 * * *"
  , timezone = "UTC"
  , time = [0, 0]
  , options, configPath, config;

cli
  .enable('version')
  .setApp(pkg.name, pkg.version)
  .setUsage(cli.app + ' [OPTIONS] <path to json config>');

options = cli.parse({
  now:   ['n', 'Run sync on start'],
  restore: ['r', 'Run restore on start']
});

if(cli.args.length !== 1) {
  return cli.getUsage();
}

/* Configuration */

configPath = path.resolve(process.cwd(), cli.args[0]);
backup.log('Loading config file (' + configPath + ')');
config = require(configPath);

function backupAll(callback) {
  callback = callback || function () {};

  var backupFunctions = [];
  if( config.mongodb.constructor === Array ) {
    for ( var mongoConfig of config.mongodb) {
      backupFunctions.push(async.apply(backup.sync, mongoConfig, config.s3));
      if(config.numOfArchives > 0) {
        backupFunctions.push(async.apply(backup.clean, config.s3, mongoConfig.db, config.numOfArchives));
      }
    }
  } else {
    backupFunctions.push(async.apply(backup.sync, config.mongodb, config.s3));
    if(config.numOfArchives > 0) {
      backupFunctions.push(async.apply(backup.clean, config.s3, config.mongodb.db, config.numOfArchives));
    }
  }

  async.series(backupFunctions, callback);
}

function restoreAll(callback) {
  callback = callback || function () {};

  var restoreFunctions = [];
  if( config.mongodb.constructor === Array ) {
    for ( var mongoConfig of config.mongodb) {
      restoreFunctions.push(async.apply(backup.restore, mongoConfig, config.s3));
    }
  } else {
    restoreFunctions.push(async.apply(backup.restore, config.mongodb, config.s3));
  }

  async.series(restoreFunctions, callback);
}

if(options.now) {
  backupAll(function(err) {
    process.exit(err ? 1 : 0);
  });
} else {
  // If the user overrides the default cron behavior
  if(config.cron) {
    if(config.cron.crontab) {
      crontab = config.cron.crontab
    } else if(config.cron.time) {
      time = config.cron.time.split(':')
      crontab = util.format('%d %d * * *', time[1], time[0]);
    }

    if(config.cron.timezone) {
      try {
        require('time'); // Make sure the user has time installed
      } catch(e) {
        backup.log(e, "error");
        backup.log("Module 'time' is not installed by default, install it with `npm install time`", "error");
        process.exit(1);
      }

      timezone = config.cron.timezone;
      backup.log('Overriding default timezone with "' + timezone + '"');
    }
  }

  new cronJob(crontab, function(){
    backupAll(function(err) {
      if(err) {
        backup.log(err);
      }
    });
  }, null, true, timezone);
  backup.log('MongoDB S3 Backup Successfully scheduled (' + crontab + ')');

  if (options.restore) {
    restoreAll(function(err) {
      if(err) {
        process.exit(1)
      } else {
        backup.log('MongoDB S3 Restore Successfully completed');
      }
    })
  }
}
