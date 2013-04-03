var redis	= require("redis"),
async	= require("async"),
util	= require("util"),
cluster = require('cluster'),
event = require('./jsonEvents'),
mongoTest = require('./test.mongo'),
redisTest = require('./test.redis'),
ramTest = require('./test.ram'),
mysqlTest = require('./test.mysql'),
clusterTest = require('./test.cluster');

var numEvents = 100000;
var first = 1;
var forks = undefined;

var doMongoTests = true;
var doRedisTests = true;
var doMySQLTests = true;

var writeTests = true;
var readTests = true;

var jobs = [];
var preJobs = [];
var postJobs = [];

tasks();



function job(fns, p_cbk, label) {
 clusterTest.work({
  cbk: p_cbk, 
  forks: forks,
  first: first,
  max: first + numEvents - 1 ,
  jobs: fns,
  label: label
 });
}


function init(p_cbk) {
 process.argv.forEach(
  function (val, index, array) {
   var param;
   param = getParam("-num_events=", val);
   if ( param !== null ) {
    numEvents = parseInt(param, 10);
   }
   
   param = getParam("-start=", val);
   if ( param !== null ) {
    first = parseInt(param, 10);
   }

   param = getParam("-forks=", val);
   if ( param !== null ) {
    forks = parseInt(param, 10);
   }

   param = getParam("-write=", val);
   if ( param !== null ) {
    writeTests = param.toLowerCase() === "yes";
   }
   
   param = getParam("-read=", val);
   if ( param !== null ) {
    readTests = param.toLowerCase() === "yes";
   }

   param = getParam("-mongo=", val);
   if ( param !== null ) {
    doMongoTests = param.toLowerCase() === "yes";
   }
   
   param = getParam("-redis=", val);
   if ( param !== null ) {
    doRedisTests = param.toLowerCase() === "yes";
   }
   
   param = getParam("-mysql=", val);
   if ( param !== null ) {
    doMySQLTests = param.toLowerCase() === "yes";
   }


  }
  );
 p_cbk();
}

function getParam (label, value) {
 var idx = value.indexOf(label);
 if ( idx > -1 ) {
  return value.substring( idx + label.length );
 }
 return null;
}

function tasks() {
 async.series([
  function(done) {
   init(done);
  },function(done) {
   prepare(done);
  },
  function(done) {
   async.forEachSeries(preJobs, function(task, p_cbk) {  
    task(p_cbk);
   }, function (err) {
    done(err);
   }
   );
  },
  function(done) {
   job(jobs, done, 'Tests');
  },
  
  function(done) {
   async.forEachSeries(postJobs, function(task, p_cbk) {
    task(p_cbk);
   },function (err) {
    done(err);
   }
   );
  }
  ],function (err, results) {
   if (err) {
    console.log("EXIT WITH ERRORS: " + err);
   }
   return;
  });
} 

function prepare(done) {
 if (doMongoTests) {
  prepareMongoDB();
 }
 
 if (doRedisTests) {
  prepareRedis();
 }

 if (doMySQLTests) {
  prepareMySQL();
 }
 
 done();
}

function prepareMongoDB() {
 preJobs.push(mongoTest.connectMongo);

 if (cluster.isMaster) {
  if (writeTests) {
   preJobs.push(mongoTest.removeMongoEvents);
  }
  preJobs.push(mongoTest.addIndexMongo);
 }
 
 if (writeTests) {
  jobs.push({
   fn: mongoTest.insertMongoEvent,
   params: event.small
  });
 }
 
 if (readTests) {
  jobs.push({
   fn: mongoTest.getMongoEvent,
   params: event.small
  });
 }
 
 postJobs.push(mongoTest.closeMongo);
}

function prepareRedis() {
 preJobs.push(redisTest.connectRedis);

 if (cluster.isMaster) {
  if (writeTests) {
   preJobs.push(redisTest.removeRedisEvents);
  }
 }
 
 if (writeTests) {
  jobs.push({
   fn: redisTest.insertRedisEvent,
   params: JSON.stringify(event.small)
  });
 }
 
 if (readTests) {
  jobs.push({
   fn: redisTest.getRedisEvent,
   params: JSON.stringify(event.small)
  });
 }
 
 postJobs.push(redisTest.closeRedis);
}

function prepareMySQL() {
 preJobs.push(mysqlTest.connectMySQL);

 if (cluster.isMaster) {
  if (writeTests) {
   preJobs.push(mysqlTest.removeMySQLEvents);
  }
 }
 
 if (writeTests) {
  jobs.push({
   fn: mysqlTest.insertMySQLEvent,
   params: JSON.stringify(event.small)
  });
 }
 
 if (readTests) {
  jobs.push({
   fn: mysqlTest.getMySQLEvent,
   params: JSON.stringify(event.small)
  });
 }
 
 postJobs.push(mysqlTest.closeMySQL);
}
