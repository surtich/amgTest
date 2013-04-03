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

var numEvents = 1000;
var first = 1;
var forks = undefined;

var activeWorkers = 0;

tasks();

var jobs = [];

function job(fns, p_cbk, label, myEvent) {
 clusterTest.work({
  cbk: p_cbk, 
  forks: forks,
  first: first,
  max: first + numEvents - 1 ,
  jobs: fns,
  label: label,
  params: myEvent || event.small
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
  },
  function(done) {
   preTasks(done);
  },
  function(done) {
   jobs.push({fn: mongoTest.insertMongoEvent});
   jobs.push({fn: mongoTest.getMongoEvent});
   job(jobs, done, 'MongoDB tests');
  },
  function(done) {
   postTasks(done);
  }
 ],function (err, results) {
   if (err) {
    console.log("EXIT WITH ERRORS: " + err);
   }
   return;
  });
}

function preTasks(p_cbk) {
 async.series([
  function(done) {
   mongoTest.connectMongo(done);
  },
  function(done) {
   if (cluster.isMaster) {
    mongoTest.removeMongoEvents(done);
   } else {
    done();
   }
  },
  function(done) {
   if (cluster.isMaster) {
    mongoTest.addIndexMongo(done);
   } else {
    done();
   }
  },
  function(done) {
   mongoTest.closeMongo(done);
  }
 ],function (err, results) {
   if (err) {
    console.log("EXIT WITH ERRORS: " + err);
   }
   p_cbk();
  });
}


function postTasks(p_cbk) {
 async.series([  
  function(done) {
   mongoTest.closeMongo(done);
  }
 ],function (err, results) {
   if (err) {
    console.log("EXIT WITH ERRORS: " + err);
   }
   p_cbk();
  });
}
