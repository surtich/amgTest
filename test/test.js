var redis	= require("redis"),
async	= require("async"),
util	= require("util")
event = require('./jsonEvents'),
mongoTest = require('./test.mongo'),
redisTest = require('./test.redis'),
ramTest = require('./test.ram'),
mysqlTest = require('./test.mysql'),
clusterTest = require('./test.cluster');

var numEvents = 7;
var first = 3;
var forks = undefined;

var activeWorkers = 0;

tasks();

function job(fn, p_cbk, label, myEvent) {
 clusterTest.work({
  cbk: p_cbk, 
  forks: forks,
  first: first,
  max: first + numEvents - 1 ,
  job: fn,
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
   mongoTasks(done);
  }
 ],function (err, results) {
   if (err) {
    console.log("EXIT WITH ERRORS: " + err);
   }
   return;
  });
}

function x(p_cbk,i,z) {
    console.log("aqui="+process.pid);
    console.log(z);
    p_cbk();
  }

function mongoTasks(p_cbk) {
 async.series([
  function(done) {
   mongoTest.connectMongo(done);
  },
  function(done) {
   mongoTest.removeMongoEvents(done);
  },
  function(done) {
   mongoTest.addIndexMongo(done);
  },
  function(done) {
   job(mongoTest.insertMongoEvent, done, 'This is a test');
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
