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

var numEvents = 16;
var first = 1;

var activeWorkers = 0;

function job(p_cbk) {
 console.log('aqui='+process.pid)
 p_cbk();
}

async.series([
 function(done) {
  clusterTest.work({
   cbk: done, 
   forks: 4,
   first: 1,
   max: 14,
   job: job
  }) ;
 }
 ]);

