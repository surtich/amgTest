var cluster = require('cluster'),
async	= require("async"),
util = require('util');

var activeWorkers = 0;
var durations = {};


function work(settings) {
 if (!settings) {
  settings = {};
 }
 if (cluster.isMaster) {
  var forks = settings.forks || require('os').cpus().length;
  console.log("forks="+forks); 
  var num = parseInt((settings.max - settings.first + 1) / forks, 10);
  for (var i = 0; i < forks ; i++) {
   var first = settings.first + i * num;
   var worker = cluster.fork({
    num: i < forks - 1 ? num : num + (settings.max - settings.first + 1) % forks,
    first: first
   });
   worker.on('message', function(data) {
    //console.log("pid=" + data.pid + ", duration=" + data.duration +"ms");
    if (durations[data.key] === undefined || data.duration > durations[data.key]) {
     durations[data.key] = data.duration;
    }
   });
  }
  
  cluster.on('exit', function(worker, code, signal) {
   activeWorkers--;
   if (activeWorkers === 0) {        
    console.log("Task: [" + settings.label+"] Elements: [" + (settings.max - settings.first + 1) + "] Max durations (in ms): " + util.inspect(durations));
    if (settings.cbk) {
     settings.cbk();
    }
   }
  });
 
  cluster.on('fork', function(worker) {
   activeWorkers++;
  });
  
 } else {
  async.forEachSeries(settings.jobs, function(job, p_cbk) {   
     
   var fn = job.multiplier || doParallel;

   stats(job.fn.name, function(after) {
    fn(job.fn, function() {
     after(p_cbk);
    }, job.params);
   });
  }, function(err) {
   if (cluster.worker.kill) {
    cluster.worker.kill();
   } else {
    cluster.worker.destroy();
   };
  });
 }
}

function doSequential(fn, p_cbk, params) {
 var i = parseInt(process.env.first, 10);
 var num = parseInt(process.env.num, 10);
 var top = num + parseInt(process.env.first, 10);
 async.whilst(
  function() {
   return i < top;
  },
  function(done) {
   fn(done, i, params);
   i++;
  },
  function(err) {
   p_cbk(err);
  });
}

function doParallel(fn, p_cbk, params) {
 var i = parseInt(process.env.first, 10);
 var num = parseInt(process.env.num, 10);
 var max = num + i;
 var count = 0;
 for (; i < max; i++) {
  fn(function() {
   count++;
   if (count === num) {
    p_cbk();
    return;
   }
  }, i, params);
 }
}

function stats(key, job) {
 var start = new Date().getTime();
 job (function (p_cbk) {
  var end = new Date().getTime();
  var duration = end - start;
  process.send({
   duration: duration,   
   pid: process.pid,
   key: key
  });
  p_cbk();
 });
}

module.exports = {
 work: work,
 doSequential: doSequential,
 doParallel: doParallel
};
