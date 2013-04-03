var cluster = require('cluster'),
util = require('util');

var activeWorkers = 0;
var duration = -1;


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
    if (data.duration > duration) {
	duration = data.duration;
    }
   });
  }
  
  cluster.on('exit', function(worker, code, signal) {
   activeWorkers--;
   if (activeWorkers === 0) {
    console.log("Task: " + settings.label+". Elements: " + (settings.max - settings.first + 1) + ". Max duration: " + duration + "ms");
    if (settings.cbk) {
     settings.cbk();
    }
   }
  });
 
  cluster.on('fork', function(worker) {
   activeWorkers++;
  });
  
 } else {
  var fn = doParallel;
  if (settings.isSequential) {
   fn = doSequential;
  }
  
  var job = function(p_cbk) {
   fn(settings.job, function() {
    if (cluster.worker.kill) {
     cluster.worker.kill();
    } else {
     cluster.worker.destroy();
    }
    p_cbk();
   }, settings.params);
  };
  
  stats(job);
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

function stats(job) {
 var start = new Date().getTime();
 job (function () {
  var end = new Date().getTime();
  var duration = end - start;
  process.send({duration: duration, pid: process.pid});
 });
}

module.exports = {
 work: work
};
