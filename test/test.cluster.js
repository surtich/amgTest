var cluster = require('cluster'),
util = require('util');

var activeWorkers = 0;


function work(settings) {
 if (!settings) {
  settings = {};
 }
 if (cluster.isMaster) {
  var forks = settings.forks || require('os').cpus().length;
  console.log("forks="+forks);
  var num = parseInt(settings.max / forks, 10);
  for (var i = 0; i < forks ; i++) {
   var first = settings.first + i * num;
   if (i === forks - 1 && settings.max % forks !== 0) {
    num += settings.max % forks;
   }
   cluster.fork({
    num: num + settings.max % forks,
    first: first
   });
  }
  
  cluster.on('exit', function(worker, code, signal) {
   activeWorkers--;
   console.log(worker.process);
   if (activeWorkers === 0) {
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
   fn(settings.job, p_cbk)
  }
  
  stats(job, 'process.pid='+process.pid, function() {
   if (cluster.worker.kill) {
    cluster.worker.kill();
   } else {
    cluster.worker.destroy();
   }
  }
  );
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

function stats(fn, label, p_cbk, params) {
 var start = new Date().getTime();
 fn (function () {
  var end = new Date().getTime();
  var duration = end - start;
  var msg = label + "  " + duration +"ms";
  process.env.duration = duration;
  console.log(msg);
  p_cbk();
 }, params);
}


module.exports = {
 work: work
};