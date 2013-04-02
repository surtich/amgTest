var mongodb	= require("mongodb"),
redis	= require("redis"),
async	= require("async"),
util	= require("util"),
extend	= require("node.extend"),
event = require('./jsonEvents'),
mysql = require('mysql'),
cluster =require('cluster');

var numEvents = 50000;
var first = 1;

var clientMongo = null;
var colMongoEvents = null;

var clientRedis = null;

var clientMySQL = null;

var clientRAM = {};

var activeWorkers = 0;

if (cluster.isMaster) {
 init(function() {
  var numCPUs = require('os').cpus().length;
 
  // Fork workers. 
  for (var i = 0; i < numCPUs; i++) {
   var worker = cluster.fork({
    numEvents: numEvents / numCPUs, 
    first: first + i * numEvents / numCPUs
   });
  }
 
  console.log("numCPUs = " + numCPUs);
  console.log("Test Events " + util.inspect(event.stats));

  cluster.on('exit', function(worker, code, signal) {
   activeWorkers--;
   if (activeWorkers === 0) {
   }
  });
 
  cluster.on('fork', function(worker) {
   activeWorkers++;
  });
 
 });
 
} else {
 jobs();
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


function connectMongo(p_cbk) {
 mongodb.Db.connect("mongodb://localhost/amg-database", {}, function(err, client) {
  if(err) { 
   p_cbk(err);
  } else {
   clientMongo = client;
   colMongoEvents = new mongodb.Collection(client, 'events');
   p_cbk(null);
   
  /*clientMongo.dropCollection("events", function(err, result) {
    colMongoEvents = new mongodb.Collection(client, 'events');
    p_cbk(null);
   });*/
  }
 });
}

function connectRedis(p_cbk) {
 clientRedis = redis.createClient();
 clientRedis.on("ready", function() {
  p_cbk();
 });
}

function connectMySQL(p_cbk) {
 clientMySQL = mysql.createPool({
  host: 'localhost',
  database: 'agm',
  user: 'root'
 });
 p_cbk();
}

function addIndexMongo(p_cbk) {
 colMongoEvents.ensureIndex({
  uid:1
 }, {
  unique:true, 
  w:1
 }, function(err, result) {
  p_cbk(err);
 });
}

function closeMongo(p_cbk) {
 clientMongo.close();
 p_cbk();
}

function closeRedis(p_cbk) {
 clientRedis.end();
 p_cbk();
}

function closeMySQL(p_cbk) {
 clientMySQL.end();
 p_cbk();
}

function stats(fn, label, p_cbk, params) {
 var start = new Date().getTime();
 fn (function () {
  var end = new Date().getTime();
  var duration = end - start;
  var msg = label + "  " + duration +"ms";
  console.log(msg);
  p_cbk();
 }, params);
 
}


function doSequential(fn, p_cbk, params) {
 var i = parseInt(process.env.first, 10);
 var num = parseInt(process.env.numEvents, 10);
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
 var num = parseInt(process.env.numEvents, 10);
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


function removeMongoEvents(p_cbk) {
 colMongoEvents.remove({}, {
  w:1
 }, function(err, res){
  p_cbk(err);
 });
}

function removeRedisEvents(p_cbk) {
 clientRedis.flushall( function (didSucceed) {
  p_cbk();        
 });
}

function removeMySQLEvents(p_cbk) {
 clientMySQL.getConnection(function(err, connection) {
  connection.query( 'DELETE FROM events', function(err, rows) {
   connection.end();
   p_cbk(err);
  });
 });
}


function insertMongoEvent(p_cbk, uid, e) {
 var event = extend({}, e);
 event.uid = uid;
 colMongoEvents.insert(event, {
  w: 1
 }, function(err, res){
  if (p_cbk) {
   if (err) {
    console.log("Error in insertMongoEvent: " + err)
   }
   p_cbk(err);
  }
 });
}

function insertRedisEvent(p_cbk, uid, e) {
 clientRedis.set(String(uid), e, p_cbk);
}

function insertMySQLEvent(p_cbk, uid, e) {
 clientMySQL.getConnection(function(err, connection) {
  connection.query( 'INSERT INTO events SET ?', {
   uid: uid, 
   value: e
  }, function(err, rows) {
   connection.end();
   p_cbk(err);
  });
 });
}


function insertRAMEvent(p_cbk, uid, e) {
 var event = extend({}, e);
 event.uid = uid;
 clientRAM[uid] = event;
 p_cbk();
}

function getMongoEvent(p_cbk, uid) {
 var find = {
  uid: uid
 };
 
 colMongoEvents.findOne(find, function(err, res){
  p_cbk(err, res);
 });
 
}

function getRedisEvent(p_cbk, uid) {
 clientRedis.get(String(uid), p_cbk);
}

function getRAMEvent(p_cbk, uid) {
 var event = clientRAM[uid];
 p_cbk();
}

function getMySQLEvent(p_cbk, uid) {
 clientMySQL.getConnection(function(err, connection) {
  connection.query( 'SELECT * FROM events WHERE ?', {
   uid: uid
  }, function(err, results) {
   connection.end();
   p_cbk(err);
  });
 });
}


function insertMongoEventsSequential(p_cbk, event) {
 doSequential(insertMongoEvent, p_cbk, event);
}

function insertMongoEventsParallel(p_cbk, event) {
 doParallel(insertMongoEvent, p_cbk, event);
}

function getMongoEventsSequential(p_cbk) {
 doSequential(getMongoEvent, p_cbk);
}

function getMongoEventsParallel(p_cbk) {
 doParallel(getMongoEvent, p_cbk);
}

function insertRedisEvents(p_cbk, event) {
 doSequential(insertRedisEvent, p_cbk, event);
}

function getRedisEvents(p_cbk) {
 doSequential(getRedisEvent, p_cbk);
}

function insertRAMEvents(p_cbk, event) {
 doParallel(insertRAMEvent, p_cbk, event);
}

function getRAMEvents(p_cbk) {
 doParallel(getRAMEvent, p_cbk);
}

function insertMySQLEvents(p_cbk, event) {
 doParallel(insertMySQLEvent, p_cbk, event);
}

function getMySQLEvents(p_cbk) {
 doParallel(getMySQLEvent, p_cbk);
}

function jobs() {
 async.series([
  /*function(done) {
   connectMongo(done);
  },
 function(done) {
  removeMongoEvents(done);
 },
 function(done) {
  addIndexMongo(done);
 },
  function (done) {
   stats(insertMongoEventsParallel, "INDEXED: Inserted " + process.env.numEvents + " small events in MongoDB", done, event.small);
  },
  function (done) {
   stats(getMongoEventsParallel, "INDEXED: Retreived " + process.env.numEvents + " small events in MongoDB parallel order", done);
  },
  function(done) {
   closeMongo(done);
  },
 function(done) {
  connectRedis(done);
 },
 function(done) {
  removeRedisEvents(done);
 },
 function(done) {
  stats(insertRedisEvents, "Inserted " + process.env.numEvents + " small events in Redis", done, JSON.stringify(event.small));
 },
 function (done) {
  stats(getRedisEvents, "Retreived " + process.env.numEvents + " small events in Redis", done);
 },
 function(done) {
  closeRedis(done);
 },
 function(done) {
  stats(insertRAMEvents, "Inserted " + numEvents + " small events in RAM", done, event.small);
 },
 function(done) {
  stats(insertRAMEvents, "Inserted " + numEvents + " big events in RAM", done, event.big);
 },
 function (done) {
  stats(getRAMEvents, "Retreived " + numEvents + " big events in RAM", done);
 },*/
  function(done) {
   connectMySQL(done);
  }/*,
 function(done) {
  removeMySQLEvents(done);
 }*/,
  function(done) {
   stats(insertMySQLEvents, "Inserted " + process.env.numEvents + " small events in MySQL", done, JSON.stringify(event.small));
  },
  function (done) {
   stats(getMySQLEvents, "Retreived " + process.env.numEvents + " small events in MySQL", done);
  },
  function(done) {
   closeMySQL(done);
  }
  ], function (err, results) {
   if (err) {
    console.log("EXIT WITH ERRORS: " + err);
   }
   if (cluster.worker.kill) {
    cluster.worker.kill();
   } else {
    cluster.worker.destroy();
   }
   
   return;
  }); 
}

