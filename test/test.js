var mongodb	= require("mongodb"),
redis	= require("redis"),
async	= require("async"),
util	= require("util"),
extend	= require("node.extend"),
event = require('./jsonEvents'),
mysql = require('mysql');

console.log("Test Events " + util.inspect(event.stats));

var numEvents = 5;
var first = 1;

var clientMongo = null;
var colMongoEvents = null;

var clientRedis = null;

var clientMySQL = null;

var clientRAM = {};

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
  console.log(label + "  " + (end - start) +"ms");
  p_cbk();
 }, params);
 
}


function doSequential(fn, p_cbk, params) {
 var i = first - 1;
 async.whilst(
  function() {
   i++;
   return i < numEvents + first;
  },
  function(done) {
   fn(done, i, params);
  },
  function(err) {
   p_cbk(err);
  });
}

function doParallel(fn, p_cbk, params) {
 var i = first;
 var count = 0;
 for (; i < numEvents + first; i++) {
  fn(function() {
   count++;
   if (count === numEvents) {
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
 colMongoEvents.insert(event, {w: 1}, function(err, res){
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

async.series([
 function(done) {
  init(done);
 },
 function(done) {
  connectMongo(done);
 }/*,
 function(done) {
  removeMongoEvents(done);
 },
 function(done) {
  addIndexMongo(done);
 },
 function (done) {
  stats(insertMongoEventsParallel, "INDEXED: Inserted " + numEvents + " small events in MongoDB parallel order", done, event.small);
 }*/,
 function (done) {
  stats(getMongoEventsParallel, "INDEXED: Retreived " + numEvents + " small events in MongoDB parallel order", done);
 }/*,
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
  stats(insertRedisEvents, "Inserted " + numEvents + " small events in Redis", done, JSON.stringify(event.small));
 },
 function (done) {
  stats(getRedisEvents, "Retreived " + numEvents + " small events in Redis", done);
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
 },
 function(done) {
  connectMySQL(done);
 },
 function(done) {
  removeMySQLEvents(done);
 },
 function(done) {
  stats(insertMySQLEvents, "Inserted " + numEvents + " small events in MySQL", done, JSON.stringify(event.small));
 },
 function (done) {
  stats(getMySQLEvents, "Retreived " + numEvents + " small events in MySQL", done);
 },
 function(done) {
  closeMySQL(done);
 }*/
 ], function (err, results) {
  if (err) {
   console.log("EXIT WITH ERRORS: " + err);
  }
  return;
 });
