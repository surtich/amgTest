var mongodb	= require("mongodb"),
redis	= require("redis"),
async	= require("async"),
util	= require("util"),
extend	= require("node.extend"),
event = require('./jsonEvents');

console.log("Test Events " + util.inspect(event.stats));

var NUM_EVENTS = 50000;

var clientMongo = null;
var colMongoEvents = null;

var clientRedis = null;

var clientRAM = {};


function connectMongo(p_cbk) {
 mongodb.Db.connect("mongodb://localhost/amg-database2", {}, function(err, client) {
  if(err) { 
   p_cbk(err);
  } else {
   clientMongo = client;
   clientMongo.dropCollection("events", function(err, result) {
    colMongoEvents = new mongodb.Collection(client, 'events');
    p_cbk(null);
   });
  }
 });
}

function connectRedis(p_cbk) {
 clientRedis = redis.createClient();
 clientRedis.on("ready", function() {
  p_cbk();
 });
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

function stats(fn, label, p_cbk, params) {
 var start = new Date().getTime();
 fn (function () {
  var end = new Date().getTime();
  console.log(label + "  " + (end - start) +"ms");
  p_cbk();
 }, params);
 
}


function doSequential(fn, p_cbk, params) {
 var i = 0;
 async.whilst(
  function() {
   i++;
   return i <= NUM_EVENTS;
  },
  function(done) {
   fn(done, i, params);
  },
  function(err) {
   p_cbk(err);
  });
}

function doParallel(fn, p_cbk, params) {
 var i = 1;
 var count = 0;
 for (; i <= NUM_EVENTS; i++) {
  fn(function() {
   count++;
   if (count === NUM_EVENTS) {
    p_cbk();
    return;
   }
  }, i, params);
 }
}


function removeEvents(p_cbk) {
 colMongoEvents.remove({}, {
  w:1
 }, function(err, res){
  p_cbk(err);
 });
}

function insertMongoEvent(p_cbk, uid, e) {
 var event = extend({}, e);
 event.uid = uid;
 colMongoEvents.insert(event, {
  w:1
 }, function(err, res){
  if (p_cbk) {
   p_cbk(err);
  }
 });
}

function insertRedisEvent(p_cbk, uid, e) {
 var event = extend({}, e);
 event.uid = String(uid);
 clientRedis.set(event.uid, JSON.stringify(event), p_cbk);
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

async.series([
 function(done) {
  connectMongo(done);
 },
 function(done) {
  removeEvents(done);
 },
 function (done) {
  stats(insertMongoEventsSequential, "Inserted " + NUM_EVENTS + " small events in MongoDB sequential order", done, event.small);
 },
 function(done) {
  removeEvents(done);
 },
 function (done) {
  stats(insertMongoEventsParallel, "Inserted " + NUM_EVENTS + " small events in MongoDB parallel order", done, event.small);
 },
 function(done) {
  removeEvents(done);
 },
 function (done) {
  stats(insertMongoEventsSequential, "Inserted " + NUM_EVENTS + " big events in MongoDB sequential order", done, event.big);
 },
 function(done) {
  removeEvents(done);
 },
 function (done) {
  stats(insertMongoEventsParallel, "Inserted " + NUM_EVENTS + " big events in MongoDB parallel order", done, event.big);
 },
 function (done) {
  stats(getMongoEventsSequential, "Retreived " + NUM_EVENTS + " big events in MongoDB sequential order", done);
 },
 function (done) {
  stats(getMongoEventsParallel, "Retreived " + NUM_EVENTS + " big events in MongoDB parallel order", done);
 },
 function(done) {
  removeEvents(done);
 },
 function(done) {
  addIndexMongo(done);
 },
 function (done) {
  stats(insertMongoEventsSequential, "INDEXED: Inserted " + NUM_EVENTS + " big events in MongoDB sequential order", done, event.big);
 },
 function(done) {
  removeEvents(done);
 },
 function (done) {
  stats(insertMongoEventsParallel, "INDEXED: Inserted " + NUM_EVENTS + " big events in MongoDB parallel order", done, event.big);
 },
 function (done) {
  stats(getMongoEventsSequential, "INDEXED: Retreived " + NUM_EVENTS + " big events in MongoDB sequential order", done);
 },
 function (done) {
  stats(getMongoEventsParallel, "INDEXED: Retreived " + NUM_EVENTS + " big events in MongoDB parallel order", done);
 },
 function(done) {
  closeMongo(done);
 },
 function(done) {
  connectRedis(done);
 },
 function(done) {
  stats(insertRedisEvents, "Inserted " + NUM_EVENTS + " small events in Redis", done, event.small);
 },
 function(done) {
  stats(insertRedisEvents, "Inserted " + NUM_EVENTS + " big events in Redis", done, event.big);
 },
 function (done) {
  stats(getRedisEvents, "Retreived " + NUM_EVENTS + " big events in Redis", done);
 },
 function(done) {
  stats(insertRAMEvents, "Inserted " + NUM_EVENTS + " small events in RAM", done, event.small);
 },
 function(done) {
  stats(insertRAMEvents, "Inserted " + NUM_EVENTS + " big events in RAM", done, event.big);
 },
 function (done) {
  stats(getRAMEvents, "Retreived " + NUM_EVENTS + " big events in RAM", done);
 },
 function(done) {
  closeRedis(done);
 }
 ]);


