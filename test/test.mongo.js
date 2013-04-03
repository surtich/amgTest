var mongodb	= require("mongodb"),
extend	= require("node.extend");

var clientMongo = null;
var colMongoEvents = null;

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

function removeMongoEvents(p_cbk) {
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

function getMongoEvent(p_cbk, uid) {
 var find = {
  uid: uid
 };
 
 colMongoEvents.findOne(find, function(err, res){
  p_cbk(err, res);
 });
 
}

module.exports = {
 connectMongo: connectMongo,
 addIndexMongo: addIndexMongo,
 closeMongo: closeMongo,
 removeMongoEvents: removeMongoEvents,
 insertMongoEvent: insertMongoEvent,
 getMongoEvent: getMongoEvent
};