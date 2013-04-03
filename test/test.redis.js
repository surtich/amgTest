var redis	= require("redis");

var clientRedis = null;

function connectRedis(p_cbk) {
 clientRedis = redis.createClient();
 clientRedis.on("ready", function() {
  p_cbk();
 });
}

function closeRedis(p_cbk) {
 clientRedis.end();
 p_cbk();
}

function removeRedisEvents(p_cbk) {
 clientRedis.flushall( function (didSucceed) {
  p_cbk();        
 });
}

function insertRedisEvent(p_cbk, uid, e) {
 clientRedis.set(String(uid), e, p_cbk);
}

function getRedisEvent(p_cbk, uid) {
 clientRedis.get(String(uid), p_cbk);
}



module.exports = {
 connectRedis: connectRedis,
 closeRedis: closeRedis,
 removeRedisEvents: removeRedisEvents,
 insertRedisEvent: insertRedisEvent,
 getRedisEvent: getRedisEvent
};