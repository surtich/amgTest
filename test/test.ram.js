var clientRAM = {};

function insertRAMEvent(p_cbk, uid, e) {
 var event = extend({}, e);
 event.uid = uid;
 clientRAM[uid] = event;
 p_cbk();
}

function getRAMEvent(p_cbk, uid) {
 var event = clientRAM[uid];
 p_cbk();
}

module.exports = {
 insertRAMEvent: insertRAMEvent,
 getRAMEvent: getRAMEvent
};