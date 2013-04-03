var mysql = require('mysql');

var clientMySQL = null;

function connectMySQL(p_cbk) {
 clientMySQL = mysql.createPool({
  host: 'localhost',
  database: 'agm',
  user: 'root',
  password: '1'
 });
 p_cbk();
}

function closeMySQL(p_cbk) {
 clientMySQL.end();
 p_cbk();
}

function removeMySQLEvents(p_cbk) {
 clientMySQL.getConnection(function(err, connection) {
  connection.query( 'DELETE FROM events', function(err, rows) {
   connection.end();
   p_cbk(err);
  });
 });
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

module.exports = {
 connectMySQL: connectMySQL,
 closeMySQL: closeMySQL,
 removeMySQLEvents: removeMySQLEvents,
 insertMySQLEvent: insertMySQLEvent,
 getMySQLEvent: getMySQLEvent
};