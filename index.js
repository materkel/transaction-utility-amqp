'use strict';

var uuid = require('uuid');
var amqp = require('amqplib');

module.exports = function () {
  var _ref = arguments.length <= 0 || arguments[0] === undefined ? {} : arguments[0];

  var _ref$url = _ref.url;
  var url = _ref$url === undefined ? 'amqp://localhost' : _ref$url;

  var open = amqp.connect(url);
  var exchange = 'transactions';

  function sendMessage(id, action) {
    return open.then(function (conn) {
      return conn.createChannel();
    }).then(function (ch) {
      ch.publish(exchange, id, new Buffer(JSON.stringify({ id: id, action: action })));
    });
  }

  return {
    generateId: function generateId() {
      return uuid.v1();
    },

    rollback: function rollback(transactionId) {
      sendMessage(transactionId, 'r');
    },

    commit: function commit(transactionId) {
      sendMessage(transactionId, 'c');
    },

    listener: function listener(service, cb) {
      return open.then(function (conn) {
        return conn.createChannel();
      }).then(function (ch) {
        ch.assertExchange(exchange, 'direct', { durable: true });
        return ch.assertQueue(service, { durable: true }).then(function (q) {
          return ch.consume(service, cb, { noAck: true });
        });
      });
    },

    listen: function listen(service, transactionId) {
      return open.then(function (conn) {
        return conn.createChannel();
      }).then(function (ch) {
        return ch.bindQueue(service, exchange, transactionId);
      });
    }
  };
};