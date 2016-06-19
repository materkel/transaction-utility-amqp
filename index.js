'use strict';

var uuid = require('uuid');
var amqp = require('amqplib');

/**
 * TransactionUtility Factory
 * @param  {Object} config
 * @param  {string} config.url - amqp connection url
 * @param  {string} config.exchange - name of the exchange the messages will be sent to
 * @return {Object}
 */
module.exports = function (_ref) {
  var _ref$url = _ref.url;
  var url = _ref$url === undefined ? 'amqp://localhost' : _ref$url;
  var _ref$exchange = _ref.exchange;
  var exchange = _ref$exchange === undefined ? 'transactions' : _ref$exchange;

  var open = amqp.connect(url);

  /**
   * Send a message to all listeners for a specific id
   * @param  {string} id
   * @param  {string} action - the message which will be delivered to the listeners
   * @return {Promise}
   */
  function sendMessage(id, action) {
    return open.then(function (conn) {
      return conn.createChannel();
    }).then(function (ch) {
      return ch.publish(exchange, id, new Buffer(JSON.stringify({ id: id, action: action })));
    });
  }

  return {
    /**
     * @return {string}
     */
    generateId: function generateId() {
      return uuid.v1();
    },

    /**
     * @param  {string} transactionId
     * @return {Promise}
     */
    rollback: function rollback(transactionId) {
      sendMessage(transactionId, 'r');
    },

    /**
     * @param  {string} transactionId
     * @return {Promise}
     */
    commit: function commit(transactionId) {
      sendMessage(transactionId, 'c');
    },

    /**
     * Sets up a listener queue, which calls a callback function when a message is received
     * @param  {string} queueName - name of the listener queue
     * @param  {function} fn - function which takes 1 parameter (msg) and is called when a message is received
     * @return {Promise}
     */
    listener: function listener(queueName, fn) {
      return open.then(function (conn) {
        return conn.createChannel();
      }).then(function (ch) {
        ch.assertExchange(exchange, 'direct', { durable: true });
        return ch.assertQueue(queueName, { durable: true }).then(function (q) {
          return ch.consume(queueName, fn, { noAck: true });
        });
      });
    },

    /**
     * Binds a specific transactionId routing path between listener queue and exchange
     * @param  {string} queueName - name of the listener queue
     * @param  {function} fn - function which takes 1 parameter (msg) and is called when a message is received
     * @return {Promise}
     */
    listen: function listen(queueName, transactionId) {
      return open.then(function (conn) {
        return conn.createChannel();
      }).then(function (ch) {
        return ch.bindQueue(queueName, exchange, transactionId);
      });
    },

    /**
     * Unbinds a specific transactionId routing path between listener queue and exchange
     * @param  {string} queueName - name of the listener queue
     * @param  {function} fn - function which takes 1 parameter (msg) and is called when a message is received
     * @return {Promise}
     */
    unbind: function unbind(queueName, transactionId) {
      return open.then(function (conn) {
        return conn.createChannel();
      }).then(function (ch) {
        return ch.unbindQueue(queueName, exchange, transactionId);
      });
    }
  };
};