'use strict';

var uuid = require('uuid');
var amqp = require('amqplib');
var retry = require('amqplib-retry');

/**
 * TransactionUtility Factory
 * @param  {Object} config
 * @param  {string} [config.url = 'amqp://localhost'] - amqp connection url
 * @param  {string} [config.exchange = 'transactions'] - name of the exchange the messages will be sent to
 * @return {Object}
 */
module.exports = function () {
  var _ref = arguments.length <= 0 || arguments[0] === undefined ? {} : arguments[0];

  var _ref$url = _ref.url;
  var url = _ref$url === undefined ? 'amqp://localhost' : _ref$url;
  var _ref$exchange = _ref.exchange;
  var exchange = _ref$exchange === undefined ? 'transactions' : _ref$exchange;
  var _ref$ack = _ref.ack;
  var ack = _ref$ack === undefined ? false : _ref$ack;

  var open = amqp.connect(url);
  var noAck = !ack;

  /**
   * Send a message to all listeners for a specific id
   * @param  {string} id
   * @param  {string} msg - the message which will be delivered to the listeners
   * @return {Promise}
   */
  function send(id, action) {
    return open.then(function (conn) {
      return conn.createChannel();
    }).then(function (ch) {
      return ch.publish(exchange, id, new Buffer(JSON.stringify({ id: id, action: action })));
    });
  }

  /**
   * @return {string}
   */
  function generateId() {
    return uuid.v1();
  }

  /**
   * @param  {string} transactionId
   * @return {Promise}
   */
  function rollback(transactionId) {
    return send(transactionId, 'r');
  }

  /**
   * @param  {string} transactionId
   * @return {Promise}
   */
  function commit(transactionId) {
    return send(transactionId, 'c');
  }

  /**
   * Sets up a listener queue, which calls a callback function when a message is received
   * @param  {string} queueName - name of the listener queue
   * @param  {function} fn - function which takes 1 parameter (msg) and is called when a message is received
   * @return {Promise}
   */
  function listener(queueName, fn) {
    return open.then(function (conn) {
      return conn.createChannel();
    }).then(function (ch) {
      ch.assertExchange(exchange, 'direct', { durable: true });
      return ch.assertQueue(queueName, { durable: true }).then(function (q) {
        return ch.consume(queueName, fn, { noAck: noAck });
      });
    });
  }

  /**
   * Binds a specific transactionId routing path between listener queue and exchange
   * @param  {string} queueName - name of the listener queue
   * @param  {function} fn - function which takes 1 parameter (msg) and is called when a message is received
   * @return {Promise}
   */
  function listen(queueName, transactionId) {
    return open.then(function (conn) {
      return conn.createChannel();
    }).then(function (ch) {
      return ch.bindQueue(queueName, exchange, transactionId);
    });
  }

  /**
   * Unbinds a specific transactionId routing path between listener queue and exchange
   * @param  {string} queueName - name of the listener queue
   * @param  {function} fn - function which takes 1 parameter (msg) and is called when a message is received
   * @return {Promise}
   */
  function unbind(queueName, transactionId) {
    return open.then(function (conn) {
      return conn.createChannel();
    }).then(function (ch) {
      return ch.unbindQueue(queueName, exchange, transactionId);
    });
  }

  return {
    generateId: generateId,
    send: send,
    commit: commit,
    rollback: rollback,
    listener: listener,
    listen: listen,
    unbind: unbind
  };
};