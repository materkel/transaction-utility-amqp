const uuid = require('uuid');
const amqp = require('amqplib');
const retry = require('amqplib-retry');

/**
 * TransactionUtility Factory
 * @param  {Object} config
 * @param  {string} [config.url = 'amqp://localhost'] - amqp connection url
 * @param  {string} [config.exchange = 'transactions'] - name of the exchange the messages will be sent to
 * @return {Object}
 */
module.exports = ({ url: url = 'amqp://localhost', exchange: exchange = 'transactions', ack: ack = false } = {}) => {
  const open = amqp.connect(url);
  const noAck = !ack;

  /**
   * Send a message to all listeners for a specific id
   * @param  {string} id
   * @param  {string} msg - the message which will be delivered to the listeners
   * @return {Promise}
   */
  function send(id, action) {
    return open
      .then(conn => conn.createChannel())
      .then(ch => ch.publish(exchange, id, new Buffer(JSON.stringify({ id, action }))));
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
    return open
      .then(conn => conn.createChannel())
      .then(ch => {
        ch.assertExchange(exchange, 'direct', { durable: true });
        return ch.assertQueue(queueName, { durable: true })
          .then(q => ch.consume(queueName, fn, { noAck }));
      });
  }

  /**
   * Binds a specific transactionId routing path between listener queue and exchange
   * @param  {string} queueName - name of the listener queue
   * @param  {function} fn - function which takes 1 parameter (msg) and is called when a message is received
   * @return {Promise}
   */
  function listen(queueName, transactionId) {
    return open
      .then(conn => conn.createChannel())
      .then(ch => ch.bindQueue(queueName, exchange, transactionId));
  }

  /**
   * Unbinds a specific transactionId routing path between listener queue and exchange
   * @param  {string} queueName - name of the listener queue
   * @param  {function} fn - function which takes 1 parameter (msg) and is called when a message is received
   * @return {Promise}
   */
  function unbind(queueName, transactionId) {
    return open
      .then(conn => conn.createChannel())
      .then(ch => ch.unbindQueue(queueName, exchange, transactionId));
  }

  return {
    generateId,
    send,
    commit,
    rollback,
    listener,
    listen,
    unbind
  }
};
