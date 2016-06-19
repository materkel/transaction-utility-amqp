const uuid = require('uuid');
const amqp = require('amqplib');

/**
 * TransactionUtility Factory
 * @param  {Object} config
 * @param  {string} config.url - amqp connection url
 * @param  {string} config.exchange - name of the exchange the messages will be sent to
 * @return {Object}
 */
module.exports = ({ url: url = 'amqp://localhost', exchange: exchange = 'transactions' }) => {
  const open = amqp.connect(url);

  /**
   * Send a message to all listeners for a specific id
   * @param  {string} id
   * @param  {string} action - the message which will be delivered to the listeners
   * @return {Promise}
   */
  function sendMessage(id, action) {
    return open
      .then(conn => conn.createChannel())
      .then(ch => ch.publish(exchange, id, new Buffer(JSON.stringify({ id, action }))));
  }

  return {
    /**
     * @return {string}
     */
    generateId: () => uuid.v1(),

    /**
     * @param  {string} transactionId
     * @return {Promise}
     */
    rollback: (transactionId) => {
      sendMessage(transactionId, 'r');
    },

    /**
     * @param  {string} transactionId
     * @return {Promise}
     */
    commit: (transactionId) => {
      sendMessage(transactionId, 'c');
    },

    /**
     * Sets up a listener queue, which calls a callback function when a message is received
     * @param  {string} queueName - name of the listener queue
     * @param  {function} fn - function which takes 1 parameter (msg) and is called when a message is received
     * @return {Promise}
     */
    listener: (queueName, fn) => {
      return open
        .then(conn => conn.createChannel())
        .then(ch => {
          ch.assertExchange(exchange, 'direct', {durable: true});
          return ch.assertQueue(queueName, {durable: true})
            .then(q => ch.consume(queueName, fn, { noAck: true }));
        });
    },

    /**
     * Binds a specific transactionId routing path between listener queue and exchange
     * @param  {string} queueName - name of the listener queue
     * @param  {function} fn - function which takes 1 parameter (msg) and is called when a message is received
     * @return {Promise}
     */
    listen: (queueName, transactionId) => {
      return open
        .then(conn => conn.createChannel())
        .then(ch => ch.bindQueue(queueName, exchange, transactionId));
    },

    /**
     * Unbinds a specific transactionId routing path between listener queue and exchange
     * @param  {string} queueName - name of the listener queue
     * @param  {function} fn - function which takes 1 parameter (msg) and is called when a message is received
     * @return {Promise}
     */
    unbind: (queueName, transactionId) => {
      return open
        .then(conn => conn.createChannel())
        .then(ch => ch.unbindQueue(queueName, exchange, transactionId));
    }
  }
};
