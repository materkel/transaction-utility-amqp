const uuid = require('uuid');
const amqp = require('amqplib');

module.exports = ({ url: url = 'amqp://localhost' } = {}) => {
  const open = amqp.connect(url);
  const exchange = 'transactions';

  function sendMessage(id, action) {
    return open
      .then(conn => {
        let ok = conn.createChannel()
        ok = ok.then(ch => {
          ch.assertExchange(exchange, 'direct', {durable: false});
          ch.publish(exchange, id, new Buffer(JSON.stringify({ id, action })));
        });
        return ok;
      });
  }

  return {
    generateId: () => uuid.v1(),

    rollback: (transactionId) => {
      sendMessage(transactionId, 'r');
    },

    commit: (transactionId) => {
      sendMessage(transactionId, 'c');
    },

    listener: (service, cb) => {
      return open
        .then(conn => {
            conn
              .createChannel()
              .then(ch => {
                ch.assertExchange(exchange, 'direct', {durable: false});
                ch.assertQueue(service).then(q => {
                  ch.consume(service, cb, { noAck: true });
                });
              });
        });
    },

    listen: (service, transactionId) => {
      return open
        .then(conn => {
          conn
            .createChannel()
            .then(ch => {
              ch.bindQueue(service, exchange, transactionId);
            });
        });
    }
  }
};
