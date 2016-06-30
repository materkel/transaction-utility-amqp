'use strict';

const chai = require('chai');
const amqp = require('amqplib');
const expect = chai.expect;
const config = {
  url: 'amqp://guest:guest@localhost:5672',
  ack: true
};
const transactionUtilityLib = require('../index');
const transactionUtility = transactionUtilityLib(config)

function cleanUpRabbitMQ(done) {
  amqp
    .connect(config.url)
    .then(conn => {
      conn
        .createChannel()
        .then(ch => {
          return Promise.all([
            ch.deleteQueue('api1'),
            ch.deleteQueue('api2'),
            ch.deleteQueue('api3'),
            ch.deleteQueue('api4'),
            ch.deleteExchange('transactions')
          ]);
        })
        .then(() => {
          done();
        })
    });
}

describe('Transaction Utility', () => {
  before(done => cleanUpRabbitMQ(done));

  it('should generate a transaction id', () => {
    const transactionId = transactionUtility.generateId();
    expect(transactionId).to.be.defined;
    expect(transactionId).to.be.a('string');
  });

  it('should create a transaction listener', done => {
    // Set up listener
    transactionUtility
      .listener('api1', (res) => {})
      .then(() => {
        // Check if listener exists
        amqp
          .connect(config.url)
          .then(conn => {
            conn
              .createChannel()
              .then(ch => {
                ch.checkQueue('api1')
                  .then(res => {
                    done();
                  });
              });
          });
      });
  });

  it('should handle a rollback message', done => {
    const transactionId = transactionUtility.generateId();
    transactionUtility.listener('api2', (res) => {
      res = JSON.parse(res.content.toString());
      expect(res.id).to.be.defined;
      expect(res.action).to.be.defined;
      expect(res.id).to.be.equal(transactionId);
      expect(res.action).to.be.equal('r');
      done();
    })
    // listen for transaction outcome with id 'transaction2' on listener 'api2'
    .then(res => {
      transactionUtility.listen('api2', transactionId);
    })
    .then(res => transactionUtility.rollback(transactionId));
  });

  it('should handle a commit message', done => {
    const transactionId = transactionUtility.generateId();
    transactionUtility.listener('api3', (res) => {
      res = JSON.parse(res.content.toString());
      expect(res.id).to.be.defined;
      expect(res.action).to.be.defined;
      expect(res.id).to.be.equal(transactionId);
      expect(res.action).to.be.equal('c');
      done();
    })
    // listen for transaction outcome with id 'transaction3' on listener 'api3'
    .then(res => transactionUtility.listen('api3', transactionId))
    .then(res => transactionUtility.commit(transactionId));
  });

  it('should reschedule a message after an Error', function(done) {
    this.timeout(30000);
    const transactionId = transactionUtility.generateId();
    transactionUtility.listener('api4', (res) => {
      let initialResponse = res;
      if (initialResponse.fields.deliveryTag < 2) {
        return Promise.reject(new Error('err'));
      } else {
        res = JSON.parse(res.content.toString());
        expect(res.id).to.be.defined;
        expect(res.action).to.be.defined;
        expect(res.id).to.be.equal(transactionId);
        expect(res.action).to.be.equal('r');
        done();
      }
    })
    // listen for transaction outcome with id 'transaction2' on listener 'api2'
    .then(res => {
      transactionUtility.listen('api4', transactionId);
    })
    .then(res => transactionUtility.rollback(transactionId));
  });
});
