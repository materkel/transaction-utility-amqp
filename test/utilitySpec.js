'use strict';

const chai = require('chai');
const amqp = require('amqplib');
const expect = chai.expect;
const config = {
  url: 'amqp://guest:guest@localhost:5672'
};
const transactionUtilityLib = require('../index');
const transactionUtility = transactionUtilityLib(config)

describe('Transaction Utility', () => {
  it('should generate a transaction id', () => {
    const transactionId = transactionUtility.generateId();
    expect(transactionId).to.be.defined;
    expect(transactionId).to.be.a('string');
  });

  it('should create a transaction listener', done => {
    // Set up listener
    transactionUtility.listener('api1', (res) => {});
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
});
