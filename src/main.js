'use strict'

class Lawos {
  constructor (queueUrl, sqs={}, lambda) {
    this.maxMessages = 10
    this.queueUrl = queueUrl
    this.waitTime = null
    if(typeof queueUrl === 'object'){
      this.maxMessages = queueUrl.maxMessages || 10
      this.queueUrl = queueUrl.queueUrl
      this.waitTime = queueUrl.waitTime || null
      this.greedy = queueUrl.greedy || false
      this.removeReceived = queueUrl.removeReceived || false
    }
    this.aws = {
      sqs: sqs,
      lambda: lambda
    }

    this.done = false

    this.handler = {
      item: () => Promise.resolve(),
      list: () => Promise.resolve()
    }

    this.metrics = {
      processed: 0,
      resolved: 0,
      rejected: 0,
      iterations: 0
    }

    if (!this.queueUrl) {
      throw new Error('Missing URL for SQS Queue')
    }
  }

  invokeLambda (arn, data) {
    return new Promise(resolve => {
      this.aws.lambda.invoke(
        {
          FunctionName: arn,
          InvocationType: 'Event',
          LogType: 'None',
          Payload: JSON.stringify(data)
        },
        (err, res) => {
          resolve(err || res)
        }
      )
    })
  }

  handleKey (key, data) {
    if (typeof this.handler[key] === 'string') {
      return this.invokeLambda(this.handler[key], data)
    }

    return this.handler[key](data)
  }

  handleItem (item) {
    return this.handleKey('item', item)
  }

  handleList (list) {
    return this.handleKey('list', list)
  }

  delete (id) {
    return this.aws.sqs.deleteMessage(
      {
        QueueUrl: this.queueUrl,
        ReceiptHandle: id
      }
    ).promise()
  }

  load () {
    return this.aws.sqs.receiveMessage(
      {
        MaxNumberOfMessages: this.maxMessages,
        AttributeNames: ['All'],
        MessageAttributeNames: ['All'],
        QueueUrl: this.queueUrl,
        WaitTimeSeconds: this.waitTime
      }
    ).promise().then(
      list => {
        this.metrics.iterations += 1
        if (list && list.Messages) {
          return list.Messages
        }

        if(this.greedy){
          this.done = true
          return [];
        }else{
          return this.quit()
        }
      }
    )
      .catch(e => {
        throw e
      })
  }

  list (func) {
    this.handler.list = func

    return this
  }

  item (func) {
    this.handler.item = func

    return this
  }

  process (list) {
    let results = []
    return Promise.all(
      list.map(
        item => {
          this.metrics.processed += 1

          return this.handleItem(item)
          .then(result => {
            this.metrics.resolved += 1
            return {
              item,
              result,
              success: true
            }
          })
          .catch(error => {
            this.metrics.rejected += 1
            return {
              item,
              error,
              success: false
            }
          })
        }
      )
    )
      .then(itemResults => {
        results = itemResults
      }
    )
      .then(
      () => this.handleList(results.map(r => r.item))
    )
      .then(() => Promise.all(
        results.map(
          result => result.success && this.removeReceived ? this.delete(result.item.ReceiptHandle) : null
        )
      )
    )
      .then(() => results)
  }

  quit () {
    return Promise.resolve(this.metrics)
  }

  work (condition) {
    return condition().then(
      stop => {
        if (stop || this.done) {
          return this.quit()
        }

        return this.load().then(
          list => this.process(list)
        ).then(
          () => this.work(condition)
        )
          .catch(e => {
            // make it bubble up
            throw e
          })
      }
    ).catch(
      err => {
        this.metrics.queueError = err
        return this.quit()
      }
    )
  }
}

module.exports = Lawos
