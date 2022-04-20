const Kafka = require('node-rdkafka')
const os = require("os");

let producerManager = null;
const retryConsumerTopics = [/^retry_consumer\.[^.]*\.alerts.errors/];
const sleep = delay => new Promise(resolve => setTimeout(resolve, delay));

async function init(){
  producerManager = await initErrorHandlerProducer()
  await initRetryConsumer()
}

async function initErrorHandlerProducer() {
  const retryProducer = new Kafka.Producer({
    'metadata.broker.list': 'localhost:29092',
    'dr_cb': true
  });

  //Wait for the ready event before producing
  retryProducer.on('ready', function(arg) {
    console.log('producer ready.' + JSON.stringify(arg));
  });

  retryProducer.connect();

  return retryProducer;
}

async function manageError(rawMessage) {
  let origTopic = rawMessage.topic;
  let extractedRetryHeaders = extractRetryHeaders(rawMessage.headers);

  if(!rawMessage.headers || !extractedRetryHeaders.retryCount) { // got the message for first time - send to retry topic
    let messageHeaders = defineRetryHeaders(rawMessage);
    const retryTopic = `retry_${origTopic}`;
    await produceErroredMessage(retryTopic, rawMessage.value, messageHeaders,100);
  }else{
    if (extractedRetryHeaders.retryCount < 1){
      let retryIndex = rawMessage.headers.findIndex((obj => obj.hasOwnProperty('retryCount')));
      rawMessage.headers[retryIndex].retryCount = Number(extractedRetryHeaders.retryCount) + 1
      await produceErroredMessage(extractedRetryHeaders.origTopic,rawMessage.value,rawMessage.headers,200);
    } else {
      const dlqTopic = `dlq_${origTopic}`;
      await produceErroredMessage(dlqTopic,rawMessage.value,rawMessage.headers,0);
    }
  }
}

async function produceErroredMessage(topic,message,headers, delay = 0){
  try {
    console.log(`KafkaErrorsHandler: producing message to topic ${topic}`);
    await sleep(delay);
    return await producerManager.produce(
        topic,
        -1,
        Buffer.from(message),
        null,
        null,
        null,
        headers
    );
  } catch (e) {
    throw new Error(e);
  }
}

function initRetryConsumer() {
  let retryStream =  Kafka.createReadStream({
    'group.id': `${os.hostname()}`,
    'metadata.broker.list': 'localhost:29092',
    'enable.auto.commit': false,
    'socket.keepalive.enable': true,
    'partition.assignment.strategy': 'roundrobin',
    'topic.metadata.refresh.interval.ms': 30 * 1000,
    'queued.max.messages.kbytes': 2000,
    'queued.min.messages': 1,
    'fetch.message.max.bytes': 1000,
    'message.max.bytes': 2000,
    'fetch.max.bytes': 3000,
    'retry.backoff.ms': 200,
    retries: 5,
    'fetch.wait.max.ms': 200,
    offset_commit_cb: commitCB()
  }, {'auto.offset.reset': 'earliest'}, {
    topics: retryConsumerTopics,
    waitInterval: 0,
    fetchSize: 0
  })

  retryStream.on('error', (err) => {
    console.error('Error in kafka retry consumer stream', {
      error_msg: err.message,
      error_name: err.name
    });
  });

  retryStream.consumer.on('event.error', (err) => {
    console.error('Error in kafka retry consumer', {
      error_msg: err.message
    });

    retryStream.emit('rd-kafka-error', err);
  });

  retryStream.on('data', async (rawMessage) => {
    await manageError(rawMessage)
    retryStream.consumer.commit({
      topic: rawMessage.topic,
      partition: rawMessage.partition,
      offset: rawMessage.offset + 1
    });
  });

  function commitCB() {
    return (err, topicPartition) => {
      if (err) {
        topicPartition.forEach(({ topic, partition }) => {
          console.error('Error in commit for retry kafka message', {
            error_code: err,
            topic: topic,
            partition: partition
          });
        });
      }
    };
  }
}

function defineRetryHeaders(rawMessage){
  let headersToSet;
  if(!rawMessage.headers){
    headersToSet = [{
      'retryCount': 0
    }, {
      'origTopic': rawMessage.topic
    }]
  }else{
    headersToSet = rawMessage.headers.concat([{
      'retryCount': 0
    }, {
      'origTopic': rawMessage.topic
    }])
  }
  return headersToSet;
}

function extractRetryHeaders(headers) {
  let retryCount = false;
  let origTopic = false;
  if (headers.length >0) {
    let retryObj = headers.find(obj => obj.hasOwnProperty('retryCount'));
    let origTopicObj = headers.find(obj => obj.hasOwnProperty('origTopic'));
    if (retryObj) {
      retryCount = retryObj.retryCount.toString();
    }
    if (origTopicObj) {
      origTopic = origTopicObj.origTopic.toString();
    }
  }
  return {
    retryCount: retryCount,
    origTopic: origTopic
  }
}

module.exports = {
  init,
  manageError
}