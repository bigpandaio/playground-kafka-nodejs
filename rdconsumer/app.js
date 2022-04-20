const Kafka = require('node-rdkafka')
const express = require('express')
const os = require("os");
const app = express();
const kafkaErrorsHandler = require('./kafkaErrorsHandler');

const topics = [/^consumer\.[^.]*\.alerts.errors/];

kafkaErrorsHandler.init();

const initStream = ()=>{
    let stream =  Kafka.createReadStream({
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
        topics: topics,
        waitInterval: 0,
        fetchSize: 0
    })

    stream.on('error', (err) => {
        console.log('Error in kafka consumer stream', {
            error_msg: err.message,
            error_name: err.name
        });
    });

    stream.consumer.on('event.error', (err) => {
        console.log('Error in kafka consumer', {
            error_msg: err.message
        });

        stream.emit('rd-kafka-error', err);
    });

    return stream;
};

const kafkaStream = initStream();

function commitCB() {
    return (err, topicPartition) => {
        if (err) {
            topicPartition.forEach(({ topic, partition }) => {
                console.log('Error in commit for inbound kafka message', {
                    error_code: err,
                    topic: topic,
                    partition: partition
                });
            });
        }
    };
}

async function processMessage(message) {
    try {
        let messageParsed = JSON.parse(message.value.toString());
        console.log(`Message from topic ${message.topic} processed successfully`);
        return messageParsed;
    } catch (err) {
        console.log('Error in message processing, moving it to retry topic');
        await kafkaErrorsHandler.manageError(message);
        return err;
    }
}

kafkaStream.on('data', async (rawMessage) => {
    const {
        topic, partition, offset
    } = rawMessage;
    await processMessage (rawMessage);
    kafkaStream.consumer.commit({
        topic: topic,
        partition: partition,
        offset: offset + 1
    });
});

app.get('/', (req, res) => res.send('Ready to consume messages!'))

app.listen(5001, () => console.log('Consumer is listening on port 5001!'))