import Kafka, {createReadStream} from "node-rdkafka";
import os from "os";
import crypto from "crypto";
import * as highland from 'highland';

export interface RetryHeaders {
    retryCount?: number | boolean;
    origTopic?: string | boolean;
}

export type RawKafkaMessage = {
    value: Buffer,
    headers:Array<any>,
    topic: string,
    offset: number,
    partition: number,
    timestamp: number
}


export class KafkaErrorsHandler {
    private producerManager;
    private retryConsumerTopics = [/^retry_consumer\.[^.]*\.alerts.errors/];
    private kafkaBroker: string = process.env.KAFKA_BROKER || 'localhost:29092';

    constructor() {
      this.init().then(() => {
        console.log("KafkaErrorsHandler initialized");
      });
    }

    private async init(): Promise<highland.Stream<RawKafkaMessage>> {
      await this.initErrorHandlerProducer();
      return this.retryMessageStream(this.defineRetryTopicsConsumer()).resume();
    }

    private async initErrorHandlerProducer() {
        const retryProducer = new Kafka.Producer({
            'metadata.broker.list': this.kafkaBroker,
            'dr_cb': true
        });

        retryProducer.on('ready', function(arg) {
            console.log('producer ready.' + JSON.stringify(arg));
        });

        retryProducer.connect();

        this.producerManager = retryProducer;
    }

    private defineRetryTopicsConsumer = () => (topics:Array<string>, clientId:string) => {
        const stream =  createReadStream({
            'group.id': `${os.hostname()}`,
            'metadata.broker.list': this.kafkaBroker,
            'client.id': `retry-group-${clientId}`,
            'enable.auto.commit': false,
            offset_commit_cb: commitCB()
        }, {'auto.offset.reset': 'earliest'}, {
            topics: topics,
            waitInterval: 0,
            fetchSize: 0
        });

        function commitCB() {
            return (err, topicPartition) => {
                if (err) {
                    topicPartition.forEach(({ topic, partition }) => {
                        console.log('Error in commit for retry kafka message', {
                            error_code: err,
                            topic: topic,
                            partition: partition
                        });
                    });
                }
            };
        }

        stream.on('error', (err) => {
            console.log('Error in kafka retry consumer stream', {
                error_msg: err.message,
                error_name: err.name
            });
        });

        stream.consumer.on('event.error', (err) => {
            console.log('Error in kafka retry consumer', {
                error_msg: err.message
            });

            stream.emit('rd-kafka-error', err);
        });

        return stream;
    };

    private retryMessageStream(kafkaStream): highland.Stream<RawKafkaMessage> {
        const parts = [os.hostname(), process.pid, +(new Date())];
        const hash = crypto.createHash('md5').update(parts.join(''));
        const stream = kafkaStream(this.retryConsumerTopics, hash.digest('hex'));

        function commitOffset (origKafkaMessage:RawKafkaMessage){
            try{
                const {
                    topic, partition, offset
                } = origKafkaMessage;

                stream.consumer.commit({
                    topic: topic,
                    partition: partition,
                    offset: offset + 1
                });

            } catch (e) {
                console.log('Error in commitOffset', e);
            }
        }

        return highland.default<RawKafkaMessage>(stream)
            .errors((err) => {
                console.log(err);
            })
            .flatMap((origKafkaMessage) => {
                return highland.default(this.handleErroredMessage(origKafkaMessage)).map((): RawKafkaMessage => origKafkaMessage);
            })
            .doto(commitOffset)
            .errors((err) => {
                console.log('Error in kafka retry message stream', {
                    error_msg: err.message,
                    error_name: err.name
                });
            })
    }

    async handleErroredMessage(rawMessage: RawKafkaMessage) : Promise<void> {
        let extractedRetryHeaders: RetryHeaders = this.extractRetryHeaders(rawMessage.headers);

        if(!rawMessage.headers || !extractedRetryHeaders.retryCount) { // got the message for first time - send to retry topic
            let messageHeaders = this.defineRetryHeaders(rawMessage);
            let origTopic = rawMessage.topic;
            const retryTopic = `retry_${origTopic}`;
            await this.produceErroredMessage(retryTopic, rawMessage.value, messageHeaders,1000);
        }else{
            if (extractedRetryHeaders.retryCount < 1){
                let retryIndex = rawMessage.headers.findIndex((obj => obj.hasOwnProperty('retryCount')));
                rawMessage.headers[retryIndex].retryCount = Number(extractedRetryHeaders.retryCount) + 1
                await this.produceErroredMessage(extractedRetryHeaders.origTopic,rawMessage.value,rawMessage.headers,3000);
            } else {
                const dlqTopic = `dlq_${extractedRetryHeaders.origTopic}`;
                await this.produceErroredMessage(dlqTopic,rawMessage.value,rawMessage.headers,0);
            }
        }
    }

    private async produceErroredMessage(topic: string | boolean, message: Buffer, headers: Array<any>, delay: number = 0): Promise<void> {
        if(!topic) {
            console.error('Error in kafka retry producer', {
                error_msg: 'Topic is not defined'
            });
            return;
        }
        try {
            console.log(`KafkaErrorsHandler: sleep for ${delay} before producing to topic ${topic}`);
            await this.sleep(delay);
            console.log(`KafkaErrorsHandler: producing message to topic ${topic}`);
            return await this.producerManager.produce(
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

    private sleep = delay => new Promise(resolve => setTimeout(resolve, delay));

    private defineRetryHeaders(rawMessage): any[] {
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

    private extractRetryHeaders(headers) {
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
}