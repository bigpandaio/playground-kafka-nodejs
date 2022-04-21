import express from 'express';
import * as http from 'http';
import bodyParser from 'body-parser';
import { createReadStream } from "node-rdkafka";
import os from "os";
import * as highland from 'highland';
import crypto from 'crypto';
import { KafkaErrorsHandler } from "./KafkaErrorsHandler";

export type RawKafkaMessage = { value: Buffer, topic: string, offset: number, partition: number, timestamp: number, headers:Array<any>, }

export class KafkaRdConsumer {
    private app = express()
    private server = http.createServer(this.app)
    private stream
    private kafkaErrorHandler: KafkaErrorsHandler

    /**
     * Init server middlewares
     * @return {void}
     */
    public async init(): Promise<void> {
        try {
            this.app.use(<any>await bodyParser.json({limit: '10mb'}));
            this.setupListener();
            this.kafkaErrorHandler = new KafkaErrorsHandler();
            console.info('Kafka client connected');
            this.stream = await this.defineConsumer()
            this.initMessagesStream()
            this.getRoute();
        } catch (error) {
            console.log('Error in init flow: ', error);
        }
    }

    /**
     * Init server listener
     * @return {void}
     */
    private setupListener():void {
        this.server.listen(process.env.HTTPPORT || 5001, ():void => {
            const {address, port} = <{ port: number; family: string; address: string; }>this.server.address();
            console.info(`running on  ${address}:${port}`);
        });
    }

    getRoute() {
        this.app.get('/', (req, res) => {
            return res.status(200).send('Ready to consume messages!');
        });
    }

    private defineConsumer = () => (topics:Array<string>, clientId:string) => {
        const stream =  createReadStream({
            'group.id': `${os.hostname()}`,
            'metadata.broker.list': 'localhost:29092',
            'client.id': `group1-${clientId}`,
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
                        console.log('Error in commit for inbound kafka message', {
                            error_code: err,
                            topic: topic,
                            partition: partition
                        });
                    });
                }
            };
        }

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

    private initMessagesStream(){
        return this.startMessageStream(this.stream).resume();
    }

    /**
     * Start consuming messages
     * @return {void}
     */
    startMessageStream(kafkaStream) {
        const parts = [os.hostname(), process.pid, +(new Date())];
        const hash = crypto.createHash('md5').update(parts.join(''));
        const topics = [/^consumer\.[^.]*\.alerts.errors/];
        const stream = kafkaStream(topics, hash.digest('hex'));

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
                return highland.default(this.processMessage(origKafkaMessage)).map((): RawKafkaMessage => origKafkaMessage);
            })
            .doto(commitOffset)
            .errors((err) => {
                console.log('Error in kafka message stream', {
                    error_msg: err.message,
                    error_name: err.name
                });
            })
    }

    private async processMessage(origKafkaMessage:RawKafkaMessage): Promise<RawKafkaMessage> {
        try {
            JSON.parse(origKafkaMessage.value.toString());
            console.log(`Message from topic ${origKafkaMessage.topic} processed successfully`);
            return origKafkaMessage;
        } catch (err) {
            console.log(`Error in processing message from topic ${origKafkaMessage.topic}, moving it to retry topic`);
            await this.kafkaErrorHandler.handleErroredMessage(origKafkaMessage);
            return origKafkaMessage;
        }
    }
}