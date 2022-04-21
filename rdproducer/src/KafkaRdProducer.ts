import express from 'express';
import * as http from 'http';
import bodyParser from 'body-parser';
import { Producer } from 'node-rdkafka';

export class KafkaRdProducer {
    private app = express()
    private server = http.createServer(this.app)
    private producer: Producer

    constructor() {
        this.producer = this.createProducer()
    }

    /**
     * Init server middlewares
     * @return {void}
     */
    public async init(): Promise<void> {
        try {
            this.app.use(<any>await bodyParser.json({limit: '10mb'}))
            this.setupListener()
            await this.producer.connect()
            console.info('Kafka client connected')
            this.routes()
        } catch (error) {
            console.log('Error connecting the producer: ', error)
        }
    }

    /**
     * Create Kafka producer
     * @return {Producer}
     */
    private createProducer() : Producer {
        let producer = new Producer({
            'metadata.broker.list': 'localhost:29092',
            'dr_cb': true
        });

        producer.on('ready', function(arg) {
            console.log('producer ready.' + JSON.stringify(arg));
        });

        return producer;
    }

    /**
     * Init server listener
     * @return {void}
     */
    private setupListener():void {
        this.server.listen(process.env.HTTPPORT || 5000, ():void => {
            const {address, port} = <{ port: number; family: string; address: string; }>this.server.address()
            console.info(`running on  ${address}:${port}`)
        });
    }

    /**
     * Init routes
     * @return {void}
     */
    private routes():void {
        this.getRoute()
        this.produceRoute()
    }

    private generatedRandomMessage(messageType: string): string {
        const levelArray = [
            'info',
            'warn',
            'error'
        ];
        const randomLevelNumber = Math.floor(Math.random()*levelArray.length);

        const hostsArray = [
            'aaa.com',
            'bbb.com',
            'ccc.com'
        ];
        const randomHostsNumber = Math.floor(Math.random()*hostsArray.length);

        const checkArray = [
            'db fail',
            'gateway not reachable',
            'API service is down'
        ];
        const randomCheckNumber = Math.floor(Math.random()*checkArray.length);

        const messages = {
            good : JSON.stringify(
                {
                    "logType": "invalid_payload",
                    "level": "error",
                    "componentName": "inbound",
                    "resourceId": "api.test",
                    "organization": "mid_3",
                    "message":"adidas",
                    "payloadId": "",
                    "payload": {"warning":`${levelArray[randomLevelNumber]}`,"host":`${hostsArray[randomHostsNumber]}`,"check":`${checkArray[randomCheckNumber]}`}
                }
            ),
            bad : `Hello there!`
        }

        return messages[messageType]
    }


    getRoute () {
        this.app.get('/', (req, res, next) => {
            return res.status(200).send('Producer server is up and running')
        });
    }

    produceRoute () {
        this.app.post('/send', async (req, res, next) => {
            //const topic = `consumer.midev_${Math.floor(Math.random() * 115)}.alerts.errors`;
            const topic = `consumer.midev_68.alerts.errors`;
            const messageType = req.body.messageType;
            console.log(`Producing message from type ${messageType}`);
            await this.producer.produce(
                topic,
                -1,
                Buffer.from(this.generatedRandomMessage(messageType)),
                null,
                null,
                null,
                [{stam:'stam'}]
            );
            console.log(`Message produced to topic ${topic}`);
            res.send(`Message produced to topic ${topic}`)
        });
    }
}