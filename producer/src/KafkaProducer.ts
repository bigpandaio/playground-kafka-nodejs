import express = require('express')
import * as core from 'express-serve-static-core'
import * as http from 'http'
import cors = require('cors')
import bodyParser = require('body-parser')
import { Kafka, Producer, CompressionTypes} from 'kafkajs'
import * as dotenv from "dotenv";
dotenv.config({ path: __dirname+'/../.env' });


export class KafkaProducer {
    private app: core.Express = express()
    private server = http.createServer(this.app)
    private producer: Producer
    private topics:Array<string> = process.env.KAFKATOPICS.split(',')

    constructor() {
        this.producer = this.createProducer()
    }

    /**
     * Init server middlewares
     * @return {void}
     */
    public async init(): Promise<void> {
        try {
            this.app.use(<any>cors())
            this.app.use(<any>bodyParser.json({limit: '10mb'}))
            this.setupListener()
            this.printServerConfig()
            await this.producer.connect()
            console.info('Kafka client connected')
            this.routes()
        } catch (error) {
            console.log('Error connecting the producer: ', error)
        }
    }

    /**
     * Init server
     * @return {void}
     */
     private setupListener():void {
        this.server.listen(process.env.HTTPPORT || 3001, ():void => {
            const {address, port} = <{ port: number; family: string; address: string; }>this.server.address()
            console.info(`running on  ${address}:${port}`)
        });
    }

    private printServerConfig():void {
        const config = {
            KAFKAHOST: process.env.KAFKAHOST,
            KAFKAPORT: process.env.KAFKAPORT,
            KAFKACLIENTID: process.env.KAFKACLIENTID,
            HTTPPORT: process.env.HTTPPORT,
            KAFKATOPICS: process.env.KAFKATOPICS.split(',')
        }

        console.log(`Server config: ${JSON.stringify(config)}`)
    }

    /**
     * Init routes
     * @return {void}
     */
     private routes():void {
        this.healthCheck()
        this.batchRoute()
        this.singleRoute()
        this.stopRoute()
        this.badSingleRoute()

    }

    private async shutdown(): Promise<void> {
        await this.producer.disconnect()
    }

    private async sendBatchMessage(): Promise<void> {
        try{
            const randomNumber = this.getRandomNumber()
            const pushRes = await this.producer.send({
                topic:this.topics[0],
                compression: CompressionTypes.GZIP,
                messages: Array(randomNumber)
                    .fill(randomNumber)
                    .map((unused,index) => {
                        return this.createMessage(randomNumber,index)
                    }),
            })
            console.log(pushRes)
        } catch (e) {
            console.error(`[error/producer] ${e.message}`, e)
        }

                
    }

    private async sendSingleMessage(): Promise<void> {
        try{
            const pushRes = await this.producer.send({
                topic:this.topics[1],
                compression: CompressionTypes.GZIP,
                messages: [
                    {
                        key: 'single-key',
                        value: JSON.stringify({entry:`new single message on - ${new Date().toISOString()}`})
                    }
                ]
            })
            console.log(pushRes)
        } catch (e) {
            console.error(`[error/producer] ${e.message}`, e)
        }

                
    }

    private async sendBadSingleMessage(): Promise<void> {
        return this.producer
            .send({
                topic:this.topics[1],
                compression: CompressionTypes.GZIP,
                messages: [
                    {
                        key: 'single-key',
                        value: `new single message on - ${new Date().toISOString()}`
                    }
                ]
            })
            .then(console.log)
            .catch(e => console.error(`[example/producer] ${e.message}`, e))


    }

    private getRandomNumber():number { return Math.round(Math.random() * 1000) }

    private createMessage(num:number,index:number):{key:string,value:string} {
        return {
            key: `key-${num}`,
            value: JSON.stringify({
                entry: `value-${num}-${new Date().toISOString()}`,
                order: `messageOrder: #${index}`
            })
          }
    }


    healthCheck() {
        this.app.get('/health', (req, res, next) => {
            return res.status(200).send(`Hello from kafka producer`)
        });
    }

    batchRoute () {
        this.app.get('/batch', async (req, res, next) => {
            const sentMessage = await this.sendBatchMessage();
            return res.status(200).json(sentMessage);
        });
    }

    singleRoute () {
        this.app.get('/single', async (req, res, next) => {
            const sentMessage = await this.sendSingleMessage();
            return res.status(200).json(sentMessage);
        });
    }

    badSingleRoute () {
        this.app.get('/single-bad', async (req, res, next) => {
            const sentMessage = await this.sendBadSingleMessage();
            return res.status(200).json(sentMessage);
        });
    }

    stopRoute () {
        this.app.get('/start', async (req, res, next) => {
            await this.shutdown()
        });
    }

    private createProducer() : Producer {
        const kafka = new Kafka({
            clientId: `${process.env.KAFKACLIENTID}`,
            brokers: [`${process.env.KAFKAHOST}:${process.env.KAFKAPORT}`]
        })

        return kafka.producer()
    }
}
