import express = require('express')
import * as core from 'express-serve-static-core'
import * as http from 'http'
import cors = require('cors')
import bodyParser = require('body-parser')
import * as dotenv from "dotenv";
dotenv.config({ path: __dirname+'/../.env' })
import {KafkaConsumer} from "./KafkaConsumer";

export class WebServer{
    private app: core.Express = express()
    private server = http.createServer(this.app)
    private kafkaConsumer:KafkaConsumer
    readonly autoJoin:boolean

    /**
     * @constructor
     */
    public constructor() {
        this.autoJoin = (process.env.KAFKACONSUMERAUTOJOIN !== 'false')
    }

    /**
     * Init routes, configuration and server listener
     * @return {void}
     */
    public async init(): Promise<void> {
        this.configApp()
        this.routes()
        this.setupListener()
        this.printServerConfig()
        this.kafkaConsumer = new KafkaConsumer()
        if(this.autoJoin) await this.initConsumer()
        return;
    }

    /**
    * Init server middlewares
    * @return {void}
    */
    private configApp():void {
        this.app.use(<any>cors())
        this.app.use(<any>bodyParser.json({limit: '10mb'}))
    }

    /**
     * Init routes
     * @return {void}
     */
    private routes():void {
        this.healthCheck()
        this.shutdownRoute()
        this.startRoute()
    }

    /**
     * Init server
     * @return {void}
     */
    private setupListener():void {
        this.server.listen(process.env.HTTPPORT || 3002, ():void => {
            const {address, port} = <{ port: number; family: string; address: string; }>this.server.address()
            console.log(`running on  ${address}:${port}`)
        });
    }

    private printServerConfig():void {
        const config = {
            KAFKAHOST: process.env.KAFKAHOST,
            KAFKAPORT: process.env.KAFKAPORT,
            KAFKACONSUMERAUTOJOIN: process.env.KAFKACONSUMERAUTOJOIN,
            KAFKACLIENTID: process.env.KAFKACLIENTID,
            KAFKAGROUPID: process.env.KAFKAGROUPID,
            AUTOCOMMIT: process.env.AUTOCOMMIT,
            HTTPPORT: process.env.HTTPPORT,
            KAFKATOPICS: process.env.KAFKATOPICS.split(',')
        }

        console.log(`Server config: ${JSON.stringify(config)}`)
    }

    private async initConsumer():Promise<void> {
        await this.kafkaConsumer.start()
    }

    healthCheck() {
        this.app.get('/health', (req, res, next) => {
            return res.status(200).send(`Hello from consumer server`)
        });
    }

    shutdownRoute() {
        this.app.get('/stop', async (req, res, next) => {
            await this.kafkaConsumer.shutdown()
            return res.status(200).send(`Stopping the consuming action`)
        });
    }

    startRoute() {
        this.app.get('/start', async (req, res, next) => {
            await this.kafkaConsumer.start()
            return res.status(200).send(`Starting the consuming action`)
        });
    }
}
