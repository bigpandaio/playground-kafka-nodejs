"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    Object.defineProperty(o, k2, { enumerable: true, get: function() { return m[k]; } });
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.KafkaProducer = void 0;
const express = require("express");
const http = __importStar(require("http"));
const cors = require("cors");
const bodyParser = require("body-parser");
const kafkajs_1 = require("kafkajs");
const dotenv = __importStar(require("dotenv"));
dotenv.config({ path: __dirname + '/../.env' });
class KafkaProducer {
    constructor() {
        this.app = express();
        this.server = http.createServer(this.app);
        this.topics = process.env.KAFKATOPICS.split(',');
        this.producer = this.createProducer();
    }
    /**
     * Init server middlewares
     * @return {void}
     */
    init() {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                this.app.use(cors());
                this.app.use(bodyParser.json({ limit: '10mb' }));
                this.setupListener();
                this.printServerConfig();
                yield this.producer.connect();
                console.info('Kafka client connected');
                this.routes();
            }
            catch (error) {
                console.log('Error connecting the producer: ', error);
            }
        });
    }
    /**
     * Init server
     * @return {void}
     */
    setupListener() {
        this.server.listen(process.env.HTTPPORT || 3001, () => {
            const { address, port } = this.server.address();
            console.info(`running on  ${address}:${port}`);
        });
    }
    printServerConfig() {
        const config = {
            KAFKAHOST: process.env.KAFKAHOST,
            KAFKAPORT: process.env.KAFKAPORT,
            KAFKACLIENTID: process.env.KAFKACLIENTID,
            HTTPPORT: process.env.HTTPPORT,
            KAFKATOPICS: process.env.KAFKATOPICS.split(',')
        };
        console.log(`Server config: ${JSON.stringify(config)}`);
    }
    /**
     * Init routes
     * @return {void}
     */
    routes() {
        this.healthCheck();
        this.batchRoute();
        this.singleRoute();
        this.stopRoute();
        this.badSingleRoute();
    }
    shutdown() {
        return __awaiter(this, void 0, void 0, function* () {
            yield this.producer.disconnect();
        });
    }
    sendBatchMessage() {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                const randomNumber = this.getRandomNumber();
                const pushRes = yield this.producer.send({
                    topic: this.topics[0],
                    compression: kafkajs_1.CompressionTypes.GZIP,
                    messages: Array(randomNumber)
                        .fill(randomNumber)
                        .map((unused, index) => {
                        return this.createMessage(randomNumber, index);
                    }),
                });
                console.log(pushRes);
            }
            catch (e) {
                console.error(`[error/producer] ${e.message}`, e);
            }
        });
    }
    sendSingleMessage() {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                const pushRes = yield this.producer.send({
                    topic: this.topics[1],
                    compression: kafkajs_1.CompressionTypes.GZIP,
                    messages: [
                        {
                            key: 'single-key',
                            value: JSON.stringify({ entry: `new single message on - ${new Date().toISOString()}` })
                        }
                    ]
                });
                console.log(pushRes);
            }
            catch (e) {
                console.error(`[error/producer] ${e.message}`, e);
            }
        });
    }
    sendBadSingleMessage() {
        return __awaiter(this, void 0, void 0, function* () {
            return this.producer
                .send({
                topic: this.topics[1],
                compression: kafkajs_1.CompressionTypes.GZIP,
                messages: [
                    {
                        key: 'single-key',
                        value: `new single message on - ${new Date().toISOString()}`
                    }
                ]
            })
                .then(console.log)
                .catch(e => console.error(`[example/producer] ${e.message}`, e));
        });
    }
    getRandomNumber() { return Math.round(Math.random() * 1000); }
    createMessage(num, index) {
        return {
            key: `key-${num}`,
            value: JSON.stringify({
                entry: `value-${num}-${new Date().toISOString()}`,
                order: `messageOrder: #${index}`
            })
        };
    }
    healthCheck() {
        this.app.get('/health', (req, res, next) => {
            return res.status(200).send(`Hello from kafka producer`);
        });
    }
    batchRoute() {
        this.app.get('/batch', (req, res, next) => __awaiter(this, void 0, void 0, function* () {
            const sentMessage = yield this.sendBatchMessage();
            return res.status(200).json(sentMessage);
        }));
    }
    singleRoute() {
        this.app.get('/single', (req, res, next) => __awaiter(this, void 0, void 0, function* () {
            const sentMessage = yield this.sendSingleMessage();
            return res.status(200).json(sentMessage);
        }));
    }
    badSingleRoute() {
        this.app.get('/single-bad', (req, res, next) => __awaiter(this, void 0, void 0, function* () {
            const sentMessage = yield this.sendBadSingleMessage();
            return res.status(200).json(sentMessage);
        }));
    }
    stopRoute() {
        this.app.get('/start', (req, res, next) => __awaiter(this, void 0, void 0, function* () {
            yield this.shutdown();
        }));
    }
    createProducer() {
        const kafka = new kafkajs_1.Kafka({
            clientId: `${process.env.KAFKACLIENTID}`,
            brokers: [`${process.env.KAFKAHOST}:${process.env.KAFKAPORT}`]
        });
        return kafka.producer();
    }
}
exports.KafkaProducer = KafkaProducer;
//# sourceMappingURL=KafkaProducer.js.map