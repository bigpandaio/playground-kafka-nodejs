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
exports.WebServer = void 0;
const express = require("express");
const http = __importStar(require("http"));
const cors = require("cors");
const bodyParser = require("body-parser");
const dotenv = __importStar(require("dotenv"));
dotenv.config({ path: __dirname + '/../.env' });
const KafkaConsumer_1 = require("./KafkaConsumer");
class WebServer {
    /**
     * @constructor
     */
    constructor() {
        this.app = express();
        this.server = http.createServer(this.app);
    }
    /**
     * Init routes, configuration and server listener
     * @return {void}
     */
    init() {
        return __awaiter(this, void 0, void 0, function* () {
            this.configApp();
            this.routes();
            this.setupListener();
            this.printServerConfig();
            this.kafkaConsumer = new KafkaConsumer_1.KafkaConsumer();
            return;
        });
    }
    /**
    * Init server middlewares
    * @return {void}
    */
    configApp() {
        this.app.use(cors());
        this.app.use(bodyParser.json({ limit: '10mb' }));
    }
    /**
     * Init routes
     * @return {void}
     */
    routes() {
        this.healthCheck();
        this.shutdownRoute();
        this.startRoute();
    }
    /**
     * Init server
     * @return {void}
     */
    setupListener() {
        this.server.listen(process.env.HTTPPORT || 3002, () => {
            const { address, port } = this.server.address();
            console.log(`running on  ${address}:${port}`);
        });
    }
    printServerConfig() {
        const config = {
            KAFKAHOST: process.env.KAFKAHOST,
            KAFKAPORT: process.env.KAFKAPORT,
            KAFKACLIENTID: process.env.KAFKACLIENTID,
            KAFKAGROUPID: process.env.KAFKAGROUPID,
            AUTOCOMMIT: process.env.AUTOCOMMIT,
            HTTPPORT: process.env.HTTPPORT,
            KAFKATOPICS: process.env.KAFKATOPICS.split(',')
        };
        console.log(`Server config: ${JSON.stringify(config)}`);
    }
    initConsumer() {
        return __awaiter(this, void 0, void 0, function* () {
            yield this.kafkaConsumer.start();
        });
    }
    healthCheck() {
        this.app.get('/health', (req, res, next) => {
            return res.status(200).send(`Hello from consumer server`);
        });
    }
    shutdownRoute() {
        this.app.get('/stop', (req, res, next) => __awaiter(this, void 0, void 0, function* () {
            yield this.kafkaConsumer.shutdown();
            return res.status(200).send(`Stopping the consuming action`);
        }));
    }
    startRoute() {
        this.app.get('/start', (req, res, next) => __awaiter(this, void 0, void 0, function* () {
            yield this.kafkaConsumer.start();
            return res.status(200).send(`Starting the consuming action`);
        }));
    }
}
exports.WebServer = WebServer;
//# sourceMappingURL=WebServer.js.map