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
exports.KafkaConsumer = void 0;
const kafkajs_1 = require("kafkajs");
const dotenv = __importStar(require("dotenv"));
dotenv.config({ path: __dirname + '/../.env' });
class KafkaConsumer {
    constructor() {
        this.topics = process.env.KAFKATOPICS.split(',');
        this.kafkaProducer = this.createKafkaProducer();
    }
    start() {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                this.kafkaConsumer = this.createKafkaConsumer();
                yield this.kafkaProducer.connect();
                this.autoCommit = process.env.AUTOCOMMIT;
                yield this.kafkaConsumer.connect();
                for (const topic of this.topics) {
                    yield this.kafkaConsumer.subscribe({ topic });
                }
                yield this.kafkaConsumer.run({
                    autoCommit: (this.autoCommit !== 'false'),
                    eachMessage: (messagePayload) => __awaiter(this, void 0, void 0, function* () { return this.processAndCommitMessage(messagePayload); })
                });
            }
            catch (error) {
                console.log('Error: ', error);
            }
        });
    }
    processAndCommitMessage(payload) {
        return __awaiter(this, void 0, void 0, function* () {
            const { topic, partition, message } = payload;
            const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`;
            console.log(`- ${prefix} ${message.key}#${message.value}`);
            try {
                const parsedValue = JSON.parse(message.value.toString());
                const heavyTask = () => {
                    let count = 0;
                    for (let i = 0; i <= 50000000; i++)
                        count += 1;
                    return count;
                };
                parsedValue.enrichment = heavyTask();
                console.log(parsedValue);
            }
            catch (e) {
                console.log(e.message);
                const errorMessage = { key: message.key.toString(), value: message.value.toString() };
                yield this.pushToErrorsTopic(errorMessage);
            }
            finally {
                if (this.autoCommit !== 'false')
                    yield this.commit(payload);
            }
        });
    }
    /**
     * Commits the offsets specified by the message
     *
     * @param message - Message from which to get the offset
     */
    commit(payload) {
        return __awaiter(this, void 0, void 0, function* () {
            yield this.kafkaConsumer.commitOffsets([
                {
                    offset: (parseInt(payload.message.offset, 10) + 1).toString(),
                    topic: payload.topic,
                    partition: payload.partition
                }
            ]);
        });
    }
    shutdown() {
        return __awaiter(this, void 0, void 0, function* () {
            yield this.kafkaConsumer.disconnect();
        });
    }
    pushToErrorsTopic(message) {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                yield this.kafkaProducer.send({
                    topic: 'topic-errors',
                    compression: kafkajs_1.CompressionTypes.None,
                    messages: [message]
                });
                console.log(`Broken message sent to \'topic-errors\'`);
            }
            catch (e) {
                console.error(`[error/producer] ${e.message}`, e);
            }
        });
    }
    createKafkaConsumer() {
        const kafka = new kafkajs_1.Kafka({
            clientId: `${process.env.KAFKACLIENTID}`,
            brokers: [`${process.env.KAFKAHOST}:${process.env.KAFKAPORT}`]
        });
        const consumer = kafka.consumer({ groupId: `${process.env.KAFKAGROUPID}` });
        return consumer;
    }
    createKafkaProducer() {
        const kafka = new kafkajs_1.Kafka({
            clientId: `errors-client`,
            brokers: [`${process.env.KAFKAHOST}:${process.env.KAFKAPORT}`]
        });
        return kafka.producer();
    }
}
exports.KafkaConsumer = KafkaConsumer;
//# sourceMappingURL=KafkaConsumer.js.map