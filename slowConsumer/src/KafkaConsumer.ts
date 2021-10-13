import { Kafka, Consumer, Producer, EachMessagePayload,CompressionTypes} from 'kafkajs'
import * as dotenv from 'dotenv'
dotenv.config({ path: __dirname+'/../.env' });

export class KafkaConsumer {
    private topics:Array<string> = process.env.KAFKATOPICS.split(',')
    private kafkaConsumer: Consumer
    private autoCommit: string
    private kafkaProducer: Producer

    public constructor() {
        this.kafkaProducer = this.createKafkaProducer()
    }

    public async start(): Promise<void> {
        try {
            this.kafkaConsumer = this.createKafkaConsumer()
            await this.kafkaProducer.connect()
            this.autoCommit = process.env.AUTOCOMMIT
            await this.kafkaConsumer.connect()
            for (const topic of this.topics) {
                await this.kafkaConsumer.subscribe({ topic })
            }

            await this.kafkaConsumer.run({
                autoCommit: (this.autoCommit !== 'false'),
                eachMessage: async (messagePayload: EachMessagePayload) => this.processAndCommitMessage(messagePayload)
            })
        } catch (error) {
            console.log('Error: ', error)
        }
    }
    
    private async processAndCommitMessage(payload:EachMessagePayload): Promise<void> {
        const { topic, partition, message } = payload
        const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`
        console.log(`- ${prefix} ${message.key}#${message.value}`)
        try{
            const parsedValue  = JSON.parse(message.value.toString())
            const heavyTask = () =>{
                let count = 0
                for (let i = 0; i <= 50000000; i++)
                    count +=1
                return count
            }
            parsedValue.enrichment = heavyTask()
            console.log(parsedValue)
        }catch (e) {
            console.log(e.message)
            const errorMessage = { key: message.key, value: message.value}
            await this.pushToErrorsTopic(errorMessage)
        } finally {
            if(this.autoCommit !== 'false') await this.commit(payload)
        }
    }

    /**
     * Commits the offsets specified by the message
     *
     * @param message - Message from which to get the offset
     */
    public async commit(payload): Promise<void> {
        await this.kafkaConsumer.commitOffsets([
            {
                offset: (parseInt(payload.message.offset, 10) + 1).toString(),
                topic: payload.topic,
                partition: payload.partition
            }
        ])
    }

    public async shutdown(): Promise<void> {
        await this.kafkaConsumer.disconnect()
    }

    private async pushToErrorsTopic(message): Promise<void> {
        try{
            await this.kafkaProducer.send({
                    topic:'topic-errors',
                    compression: CompressionTypes.GZIP,
                    messages: [ message ]
            })
            console.log(`Broken message sent to \'topic-errors\'`)
        } catch (e) {
            console.error(`[error/producer] ${e.message}`, e)
        }
    }


    private createKafkaConsumer(): Consumer {
        const kafka = new Kafka({ 
            clientId: `${process.env.KAFKACLIENTID}`,
            brokers: [`${process.env.KAFKAHOST}:${process.env.KAFKAPORT}`]
        })
        const consumer = kafka.consumer({ groupId: `${process.env.KAFKAGROUPID}` })
        return consumer
    }

    private createKafkaProducer() : Producer {
        const kafka = new Kafka({
            clientId: `errors-client`,
            brokers: [`${process.env.KAFKAHOST}:${process.env.KAFKAPORT}`]
        })

        return kafka.producer()
    }
}
