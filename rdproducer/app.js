const Kafka = require('node-rdkafka')
const express = require('express')
const app = express()
const bodyParser = require('body-parser')

app.use(bodyParser.json({limit: '10mb'}));

let producer = new Kafka.Producer({
  'metadata.broker.list': 'localhost:29092',
  'dr_cb': true
});

const generatedRandomMessage = (messageType) => {
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
      'gateaway not reachable',
      'API sevice is down'
  ];
  const randomCheckNumber = Math.floor(Math.random()*checkArray.length);

  const messages = {
    good : JSON.stringify(
      {
        "logType": "invalid_payload",
        "level": "error",
        "componentName": "inbound",
        "resourceId": "api.test",
        "organization": "midev_3",
        "message":"asdasdasd",
        "payloadId": "",
        "payload": {"warning":`${levelArray[randomLevelNumber]}`,"host":`${hostsArray[randomHostsNumber]}`,"check":`${checkArray[randomCheckNumber]}`}
      }
    ),
    bad : `Hello there!`
  }

  return messages[messageType]
}

producer.on('ready', function(arg) {
  console.log('producer ready.' + JSON.stringify(arg));
});

//starting the producer
producer.connect();


app.get('/', (req, res) => res.send('Ready to send messages!'))

app.post('/send', async (req, res) => {
    const topic = `consumer.midev_${Math.floor(Math.random() * 115)}.alerts.errors`;
    const messageType = req.body.messageType;
    console.log(`Producing message from type ${messageType}`);
    await producer.produce(
        topic,
      -1,
        Buffer.from(generatedRandomMessage(messageType)),
        null,
        null,
        null,
        [{stam:'stam'}]
    );
    console.log(`Message produced to topic ${topic}`);
    res.send(`Message produced to topic ${topic}`)
});

app.listen(5000, () => console.log('Example app listening on port 5000!'))





