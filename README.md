# KAFKA PLAYGROUND NODE.JS

The purpose of this repo is to play with kafka, producers, consumers, etc. Feel free to add features to the repo.

Some explanations about playground structure and execution:

1. Clone the repo from the source and cd to repo folder.
2. Running Kafka instance locally: 
   1. You will find docker compose file "docker-compose.yml" in the repo folder. This file contains all you need to run Kafka locally
   2. Be sure you have docker engine and docker compose installed on your machine and just run "docker-compose up -d". Kafka is up!
   3. Kafka cluster contains 2 brokers and leader will expose port "29092" for localhost connectivity.
3. Once Kafka is up lets start with configuring producer and consumers to work with Kafka server:
   1. Inside playground folder you can find two consumers (simpleConsumer and slowConsumer) and one producer server
   2. Both consumers and producer are Nodejs servers with some configuration options and endpoints to control the flow.
   3. Consumers are same in terms of behavior and configuration, the only difference is the BI inside:
      1. simpleConsumer will print to console the consumed message and that's it :)
      2. slowConsumer will perform kind of heavy computation before printing the consumed message to the screen
      3. The reason for that is to show the difference in consuming speed and to see how different consuming groups may consume same messages in different speed
      4. Consumers configurations are inside .env file for each consumer (or can be pushed via environment variables for docker instance)
      5. Configurations description for consumers:

         KAFKAHOST=localhost // broker host - localhost for local environment
      
         KAFKAPORT=29092 // broker port - 29092 for local environment 
      
         KAFKACONSUMERAUTOJOIN=true // should consumer to join the group automatically or via endpoint activation
      
         KAFKACLIENTID=simpleconsumer // client id
      
         KAFKAGROUPID=simple // group id
      
         AUTOCOMMIT=true // play with autocommit for "at most once" or "at least once" paradigm
      
         HTTPPORT=3002 // express webserver port for incoming API
      
         KAFKATOPICS=topic-batch,topic-single // list of topics to consume
      6. Consumers endpoints description:
      
            GET '/health' - health check for server is up and running
      
            GET '/start' - activate the consumer for case the "KAFKACONSUMERAUTOJOIN" is false in config or consumer stopped via "stop" endpoint
      
            GET '/stop' - deactivate the consumer
      7. Configuration options for producer

         KAFKAHOST=localhost // broker host - localhost for local environment

         KAFKAPORT=29092 // broker port - 29092 for local environment

         KAFKACLIENTID=myproducer // client id
      
         HTTPPORT=3001 // express webserver port for incoming API
      
         KAFKATOPICS=topic-batch,topic-single // list of topics to produce to
      8. Producer endpoints description:
      
          GET '/batch' will produce batch of random messages to "topic-batch" topic 
      
         GET '/single' will produce single message to "topic-single" topic

         GET '/single-bad' will produce bad (broken JSON ) single message to "topic-single" topic
4. Everything is configured? Cool! Let's start the game, engines up!
   1. All servers have preconfigured npm option for start up. Just run "npm run dev" to start the server. Do it for each server separately.
   2. Once both consumers are up and consuming, open their consoles in separate window to see the output for consuming messages
   3. Perform API calls to producer to start producing messages and see output to console for consumers.
5. Additional options:
   1. You may want to add more consumers to the same group. At this case the easiest option is to build docker image and add it to docker-compose with relevant configurations
   2. You can find the commented example for running consumer as part of docker compose setup.
   3. Building image is pretty simple. Just run "npm run upload" from specific server folder. This will create "simpleconsumer" or "slowconsumer" or "producer" images accordingly.
   4. All magic for running servers locally or creating images is in package.json file.
