## A Task from Theoremus
#### Python producer and Kafka Streams consumer

### Instructions on how to run it

1. After cloning the repo make sure that the docker server is running and run the compose file from the terminal -> docker-compose -f kafka-wurst.yml up -d
  - If the container is already created, just start it (from the docker desktop) 
2. Two kafka topics need to be created after the docker compose file is running. First in your terminal execute - docker exec -it kafka /bin/sh - to get into the kafka shell. Then inside the shell run - cd opt - and with ls copy the kafka with its version "kafka_2.13-2.8.1" or sth like this. Then navigate to - cd kafka_2.13-2.8.1/bin - After that run the following commands for creating a topic
  - 1 for the input - "json-messages-input" using this command - kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic json-messages-input
  - 1 for the output - "transformed-messages-output" using this command - kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic transformed-messages-output
3. Run the java maven project located in the root folder (the kafka streams app)
  - If you are opening it in Eclipse you can import the "maven_project_dsl_streams" folder as an existing project or you can also import it as an archive file using the zip folder which is also in the repo
4. After the maven project is running, navigate to the kafka-python-code folder and open two terminals 
  - in the first terminal start the python script for the consumer - python consumer.py
  - in the second terminal start the python scipt for the producer - python producer.py
  
