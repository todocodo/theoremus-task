## A Task from Theoremus
#### Python producer and Kafka Streams consumer

### Instructions on how to run it

1. After cloning the repo make sure that the docker server is running and run the compose file from the terminal -> docker-compose -f kafka-wurst.yml up -d
  - If the container is already created, just start it (from the docker desktop) 
2. Two kafka topics need to be created after the docker compose file is running. In the root folder run in the teminal the "make_topics.py" python script
3. Run the java maven project located in the root folder (the kafka streams app)
  - If you are opening it in Eclipse you can import the "maven_project_dsl_streams" folder as an existing project or you can also import it as an archive file using the zip folder which is also in the repo
4. After the maven project is running, navigate to the kafka-python-code folder and open two terminals 
  - in the first terminal start the python script for the consumer - python consumer.py
  - in the second terminal start the python scipt for the producer - python producer.py
  
