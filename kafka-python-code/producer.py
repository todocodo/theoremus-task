import time 
import json 
import random
from datetime import datetime
from data_generator_from_csv import generate_message
from kafka import KafkaProducer

#Messages will be serialized as JSON
def serializer(message):
    return json.dumps(message).encode('utf-8')


#Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=serializer
)

if __name__ == '__main__':
    
    #Infinite loop - runs untill you kill the program
    while True:
        #Generate a message

        current_message = generate_message()

        #Send it to our input topic
        print(type(current_message))
        print("------------------------------------------")
        print(f'Producing message @ {datetime.now()} | Message => {str(current_message)}')
        producer.send('json-messages-input', current_message)

        #Sleep for a random number of seconds between 1-2
        time_to_sleep = random.randint(1,2)
        time.sleep(time_to_sleep)