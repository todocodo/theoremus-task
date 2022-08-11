import json 
from kafka import KafkaConsumer

if __name__ == '__main__':
    # Kafka Consumer 

    # the auto_offset_reset (latest) will ensure that it will start from the last added message 
    consumer = KafkaConsumer(
        'transformed-messages-output',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='latest'
    )

    for message in consumer:
        consumed_message = json.loads(message.value)

        print(json.dumps(consumed_message))