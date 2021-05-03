import json
import KafkaConsumerConfig
import PostgresService
from Message import Message


kafka_consumer = KafkaConsumerConfig.consumer()


def consumer_lister(consumer=kafka_consumer):
    for message in consumer:
        json_map_list_messages(message)


def json_map_list_messages(msg):
    messages = []
    kafka_message_value = msg.value
    if isinstance(kafka_message_value, list):
        for value in kafka_message_value:
            json_msg = json.loads(json.dumps(value))
            messages.append(Message(**json_msg))
    else:
        json_msg = json.loads(json.dumps(kafka_message_value))
        messages.append(Message(**json_msg))
    PostgresService.pushMessage(messages)
    for msg in messages:
        print(str(msg))


