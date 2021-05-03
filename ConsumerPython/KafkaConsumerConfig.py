import configparser
import json
import os
from kafka import KafkaConsumer

config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), 'resources/config.ini'))


def consumer(topic_name=config["KAFKA_CONSUMER"]["TopicName"], group_id=config["KAFKA_CONSUMER"]["GroupId"]):
    return KafkaConsumer(
        topic_name,
        bootstrap_servers=[config["KAFKA_CONNECTION"]["BootstrapServer"]],
        value_deserializer=lambda m: json.loads(m.decode('utf_8')),
        group_id=group_id
    )
