import configparser
import json
import logging
import os

import mysql.connector
import mysql.connector.errors
from kafka import KafkaConsumer


def consumer():
    """It returns a consumer of Kafka object"""
    return KafkaConsumer(
        config["KAFKA_CONSUMER"]["TopicName"],
        bootstrap_servers=[config["KAFKA_CONNECTION"]["BootstrapServer"]],
        value_deserializer=lambda m: json.loads(m.decode('utf_8')),
        group_id=config["KAFKA_CONSUMER"]["GroupId"]
    )


def connectToMySQL():
    """It returns a connection object to MySQL"""
    return mysql.connector.connect(host=config["MY_SQL"]["host"], port=config["MY_SQL"]["port"],
                                   user=config["MY_SQL"]["user"],
                                   password=config["MY_SQL"]["pass"], database=config["MY_SQL"]["db"])


def consumer_lister(consumer):
    """The function is listening to Kafka and when receive message it call message_handler(message) function"""
    for message in consumer:
        log.info("A message was received")
        messages = message_handler(message)
        pushMessage(connectToMySQL(), messages, TABLE_NAME)


def message_handler(consumer_record):
    """The function gets a ConsumerRecord object and returns a list of messages"""
    kafka_message_value = consumer_record.value

    list_msg = lambda lm: list(map(lambda m: json.loads(json.dumps(kafka_message_value)), kafka_message_value))
    singleMsg = lambda m: [json.loads(json.dumps(kafka_message_value))]
    messagesChk = lambda v: list_msg(v) if (isinstance(v, list)) else singleMsg(v)

    return messagesChk(kafka_message_value)


def pushMessage(connect, messages, table_name):
    """The function writes messages into the table of MySQL DB"""
    with connect:
        cursor = connect.cursor()
        try:
            cursor.execute(f"CREATE TABLE {table_name} (id BIGINT, message VARCHAR(255))")
            log.info(f"The table {table_name} was created")
        except mysql.connector.errors.ProgrammingError:
            log.exception(f"Table {table_name} already exists")
        try:
            for message in messages:
                sql = f"INSERT INTO {table_name} (id, message) VALUES (%s, %s)"
                val = (message["id"], message["message"])
                cursor.execute(sql, val)
                log.info(cursor.rowcount + " was/were inserted")
            connect.commit()
        except:
            log.exception("Something went wrong")


TABLE_NAME = "messages"
logging.basicConfig(filename="simpleLog.log", level=logging.INFO)
log = logging.getLogger("MyLogger")
config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), '../resources/config.ini'))

consumer_lister(consumer())
