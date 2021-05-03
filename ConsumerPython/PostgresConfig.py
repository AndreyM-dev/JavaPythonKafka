import mysql.connector
import mysql.connector.errors
import configparser
import os

config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), 'resources/config.ini'))


def connect():
    return mysql.connector.connect(host=config["MY_SQL"]["host"], port=config["MY_SQL"]["port"],
                                   user=config["MY_SQL"]["user"],
                                   password=config["MY_SQL"]["pass"], database=config["MY_SQL"]["db"])
