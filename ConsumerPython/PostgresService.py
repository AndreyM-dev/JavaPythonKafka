import mysql.connector
import Message

import PostgresConfig

tableName = "message"


def pushMessage(messages):
    """Write Message to table"""
    connect = PostgresConfig.connect()
    with connect:
        cursor = connect.cursor()
        try:
            cursor.execute(f"CREATE TABLE {tableName} (id BIGINT, message VARCHAR(255))")
        except mysql.connector.errors.ProgrammingError:
            print(f"Table {tableName} already exists")
        try:
            for message in messages:
                sql = f"INSERT INTO {tableName} (id, message) VALUES (%s, %s)"
                val = (message.id, message.message)
                cursor.execute(sql, val)
            connect.commit()
        except:
            print("Something went wrong")


