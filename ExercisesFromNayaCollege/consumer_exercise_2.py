from kafka import KafkaConsumer
import json
from datetime import datetime
import re
import pyarrow as pa
import mysql.connector as mc

hdfs_event_counter = 0
hdfs_stg_dir = '/tmp/staging/kafka/'
hdfs_flush_iteration = 10
host = 'localhost'
topic3 = 'kafka-tst-07'
brokers = ['localhost:9092']

# MySQL
mysql_port = 3306
mysql_database_name = 'srcdb'
mysql_table_name = 'src_events'
mysql_username = 'naya'
mysql_password = 'naya'

# connector to hdfs
fs = pa.hdfs.connect(
    host='localhost',
    port=8020,
    user='hdfs',
    kerb_ticket=None,
    # driver='libhdfs',
    extra_conf=None)


# Set the consumer
consumer = KafkaConsumer(
    topic3,
    group_id='File_MySQL_HDFS',
    bootstrap_servers=brokers,
    auto_offset_reset='latest',
    enable_auto_commit=True,
    auto_commit_interval_ms=1000)

insert_statement = """
    INSERT INTO srcdb.kafka_pipeline(event_id, hostname, event_ts) 
    VALUES ('{}', '{}', '{}');"""

# connector to mysql
mysql_conn = mc.connect(
    user=mysql_username,
    password=mysql_password,
    host=host,
    port=mysql_port,
    autocommit=True,  # <--
    database=mysql_database_name)

def mysql_event_insert(mysql_conn, event_id, hostname, event_ts):
    mysql_cursor = mysql_conn.cursor()
    sql = insert_statement.format(event_id, hostname, event_ts)
    #     print(sql)
    mysql_cursor.execute(sql)
    mysql_cursor.close()


# if not exist staging change it
if fs.exists(hdfs_stg_dir):
    fs.rm(hdfs_stg_dir, recursive=True)
    fs.mkdir(hdfs_stg_dir)
else:
    fs.mkdir(hdfs_stg_dir)

for message in consumer:
    # Write to MySQL
    events = json.loads(message.value)
    for event in events:
        event_id, event_host, event_ts = event.split('|')
        event_ts = datetime.strptime(event_ts.strip(), '%a %b %d %H:%M:%S %Y')
        # mysql_event_insert(mysql_conn, event_id, event_host, event_ts)

    # HDFS Section
    event_id = re.sub(" ", "", event_id)
    event_ts = str(event_ts).replace(" ", "_").replace(":", "").replace("-", "_")

    if hdfs_event_counter == 0:
        hdfs_file_name = event_ts + '.csv'
        f = open(hdfs_file_name, 'w')

    if 0 <= hdfs_event_counter <= hdfs_flush_iteration:
        f.write(','.join([event_id, event_host, event_ts]) + '\n')
        f.flush()
        hdfs_event_counter += 1

    if hdfs_event_counter == hdfs_flush_iteration:
        hdfs_event_counter = 0
        print(hdfs_stg_dir + hdfs_file_name)
        with open(hdfs_file_name,'rb') as f:
            fs.upload('hdfs://localhost:8020/{}'.format(hdfs_stg_dir + hdfs_file_name), f)
        print('10 iterations passed. Putting file in HDFS.')
