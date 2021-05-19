from confluent_kafka import Producer
import confuse
import time
from datetime import datetime


config = confuse.Configuration('ProducerKafka', __name__)
config.set_file('./resources/config.yaml')

brokers = str(config['kafka']['bootstrapServer'])
topic = str(config['kafka']['topic'])

print(f"bootstrapServer = {brokers}")
print(f"Topic = {topic}")

prod = Producer({'bootstrap.servers': brokers})


while True:
    time.sleep(10)
    data = f"Time now is {str(datetime.now())}".encode('utf-8')
    prod.produce(topic, data)
    prod.flush(timeout=5)

