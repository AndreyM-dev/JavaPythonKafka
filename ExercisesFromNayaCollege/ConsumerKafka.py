from kafka import KafkaConsumer
from datetime import datetime
import confuse

config = confuse.Configuration('KafkaExercises', __name__)
config.set_file('./resources/config.yaml')

broker = config['kafka']['bootstrapServer'].get()
topic = config['kafka']['topic'].get()

consumer = KafkaConsumer(
    topic,
    bootstrap_servers=broker,
    group_id='group1'
)

for message in consumer:
    print("-----------------------------------START----------------------------------------------")
    print("The message offset ", message.offset)
    dt = datetime.fromtimestamp(message.timestamp/1000)

    print("The message date", dt)
    print("The message value ", message.value)
    print("===================================The END============================================")

