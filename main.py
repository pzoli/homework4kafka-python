import os
import json

from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer
load_dotenv()

producer = KafkaProducer(bootstrap_servers=[os.getenv("KAFKA_URL")],
                        security_protocol = 'SASL_PLAINTEXT',
                        sasl_mechanism = 'PLAIN',
                        sasl_plain_username = os.getenv("KAFKA_USER"),
                        sasl_plain_password = os.getenv("KAFKA_PASSWORD")
                         )
print(f"producer connected: {producer.bootstrap_connected()}")
for i in range(5):
    msg = '{"msg":' + str(i) + '}'
    producer.send(os.getenv("KAFKA_TOPIC"), msg.encode('utf-8') )
producer.flush()
producer.close()
print("Messages are sent")

print("start consumer")
consumer = KafkaConsumer(os.getenv("KAFKA_TOPIC"),
                         #group_id='group1',
                         group_id=None,
                         bootstrap_servers=[os.getenv("KAFKA_URL")],
                         security_protocol='SASL_PLAINTEXT',
                         sasl_mechanism='PLAIN',
                         sasl_plain_username=os.getenv("KAFKA_USER"),
                         sasl_plain_password=os.getenv("KAFKA_PASSWORD"),
                          auto_offset_reset=os.getenv("KAFKA_OFFSET")
                         )
print(f"consumer connected: {consumer.bootstrap_connected()}")

for message in consumer:
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          message.value))
    try:
        data = json.loads(message.value.decode('utf-8'))
        print(data)
    except Exception as e:
        print(str(e))