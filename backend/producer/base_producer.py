import json
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

class BaseKafkaProducer:
    def __init__(self, bootstrap_servers='kafka-1:9092,kafka-2:9092,kafka-3:9092'):
        retries = 20
        while retries > 0:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=bootstrap_servers.split(','),
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                )
                break
            except NoBrokersAvailable:
                retries -= 1
                time.sleep(3)
        
        if retries == 0:
            raise Exception("Не удалось подключиться к Kafka")

    def send(self, topic, data):
        self.producer.send(topic, data)
        self.producer.flush()