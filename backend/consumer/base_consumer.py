import json
import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

class BaseKafkaConsumer:
    def __init__(self, topics, group_id, bootstrap_servers='kafka-1:9092,kafka-2:9092,kafka-3:9092'):
        topics_list = [topics] if isinstance(topics, str) else topics
        
        retries = 20
        while retries > 0:
            try:
                self.consumer = KafkaConsumer(
                    *topics_list,
                    bootstrap_servers=bootstrap_servers.split(','),
                    group_id=group_id,
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    auto_offset_reset='earliest'
                )
                break
            except NoBrokersAvailable:
                retries -= 1
                time.sleep(3)
                
        if retries == 0:
            raise Exception("Не удалось подключиться к Kafka")

    def consume(self):
        for message in self.consumer:
            self.process_message(message.topic, message.value)
            
    def process_message(self, topic, data):
        pass