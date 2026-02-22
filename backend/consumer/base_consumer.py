import json
import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

class BaseKafkaConsumer:
    # грузим всех брокеров, иначе консьюмер умрёт вместе с брокером
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
                    auto_offset_reset='earliest',
                    api_version=(3, 0, 0),
                    session_timeout_ms=30000,
                    heartbeat_interval_ms=10000,
                    request_timeout_ms=31000,
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