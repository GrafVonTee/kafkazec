import os
import csv
import json
import logging
import time
import random
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def on_send_success(record_metadata):
    logging.debug(
        f"Успех: топик {record_metadata.topic}, "
        f"партиция {record_metadata.partition}, "
        f"смещение {record_metadata.offset}"
    )


def on_send_error(excp):
    logging.error(f"Ошибка при отправке сообщения: {excp}", exc_info=True)


def main():
    producer = None
    retries = 30
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(',')

    logging.info("Ожидание запуска Kafka...")
    while retries > 0:
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                acks='all',
                retries=5,
                linger_ms=10,
            )
            logging.info("Kafka Producer успешно инициализирован.")
            break
        except NoBrokersAvailable:
            retries -= 1
            logging.warning(f"Брокер недоступен. Ждем 2 сек... (Осталось попыток: {retries})")
            time.sleep(2)
        except Exception as e:
            logging.critical(f"Не удалось подключиться к Kafka: {e}")
            return

    if not producer:
        logging.critical("Не удалось дождаться запуска Kafka. Выход.")
        return

    csv_file_path = 'data/train.csv'
    topic_name = 'new_york_taxi'

    with open(csv_file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)

        logging.info(f"Чтение файла {csv_file_path} и отправка в {topic_name}...")

        for count, row in enumerate(reader, start=1):
            producer.send(topic_name, value=row) \
                    .add_callback(on_send_success) \
                    .add_errback(on_send_error)

            time.sleep(random.random()*5)

    logging.info("Сброс буфера. Ожидание отправки всех оставшихся сообщений (flush)...")
    producer.flush()

    logging.info("Закрытие соединения producer...")
    producer.close()
    logging.info("Producer корректно остановлен.")


if __name__ == '__main__':
    main()
