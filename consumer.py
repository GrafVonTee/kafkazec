import os
import json
import logging
import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def main():
    topic_name = 'new_york_taxi'
    consumer = None
    retries = 30
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(',')

    while retries > 0:
        try:
            consumer = KafkaConsumer(
                topic_name,
                bootstrap_servers=bootstrap_servers,
                group_id='taxi_trip_processors',
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                auto_commit_interval_ms=1000,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            )
            logging.info(f"Consumer успешно подключился к топику '{topic_name}'.")
            break
        except NoBrokersAvailable:
            retries -= 1
            logging.warning(f"Брокер недоступен. Ждем 2 сек... (Осталось попыток: {retries})")
            time.sleep(2)
        except Exception as e:
            logging.critical(f"Не удалось подключиться к Kafka: {e}")
            return

    if not consumer:
        logging.critical("Не удалось дождаться запуска Kafka. Выход.")
        return

    logging.info("Ожидание сообщений... (Нажми Ctrl+C для остановки)")
    count = 0
    for message in consumer:
        data = message.value
        count += 1

        if count == 1:
            logging.info(f"Пример полученных данных: {data}")

        if count % 10 == 0:
            logging.info(
                f"Обработано {count} сообщений. "
                f"Текущая партиция: {message.partition}, "
                f"смещение: {message.offset}"
            )

        # Здесь должна быть твоя бизнес-логика:
        # Например, запись в базу данных, передача в ML-модель, фильтрация и т.д.

    logging.info("Закрытие соединения consumer...")
    consumer.close()
    logging.info(f"Consumer корректно остановлен. Всего обработано сообщений: {count}.")


if __name__ == '__main__':
    main()
