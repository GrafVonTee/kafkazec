import csv
import random
import asyncio
from producer.base_producer import BaseKafkaProducer

async def send_delayed_duration(producer, trip_id, duration):
    await asyncio.sleep(random.uniform(3, 7))
    producer.send('finished_trips', {"id": trip_id, "actual_sec": duration})


async def main():
    producer = BaseKafkaProducer()
    csv_file_path = 'data/train.csv'

    with open(csv_file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        
        for row in reader:
            await asyncio.sleep(random.uniform(2, 5))
            
            trip_id = row["id"]
            duration = float(row["trip_duration"])
            
            data = {
                "id": trip_id,
                "vendor_id": int(row["vendor_id"]),
                "pickup_datetime": row["pickup_datetime"],
                "passenger_count": int(row["passenger_count"]),
                "pickup_longitude": float(row["pickup_longitude"]),
                "pickup_latitude": float(row["pickup_latitude"]),
                "dropoff_longitude": float(row["dropoff_longitude"]),
                "dropoff_latitude": float(row["dropoff_latitude"]),
                "store_and_fwd_flag": row["store_and_fwd_flag"]
            }
            
            producer.send('new_trips', data)
            asyncio.create_task(send_delayed_duration(producer, trip_id, duration))

if __name__ == '__main__':
    asyncio.run(main())