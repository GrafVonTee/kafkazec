import csv
import random
import asyncio
import math
from producer.base_producer import BaseKafkaProducer

def calculate_heuristic_duration(lat1, lon1, lat2, lon2):
    distance = math.sqrt((lat2 - lat1)**2 + (lon2 - lon1)**2)
    evristik_value = 20000
    return round(distance * evristik_value, 2)


async def send_delayed_duration(producer, trip_id, duration):
    await asyncio.sleep(random.uniform(3, 7))
    producer.send('finished_trips', {"id": trip_id, "actual_sec": duration})


async def main():
    producer = BaseKafkaProducer()
    csv_file_path = 'data/test.csv'

    with open(csv_file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        
        for row in reader:
            await asyncio.sleep(random.uniform(1, 5))
            
            trip_id = row["id"]
            lat1, lon1 = float(row["pickup_latitude"]), float(row["pickup_longitude"])
            lat2, lon2 = float(row["dropoff_latitude"]), float(row["dropoff_longitude"])
            
            heuristic_duration = calculate_heuristic_duration(lat1, lon1, lat2, lon2)
            data = {
                "id": trip_id,
                "vendor_id": int(row["vendor_id"]),
                "pickup_datetime": row["pickup_datetime"],
                "passenger_count": int(row["passenger_count"]),
                "pickup_longitude": lon1,
                "pickup_latitude": lat1,
                "dropoff_longitude": lon2,
                "dropoff_latitude": lat2,
                "store_and_fwd_flag": row["store_and_fwd_flag"]
            }
            
            producer.send('new_trips', data)
            asyncio.create_task(send_delayed_duration(producer, trip_id, heuristic_duration))


if __name__ == '__main__':
    asyncio.run(main())