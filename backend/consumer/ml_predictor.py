import joblib
import pandas as pd
import numpy as np
import sys
import os

from sklearn.preprocessing import FunctionTransformer
from sklearn.compose import TransformedTargetRegressor
from sklearn.pipeline import Pipeline
from catboost import CatBoostRegressor

from consumer.taxi_utils import preprocess_taxi_data, manhattan_distance, haversine_distance

# костыль для joblib, т.к. я обучал всё в блокноте
import __main__
__main__.preprocess_taxi_data = preprocess_taxi_data
__main__.manhattan_distance = manhattan_distance
__main__.haversine_distance = haversine_distance

from consumer.base_consumer import BaseKafkaConsumer
from producer.base_producer import BaseKafkaProducer


class MLPredictor(BaseKafkaConsumer):
    def __init__(self):
        super().__init__('new_trips', group_id='ml_group')
        self.producer = BaseKafkaProducer()
        model_path = 'models/taxi_pipeline.pkl'
        self.pipeline = joblib.load(model_path)

    def process_message(self, topic, data):
        df = pd.DataFrame([data])
        prediction = self.pipeline.predict(df)[0]
        result = {
            "id": data["id"],
            "predicted_sec": float(prediction)
        }
        self.producer.send('ml_predictions', result)


if __name__ == "__main__":
    MLPredictor().consume()