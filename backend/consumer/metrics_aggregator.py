from consumer.base_consumer import BaseKafkaConsumer
from producer.base_producer import BaseKafkaProducer


class MetricsAggregator(BaseKafkaConsumer):
    def __init__(self):
        super().__init__(['ml_predictions', 'finished_trips'], group_id='metrics_group')
        self.producer = BaseKafkaProducer()
        self.cache = {}

    def process_message(self, topic, data):
        id = data["id"]
        
        if id not in self.cache:
            self.cache[id] = {}
            
        if topic == 'ml_predictions':
            self.cache[id]["predicted"] = data["predicted_sec"]
        elif topic == 'finished_trips':
            self.cache[id]["actual"] = data["actual_sec"]
            
        if "predicted" in self.cache[id] and "actual" in self.cache[id]:
            pred = self.cache[id]["predicted"]
            act = self.cache[id]["actual"]
            
            metric = {
                "id": id,
                "predicted": pred,
                "actual": act,
                "error_sec": abs(pred - act)
            }
            
            self.producer.send('model_metrics', metric)
            del self.cache[id]


if __name__ == "__main__":
    MetricsAggregator().consume()