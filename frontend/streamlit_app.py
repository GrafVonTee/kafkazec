import streamlit as st
import pandas as pd
import json
import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

st.set_page_config(page_title="Taxi ML Streaming", layout="wide")
st.title("🚖 Real-time Taxi Trip Dashboard")

@st.cache_resource
def get_consumers():
    bootstrap_servers = ['kafka-1:9092', 'kafka-2:9092', 'kafka-3:9092']
    retries = 10
    
    while retries > 0:
        try:
            st.info(f"Connecting to Kafka... (attempts left: {retries})")
            consumer_trips = KafkaConsumer(
                'new_trips',
                bootstrap_servers=bootstrap_servers,
                group_id='streamlit_map_group',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest',
                api_version=(3, 0, 0) # Явно указываем версию для стабильности
            )
            
            consumer_metrics = KafkaConsumer(
                'model_metrics',
                bootstrap_servers=bootstrap_servers,
                group_id='streamlit_metrics_group',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest',
                api_version=(3, 0, 0)
            )
            st.success("Successfully connected to Kafka!")
            return consumer_trips, consumer_metrics
            
        except NoBrokersAvailable:
            retries -= 1
            time.sleep(5) # Ждем 5 секунд перед следующей попыткой
            
    st.error("Could not connect to Kafka. Please check if the brokers are running.")
    st.stop()


consumer_trips, consumer_metrics = get_consumers()


if 'map_data' not in st.session_state:
    st.session_state.map_data = []
if 'metrics_data' not in st.session_state:
    st.session_state.metrics_data = []


col1, col2 = st.columns([2, 1])


with col1:
    st.subheader("Карта стартов поездок")
    map_placeholder = st.empty()

with col2:
    st.subheader("Метрики модели (Real-time)")
    mae_placeholder = st.empty()
    table_placeholder = st.empty()


def fetch_data():
    trips_msgs = consumer_trips.poll(timeout_ms=100)
    for tp, messages in trips_msgs.items():
        for msg in messages:
            data = msg.value
            st.session_state.map_data.append({
                "lat": data["pickup_latitude"],
                "lon": data["pickup_longitude"]
            })
            if len(st.session_state.map_data) > 500:
                st.session_state.map_data.pop(0)

    metrics_msgs = consumer_metrics.poll(timeout_ms=100)
    for tp, messages in metrics_msgs.items():
        for msg in messages:
            st.session_state.metrics_data.append(msg.value)
            
            if len(st.session_state.metrics_data) > 50:
                st.session_state.metrics_data.pop(0)


while True:
    fetch_data()

    if st.session_state.map_data:
        df_map = pd.DataFrame(st.session_state.map_data)
        map_placeholder.map(df_map, zoom=10)

    if st.session_state.metrics_data:
        df_metrics = pd.DataFrame(st.session_state.metrics_data)
        
        current_mae = df_metrics["error_sec"].mean()
        mae_placeholder.metric(label="Mean Absolute Error (sec)", value=f"{current_mae:.1f}")

        table_placeholder.dataframe(df_metrics.iloc[::-1].head(15), use_container_width=True)
        
    time.sleep(1)