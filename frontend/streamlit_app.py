import streamlit as st
import pandas as pd
import numpy as np
import json
import time
import pydeck as pdk
import uuid
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

st.set_page_config(page_title="Мониторинг такси", layout="wide")
st.title("Система мониторинга такси в реальном времени")


@st.cache_resource
def get_consumers():
    session_group_id = f"streamlit_group_{uuid.uuid4().hex[:8]}"
    bootstrap_servers = ['kafka-1:9092', 'kafka-2:9092', 'kafka-3:9092'] # грузим всех брокеров, иначе консьюмер умрёт вместе с брокером
    config = {
        'bootstrap_servers': bootstrap_servers,
        'value_deserializer': lambda m: json.loads(m.decode('utf-8')),
        'auto_offset_reset': 'earliest', # читаем с начала, чтобы подгрузить историю
        'api_version': (3, 0, 0),
    }
    
    # инициализируем наши топики
    c_trips = KafkaConsumer('new_trips', group_id=f"{session_group_id}_trips", **config)
    c_preds = KafkaConsumer('ml_predictions', group_id=f"{session_group_id}_preds", **config)
    c_metrics = KafkaConsumer('model_metrics', group_id=f"{session_group_id}_metrics", **config)
    return c_trips, c_preds, c_metrics


consumer_trips, consumer_preds, consumer_metrics = get_consumers()

if not consumer_trips:
    st.error("Ошибка подключения к брокерам Kafka.")
    st.stop()


if 'trips_registry' not in st.session_state:
    st.session_state.trips_registry = {} 
if 'metrics_history' not in st.session_state:
    st.session_state.metrics_history = []
if 'overall_counter' not in st.session_state:
    st.session_state.overall_counter = 0


def process_data():
    msgs = consumer_trips.poll(timeout_ms=50)
    for tp, messages in msgs.items():
        for msg in messages:
            d = msg.value
            st.session_state.trips_registry[d["id"]] = {
                "id": d["id"],
                "pickup_lat": d["pickup_latitude"], "pickup_lon": d["pickup_longitude"],
                "dropoff_lat": d["dropoff_latitude"], "dropoff_lon": d["dropoff_longitude"],
                "start_time": d["pickup_datetime"],
                "status": "в пути", "color": [255, 0, 0, 150],
                "predicted": None
            }
            st.session_state.overall_counter += 1
            
    p_msgs = consumer_preds.poll(timeout_ms=50)
    for tp, messages in p_msgs.items():
        for msg in messages:
            p = msg.value
            tid = p["id"]
            if tid in st.session_state.trips_registry:
                st.session_state.trips_registry[tid]["predicted"] = round(p["predicted_sec"], 2)

    m_msgs = consumer_metrics.poll(timeout_ms=50)
    for tp, messages in m_msgs.items():
        for msg in messages:
            m = msg.value
            tid = m["id"]
            if tid in st.session_state.trips_registry:
                st.session_state.trips_registry[tid].update({
                    "status": "завершена",
                    "color": [0, 0, 255, 200],
                    "actual": m["actual"],
                    "error_sec": round(m["error_sec"], 2),
                    "finish_timestamp": time.time()
                })
            st.session_state.metrics_history.append(m)

    finished_ids = [tid for tid, data in st.session_state.trips_registry.items() if data['status'] == 'завершена']
    if len(finished_ids) > 10:
        finished_ids.sort(key=lambda x: st.session_state.trips_registry[x].get('finish_timestamp', 0))
        for tid in finished_ids[:-10]:
            del st.session_state.trips_registry[tid]

process_data()

mae, mape = 0.0, 0.0
if st.session_state.metrics_history:
    m_df = pd.DataFrame(st.session_state.metrics_history)
    mae = m_df["error_sec"].mean()
    mape = (np.abs(m_df["predicted"] - m_df["actual"]) / m_df["actual"].replace(0, 1)).mean() * 100

# Верхняя панель метрик
m_col1, m_col2, m_col3 = st.columns(3)
m_col1.metric("Всего обработано заказов", st.session_state.overall_counter)
m_col2.metric("Средняя ошибка (MAE), сек", f"{mae:.2f}")
m_col3.metric("Ошибка (MAPE), %", f"{mape:.2f}%")

st.divider()

# Основной блок: Карта и Активные поездки
top_col1, top_col2 = st.columns([2, 1])
all_data_df = pd.DataFrame(list(st.session_state.trips_registry.values()))

with top_col1:
    st.subheader("Карта текущих маршрутов")
    if not all_data_df.empty:
        layer = pdk.Layer(
            "LineLayer", all_data_df,
            get_source_position="[pickup_lon, pickup_lat]",
            get_target_position="[dropoff_lon, dropoff_lat]",
            get_color="color", get_width=4, pickable=True,
        )
        st.pydeck_chart(pdk.Deck(
            layers=[layer], 
            initial_view_state=pdk.ViewState(latitude=40.73, longitude=-73.93, zoom=11, pitch=45),
            tooltip={"text": "ID: {id}\nСтатус: {status}"}
        ))

with top_col2:
    st.subheader("Активные поездки")
    if not all_data_df.empty:
        ongoing_df = all_data_df[all_data_df['status'] == 'в пути'].iloc[::-1]
        if not ongoing_df.empty:
            st.dataframe(
                ongoing_df[['id', 'start_time', 'predicted']], 
                use_container_width=True, height=400
            )
        else:
            st.info("Нет активных поездок.")

st.divider()

# Нижний блок: Полная история результатов
st.subheader("История завершенных поездок и точность прогноза")
if st.session_state.metrics_history:
    history_df = pd.DataFrame(st.session_state.metrics_history).iloc[::-1].head(20)
    st.dataframe(history_df, use_container_width=True)
else:
    st.info("История пуста. Ожидайте завершения первых поездок.")

time.sleep(1)
st.rerun()