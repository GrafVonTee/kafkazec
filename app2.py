import streamlit as st
from confluent_kafka import Consumer, KafkaError
import json
import time

st.set_page_config(page_title="Kafka Dashboard", layout="wide")

st.title("📈 Kafka Real-Time Dashboard")

# 1. КЭШИРОВАНИЕ: Создаем Consumer один раз и держим соединение открытым
@st.cache_resource
def create_consumer():
    config = {
        'bootstrap.servers': "localhost:9094", # Убедитесь, что порт верный
        'group.id': "streamlit-dashboard-group", # Лучше уникальный ID для тестов
        'auto.offset.reset': 'earliest', # Читать с начала, если нет сохраненного оффсета
        'enable.auto.commit': True
    }
    c = Consumer(config)
    c.subscribe(["Aboba"])
    return c

# Инициализируем состояние для данных
if "price" not in st.session_state:
    st.session_state["price"] = []

# Получаем закэшированного потребителя
consumer = create_consumer()

# Контейнер для графика
chart_placeholder = st.empty()
# Контейнер для статуса/ошибок
status_placeholder = st.empty()

# Кнопка остановки (опционально)
stop_button = st.sidebar.button("Остановить считывание")

if not stop_button:
    status_placeholder.info("Ожидание сообщений из Kafka...")

    # Основной цикл
    while True:
        # poll(0.1) - короткий таймаут, чтобы интерфейс не вис намертво
        message = consumer.poll(1)

        if message is None:
            continue

        if message.error():
            # Обработка ошибок Kafka (например, конец раздела - это не фатальная ошибка)
            if message.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                status_placeholder.error(f"Ошибка Kafka: {message.error()}")
                continue

        # Обработка данных
        try:
            # 2. ДЕКОДИРОВАНИЕ: Превращаем байты в строку, потом в JSON
            raw_value = message.value().decode('utf-8')
            data = json.loads(raw_value)

            # Проверяем, есть ли цена в JSON
            if "price" in data:
                current_price = float(data["price"])
                st.session_state["price"].append(current_price)

                # Ограничим график последними 100 точками, чтобы память не текла
                if len(st.session_state["price"]) > 100:
                    st.session_state["price"].pop(0)

                # 3. ОТРИСОВКА
                with chart_placeholder.container():
                    st.line_chart(st.session_state["price"])
                    st.metric(label="Текущая цена", value=current_price)
            else:
                print(f"Ключ 'price' не найден: {data}")

        except json.JSONDecodeError:
            print("Пришло не JSON сообщение")
        except Exception as e:
            status_placeholder.error(f"Ошибка обработки: {e}")

else:
    status_placeholder.warning("Считывание остановлено.")
