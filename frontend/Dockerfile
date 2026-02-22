FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir streamlit kafka-python pandas numpy pydeck

COPY . .

EXPOSE 8501

CMD ["streamlit", "run", "streamlit_app.py", "--server.address=0.0.0.0"]