FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir kafka-python pandas scikit-learn numpy catboost

COPY . .

ENV PYTHONPATH=/app