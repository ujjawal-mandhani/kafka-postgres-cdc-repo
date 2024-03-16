import requests
import json
import time 
from confluent_kafka import Consumer, KafkaException
import os
import ipaddress

print("::::Start Sleepping")
time.sleep(15)
print("::::Stop Sleepping")

url = "http://postgres-connector:8083/connectors/"
headers = {
  'Accept': 'application/json'
}
response = requests.get(url, headers=headers)

print(response.text)

payload = {
    "name": "pg-orders-source",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "postgres",
        "database.port": "5432",
        "database.user": "postgres_user",
        "database.password": "postgres_password",
        "database.dbname": "postgres",
        "database.server.name": "postgres",
        "plugin.name": "pgoutput", 
        "topic.prefix": "my_prefix",
        "table.include.list": "public.customers",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter"
    }
}

response = requests.request("POST", url, headers=headers, json=payload)

print(response.text)

kafka_config = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': "python-consumer",
    'auto.offset.reset': 'earliest'
}   

try:
    kafka_consumer = Consumer(kafka_config)
except KafkaException as e:
    print(f"Failed to create Kafka consumer: {e}")
    exit(1)
time.sleep(10)
print("::::Starting subscribing to kafka topic")
kafka_consumer.subscribe(["my_prefix.public.customers"])
print("::::Done subscribing to kafka topic")
try:
    while True:
        message = kafka_consumer.poll(timeout=25)
        if message is None:
            continue
        if message.value() is None:
            continue
        if message.error():
            print(f"Consumer error: {message.error()}")
        print(f"Received message value: {message.value().decode('utf-8')}")
        if message.key() is None:
            continue
        print(f"Received message key: {message.key().decode('utf-8')}")
except KeyboardInterrupt:
    kafka_consumer.close()
