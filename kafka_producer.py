from kafka import KafkaProducer # type: ignore
import json

producer = KafkaProducer (
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


def publish_to_kafka(topic ,message):
    producer.send(topic , value = message)
    producer.flush()