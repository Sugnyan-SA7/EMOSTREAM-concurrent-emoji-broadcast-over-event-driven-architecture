from kafka import KafkaConsumer
from json import loads

KAFKA_TOPIC = "emoji_stream"
KAFKA_SERVER = "localhost:9092" 

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_SERVER],
    auto_offset_reset='earliest', 
    enable_auto_commit=True,
    group_id='emoji-consumer-group',  
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

print(f"Connected to Kafka server and listening to topic '{KAFKA_TOPIC}'...")

try:
    for message in consumer:
        emoji_data = message.value 
        print(f"{emoji_data}")
except KeyboardInterrupt:
    print("Consumer interrupted and stopped.")
finally:
    consumer.close()