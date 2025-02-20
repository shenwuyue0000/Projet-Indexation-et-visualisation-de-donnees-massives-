from kafka import KafkaConsumer
import json

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'movies_topic_2'
local_path = 'D:/hadoop/movies_data-2.json'

# Initialize Consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='earliest',  # Commencez à consommer dès le premier message
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    api_version=(2, 7, 0)
)

# Write consumed messages to JSON file
with open(local_path, 'w', encoding='utf-8') as file:
    print("🚀 Consumer started, waiting for messages...")
    for message in consumer:
        movie = message.value
        try:
            file.write(json.dumps(movie) + "\n")
            file.flush()  # Garantit que chaque message est écrit immédiatement sur le disque
            print(f"💾 Successfully saved: {movie['title']}")
        except Exception as e:
            print(f"❌ Failed to save {movie['title']}: {e}")
