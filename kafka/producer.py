from kafka import KafkaProducer
import json
import requests
import time

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'movies_topic_2'
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retries=5,
    request_timeout_ms=30000,
    max_block_ms=120000,
    acks='all',
    api_version=(2, 7, 0)
)

# Fetch movie data
def fetch_movies(pages=5):
    api_key = "826ee045e05169e449e5d5281d940575"
    all_movies = []
    for page in range(1, pages + 1):
        url = f"https://api.themoviedb.org/3/movie/popular?api_key={api_key}&page={page}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            movies = data.get("results", [])
            all_movies.extend(movies)
            print(f"✅ Fetched page {page} with {len(movies)} movies")
        except requests.RequestException as e:
            print(f"❌ Failed to fetch page {page}: {e}")
    return all_movies

# Send movies to Kafka
while True:
    print("🚀 Starting movie data fetch...")
    movies = fetch_movies(pages=20)
    for movie in movies:
        try:
            producer.send(KAFKA_TOPIC, movie)
            print(f"📤 Sent to Kafka: {movie['title']}")
        except Exception as e:
            print(f"❌ Failed to send {movie['title']}: {e}")
    print("⏳ Waiting 1 hour before next fetch...\n")
    time.sleep(3600)
