import time
import random
from datetime import datetime
import requests
from kafka import KafkaProducer

bootstrap_servers = ['localhost:9092']
kafka_topic = 'Assignment_3'
producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda v: v.encode('utf-8'))

sources = ["abc-news", "nbc-news", "fox-sports", "the-washington-post", "cnn"]

def fetch_and_push_news():
    try:
        src = random.choice(sources)
        url = "https://newsapi.org/v2/everything"
        params = {
            "sources": src,
            "apiKey": "7cbad3d091974695bed59b96eef031d6"
        }

        res = requests.get(url, params=params)
        articles = res.json().get('articles', [])

        if not articles:
            print(f"No articles found")
            return

        for article in articles:
            content = article.get('content')
            if content:
                producer.send(kafka_topic, value=content)

    except Exception as e:
        print(f"Error while fetching or pushing news: {e}")

if __name__ == "__main__":
    attempts = 60

    for attempt in range(attempts):
        fetch_and_push_news()
        time.sleep(60)