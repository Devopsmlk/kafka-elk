import json
import time
from datetime import datetime
from kafka import KafkaProducer
from fastapi import FastAPI

app = FastAPI(title="Test Service")

# Wait for Kafka to be ready
def create_kafka_producer():
    max_retries = 30
    for i in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=['kafka:9092'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                retries=5,
                retry_backoff_ms=1000
            )
            print("Connected to Kafka successfully!")
            return producer
        except Exception as e:
            print(f"⏳ Waiting for Kafka... attempt {i+1}/{max_retries}")
            time.sleep(2)
    
    raise Exception("Could not connect to Kafka after 30 attempts")

# Initialize producer with retry logic
producer = create_kafka_producer()

@app.get("/")
def home():
    return {"message": "Test Service is running!"}

@app.post("/send-log")
def send_log(message: str = "Hello from Test Service"):
    log_data = {
        "timestamp": datetime.now().isoformat(),
        "level": "INFO",
        "service": "test-service",
        "message": message,
        "user_id": 12345
    }
    
    try:
        producer.send('app-logs', log_data)
        producer.flush()
        return {"status": "success", "data": log_data}
    except Exception as e:
        return {"status": "error", "error": str(e)}

@app.post("/send-event")
def send_event(event_type: str = "user_signup"):
    event_data = {
        "timestamp": datetime.now().isoformat(),
        "event_type": event_type,
        "user_id": 67890,
        "source": "test-service",
        "metadata": {
            "ip": "192.168.1.100",
            "browser": "Chrome"
        }
    }
    
    try:
        producer.send('user-events', event_data)
        producer.flush()
        return {"status": "success", "data": event_data}
    except Exception as e:
        return {"status": "error", "error": str(e)}