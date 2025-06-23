import asyncio
import random
from fastapi import FastAPI
from data_generator import generate_random_event
from kafka_producer import publish_to_kafka
from datetime import datetime
from prometheus_fastapi_instrumentator import Instrumentator

app = FastAPI()

instrumentator = Instrumentator().instrument(app).expose(app)


# Toggle for controlling the streaming
is_streaming = False

@app.get("/")
def root():
    return {"message": "TV Event API is running 👋"}

@app.get("/generate_event")
def generate_event():
    return generate_random_event()

@app.post('/send_event')
def send_event_to_kafka():
    event = generate_random_event()
    publish_to_kafka("tv-events", event)
    return {'message': 'Sent to Kafka at ' + str(datetime.now().isoformat())}

@app.post("/start_stream")
def start_stream():
    global is_streaming
    is_streaming = True
    return {"message": "Streaming started"}

@app.post("/stop_stream")
def stop_stream():
    global is_streaming
    is_streaming = False
    return {"message": "Streaming stopped"}

@app.on_event("startup")
async def stream_events():
    async def producer_loop():
        global is_streaming
        while True:
            if is_streaming:
                event = generate_random_event()
                publish_to_kafka("tv-events", event)
                print(f"Sent: {event}")
                await asyncio.sleep(0.01)  # 10ms delay
            else:
                await asyncio.sleep(1)  # wait and check again

    asyncio.create_task(producer_loop())  # don't block startup
