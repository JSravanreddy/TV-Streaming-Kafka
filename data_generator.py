import time
import random
import string
import json
from datetime import datetime  # Fixed import
from kafka import KafkaProducer # type: ignore
from fastapi import FastAPI


CHANNELS = [
    {"id": "BBC", "genre": "News"},
    {"id": "ESPN", "genre": "Sports"},
    {"id": "HBO", "genre": "Movies"},
    {"id": "NatGeo", "genre": "Documentary"},
    {"id": "CartoonNet", "genre": "Kids"}
]

REGIONS = ["Brussels", "Antwerp", "Ghent", "Charleroi", "Liege"]
AGE_GROUPS = ["18-25", "26-35", "36-50", "50+"]
DEVICE_TYPES = ["SmartTV"]

EVENT_TYPES = [
    "channel_start", 
    "channel_end",
    "channel_switch",
    "ad_start",
    "ad_complete",
    "ad_skip",
    "buffer_event",
    "error_event",
    "menu_open",
    "startup",
    "shutdown"
]


def generate_random_event():
    user_id = 'U_' + "".join(random.choices(string.digits, k=7))
    household_id = 'HH_' + "".join(random.choices(string.digits, k=4))
    tv_id = 'TV_' + "".join(random.choices(string.digits, k=6))
    region = random.choice(REGIONS)  # Use choice instead of choices
    age_group = random.choice(AGE_GROUPS)
    device = random.choice(DEVICE_TYPES)
    timestamp = datetime.now().isoformat()
    event_type = random.choice(EVENT_TYPES)

    base_event = {
        "userId": user_id,
        "householdId": household_id,
        "tvId": tv_id,
        "region": region,
        "ageGroup": age_group,
        "deviceType": device,
        "timestamp": timestamp,
        "event": event_type
    }

    if event_type in ['channel_start', 'channel_switch']:
        channel = random.choice(CHANNELS)
        base_event.update({'channel_id': channel['id'], 'genre': channel['genre']})

    elif event_type == 'channel_end':
        base_event['watchDurationSec'] = random.randint(60, 3600)

    elif event_type.startswith('ad_'):
        base_event["adId"] = 'Ad_' + "".join(random.choices(string.digits, k=6))

    elif event_type == "buffer_event":
        base_event["bufferDurationMs"] = random.randint(100, 5000)

    elif event_type == "error_event":
        base_event["errorCode"] = random.choice(["E101", "E203", "E305"])

    return base_event
