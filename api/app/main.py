import json
import os
import threading
from typing import Optional

import paho.mqtt.client as mqtt
from fastapi import FastAPI, HTTPException, Query

from app.db import (
    fetch_alerts,
    fetch_device_history,
    fetch_device_latest,
    fetch_device_security,
    fetch_devices,
    fetch_latest,
    fetch_organizations,
    fetch_security_events,
    fetch_security_summary,
    init_db,
    ping_database,
    store_measurement,
    store_security_event,
)
from app.models import MeasurementIn
from app.raw_lora_secure_demo import run_demo

app = FastAPI(title="Demo LoRaWAN Security API", version="1.0.0")

CHIRPSTACK_MQTT_ENABLED = os.environ.get("CHIRPSTACK_MQTT_ENABLED", "false").lower() == "true"
CHIRPSTACK_MQTT_HOST = os.environ.get("CHIRPSTACK_MQTT_HOST", "chirpstack-mosquitto")
CHIRPSTACK_MQTT_PORT = int(os.environ.get("CHIRPSTACK_MQTT_PORT", "1883"))
CHIRPSTACK_MQTT_TOPIC = os.environ.get("CHIRPSTACK_MQTT_TOPIC", "application/+/device/+/event/+")

MQTT_THREAD_STARTED = False


def on_mqtt_connect(client, userdata, flags, rc, properties=None):
    client.subscribe(CHIRPSTACK_MQTT_TOPIC, qos=0)


def on_mqtt_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
    except Exception:
        return

    try:
        store_security_event(msg.topic, payload)
    except Exception as exc:
        print(f"[mqtt-bridge] failed to store event from {msg.topic}: {exc}")


def start_mqtt_bridge():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_mqtt_connect
    client.on_message = on_mqtt_message
    client.connect(CHIRPSTACK_MQTT_HOST, CHIRPSTACK_MQTT_PORT, 60)
    client.loop_forever()


@app.on_event("startup")
def on_startup():
    global MQTT_THREAD_STARTED
    init_db()
    if CHIRPSTACK_MQTT_ENABLED and not MQTT_THREAD_STARTED:
        thread = threading.Thread(target=start_mqtt_bridge, daemon=True)
        thread.start()
        MQTT_THREAD_STARTED = True


@app.get("/health")
def health():
    return {
        "ok": True,
        "service": "vb-api",
        "version": app.version,
        "chirpstack_mqtt_enabled": CHIRPSTACK_MQTT_ENABLED,
    }


@app.get("/db-check")
def db_check():
    return ping_database()


@app.post("/ingest")
def ingest(m: MeasurementIn):
    store_measurement(m)
    return {
        "stored": True,
        "device_eui": m.device_eui,
        "room": m.room_name,
        "site": m.site_name,
        "organization": m.organization_name,
    }


@app.get("/organizations")
def list_organizations():
    return fetch_organizations()


@app.get("/devices")
def list_devices():
    return fetch_devices()


@app.get("/latest")
def latest(limit: int = Query(default=50, ge=1, le=500)):
    return fetch_latest(limit)


@app.get("/devices/{device_eui}/latest")
def device_latest(device_eui: str):
    row = fetch_device_latest(device_eui)
    if row is None:
        raise HTTPException(status_code=404, detail="Device not found")
    return row


@app.get("/devices/{device_eui}/history")
def device_history(
    device_eui: str,
    hours: int = Query(default=24, ge=1, le=24 * 365),
    limit: int = Query(default=1000, ge=1, le=5000),
):
    rows = fetch_device_history(device_eui, hours, limit)
    if rows is None:
        raise HTTPException(status_code=404, detail="Device not found")
    return rows


@app.get("/alerts")
def list_alerts(active_only: bool = True, limit: int = Query(default=100, ge=1, le=1000)):
    return fetch_alerts(active_only, limit)


@app.get("/security/events")
def security_events(limit: int = Query(default=100, ge=1, le=1000), event_type: Optional[str] = None):
    return fetch_security_events(limit, event_type)


@app.get("/devices/{device_eui}/security")
def device_security(device_eui: str, limit: int = Query(default=50, ge=1, le=500)):
    payload = fetch_device_security(device_eui, limit)
    if payload is None:
        raise HTTPException(status_code=404, detail="Device security state not found")
    return payload


@app.get("/security/summary")
def security_summary():
    return fetch_security_summary()


@app.get("/security/raw-demo")
def security_raw_demo():
    return {"results": run_demo()}
