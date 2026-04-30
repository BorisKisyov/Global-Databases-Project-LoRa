import base64
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, Tuple

import paho.mqtt.client as mqtt
import requests

MQTT_HOST = os.getenv("MQTT_HOST", "chirpstack-mosquitto")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
API_BASE = os.getenv("API_BASE", "http://vb-api:8000")
DEMO_DEVICE_COUNT = int(os.getenv("DEMO_DEVICE_COUNT", "5"))
DEMO_PUBLISH_INTERVAL = int(os.getenv("DEMO_PUBLISH_INTERVAL", "30"))
DEMO_SITE_NAME = os.getenv("DEMO_SITE_NAME", "Demo Site")
DEMO_TENANT_NAME = os.getenv("DEMO_TENANT_NAME", "Demo Tenant")
DEMO_APPLICATION_NAME = os.getenv("DEMO_APPLICATION_NAME", "Demo Application")
DEMO_GATEWAY_ID = os.getenv("DEMO_GATEWAY_ID", "demo-gateway-0001")
DEMO_ATTACKS_ENABLED = os.getenv("DEMO_ATTACKS_ENABLED", "true").lower() == "true"
DEMO_ATTACK_MIN_INTERVAL = int(os.getenv("DEMO_ATTACK_MIN_INTERVAL", "15"))
DEMO_ATTACK_MAX_INTERVAL = int(os.getenv("DEMO_ATTACK_MAX_INTERVAL", "30"))
DEMO_STATUS_EVERY_N_BATCHES = max(1, int(os.getenv("DEMO_STATUS_EVERY_N_BATCHES", "2")))
DEMO_ACK_EVERY_N_BATCHES = max(1, int(os.getenv("DEMO_ACK_EVERY_N_BATCHES", "3")))
DEMO_STARTUP_DELAY = int(os.getenv("DEMO_STARTUP_DELAY", "10"))
DEMO_API_WAIT_TIMEOUT = int(os.getenv("DEMO_API_WAIT_TIMEOUT", "180"))
DEMO_MQTT_WAIT_TIMEOUT = int(os.getenv("DEMO_MQTT_WAIT_TIMEOUT", "180"))

if DEMO_ATTACK_MIN_INTERVAL > DEMO_ATTACK_MAX_INTERVAL:
    DEMO_ATTACK_MIN_INTERVAL, DEMO_ATTACK_MAX_INTERVAL = DEMO_ATTACK_MAX_INTERVAL, DEMO_ATTACK_MIN_INTERVAL

session = requests.Session()


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_dev_eui(i: int) -> str:
    return f"{i + 1:016x}"


def make_device_info(dev_eui: str) -> Dict:
    return {
        "tenantName": DEMO_TENANT_NAME,
        "applicationName": DEMO_APPLICATION_NAME,
        "deviceProfileName": "Demo Profile",
        "deviceName": dev_eui,
        "devEui": dev_eui,
        "tags": {"site": DEMO_SITE_NAME},
    }


def measurement_values(device_index: int, fcnt: int) -> Tuple[int, float, int, float, int, float]:
    co2 = 780 + device_index * 55 + (fcnt % 6) * 45
    temp_c = round(20.2 + device_index * 0.4 + (fcnt % 5) * 0.2, 1)
    rh = 42 + ((device_index * 3 + fcnt) % 12)
    battery_v = round(3.55 - device_index * 0.03 - ((fcnt % 20) * 0.002), 2)
    rssi = -78 - device_index * 3 - (fcnt % 4)
    snr = round(8.5 - device_index * 0.6 - ((fcnt % 3) * 0.4), 1)
    return co2, temp_c, rh, battery_v, rssi, snr


def encode_payload_b64(co2: int, temp_c: float, rh: int, battery_v: float) -> str:
    payload = bytearray()
    payload.extend(int(co2).to_bytes(2, "big", signed=False))
    payload.extend(int(round(temp_c * 100)).to_bytes(2, "big", signed=False))
    payload.append(int(rh) & 0xFF)
    payload.extend(int(round(battery_v * 1000)).to_bytes(2, "big", signed=False))
    return base64.b64encode(bytes(payload)).decode()


def wait_for_http(url: str, timeout_s: int) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            r = session.get(url, timeout=5)
            if r.ok:
                print(f"[demo-publisher] API ready at {url}")
                return
        except Exception:
            pass
        time.sleep(2)
    raise RuntimeError(f"Timed out waiting for API at {url}")


def wait_for_mqtt(host: str, port: int, timeout_s: int) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        client = None
        try:
            client = mqtt.Client(client_id=f"GD-mqtt-check-{uuid.uuid4().hex[:8]}")
            client.connect(host, port, 10)
            client.disconnect()
            print(f"[demo-publisher] MQTT ready at {host}:{port}")
            return
        except Exception:
            time.sleep(2)
        finally:
            if client is not None:
                try:
                    client.disconnect()
                except Exception:
                    pass
    raise RuntimeError(f"Timed out waiting for MQTT at {host}:{port}")


def publish_event(client: mqtt.Client, topic: str, payload: Dict) -> None:
    info = client.publish(topic, json.dumps(payload), qos=0, retain=False)
    info.wait_for_publish()


def publish_join(client: mqtt.Client, dev_eui: str, dev_addr: str) -> None:
    topic = f"application/demo-app/device/{dev_eui}/event/join"
    payload = {
        "deduplicationId": str(uuid.uuid4()),
        "time": iso_now(),
        "deviceInfo": make_device_info(dev_eui),
        "devAddr": dev_addr,
    }
    publish_event(client, topic, payload)


def make_up_payload(dev_eui: str, dev_addr: str, fcnt: int, co2: int, temp_c: float, rh: int, battery_v: float, rssi: int, snr: float) -> Dict:
    return {
        "deduplicationId": str(uuid.uuid4()),
        "time": iso_now(),
        "deviceInfo": make_device_info(dev_eui),
        "devAddr": dev_addr,
        "adr": True,
        "dr": 5,
        "fCnt": fcnt,
        "fPort": 10,
        "confirmed": False,
        "data": encode_payload_b64(co2, temp_c, rh, battery_v),
        "rxInfo": [
            {
                "gatewayId": DEMO_GATEWAY_ID,
                "rssi": rssi,
                "snr": snr,
            }
        ],
    }


def publish_up(client: mqtt.Client, dev_eui: str, payload: Dict) -> None:
    topic = f"application/demo-app/device/{dev_eui}/event/up"
    publish_event(client, topic, payload)


def publish_status(client: mqtt.Client, dev_eui: str, battery_level: float, margin: int) -> None:
    topic = f"application/demo-app/device/{dev_eui}/event/status"
    payload = {
        "time": iso_now(),
        "deviceInfo": make_device_info(dev_eui),
        "margin": margin,
        "batteryLevel": battery_level,
    }
    publish_event(client, topic, payload)


def publish_ack(client: mqtt.Client, dev_eui: str, acknowledged: bool, f_cnt_down: int) -> None:
    topic = f"application/demo-app/device/{dev_eui}/event/ack"
    payload = {
        "time": iso_now(),
        "deviceInfo": make_device_info(dev_eui),
        "queueItemId": f"demo-queue-item-{f_cnt_down}",
        "acknowledged": acknowledged,
        "fCntDown": f_cnt_down,
    }
    publish_event(client, topic, payload)


def publish_log(client: mqtt.Client, dev_eui: str, level: str, code: str, description: str, dedup_id: str | None = None) -> None:
    topic = f"application/demo-app/device/{dev_eui}/event/log"
    payload = {
        "time": iso_now(),
        "deviceInfo": make_device_info(dev_eui),
        "level": level,
        "code": code,
        "description": description,
        "context": {"deduplication_id": dedup_id} if dedup_id else {},
    }
    publish_event(client, topic, payload)


def post_ingest(dev_eui: str, ts: str, co2: int, temp_c: float, rh: int, battery_v: float, rssi: int, snr: float) -> None:
    body = {
        "device_eui": dev_eui,
        "device_name": dev_eui,
        "ts": ts,
        "co2_ppm": co2,
        "temp_c": temp_c,
        "rh": rh,
        "battery_v": battery_v,
        "rssi": rssi,
        "snr": snr,
        "organization_name": DEMO_TENANT_NAME,
        "site_name": DEMO_SITE_NAME,
        "room_name": DEMO_APPLICATION_NAME,
        "gateway_eui": DEMO_GATEWAY_ID,
        "gateway_name": DEMO_GATEWAY_ID,
    }
    r = session.post(f"{API_BASE}/ingest", json=body, timeout=10)
    r.raise_for_status()


def inject_attack_cycle(client: mqtt.Client, device: Dict) -> None:
    last_up = device.get("last_up_payload")
    if not last_up:
        return

    duplicate_up = dict(last_up)
    duplicate_up["time"] = iso_now()
    publish_up(client, device["dev_eui"], duplicate_up)

    publish_log(
        client,
        device["dev_eui"],
        "ERROR",
        "UPLINK_MIC",
        "MIC of uplink frame is invalid, make sure keys are correct",
        dedup_id=last_up.get("deduplicationId"),
    )
    publish_log(
        client,
        device["dev_eui"],
        "WARNING",
        "FCNT_REPLAY",
        "Frame-counter or nonce replay suspected for uplink, duplicate or stale packet rejected",
        dedup_id=last_up.get("deduplicationId"),
    )
    publish_ack(client, device["dev_eui"], acknowledged=False, f_cnt_down=device["fcnt"])
    publish_status(client, device["dev_eui"], battery_level=86.5, margin=7)

    print(f"[demo-publisher] injected demo attack set for {device['dev_eui']}")


def next_attack_delay() -> int:
    return random.randint(DEMO_ATTACK_MIN_INTERVAL, DEMO_ATTACK_MAX_INTERVAL)


def main() -> None:
    wait_for_http(f"{API_BASE}/health", DEMO_API_WAIT_TIMEOUT)
    wait_for_mqtt(MQTT_HOST, MQTT_PORT, DEMO_MQTT_WAIT_TIMEOUT)

    if DEMO_STARTUP_DELAY > 0:
        print(f"[demo-publisher] startup delay {DEMO_STARTUP_DELAY}s")
        time.sleep(DEMO_STARTUP_DELAY)

    client = mqtt.Client(client_id=f"GD-demo-publisher-{uuid.uuid4().hex[:8]}")
    client.connect(MQTT_HOST, MQTT_PORT, 60)
    client.loop_start()

    devices = []
    for i in range(DEMO_DEVICE_COUNT):
        dev_eui = make_dev_eui(i)
        dev_addr = f"{0x01000000 + i:08x}"
        devices.append({"dev_eui": dev_eui, "dev_addr": dev_addr, "fcnt": 0, "last_up_payload": None})

    next_attack_at = time.monotonic() + next_attack_delay()

    try:
        for device in devices:
            publish_join(client, device["dev_eui"], device["dev_addr"])
        print(f"[demo-publisher] joins published for {DEMO_DEVICE_COUNT} devices")

        batch_no = 0
        while True:
            batch_no += 1
            for idx, device in enumerate(devices):
                device["fcnt"] += 1
                co2, temp_c, rh, battery_v, rssi, snr = measurement_values(idx, device["fcnt"])
                ts = iso_now()
                up_payload = make_up_payload(device["dev_eui"], device["dev_addr"], device["fcnt"], co2, temp_c, rh, battery_v, rssi, snr)
                publish_up(client, device["dev_eui"], up_payload)
                post_ingest(device["dev_eui"], ts, co2, temp_c, rh, battery_v, rssi, snr)
                device["last_up_payload"] = up_payload

                if batch_no % DEMO_STATUS_EVERY_N_BATCHES == 0:
                    publish_status(
                        client,
                        device["dev_eui"],
                        battery_level=max(5.0, round(battery_v / 3.6 * 100, 1)),
                        margin=max(3, int(round(snr + 4))),
                    )

                if batch_no % DEMO_ACK_EVERY_N_BATCHES == 0:
                    publish_ack(client, device["dev_eui"], acknowledged=True, f_cnt_down=device["fcnt"])

            print(f"[demo-publisher] published batch {batch_no} at {iso_now()}")

            cycle_end = time.monotonic() + DEMO_PUBLISH_INTERVAL
            while True:
                now = time.monotonic()

                if DEMO_ATTACKS_ENABLED and now >= next_attack_at:
                    target = random.choice(devices)
                    inject_attack_cycle(client, target)
                    delay = next_attack_delay()
                    next_attack_at = now + delay
                    print(f"[demo-publisher] next attack scheduled in {delay}s")

                remaining = cycle_end - now
                if remaining <= 0:
                    break

                time.sleep(min(1.0, remaining))
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
