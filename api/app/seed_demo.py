import math
import random
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from app.db import clear_demo_data, get_db, init_db, store_measurement
from app.models import MeasurementIn

random.seed(42)

MINUTES_TOTAL = 24 * 60
SOFIA_TZ = ZoneInfo("Europe/Sofia")


DEVICES = [
    {
        "device_eui": "DEMO-001",
        "device_name": "VB Lab 1 Sensor A",
        "organization_name": "Demo",
        "site_name": "Sofia Office",
        "room_name": "Lab 1",
        "gateway_eui": "GW-001",
        "gateway_name": "Gateway East",
        "firmware_version": "0.1.0",
        "battery_type": "AA x2",
        "target_co2_ppm": 1000,
        "co2_room_boost": 520,
        "temp_base": 23.3,
        "rh_base": 44.0,
        "rssi_base": -87,
        "snr_base": 8.2,
        "battery_start": 3.61,
    },
    {
        "device_eui": "DEMO-002",
        "device_name": "VB Lab 1 Sensor B",
        "organization_name": "Demo",
        "site_name": "Sofia Office",
        "room_name": "Lab 1",
        "gateway_eui": "GW-001",
        "gateway_name": "Gateway East",
        "firmware_version": "0.1.0",
        "battery_type": "AA x2",
        "target_co2_ppm": 1000,
        "co2_room_boost": 480,
        "temp_base": 23.0,
        "rh_base": 45.5,
        "rssi_base": -90,
        "snr_base": 6.9,
        "battery_start": 3.58,
    },
    {
        "device_eui": "DEMO-003",
        "device_name": "VB Electronics Bench",
        "organization_name": "Demo",
        "site_name": "Sofia Office",
        "room_name": "Lab 2",
        "gateway_eui": "GW-001",
        "gateway_name": "Gateway East",
        "firmware_version": "0.1.0",
        "battery_type": "AA x2",
        "target_co2_ppm": 1000,
        "co2_room_boost": 430,
        "temp_base": 24.0,
        "rh_base": 41.0,
        "rssi_base": -92,
        "snr_base": 6.0,
        "battery_start": 3.56,
    },
    {
        "device_eui": "DEMO-004",
        "device_name": "VB Open Office Node",
        "organization_name": "Demo",
        "site_name": "Sofia Office",
        "room_name": "Open Office",
        "gateway_eui": "GW-002",
        "gateway_name": "Gateway West",
        "firmware_version": "0.1.0",
        "battery_type": "AA x2",
        "target_co2_ppm": 950,
        "co2_room_boost": 650,
        "temp_base": 23.8,
        "rh_base": 43.0,
        "rssi_base": -85,
        "snr_base": 9.1,
        "battery_start": 3.60,
    },
    {
        "device_eui": "DEMO-005",
        "device_name": "VB Conference Room Node",
        "organization_name": "Demo",
        "site_name": "Sofia Office",
        "room_name": "Conference Room",
        "gateway_eui": "GW-002",
        "gateway_name": "Gateway West",
        "firmware_version": "0.1.0",
        "battery_type": "AA x2",
        "target_co2_ppm": 900,
        "co2_room_boost": 900,
        "temp_base": 23.5,
        "rh_base": 42.0,
        "rssi_base": -94,
        "snr_base": 5.4,
        "battery_start": 3.55,
    },
]


def occupancy_factor(local_ts: datetime, room_name: str) -> float:
    hour = local_ts.hour + (local_ts.minute / 60.0)

    if hour < 7:
        occ = 0.03
    elif hour < 9:
        occ = 0.20 + ((hour - 7) / 2.0) * 0.70
    elif hour < 12:
        occ = 0.90
    elif hour < 13:
        occ = 0.60
    elif hour < 17:
        occ = 0.95
    elif hour < 19:
        occ = max(0.12, 0.95 - ((hour - 17) / 2.0) * 0.80)
    else:
        occ = 0.05

    if room_name == "Conference Room":
        if 10 <= local_ts.hour < 11 or 14 <= local_ts.hour < 15:
            occ += 0.35
        if 16 <= local_ts.hour < 17:
            occ += 0.20
    elif room_name == "Open Office":
        occ += 0.10 if 9 <= local_ts.hour < 18 else 0.0
    elif room_name == "Lab 2":
        occ *= 0.80

    return max(0.0, min(1.35, occ))


def build_measurement(device: dict, ts: datetime, minute_idx: int) -> MeasurementIn:
    local_ts = ts.astimezone(SOFIA_TZ)
    occ = occupancy_factor(local_ts, device["room_name"])

    day_wave = math.sin((minute_idx / 1440.0) * 2 * math.pi)
    short_wave = math.sin((minute_idx / 90.0) * 2 * math.pi)

    co2 = (
        470
        + device["co2_room_boost"] * occ
        + 35 * day_wave
        + 18 * short_wave
        + random.uniform(-18, 18)
    )

    if device["room_name"] == "Conference Room" and 10 <= local_ts.hour < 11:
        co2 += 180
    if device["room_name"] == "Conference Room" and 14 <= local_ts.hour < 15:
        co2 += 140

    co2_ppm = max(420, int(round(co2)))

    temp_c = (
        device["temp_base"]
        + 0.9 * occ
        + 0.35 * math.sin((minute_idx / 180.0) * 2 * math.pi)
        + random.uniform(-0.15, 0.15)
    )

    rh = (
        device["rh_base"]
        - 1.7 * occ
        + 1.2 * math.sin((minute_idx / 240.0) * 2 * math.pi)
        + random.uniform(-0.5, 0.5)
    )

    battery_v = (
        device["battery_start"]
        - (minute_idx / (MINUTES_TOTAL - 1)) * 0.025
        + random.uniform(-0.003, 0.003)
    )

    rssi = int(round(device["rssi_base"] + random.uniform(-4, 4)))
    snr = round(device["snr_base"] + random.uniform(-1.2, 1.2), 1)

    return MeasurementIn(
        ts=ts,
        device_eui=device["device_eui"],
        device_name=device["device_name"],
        organization_name=device["organization_name"],
        site_name=device["site_name"],
        room_name=device["room_name"],
        gateway_eui=device["gateway_eui"],
        gateway_name=device["gateway_name"],
        firmware_version=device["firmware_version"],
        battery_type=device["battery_type"],
        target_co2_ppm=device["target_co2_ppm"],
        co2_ppm=co2_ppm,
        temp_c=round(temp_c, 1),
        rh=round(rh, 1),
        battery_v=round(battery_v, 2),
        rssi=rssi,
        snr=snr,
    )


def seed():
    db = get_db()
    init_db(db)
    end_ts = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    start_ts = end_ts - timedelta(minutes=MINUTES_TOTAL - 1)

    print("Clearing old demo data...")
    clear_demo_data(db)

    inserted = 0
    for minute_idx in range(MINUTES_TOTAL):
        ts = start_ts + timedelta(minutes=minute_idx)

        for device in DEVICES:
            m = build_measurement(device, ts, minute_idx)
            store_measurement(m, db)
            inserted += 1

    print(f"Done. Inserted {inserted} measurements.")
    print(f"Devices: {len(DEVICES)}")
    print(f"Minutes per device: {MINUTES_TOTAL}")
    print("Refresh http://localhost:38081")


if __name__ == "__main__":
    seed()
