import base64
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.database import Database
from pymongo.errors import ConfigurationError, DuplicateKeyError

from app.models import MeasurementIn

DEFAULT_MONGODB_URI = "mongodb://root:root@vb-db:27017/GD?authSource=admin"
DEFAULT_MONGODB_DB = "GD"
DEFAULT_SITE_NAME = os.environ.get("DEFAULT_SITE_NAME", "ChirpStack Lab")

_client: Optional[MongoClient] = None
_db: Optional[Database] = None


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def resolve_db(db: Optional[Database] = None) -> Database:
    return db if db is not None else get_db()


def get_db() -> Database:
    global _client, _db
    if _db is not None:
        return _db

    mongodb_uri = os.environ.get("MONGODB_URI", DEFAULT_MONGODB_URI)
    mongodb_db = os.environ.get("MONGODB_DB", DEFAULT_MONGODB_DB)

    _client = MongoClient(mongodb_uri, tz_aware=True)
    try:
        default_db = _client.get_default_database()
    except ConfigurationError:
        default_db = None

    _db = default_db if default_db is not None else _client[mongodb_db]
    return _db


def init_db(db: Optional[Database] = None):
    database = resolve_db(db)

    database.organizations.create_index("name", unique=True)
    database.organizations.create_index("id", unique=True)
    database.sites.create_index([("organization_id", ASCENDING), ("name", ASCENDING)], unique=True)
    database.sites.create_index("id", unique=True)
    database.rooms.create_index([("site_id", ASCENDING), ("name", ASCENDING)], unique=True)
    database.rooms.create_index("id", unique=True)
    database.gateways.create_index("gateway_eui", unique=True)
    database.gateways.create_index("id", unique=True)
    database.devices.create_index("device_eui", unique=True)
    database.devices.create_index("id", unique=True)
    database.devices.create_index("room_id")
    database.devices.create_index("site_id")
    database.measurements.create_index([("device_id", ASCENDING), ("time", ASCENDING)], unique=True)
    database.measurements.create_index([("device_id", ASCENDING), ("time", DESCENDING)])
    database.measurements.create_index([("time", DESCENDING)])
    database.device_last_state.create_index("device_id", unique=True)
    database.alerts.create_index("id", unique=True)
    database.alerts.create_index([("is_active", ASCENDING), ("triggered_at", DESCENDING)])
    database.security_events.create_index("id", unique=True)
    database.security_events.create_index([("dev_eui", ASCENDING), ("observed_at", DESCENDING)])
    database.security_events.create_index([("event_type", ASCENDING), ("observed_at", DESCENDING)])
    database.security_events.create_index(
        [("deduplication_id", ASCENDING), ("event_type", ASCENDING), ("dev_eui", ASCENDING)]
    )
    database.device_security_state.create_index("dev_eui", unique=True)


def ping_database() -> dict[str, Any]:
    database = get_db()
    database.client.admin.command("ping")
    return {"db_ok": True, "db_time": utc_now().isoformat()}


def clear_demo_data(db: Optional[Database] = None):
    database = resolve_db(db)
    database.alerts.delete_many({})
    database.device_last_state.delete_many({})
    database.measurements.delete_many({})
    database.devices.delete_many({})
    database.gateways.delete_many({})
    database.rooms.delete_many({})
    database.sites.delete_many({})
    database.organizations.delete_many({})
    database.counters.delete_many({"_id": {"$in": ["alerts", "devices", "gateways", "organizations", "rooms", "sites"]}})


def next_sequence(name: str, db: Optional[Database] = None) -> int:
    database = resolve_db(db)
    row = database.counters.find_one_and_update(
        {"_id": name},
        {"$inc": {"seq": 1}},
        upsert=True,
        return_document=True,
    )
    return int(row["seq"])


def _serialize_datetime_fields(document: dict[str, Any], *fields: str) -> dict[str, Any]:
    row = dict(document)
    for field in fields:
        value = row.get(field)
        if isinstance(value, datetime):
            row[field] = value.isoformat()
    return row


def _serialize_rows(rows: list[dict[str, Any]], *fields: str) -> list[dict[str, Any]]:
    return [_serialize_datetime_fields(row, *fields) for row in rows]


def _insert_with_numeric_id(collection, sequence_name: str, unique_filter: dict[str, Any], document: dict[str, Any]):
    candidate = dict(document)
    candidate["id"] = next_sequence(sequence_name, collection.database)
    try:
        collection.insert_one(candidate)
        return candidate
    except DuplicateKeyError:
        existing = collection.find_one(unique_filter)
        if existing is None:
            raise
        return existing


def _ensure_organization(database: Database, organization_name: str) -> int:
    organizations = database.organizations
    existing = organizations.find_one({"name": organization_name}, {"id": 1})
    if existing:
        return int(existing["id"])

    created = _insert_with_numeric_id(
        organizations,
        "organizations",
        {"name": organization_name},
        {"name": organization_name, "created_at": utc_now()},
    )
    return int(created["id"])


def _ensure_site(database: Database, organization_id: int, site_name: str) -> int:
    sites = database.sites
    key = {"organization_id": organization_id, "name": site_name}
    existing = sites.find_one(key, {"id": 1})
    if existing:
        return int(existing["id"])

    created = _insert_with_numeric_id(
        sites,
        "sites",
        key,
        {
            "organization_id": organization_id,
            "name": site_name,
            "address": None,
            "timezone": "Europe/Sofia",
            "created_at": utc_now(),
        },
    )
    return int(created["id"])


def _ensure_room(database: Database, site_id: int, room_name: str, target_co2_ppm: int) -> tuple[int, int]:
    rooms = database.rooms
    key = {"site_id": site_id, "name": room_name}
    existing = rooms.find_one(key, {"id": 1, "target_co2_ppm": 1})
    if existing:
        rooms.update_one(key, {"$set": {"target_co2_ppm": target_co2_ppm}})
        return int(existing["id"]), target_co2_ppm

    created = _insert_with_numeric_id(
        rooms,
        "rooms",
        key,
        {
            "site_id": site_id,
            "name": room_name,
            "floor": None,
            "room_type": None,
            "target_co2_ppm": target_co2_ppm,
            "created_at": utc_now(),
        },
    )
    return int(created["id"]), int(created["target_co2_ppm"])


def _ensure_gateway(
    database: Database,
    gateway_eui: Optional[str],
    gateway_name: Optional[str],
    site_id: int,
    ts: datetime,
) -> Optional[int]:
    if not gateway_eui:
        return None

    gateways = database.gateways
    existing = gateways.find_one({"gateway_eui": gateway_eui}, {"id": 1, "name": 1, "site_id": 1})
    if existing:
        update_fields: dict[str, Any] = {"last_seen_at": ts}
        if gateway_name is not None:
            update_fields["name"] = gateway_name
        if site_id is not None:
            update_fields["site_id"] = site_id
        gateways.update_one({"gateway_eui": gateway_eui}, {"$set": update_fields})
        return int(existing["id"])

    created = _insert_with_numeric_id(
        gateways,
        "gateways",
        {"gateway_eui": gateway_eui},
        {
            "gateway_eui": gateway_eui,
            "name": gateway_name,
            "site_id": site_id,
            "last_seen_at": ts,
            "created_at": utc_now(),
        },
    )
    return int(created["id"])


def _ensure_device(
    database: Database,
    m: MeasurementIn,
    organization_id: int,
    site_id: int,
    room_id: int,
    gateway_id: Optional[int],
) -> int:
    devices = database.devices
    now = utc_now()
    existing = devices.find_one({"device_eui": m.device_eui}, {"id": 1, "name": 1, "gateway_id": 1})
    if existing:
        update_fields: dict[str, Any] = {
            "organization_id": organization_id,
            "site_id": site_id,
            "room_id": room_id,
            "status": "online",
            "updated_at": now,
        }
        if m.device_name is not None:
            update_fields["name"] = m.device_name
        if gateway_id is not None:
            update_fields["gateway_id"] = gateway_id
        if m.firmware_version is not None:
            update_fields["firmware_version"] = m.firmware_version
        if m.battery_type is not None:
            update_fields["battery_type"] = m.battery_type
        devices.update_one({"device_eui": m.device_eui}, {"$set": update_fields})
        return int(existing["id"])

    created = _insert_with_numeric_id(
        devices,
        "devices",
        {"device_eui": m.device_eui},
        {
            "device_eui": m.device_eui,
            "name": m.device_name,
            "organization_id": organization_id,
            "site_id": site_id,
            "room_id": room_id,
            "gateway_id": gateway_id,
            "status": "online",
            "firmware_version": m.firmware_version,
            "battery_type": m.battery_type,
            "install_date": None,
            "created_at": now,
            "updated_at": now,
        },
    )
    return int(created["id"])


def ensure_org_site_room_gateway_device(m: MeasurementIn, db: Optional[Database] = None) -> dict[str, Any]:
    database = resolve_db(db)
    organization_id = _ensure_organization(database, m.organization_name)
    site_id = _ensure_site(database, organization_id, m.site_name)
    room_id, threshold = _ensure_room(database, site_id, m.room_name, m.target_co2_ppm or 1000)
    gateway_id = _ensure_gateway(database, m.gateway_eui, m.gateway_name, site_id, m.ts)
    device_id = _ensure_device(database, m, organization_id, site_id, room_id, gateway_id)
    return {
        "organization_id": organization_id,
        "site_id": site_id,
        "room_id": room_id,
        "gateway_id": gateway_id,
        "device_id": device_id,
        "threshold": threshold,
    }


def sync_co2_alert(
    device_id: int,
    room_id: int,
    measured_value: int,
    threshold: int,
    ts: datetime,
    db: Optional[Database] = None,
):
    database = resolve_db(db)
    alerts = database.alerts
    active_alert = alerts.find_one(
        {"device_id": device_id, "alert_type": "co2_high", "is_active": True},
        sort=[("triggered_at", DESCENDING)],
    )

    if measured_value > threshold:
        if active_alert is None:
            severity = "critical" if measured_value >= threshold + 400 else "warning"
            alerts.insert_one(
                {
                    "id": next_sequence("alerts", database),
                    "device_id": device_id,
                    "room_id": room_id,
                    "alert_type": "co2_high",
                    "severity": severity,
                    "message": f"CO2 exceeded threshold ({measured_value} ppm > {threshold} ppm).",
                    "threshold_value": threshold,
                    "measured_value": measured_value,
                    "triggered_at": ts,
                    "cleared_at": None,
                    "is_active": True,
                }
            )
    elif active_alert is not None:
        alerts.update_one(
            {"id": active_alert["id"]},
            {
                "$set": {
                    "is_active": False,
                    "cleared_at": ts,
                    "measured_value": measured_value,
                    "message": f"CO2 returned below threshold ({measured_value} ppm <= {threshold} ppm).",
                }
            },
        )


def store_measurement(m: MeasurementIn, db: Optional[Database] = None):
    database = resolve_db(db)
    ids = ensure_org_site_room_gateway_device(m, database)

    database.measurements.update_one(
        {"device_id": ids["device_id"], "time": m.ts},
        {
            "$set": {
                "device_id": ids["device_id"],
                "time": m.ts,
                "gateway_id": ids["gateway_id"],
                "co2_ppm": m.co2_ppm,
                "temp_c": m.temp_c,
                "rh": m.rh,
                "battery_v": m.battery_v,
                "rssi": m.rssi,
                "snr": m.snr,
            }
        },
        upsert=True,
    )

    database.device_last_state.update_one(
        {"device_id": ids["device_id"]},
        {
            "$set": {
                "device_id": ids["device_id"],
                "gateway_id": ids["gateway_id"],
                "last_measurement_at": m.ts,
                "co2_ppm": m.co2_ppm,
                "temp_c": m.temp_c,
                "rh": m.rh,
                "battery_v": m.battery_v,
                "rssi": m.rssi,
                "snr": m.snr,
                "updated_at": utc_now(),
            }
        },
        upsert=True,
    )

    sync_co2_alert(
        device_id=ids["device_id"],
        room_id=ids["room_id"],
        measured_value=m.co2_ppm,
        threshold=ids["threshold"],
        ts=m.ts,
        db=database,
    )


def decode_lab_payload(payload_b64: str) -> Optional[dict[str, Any]]:
    try:
        payload = base64.b64decode(payload_b64)
    except Exception:
        return None

    if len(payload) < 7:
        return None

    co2 = int.from_bytes(payload[0:2], "big")
    temp_raw = int.from_bytes(payload[2:4], "big", signed=False)
    rh = payload[4]
    batt_mv = int.from_bytes(payload[5:7], "big")
    return {
        "co2_ppm": co2,
        "temp_c": round(temp_raw / 100.0, 2),
        "rh": float(rh),
        "battery_v": round(batt_mv / 1000.0, 3),
    }


def parse_observed_at(payload: dict[str, Any]) -> datetime:
    value = payload.get("time")
    if not value:
        return utc_now()
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return utc_now()


def extract_deduplication_id(payload: dict[str, Any]) -> Optional[str]:
    if payload.get("deduplicationId"):
        return payload.get("deduplicationId")
    context = payload.get("context") or {}
    if isinstance(context, dict) and context.get("deduplication_id"):
        return context.get("deduplication_id")
    return None


def classify_security_event(event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    level = payload.get("level")
    code = payload.get("code")
    description = payload.get("description")
    acknowledged = payload.get("acknowledged")
    text = " ".join([str(code or ""), str(description or ""), str(level or "")]).upper()

    mic_status = "unknown"
    failure_class = None
    replay_hint = False

    if "MIC" in text:
        mic_status = "invalid"
        failure_class = "mic"

    replay_terms = ["REPLAY", "DUPLICATE", "FCNT", "FRAME COUNTER", "FRAME-COUNTER", "NONCE", "COUNTER RESET"]
    if any(term in text for term in replay_terms):
        replay_hint = True
        if failure_class is None:
            failure_class = "replay"

    if event_type == "ack" and acknowledged is False and failure_class is None:
        failure_class = "downlink_nack"

    if event_type == "log" and failure_class is None:
        if str(level or "").upper() == "ERROR":
            failure_class = "error"
        elif str(level or "").upper() == "WARNING":
            failure_class = "warning"

    return {
        "event_level": level,
        "code": code,
        "description": description,
        "mic_status": mic_status,
        "failure_class": failure_class,
        "replay_hint": replay_hint,
    }


def store_security_event(topic: str, payload: dict[str, Any], db: Optional[Database] = None):
    database = resolve_db(db)
    parts = topic.split("/")
    event_type = parts[-1] if parts else "unknown"
    device_info = payload.get("deviceInfo") or {}
    observed_at = parse_observed_at(payload)
    rx_info = (payload.get("rxInfo") or [None])[0] or {}
    gateway_id = payload.get("gatewayId") or rx_info.get("gatewayId")
    rssi = rx_info.get("rssi")
    snr = rx_info.get("snr")
    deduplication_id = extract_deduplication_id(payload)
    classification = classify_security_event(event_type, payload)
    battery_level = payload.get("batteryLevel")
    margin = payload.get("margin")
    dev_addr = payload.get("devAddr")
    dev_eui = device_info.get("devEui")

    replay_suspected = bool(classification["replay_hint"])
    if deduplication_id:
        existing = database.security_events.find_one(
            {
                "deduplication_id": deduplication_id,
                "event_type": event_type,
                "dev_eui": dev_eui,
            },
            {"id": 1},
        )
        replay_suspected = replay_suspected or existing is not None

    database.security_events.insert_one(
        {
            "id": next_sequence("security_events", database),
            "observed_at": observed_at,
            "source": "chirpstack",
            "event_type": event_type,
            "tenant_id": device_info.get("tenantId"),
            "tenant_name": device_info.get("tenantName"),
            "application_id": device_info.get("applicationId"),
            "application_name": device_info.get("applicationName"),
            "device_profile_id": device_info.get("deviceProfileId"),
            "device_profile_name": device_info.get("deviceProfileName"),
            "device_name": device_info.get("deviceName"),
            "dev_eui": dev_eui,
            "gateway_id": gateway_id,
            "deduplication_id": deduplication_id,
            "code": classification["code"],
            "description": classification["description"],
            "event_level": classification["event_level"],
            "failure_class": classification["failure_class"],
            "dev_addr": dev_addr,
            "battery_level": battery_level,
            "margin": margin,
            "acknowledged": payload.get("acknowledged"),
            "f_cnt_down": payload.get("fCntDown"),
            "f_port": payload.get("fPort"),
            "dr": payload.get("dr"),
            "rssi": rssi,
            "snr": snr,
            "replay_suspected": replay_suspected,
            "mic_status": classification["mic_status"],
            "raw_event": payload,
        }
    )

    if dev_eui:
        set_fields: dict[str, Any] = {
            "dev_eui": dev_eui,
            "updated_at": utc_now(),
        }
        if device_info.get("deviceName") is not None:
            set_fields["device_name"] = device_info.get("deviceName")
        if device_info.get("tenantName") is not None:
            set_fields["tenant_name"] = device_info.get("tenantName")
        if device_info.get("applicationName") is not None:
            set_fields["application_name"] = device_info.get("applicationName")
        if event_type == "join":
            set_fields["last_join_at"] = observed_at
        if event_type == "up":
            set_fields["last_up_at"] = observed_at
        if event_type == "log":
            set_fields["last_log_at"] = observed_at
        if event_type == "status":
            set_fields["last_status_at"] = observed_at
        if event_type == "ack":
            set_fields["last_ack_at"] = observed_at
        if event_type == "txack":
            set_fields["last_txack_at"] = observed_at
        if battery_level is not None:
            set_fields["last_battery_level"] = battery_level
        if margin is not None:
            set_fields["last_margin"] = margin
        if rssi is not None:
            set_fields["last_rssi"] = rssi
        if snr is not None:
            set_fields["last_snr"] = snr

        database.device_security_state.update_one(
            {"dev_eui": dev_eui},
            {
                "$set": set_fields,
                "$setOnInsert": {"dev_eui": dev_eui},
                "$inc": {
                    "join_count": 1 if event_type == "join" else 0,
                    "up_count": 1 if event_type == "up" else 0,
                    "ack_count": 1 if event_type == "ack" else 0,
                    "txack_count": 1 if event_type == "txack" else 0,
                    "status_count": 1 if event_type == "status" else 0,
                    "log_count": 1 if event_type == "log" else 0,
                    "error_count": 1 if str(classification["event_level"] or "").upper() == "ERROR" else 0,
                    "warning_count": 1 if str(classification["event_level"] or "").upper() == "WARNING" else 0,
                    "mic_error_count": 1 if classification["mic_status"] == "invalid" else 0,
                    "replay_suspected_count": 1 if replay_suspected else 0,
                },
            },
            upsert=True,
        )

    if event_type == "up":
        measurement = decode_lab_payload(payload.get("data", ""))
        if measurement:
            store_measurement(
                MeasurementIn(
                    device_eui=dev_eui or "unknown",
                    device_name=device_info.get("deviceName"),
                    organization_name=device_info.get("tenantName") or "ChirpStack",
                    site_name=DEFAULT_SITE_NAME,
                    room_name=device_info.get("applicationName") or "ChirpStack Simulation",
                    gateway_eui=gateway_id,
                    gateway_name=gateway_id,
                    ts=observed_at,
                    rssi=rssi,
                    snr=snr,
                    **measurement,
                ),
                database,
            )


def fetch_organizations(db: Optional[Database] = None) -> list[dict[str, Any]]:
    database = resolve_db(db)
    rows = list(
        database.organizations.aggregate(
            [
                {
                    "$lookup": {
                        "from": "devices",
                        "localField": "id",
                        "foreignField": "organization_id",
                        "as": "devices",
                    }
                },
                {"$addFields": {"device_count": {"$size": "$devices"}}},
                {"$project": {"_id": 0, "id": 1, "name": 1, "created_at": 1, "device_count": 1}},
                {"$sort": {"name": 1}},
            ]
        )
    )
    return _serialize_rows(rows, "created_at")


def fetch_devices(db: Optional[Database] = None) -> list[dict[str, Any]]:
    database = resolve_db(db)
    rows = list(
        database.devices.aggregate(
            [
                {"$lookup": {"from": "organizations", "localField": "organization_id", "foreignField": "id", "as": "organization"}},
                {"$lookup": {"from": "sites", "localField": "site_id", "foreignField": "id", "as": "site"}},
                {"$lookup": {"from": "rooms", "localField": "room_id", "foreignField": "id", "as": "room"}},
                {"$lookup": {"from": "device_last_state", "localField": "id", "foreignField": "device_id", "as": "last_state"}},
                {"$unwind": {"path": "$organization", "preserveNullAndEmptyArrays": True}},
                {"$unwind": {"path": "$site", "preserveNullAndEmptyArrays": True}},
                {"$unwind": {"path": "$room", "preserveNullAndEmptyArrays": True}},
                {"$unwind": {"path": "$last_state", "preserveNullAndEmptyArrays": True}},
                {
                    "$project": {
                        "_id": 0,
                        "device_eui": 1,
                        "name": 1,
                        "status": 1,
                        "firmware_version": 1,
                        "battery_type": 1,
                        "organization_name": "$organization.name",
                        "site_name": "$site.name",
                        "room_name": "$room.name",
                        "last_measurement_at": "$last_state.last_measurement_at",
                        "co2_ppm": "$last_state.co2_ppm",
                        "temp_c": "$last_state.temp_c",
                        "rh": "$last_state.rh",
                        "battery_v": "$last_state.battery_v",
                        "rssi": "$last_state.rssi",
                        "snr": "$last_state.snr",
                    }
                },
                {"$sort": {"organization_name": 1, "site_name": 1, "room_name": 1, "device_eui": 1}},
            ]
        )
    )
    return _serialize_rows(rows, "last_measurement_at")


def fetch_latest(limit: int, db: Optional[Database] = None) -> list[dict[str, Any]]:
    database = resolve_db(db)
    rows = list(
        database.device_last_state.aggregate(
            [
                {"$sort": {"last_measurement_at": -1}},
                {"$limit": limit},
                {"$lookup": {"from": "devices", "localField": "device_id", "foreignField": "id", "as": "device"}},
                {"$unwind": "$device"},
                {"$lookup": {"from": "organizations", "localField": "device.organization_id", "foreignField": "id", "as": "organization"}},
                {"$lookup": {"from": "sites", "localField": "device.site_id", "foreignField": "id", "as": "site"}},
                {"$lookup": {"from": "rooms", "localField": "device.room_id", "foreignField": "id", "as": "room"}},
                {"$unwind": {"path": "$organization", "preserveNullAndEmptyArrays": True}},
                {"$unwind": {"path": "$site", "preserveNullAndEmptyArrays": True}},
                {"$unwind": {"path": "$room", "preserveNullAndEmptyArrays": True}},
                {
                    "$project": {
                        "_id": 0,
                        "device_eui": "$device.device_eui",
                        "device_name": "$device.name",
                        "organization_name": "$organization.name",
                        "site_name": "$site.name",
                        "room_name": "$room.name",
                        "time": "$last_measurement_at",
                        "co2_ppm": 1,
                        "temp_c": 1,
                        "rh": 1,
                        "battery_v": 1,
                        "rssi": 1,
                        "snr": 1,
                    }
                },
            ]
        )
    )
    return _serialize_rows(rows, "time")


def fetch_device_latest(device_eui: str, db: Optional[Database] = None) -> Optional[dict[str, Any]]:
    database = resolve_db(db)
    rows = list(
        database.devices.aggregate(
            [
                {"$match": {"device_eui": device_eui}},
                {"$lookup": {"from": "organizations", "localField": "organization_id", "foreignField": "id", "as": "organization"}},
                {"$lookup": {"from": "sites", "localField": "site_id", "foreignField": "id", "as": "site"}},
                {"$lookup": {"from": "rooms", "localField": "room_id", "foreignField": "id", "as": "room"}},
                {"$lookup": {"from": "device_last_state", "localField": "id", "foreignField": "device_id", "as": "last_state"}},
                {"$unwind": {"path": "$organization", "preserveNullAndEmptyArrays": True}},
                {"$unwind": {"path": "$site", "preserveNullAndEmptyArrays": True}},
                {"$unwind": {"path": "$room", "preserveNullAndEmptyArrays": True}},
                {"$unwind": {"path": "$last_state", "preserveNullAndEmptyArrays": True}},
                {
                    "$project": {
                        "_id": 0,
                        "device_eui": 1,
                        "device_name": "$name",
                        "organization_name": "$organization.name",
                        "site_name": "$site.name",
                        "room_name": "$room.name",
                        "time": "$last_state.last_measurement_at",
                        "co2_ppm": "$last_state.co2_ppm",
                        "temp_c": "$last_state.temp_c",
                        "rh": "$last_state.rh",
                        "battery_v": "$last_state.battery_v",
                        "rssi": "$last_state.rssi",
                        "snr": "$last_state.snr",
                    }
                },
            ]
        )
    )
    if not rows:
        return None
    return _serialize_datetime_fields(rows[0], "time")


def get_history_bucket(hours: int) -> Optional[dict[str, Any]]:
    if hours <= 24:
        return None
    if hours <= 24 * 7:
        return {"unit": "minute", "bin_size": 15}
    if hours <= 24 * 30:
        return {"unit": "hour", "bin_size": 1}
    if hours <= 24 * 90:
        return {"unit": "hour", "bin_size": 6}
    if hours <= 24 * 180:
        return {"unit": "hour", "bin_size": 12}
    return {"unit": "day", "bin_size": 1}


def fetch_device_history(device_eui: str, hours: int, limit: int, db: Optional[Database] = None) -> Optional[list[dict[str, Any]]]:
    database = resolve_db(db)
    device = database.devices.find_one({"device_eui": device_eui}, {"id": 1})
    if device is None:
        return None

    since = utc_now() - timedelta(hours=hours)
    bucket = get_history_bucket(hours)

    if bucket is None:
        rows = list(
            database.measurements.find(
                {"device_id": device["id"], "time": {"$gte": since}},
                {
                    "_id": 0,
                    "time": 1,
                    "co2_ppm": 1,
                    "temp_c": 1,
                    "rh": 1,
                    "battery_v": 1,
                    "rssi": 1,
                    "snr": 1,
                },
            )
            .sort("time", DESCENDING)
            .limit(limit)
        )
        return _serialize_rows(rows, "time")

    trunc_expr: dict[str, Any] = {"date": "$time", "unit": bucket["unit"], "timezone": "UTC"}
    if bucket["bin_size"] != 1:
        trunc_expr["binSize"] = bucket["bin_size"]

    rows = list(
        database.measurements.aggregate(
            [
                {"$match": {"device_id": device["id"], "time": {"$gte": since}}},
                {
                    "$group": {
                        "_id": {"$dateTrunc": trunc_expr},
                        "co2_ppm": {"$avg": "$co2_ppm"},
                        "temp_c": {"$avg": "$temp_c"},
                        "rh": {"$avg": "$rh"},
                        "battery_v": {"$avg": "$battery_v"},
                        "rssi": {"$avg": "$rssi"},
                        "snr": {"$avg": "$snr"},
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "time": "$_id",
                        "co2_ppm": {"$toInt": {"$round": ["$co2_ppm", 0]}},
                        "temp_c": 1,
                        "rh": 1,
                        "battery_v": 1,
                        "rssi": {
                            "$cond": [
                                {"$eq": ["$rssi", None]},
                                None,
                                {"$toInt": {"$round": ["$rssi", 0]}},
                            ]
                        },
                        "snr": 1,
                    }
                },
                {"$sort": {"time": -1}},
                {"$limit": limit},
            ]
        )
    )
    return _serialize_rows(rows, "time")


def fetch_alerts(active_only: bool, limit: int, db: Optional[Database] = None) -> list[dict[str, Any]]:
    database = resolve_db(db)
    pipeline: list[dict[str, Any]] = []
    if active_only:
        pipeline.append({"$match": {"is_active": True}})
    pipeline.extend(
        [
            {"$sort": {"triggered_at": -1}},
            {"$limit": limit},
            {"$lookup": {"from": "devices", "localField": "device_id", "foreignField": "id", "as": "device"}},
            {"$lookup": {"from": "rooms", "localField": "room_id", "foreignField": "id", "as": "room"}},
            {"$unwind": {"path": "$device", "preserveNullAndEmptyArrays": True}},
            {"$unwind": {"path": "$room", "preserveNullAndEmptyArrays": True}},
            {
                "$project": {
                    "_id": 0,
                    "id": 1,
                    "alert_type": 1,
                    "severity": 1,
                    "message": 1,
                    "threshold_value": 1,
                    "measured_value": 1,
                    "triggered_at": 1,
                    "cleared_at": 1,
                    "is_active": 1,
                    "device_eui": "$device.device_eui",
                    "room_name": "$room.name",
                }
            },
        ]
    )
    rows = list(database.alerts.aggregate(pipeline))
    return _serialize_rows(rows, "triggered_at", "cleared_at")


def fetch_security_events(limit: int, event_type: Optional[str], db: Optional[Database] = None) -> list[dict[str, Any]]:
    database = resolve_db(db)
    filters: dict[str, Any] = {}
    if event_type:
        filters["event_type"] = event_type
    rows = list(
        database.security_events.find(
            filters,
            {
                "_id": 0,
                "id": 1,
                "observed_at": 1,
                "source": 1,
                "event_type": 1,
                "tenant_name": 1,
                "application_name": 1,
                "device_name": 1,
                "dev_eui": 1,
                "gateway_id": 1,
                "deduplication_id": 1,
                "code": 1,
                "description": 1,
                "event_level": 1,
                "failure_class": 1,
                "acknowledged": 1,
                "f_cnt_down": 1,
                "f_port": 1,
                "dr": 1,
                "battery_level": 1,
                "margin": 1,
                "rssi": 1,
                "snr": 1,
                "replay_suspected": 1,
                "mic_status": 1,
            },
        )
        .sort("observed_at", DESCENDING)
        .limit(limit)
    )
    return _serialize_rows(rows, "observed_at")


def fetch_device_security(device_eui: str, limit: int, db: Optional[Database] = None) -> Optional[dict[str, Any]]:
    database = resolve_db(db)
    state = database.device_security_state.find_one({"dev_eui": device_eui}, {"_id": 0})
    if state is None:
        return None

    events = list(
        database.security_events.find(
            {"dev_eui": device_eui},
            {
                "_id": 0,
                "id": 1,
                "observed_at": 1,
                "event_type": 1,
                "gateway_id": 1,
                "deduplication_id": 1,
                "code": 1,
                "description": 1,
                "event_level": 1,
                "failure_class": 1,
                "replay_suspected": 1,
                "mic_status": 1,
                "battery_level": 1,
                "margin": 1,
                "rssi": 1,
                "snr": 1,
            },
        )
        .sort("observed_at", DESCENDING)
        .limit(limit)
    )

    state = _serialize_datetime_fields(
        state,
        "last_join_at",
        "last_up_at",
        "last_log_at",
        "last_status_at",
        "last_ack_at",
        "last_txack_at",
        "updated_at",
    )
    events = _serialize_rows(events, "observed_at")
    return {"state": state, "events": events}


def fetch_security_summary(db: Optional[Database] = None) -> dict[str, Any]:
    database = resolve_db(db)
    summary = next(
        database.security_events.aggregate(
            [
                {
                    "$group": {
                        "_id": None,
                        "total_events": {"$sum": 1},
                        "join_events": {"$sum": {"$cond": [{"$eq": ["$event_type", "join"]}, 1, 0]}},
                        "up_events": {"$sum": {"$cond": [{"$eq": ["$event_type", "up"]}, 1, 0]}},
                        "log_events": {"$sum": {"$cond": [{"$eq": ["$event_type", "log"]}, 1, 0]}},
                        "status_events": {"$sum": {"$cond": [{"$eq": ["$event_type", "status"]}, 1, 0]}},
                        "ack_events": {"$sum": {"$cond": [{"$eq": ["$event_type", "ack"]}, 1, 0]}},
                        "txack_events": {"$sum": {"$cond": [{"$eq": ["$event_type", "txack"]}, 1, 0]}},
                        "mic_failures": {"$sum": {"$cond": [{"$eq": ["$mic_status", "invalid"]}, 1, 0]}},
                        "replay_flags": {"$sum": {"$cond": ["$replay_suspected", 1, 0]}},
                        "error_events": {"$sum": {"$cond": [{"$eq": ["$event_level", "ERROR"]}, 1, 0]}},
                        "warning_events": {"$sum": {"$cond": [{"$eq": ["$event_level", "WARNING"]}, 1, 0]}},
                        "devices_seen_values": {"$addToSet": "$dev_eui"},
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "total_events": 1,
                        "join_events": 1,
                        "up_events": 1,
                        "log_events": 1,
                        "status_events": 1,
                        "ack_events": 1,
                        "txack_events": 1,
                        "mic_failures": 1,
                        "replay_flags": 1,
                        "error_events": 1,
                        "warning_events": 1,
                        "devices_seen": {
                            "$size": {
                                "$filter": {
                                    "input": "$devices_seen_values",
                                    "as": "dev_eui",
                                    "cond": {"$ne": ["$$dev_eui", None]},
                                }
                            }
                        },
                    }
                },
            ]
        ),
        None,
    )

    if summary is None:
        summary = {
            "total_events": 0,
            "join_events": 0,
            "up_events": 0,
            "log_events": 0,
            "status_events": 0,
            "ack_events": 0,
            "txack_events": 0,
            "mic_failures": 0,
            "replay_flags": 0,
            "error_events": 0,
            "warning_events": 0,
            "devices_seen": 0,
        }

    devices = list(
        database.device_security_state.find(
            {},
            {
                "_id": 0,
                "dev_eui": 1,
                "device_name": 1,
                "tenant_name": 1,
                "application_name": 1,
                "join_count": 1,
                "up_count": 1,
                "ack_count": 1,
                "txack_count": 1,
                "status_count": 1,
                "log_count": 1,
                "error_count": 1,
                "warning_count": 1,
                "mic_error_count": 1,
                "replay_suspected_count": 1,
                "last_battery_level": 1,
                "last_margin": 1,
                "last_rssi": 1,
                "last_snr": 1,
                "updated_at": 1,
            },
        )
        .sort("updated_at", DESCENDING)
        .limit(20)
    )
    return {"summary": summary, "devices": _serialize_rows(devices, "updated_at")}


def fetch_latest_up_event(db: Optional[Database] = None) -> Optional[dict[str, Any]]:
    database = resolve_db(db)
    return database.security_events.find_one(
        {
            "event_type": "up",
            "application_id": {"$ne": None},
            "dev_eui": {"$ne": None},
        },
        {"_id": 0, "raw_event": 1, "application_id": 1, "dev_eui": 1},
        sort=[("observed_at", DESCENDING)],
    )
