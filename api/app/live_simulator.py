import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from app.db import get_db, store_measurement
from app.seed_demo import DEVICES, build_measurement

SOFIA_TZ = ZoneInfo("Europe/Sofia")


def minute_index(ts: datetime) -> int:
    local_ts = ts.astimezone(SOFIA_TZ)
    return local_ts.hour * 60 + local_ts.minute


def insert_one_tick(ts: datetime):
    idx = minute_index(ts)
    db = get_db()
    inserted = 0

    for device in DEVICES:
        m = build_measurement(device, ts, idx)
        store_measurement(m, db)
        inserted += 1

    print(f"[{ts.isoformat()}] inserted {inserted} measurements")


def wait_until_next_minute():
    now = datetime.now(timezone.utc)
    sleep_seconds = 60 - now.second - (now.microsecond / 1_000_000)
    if sleep_seconds < 0.01:
        sleep_seconds = 0.01
    time.sleep(sleep_seconds)


def main():
    print("Live simulator started. Press Ctrl+C to stop.")

    try:
        while True:
            ts = datetime.now(timezone.utc).replace(second=0, microsecond=0)
            insert_one_tick(ts)
            wait_until_next_minute()
    except KeyboardInterrupt:
        print("Live simulator stopped.")


if __name__ == "__main__":
    main()
