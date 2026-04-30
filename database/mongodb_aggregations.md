# MongoDB Model And Aggregations

This project now stores the Demo application data in MongoDB. The goal of the migration is to keep the API responses and Docker workflow familiar while replacing the old PostgreSQL / TimescaleDB queries with MongoDB collections and aggregation pipelines.

## Collections

- `organizations`
  Stores one document per tenant or organization.
- `sites`
  Stores physical sites and references `organization_id`.
- `rooms`
  Stores rooms and references `site_id`.
- `gateways`
  Stores LoRa gateways and references `site_id`.
- `devices`
  Stores device metadata and references organization, site, room, and gateway ids.
- `measurements`
  Stores the raw time-series measurement points.
- `device_last_state`
  Stores one latest snapshot per device for fast dashboard reads.
- `alerts`
  Stores active and historical CO2 threshold alerts.
- `security_events`
  Stores raw ChirpStack events plus derived security classification fields.
- `device_security_state`
  Stores the running counters and latest security state per device.
- `counters`
  Stores numeric sequences so documents can keep SQL-like `id` values.

## Important indexes

- `organizations.name` unique
- `sites.(organization_id, name)` unique
- `rooms.(site_id, name)` unique
- `gateways.gateway_eui` unique
- `devices.device_eui` unique
- `measurements.(device_id, time)` unique
- `measurements.(device_id, time desc)` for history reads
- `security_events.(dev_eui, observed_at desc)` for device security timelines
- `security_events.(event_type, observed_at desc)` for filtered event feeds
- `device_security_state.dev_eui` unique

## How the old SQL maps to MongoDB

### 1. Joins become `$lookup`

The dashboard endpoints used SQL joins across `devices`, `organizations`, `sites`, `rooms`, and `device_last_state`.

MongoDB replacement:

- Start from the main collection we want to list.
- Use `$lookup` to pull related documents by numeric id.
- Use `$unwind` with `preserveNullAndEmptyArrays` so missing relations still behave like SQL left joins.
- Use `$project` to return the same flat response shape the frontend already expects.

This is how `/latest`, `/devices`, and `/alerts` now work.

### 2. `time_bucket(...)` becomes `$dateTrunc` + `$group`

The old history endpoint used TimescaleDB:

- raw rows for short ranges
- `time_bucket()` for longer ranges

MongoDB replacement:

- `$match` on `device_id` and the time window
- `$group` by a bucket key built with `$dateTrunc`
- compute averages with `$avg`
- round integer-style metrics like `co2_ppm` and `rssi`
- `$sort` descending by bucket time
- `$limit` to keep the payload bounded

Examples:

- `15 minutes` becomes `$dateTrunc` with `unit: "minute"` and `binSize: 15`
- `1 hour` becomes `unit: "hour"`
- `6 hours` becomes `unit: "hour", binSize: 6`
- `1 day` becomes `unit: "day"`

### 3. SQL `COUNT(*) FILTER (...)` becomes conditional sums

The security summary used SQL expressions like:

- `COUNT(*) FILTER (WHERE event_type = 'join')`
- `COUNT(*) FILTER (WHERE mic_status = 'invalid')`

MongoDB replacement:

- single `$group`
- one accumulator per metric
- each metric uses `$sum` + `$cond`

Pattern:

```javascript
{
  $sum: {
    $cond: [{ $eq: ["$event_type", "join"] }, 1, 0]
  }
}
```

That is how `/security/summary` computes join counts, uplink counts, MIC failures, replay flags, warnings, and errors in one pass.

### 4. `COUNT(DISTINCT ...)` becomes `$addToSet`

The old summary also needed `COUNT(DISTINCT dev_eui)`.

MongoDB replacement:

- collect unique device ids with `$addToSet`
- remove `null`
- take the array size

Pattern:

```javascript
devices_seen_values: { $addToSet: "$dev_eui" }
```

then later:

```javascript
devices_seen: {
  $size: {
    $filter: {
      input: "$devices_seen_values",
      as: "dev_eui",
      cond: { $ne: ["$$dev_eui", null] }
    }
  }
}
```

### 5. `ON CONFLICT DO UPDATE` becomes app-level upserts

PostgreSQL used `INSERT ... ON CONFLICT DO UPDATE` heavily for:

- devices
- measurements
- device_last_state
- device_security_state

MongoDB replacement:

- unique indexes on the natural keys
- `update_one(..., upsert=True)` for documents that should be inserted or updated
- small helper logic for collections that still need numeric ids

This keeps the write behavior close to the original implementation.

## Why keep `device_last_state` and `device_security_state`

MongoDB can compute many things on the fly, but these two collections are still useful:

- `device_last_state`
  avoids scanning raw measurements just to draw the dashboard
- `device_security_state`
  avoids regrouping the whole event stream for every page refresh

They act like materialized read models for the UI.

## Docker behavior

- `vb-db` is now MongoDB
- `vb-mongo-express` replaces pgAdmin on port `5050`
- `vb-api` and `demo-seeder` connect through `MONGODB_URI`

The optional `full-lorawan` profile still keeps its separate ChirpStack PostgreSQL service because ChirpStack itself still depends on PostgreSQL.
