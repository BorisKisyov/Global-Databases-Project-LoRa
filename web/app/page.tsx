export const dynamic = "force-dynamic";

import AutoRefresh from "./AutoRefresh";

type LatestRow = {
  device_eui: string;
  device_name?: string | null;
  organization_name?: string | null;
  site_name?: string | null;
  room_name?: string | null;
  time: string;
  co2_ppm?: number | null;
  temp_c?: number | null;
  rh?: number | null;
  battery_v?: number | null;
  rssi?: number | null;
  snr?: number | null;
};

type StatusLevel = "healthy" | "warning" | "critical" | "offline";

type StatusInfo = {
  level: StatusLevel;
  label: string;
  bg: string;
  fg: string;
  border: string;
};

type RoomGroup = {
  key: string;
  organization_name: string;
  site_name: string;
  room_name: string;
  devices: LatestRow[];
};

async function getLatest(): Promise<LatestRow[]> {
  const base = process.env.API_INTERNAL_URL || "http://vb-api:8000";
  try {
    const res = await fetch(`${base}/latest?limit=50`, { cache: "no-store" });
    if (!res.ok) return [];
    return res.json();
  } catch {
    return [];
  }
}

const docsBase = process.env.NEXT_PUBLIC_API_BASE || "http://localhost:38000";

function secondsSince(ts: string) {
  const now = Date.now();
  const then = new Date(ts).getTime();
  return Math.max(0, Math.floor((now - then) / 1000));
}

function formatAge(ts: string) {
  const s = secondsSince(ts);
  if (s < 60) return `${s}s ago`;
  const m = Math.floor(s / 60);
  const rem = s % 60;
  return rem === 0 ? `${m}m ago` : `${m}m ${rem}s ago`;
}

function formatLocalTs(ts: string) {
  const parts = new Intl.DateTimeFormat("en-GB", {
    timeZone: "Europe/Sofia",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).formatToParts(new Date(ts));

  const map = Object.fromEntries(parts.map((p) => [p.type, p.value]));

  return `${map.day}/${map.month}/${map.year} ${map.hour}:${map.minute}:${map.second}`;
}

function badgeStyle(bg: string, fg: string) {
  return {
    display: "inline-block",
    padding: "6px 10px",
    borderRadius: 999,
    fontSize: 12,
    fontWeight: 700 as const,
    background: bg,
    color: fg,
    whiteSpace: "nowrap" as const,
  };
}

function formatMetric(value?: number | null, decimals = 0) {
  if (value == null) return "-";
  return value.toFixed(decimals);
}

function compactDisplayId(value?: string | null) {
  if (!value) return "-";
  const v = String(value);

  if (/^\d+$/.test(v)) {
    const trimmed = v.replace(/^0+/, "");
    return trimmed || "0";
  }

  return v.replace(/(^|\D)0+(\d+$)/, "$1$2");
}

function metricStyle() {
  return {
    minWidth: 120,
    background: "#fafafa",
    border: "1px solid #eee",
    borderRadius: 10,
    padding: 10,
  };
}

function getCo2Level(co2?: number | null) {
  if (co2 == null) return { label: "Unknown", bg: "#f3f4f6", fg: "#374151" };
  if (co2 >= 1500) return { label: "Critical CO₂", bg: "#fee2e2", fg: "#991b1b" };
  if (co2 >= 1000) return { label: "High CO₂", bg: "#fef3c7", fg: "#92400e" };
  return { label: "CO₂ OK", bg: "#dcfce7", fg: "#166534" };
}

function getBatteryLevel(v?: number | null) {
  if (v == null) return { label: "Battery ?", bg: "#f3f4f6", fg: "#374151" };
  if (v <= 3.2) return { label: "Low Battery", bg: "#fee2e2", fg: "#991b1b" };
  if (v <= 3.4) return { label: "Battery Warning", bg: "#fef3c7", fg: "#92400e" };
  return { label: "Battery OK", bg: "#dcfce7", fg: "#166534" };
}

function getRadioLevel(rssi?: number | null, snr?: number | null) {
  if (rssi == null && snr == null) {
    return { label: "Radio ?", bg: "#f3f4f6", fg: "#374151" };
  }

  if ((rssi != null && rssi <= -105) || (snr != null && snr <= 0)) {
    return { label: "Poor Radio", bg: "#fee2e2", fg: "#991b1b" };
  }

  if ((rssi != null && rssi <= -95) || (snr != null && snr <= 5)) {
    return { label: "Weak Radio", bg: "#fef3c7", fg: "#92400e" };
  }

  return { label: "Radio OK", bg: "#dcfce7", fg: "#166534" };
}

function getOverallStatus(m: LatestRow): StatusInfo {
  const ageSec = secondsSince(m.time);

  if (ageSec > 180) {
    return {
      level: "offline",
      label: "Offline",
      bg: "#e5e7eb",
      fg: "#111827",
      border: "#6b7280",
    };
  }

  if (
    (m.co2_ppm != null && m.co2_ppm >= 1500) ||
    (m.battery_v != null && m.battery_v <= 3.2) ||
    (m.rssi != null && m.rssi <= -105) ||
    (m.snr != null && m.snr <= 0)
  ) {
    return {
      level: "critical",
      label: "Critical",
      bg: "#fee2e2",
      fg: "#991b1b",
      border: "#dc2626",
    };
  }

  if (
    (m.co2_ppm != null && m.co2_ppm >= 1000) ||
    (m.battery_v != null && m.battery_v <= 3.4) ||
    (m.rssi != null && m.rssi <= -95) ||
    (m.snr != null && m.snr <= 5)
  ) {
    return {
      level: "warning",
      label: "Warning",
      bg: "#fef3c7",
      fg: "#92400e",
      border: "#f59e0b",
    };
  }

  return {
    level: "healthy",
    label: "Healthy",
    bg: "#dcfce7",
    fg: "#166534",
    border: "#22c55e",
  };
}

function severityScore(level: StatusLevel) {
  if (level === "offline") return 4;
  if (level === "critical") return 3;
  if (level === "warning") return 2;
  return 1;
}

function groupByRoom(data: LatestRow[]): RoomGroup[] {
  const map = new Map<string, RoomGroup>();

  for (const d of data) {
    const org = d.organization_name || "Unknown Org";
    const site = d.site_name || "Unknown Site";
    const room = d.room_name || "Unassigned";
    const key = `${org}__${site}__${room}`;

    if (!map.has(key)) {
      map.set(key, {
        key,
        organization_name: org,
        site_name: site,
        room_name: room,
        devices: [],
      });
    }

    map.get(key)!.devices.push(d);
  }

  const groups = Array.from(map.values());

  for (const group of groups) {
    group.devices.sort((a, b) => {
      const sa = severityScore(getOverallStatus(a).level);
      const sb = severityScore(getOverallStatus(b).level);
      if (sb !== sa) return sb - sa;

      const co2a = a.co2_ppm ?? -1;
      const co2b = b.co2_ppm ?? -1;
      return co2b - co2a;
    });
  }

  groups.sort((a, b) => {
    const aw = Math.max(...a.devices.map((d) => severityScore(getOverallStatus(d).level)));
    const bw = Math.max(...b.devices.map((d) => severityScore(getOverallStatus(d).level)));
    if (bw !== aw) return bw - aw;
    return a.room_name.localeCompare(b.room_name);
  });

  return groups;
}

function getRoomSummary(group: RoomGroup) {
  const statuses = group.devices.map(getOverallStatus);
  const worstStatus = statuses.reduce((worst, current) =>
    severityScore(current.level) > severityScore(worst.level) ? current : worst
  );

  const co2Values = group.devices
    .map((d) => d.co2_ppm)
    .filter((v): v is number => typeof v === "number");

  const avgCo2 =
    co2Values.length > 0
      ? Math.round(co2Values.reduce((sum, v) => sum + v, 0) / co2Values.length)
      : null;

  const criticalCount = statuses.filter((s) => s.level === "critical").length;
  const warningCount = statuses.filter((s) => s.level === "warning").length;
  const offlineCount = statuses.filter((s) => s.level === "offline").length;

  const worstDevice = [...group.devices].sort((a, b) => {
    const sa = severityScore(getOverallStatus(a).level);
    const sb = severityScore(getOverallStatus(b).level);
    if (sb !== sa) return sb - sa;
    return (b.co2_ppm ?? -1) - (a.co2_ppm ?? -1);
  })[0];

  return {
    worstStatus,
    avgCo2,
    criticalCount,
    warningCount,
    offlineCount,
    deviceCount: group.devices.length,
    worstDevice: compactDisplayId(worstDevice?.device_name || worstDevice?.device_eui),
  };
}

function DeviceCard({ m }: { m: LatestRow }) {
  const overall = getOverallStatus(m);
  const co2 = getCo2Level(m.co2_ppm);
  const battery = getBatteryLevel(m.battery_v);
  const radio = getRadioLevel(m.rssi, m.snr);

  return (
    <div
      style={{
        border: `1px solid ${overall.border}`,
        borderLeft: `8px solid ${overall.border}`,
        borderRadius: 14,
        padding: 14,
        background: "#fff",
        boxShadow: "0 1px 2px rgba(0,0,0,0.04)",
      }}
    >
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          gap: 12,
          flexWrap: "wrap",
          alignItems: "center",
        }}
      >
        <div>
          <div style={{ fontSize: 18, fontWeight: 700 }}>
            <a
              href={`/devices/${encodeURIComponent(m.device_eui)}`}
              style={{ color: "#111827", textDecoration: "none" }}
            >
              {compactDisplayId(m.device_name || m.device_eui)}
            </a>
          </div>
        </div>

        <div style={{ textAlign: "right" }}>
          <div>
            <span style={badgeStyle(overall.bg, overall.fg)}>{overall.label}</span>
          </div>
          <div style={{ fontSize: 12, color: "#666", marginTop: 8 }}>
            Last seen: {formatAge(m.time)}
          </div>
          <div style={{ fontSize: 12, color: "#666", marginTop: 2 }}>
            {formatLocalTs(m.time)}
          </div>
        </div>
      </div>

      <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginTop: 12 }}>
        <span style={badgeStyle(co2.bg, co2.fg)}>{co2.label}</span>
        <span style={badgeStyle(battery.bg, battery.fg)}>{battery.label}</span>
        <span style={badgeStyle(radio.bg, radio.fg)}>{radio.label}</span>
      </div>

      <div style={{ display: "flex", gap: 12, flexWrap: "wrap", marginTop: 14 }}>
        <div style={metricStyle()}>
          <div style={{ fontSize: 12, color: "#666" }}>CO₂</div>
          <div style={{ fontSize: 22, fontWeight: 700 }}>{m.co2_ppm ?? "-"}</div>
          <div style={{ fontSize: 12, color: "#666" }}>ppm</div>
        </div>

        <div style={metricStyle()}>
          <div style={{ fontSize: 12, color: "#666" }}>Temperature</div>
          <div style={{ fontSize: 22, fontWeight: 700 }}>{formatMetric(m.temp_c, 1)}</div>
          <div style={{ fontSize: 12, color: "#666" }}>°C</div>
        </div>

        <div style={metricStyle()}>
          <div style={{ fontSize: 12, color: "#666" }}>Humidity</div>
          <div style={{ fontSize: 22, fontWeight: 700 }}>{formatMetric(m.rh, 1)}</div>
          <div style={{ fontSize: 12, color: "#666" }}>% RH</div>
        </div>

        <div style={metricStyle()}>
          <div style={{ fontSize: 12, color: "#666" }}>Battery</div>
          <div style={{ fontSize: 22, fontWeight: 700 }}>{formatMetric(m.battery_v, 2)}</div>
          <div style={{ fontSize: 12, color: "#666" }}>V</div>
        </div>

        <div style={metricStyle()}>
          <div style={{ fontSize: 12, color: "#666" }}>RSSI</div>
          <div style={{ fontSize: 22, fontWeight: 700 }}>{formatMetric(m.rssi, 0)}</div>
          <div style={{ fontSize: 12, color: "#666" }}>dBm</div>
        </div>

        <div style={metricStyle()}>
          <div style={{ fontSize: 12, color: "#666" }}>SNR</div>
          <div style={{ fontSize: 22, fontWeight: 700 }}>{formatMetric(m.snr, 1)}</div>
          <div style={{ fontSize: 12, color: "#666" }}>dB</div>
        </div>
      </div>
    </div>
  );
}

export default async function Page() {
  const data = await getLatest();
  const rooms = groupByRoom(data);

  return (
    <main style={{ maxWidth: 1280, margin: "0 auto", padding: 16, fontFamily: "Arial" }}>
      <h1 style={{ marginBottom: 8 }}>Demo Dashboard (Local)</h1>

      <p style={{ fontSize: 12, color: "#555", marginTop: 0 }}>
        API docs: <a href={`${docsBase}/docs`}>{docsBase}/docs</a>
      </p>

      <AutoRefresh intervalMs={10000} />

      {rooms.length === 0 ? (
        <div style={{ border: "1px solid #ddd", borderRadius: 12, padding: 12, marginTop: 12 }}>
          No data yet.
        </div>
      ) : (
        <div style={{ marginTop: 16 }}>
          {rooms.map((room) => {
            const summary = getRoomSummary(room);

            return (
              <section
                key={room.key}
                style={{
                  marginBottom: 22,
                  border: `1px solid ${summary.worstStatus.border}`,
                  borderRadius: 16,
                  background: "#fcfcfc",
                  overflow: "hidden",
                }}
              >
                <div
                  style={{
                    padding: 14,
                    borderBottom: "1px solid #eee",
                    background: "#f9fafb",
                  }}
                >
                  <div
                    style={{
                      display: "flex",
                      justifyContent: "space-between",
                      gap: 12,
                      flexWrap: "wrap",
                      alignItems: "center",
                    }}
                  >
                    <div>
                      <div style={{ fontSize: 22, fontWeight: 700 }}>{room.room_name}</div>
                      <div style={{ fontSize: 13, color: "#555", marginTop: 4 }}>
                        {room.organization_name} / {room.site_name}
                      </div>
                    </div>

                    <div>
                      <span style={badgeStyle(summary.worstStatus.bg, summary.worstStatus.fg)}>
                        Room Status: {summary.worstStatus.label}
                      </span>
                    </div>
                  </div>

                  <div style={{ display: "flex", gap: 10, flexWrap: "wrap", marginTop: 12 }}>
                    <span style={badgeStyle("#e0f2fe", "#075985")}>
                      Devices: {summary.deviceCount}
                    </span>
                    <span style={badgeStyle("#f3f4f6", "#111827")}>
                      Avg CO₂: {summary.avgCo2 ?? "-"} ppm
                    </span>
                    <span style={badgeStyle("#fee2e2", "#991b1b")}>
                      Critical: {summary.criticalCount}
                    </span>
                    <span style={badgeStyle("#fef3c7", "#92400e")}>
                      Warning: {summary.warningCount}
                    </span>
                    <span style={badgeStyle("#e5e7eb", "#111827")}>
                      Offline: {summary.offlineCount}
                    </span>
                    <span style={badgeStyle("#ede9fe", "#5b21b6")}>
                      Worst Device: {summary.worstDevice}
                    </span>
                  </div>
                </div>

                <div
                  style={{
                    padding: 14,
                    display: "grid",
                    gridTemplateColumns: "repeat(auto-fit, minmax(420px, 1fr))",
                    gap: 14,
                  }}
                >
                  {room.devices.map((device) => (
                    <DeviceCard key={device.device_eui} m={device} />
                  ))}
                </div>
              </section>
            );
          })}
        </div>
      )}
    </main>
  );
}
