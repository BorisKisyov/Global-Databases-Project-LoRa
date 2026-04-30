import AutoRefresh from "../../AutoRefresh";

const RANGE_OPTIONS = [
  { hours: 1, label: "1h" },
  { hours: 8, label: "8h" },
  { hours: 12, label: "12h" },
  { hours: 24, label: "24h" },
  { hours: 168, label: "7d" },
  { hours: 720, label: "1m" },
  { hours: 2160, label: "3m" },
  { hours: 4320, label: "6m" },
  { hours: 8760, label: "12m" },
] as const;

const QUICK_RANGE_HOURS = [1, 8, 24, 168, 720];

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

type HistoryRow = {
  time: string;
  co2_ppm?: number | null;
  temp_c?: number | null;
  rh?: number | null;
  battery_v?: number | null;
  rssi?: number | null;
  snr?: number | null;
};

async function getLatest(deviceEui: string): Promise<LatestRow | null> {
  const base = process.env.API_INTERNAL_URL || "http://api:8000";
  const res = await fetch(`${base}/devices/${encodeURIComponent(deviceEui)}/latest`, {
    cache: "no-store",
  });
  if (!res.ok) return null;
  return res.json();
}

async function getHistory(deviceEui: string, hours: number): Promise<HistoryRow[]> {
  const base = process.env.API_INTERNAL_URL || "http://api:8000";
  const res = await fetch(
    `${base}/devices/${encodeURIComponent(deviceEui)}/history?hours=${hours}`,
    { cache: "no-store" }
  );
  if (!res.ok) return [];
  return res.json();
}

function buildPolyline(
  values: number[],
  width = 900,
  height = 240,
  padding = 24,
  forcedMin?: number,
  forcedMax?: number
) {
  if (values.length === 0) return "";

  const rawMin = Math.min(...values);
  const rawMax = Math.max(...values);

  const min = forcedMin ?? rawMin;
  const max = forcedMax ?? rawMax;
  const span = Math.max(1, max - min);

  return values
    .map((v, i) => {
      const x =
        padding + (i * (width - padding * 2)) / Math.max(1, values.length - 1);
      const y =
        height - padding - ((v - min) / span) * (height - padding * 2);
      return `${x},${y}`;
    })
    .join(" ");
}

function average(values: number[]) {
  if (values.length === 0) return null;
  return values.reduce((sum, v) => sum + v, 0) / values.length;
}

function getSeriesStats(values: number[]) {
  if (values.length === 0) {
    return { min: null, max: null, avg: null, latest: null };
  }

  return {
    min: Math.min(...values),
    max: Math.max(...values),
    avg: average(values),
    latest: values[values.length - 1],
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

function getCo2Status(value?: number | null) {
  if (value == null) return { label: "No data", bg: "#f3f4f6", color: "#374151" };
  if (value >= 1500) return { label: "Critical", bg: "#fee2e2", color: "#b91c1c" };
  if (value >= 1000) return { label: "Warning", bg: "#fef3c7", color: "#b45309" };
  return { label: "Normal", bg: "#dcfce7", color: "#166534" };
}

function getBatteryStatus(value?: number | null) {
  if (value == null) return { label: "No data", bg: "#f3f4f6", color: "#374151" };
  if (value < 3.3) return { label: "Critical", bg: "#fee2e2", color: "#b91c1c" };
  if (value < 3.5) return { label: "Warning", bg: "#fef3c7", color: "#b45309" };
  return { label: "Normal", bg: "#dcfce7", color: "#166534" };
}

function getRadioStatus(rssi?: number | null, snr?: number | null) {
  if (rssi == null && snr == null) {
    return { label: "No data", bg: "#f3f4f6", color: "#374151" };
  }

  if ((rssi != null && rssi <= -105) || (snr != null && snr < 0)) {
    return { label: "Critical", bg: "#fee2e2", color: "#b91c1c" };
  }

  if ((rssi != null && rssi <= -95) || (snr != null && snr < 5)) {
    return { label: "Warning", bg: "#fef3c7", color: "#b45309" };
  }

  return { label: "Normal", bg: "#dcfce7", color: "#166534" };
}

function StatusBadge({
  label,
  bg,
  color,
}: {
  label: string;
  bg: string;
  color: string;
}) {
  return (
    <span
      style={{
        display: "inline-block",
        marginTop: 8,
        padding: "4px 8px",
        borderRadius: 999,
        background: bg,
        color,
        fontSize: 12,
        fontWeight: 700,
      }}
    >
      {label}
    </span>
  );
}



function ChartCard({
  title,
  values,
  unit,
  color,
  decimals = 0,
  threshold,
  thresholdLabel,
}: {
  title: string;
  values: number[];
  unit: string;
  color: string;
  decimals?: number;
  threshold?: number;
  thresholdLabel?: string;
}) {
  return (
    <section style={chartSectionStyle()}>
      <h2 style={{ marginTop: 0 }}>{title}</h2>

      {values.length === 0 ? (
        <div>No data.</div>
      ) : (
        <>
          {(() => {
            const minValue = Math.min(...values);
            const maxValue = Math.max(...values);
            const chartMin =
              threshold == null ? minValue : Math.min(minValue, threshold);
            const chartMax =
              threshold == null ? maxValue : Math.max(maxValue, threshold);

            const polyline = buildPolyline(values, 900, 240, 24, chartMin, chartMax);
            const thresholdLine =
              threshold == null
                ? ""
                : buildPolyline([threshold, threshold], 900, 240, 24, chartMin, chartMax);

            const latestValue = values[values.length - 1];
            const avgValue = average(values);

            return (
              <>
                <svg
                  viewBox="0 0 900 240"
                  style={{ width: "100%", height: "auto", display: "block" }}
                >
                  <rect x="0" y="0" width="900" height="240" fill="#fafafa" />

                  {thresholdLine ? (
                    <polyline
                      fill="none"
                      stroke="#9ca3af"
                      strokeWidth="2"
                      strokeDasharray="6 6"
                      points={thresholdLine}
                    />
                  ) : null}

                  <polyline
                    fill="none"
                    stroke={color}
                    strokeWidth="3"
                    points={polyline}
                  />
                </svg>

                <div
                  style={{
                    display: "flex",
                    gap: 10,
                    flexWrap: "wrap",
                    marginTop: 10,
                    fontSize: 12,
                    color: "#555",
                  }}
                >
                  <span>Latest: {formatMetric(latestValue, decimals)} {unit}</span>
                  <span>Min: {formatMetric(minValue, decimals)} {unit}</span>
                  <span>Max: {formatMetric(maxValue, decimals)} {unit}</span>
                  <span>Avg: {formatMetric(avgValue, decimals)} {unit}</span>
                  {threshold != null ? (
                    <span>
                      {thresholdLabel || "Threshold"}: {formatMetric(threshold, decimals)} {unit}
                    </span>
                  ) : null}
                </div>
              </>
            );
          })()}
        </>
      )}
    </section>
  );
}

function chartSectionStyle() {
  return {
    border: "1px solid #ddd",
    borderRadius: 14,
    padding: 14,
    background: "#fff",
    marginBottom: 18,
  };
}

function metricBox() {
  return {
    minWidth: 140,
    background: "#fafafa",
    border: "1px solid #eee",
    borderRadius: 12,
    padding: 12,
  };
}

export default async function DevicePage({
  params,
  searchParams,
}: {
  params: Promise<{ device_eui: string }>;
  searchParams: Promise<{ hours?: string }>;
}) {
  const { device_eui } = await params;
  const sp = await searchParams;

  const allowedHours = new Set(RANGE_OPTIONS.map((r) => r.hours));
  const requestedHours = Number(sp?.hours);
  const selectedHours = allowedHours.has(requestedHours) ? requestedHours : 24;
  const selectedRangeLabel =
    RANGE_OPTIONS.find((r) => r.hours === selectedHours)?.label || "24h";

  const latest = await getLatest(device_eui);
  const history = await getHistory(device_eui, selectedHours);
  const orderedHistory = [...history].reverse();

  const co2Values = orderedHistory
    .map((h) => h.co2_ppm)
    .filter((v): v is number => typeof v === "number");

  const tempValues = orderedHistory
    .map((h) => h.temp_c)
    .filter((v): v is number => typeof v === "number");

  const rhValues = orderedHistory
    .map((h) => h.rh)
    .filter((v): v is number => typeof v === "number");

  const batteryValues = orderedHistory
    .map((h) => h.battery_v)
    .filter((v): v is number => typeof v === "number");

  const rssiValues = orderedHistory
    .map((h) => h.rssi)
    .filter((v): v is number => typeof v === "number");

  const snrValues = orderedHistory
    .map((h) => h.snr)
    .filter((v): v is number => typeof v === "number");

  const co2Status = getCo2Status(latest?.co2_ppm);
  const batteryStatus = getBatteryStatus(latest?.battery_v);
  const radioStatus = getRadioStatus(latest?.rssi, latest?.snr);
  const lastSeenText = latest?.time ? formatLocalTs(latest.time) : "-";

  const resolutionLabel =
    selectedHours <= 24
      ? "Raw samples"
      : selectedHours <= 24 * 7
      ? "15m buckets"
      : selectedHours <= 24 * 30
      ? "1h buckets"
      : selectedHours <= 24 * 90
      ? "6h buckets"
      : selectedHours <= 24 * 180
      ? "12h buckets"
      : "1d buckets";

  const pointCountText = `${history.length} point${history.length === 1 ? "" : "s"}`;
  
  const co2Stats = getSeriesStats(co2Values);
  const tempStats = getSeriesStats(tempValues);
  const rhStats = getSeriesStats(rhValues);
  const batteryStats = getSeriesStats(batteryValues);
  const rssiStats = getSeriesStats(rssiValues);
  const snrStats = getSeriesStats(snrValues);
    
  return (
    <main style={{ maxWidth: 1100, margin: "0 auto", padding: 16, fontFamily: "Arial" }}>
      <div style={{ marginBottom: 14 }}>
        <a href="/" style={{ color: "#2563eb", textDecoration: "none" }}>
          ← Back to dashboard
        </a>
      </div>

      <h1 style={{ marginBottom: 6 }}>{compactDisplayId(latest?.device_name || device_eui)}</h1>

      <div style={{ fontSize: 13, color: "#555", marginBottom: 18 }}>
        {device_eui}
        <br />
        {latest?.organization_name} / {latest?.site_name} / {latest?.room_name}
        <br />
        Last seen: {lastSeenText}
      </div>
      
      <AutoRefresh intervalMs={10000} />

      <div
        style={{
          display: "flex",
          gap: 8,
          flexWrap: "wrap",
          alignItems: "center",
          marginBottom: 18,
        }}
      >
        {RANGE_OPTIONS.filter((r) => QUICK_RANGE_HOURS.includes(r.hours)).map((r) => {
          const active = selectedHours === r.hours;

          return (
            <a
              key={r.hours}
              href={`/devices/${encodeURIComponent(device_eui)}?hours=${r.hours}`}
              style={{
                padding: "8px 12px",
                borderRadius: 10,
                border: active ? "1px solid #2563eb" : "1px solid #ddd",
                background: active ? "#eff6ff" : "#fff",
                color: active ? "#1d4ed8" : "#333",
                textDecoration: "none",
                fontSize: 14,
                fontWeight: active ? 700 : 400,
              }}
            >
              {r.label}
            </a>
          );
        })}

        <details style={{ position: "relative" }}>
          <summary
            style={{
              listStyle: "none",
              cursor: "pointer",
              padding: "8px 12px",
              borderRadius: 10,
              border: QUICK_RANGE_HOURS.includes(selectedHours)
                ? "1px solid #ddd"
                : "1px solid #2563eb",
              background: QUICK_RANGE_HOURS.includes(selectedHours) ? "#fff" : "#eff6ff",
              color: QUICK_RANGE_HOURS.includes(selectedHours) ? "#333" : "#1d4ed8",
              fontSize: 14,
              fontWeight: QUICK_RANGE_HOURS.includes(selectedHours) ? 400 : 700,
              userSelect: "none",
            }}
          >
            {QUICK_RANGE_HOURS.includes(selectedHours) ? "More" : `More (${selectedRangeLabel})`}
          </summary>

          <div
            style={{
              position: "absolute",
              top: "calc(100% + 6px)",
              left: 0,
              minWidth: 140,
              background: "#fff",
              border: "1px solid #ddd",
              borderRadius: 10,
              boxShadow: "0 8px 24px rgba(0,0,0,0.08)",
              padding: 6,
              zIndex: 20,
            }}
          >
            {RANGE_OPTIONS.filter((r) => !QUICK_RANGE_HOURS.includes(r.hours)).map((r) => {
              const active = selectedHours === r.hours;

              return (
                <a
                  key={r.hours}
                  href={`/devices/${encodeURIComponent(device_eui)}?hours=${r.hours}`}
                  style={{
                    display: "block",
                    padding: "8px 10px",
                    borderRadius: 8,
                    textDecoration: "none",
                    color: active ? "#1d4ed8" : "#333",
                    background: active ? "#eff6ff" : "#fff",
                    fontWeight: active ? 700 : 400,
                  }}
                >
                  {r.label}
                </a>
              );
            })}
          </div>
        </details>
      </div>

      <div
        style={{
          display: "flex",
          gap: 10,
          flexWrap: "wrap",
          marginBottom: 18,
          fontSize: 13,
          color: "#555",
        }}
      >
        <span
          style={{
            padding: "6px 10px",
            borderRadius: 999,
            background: "#f5f5f5",
            border: "1px solid #e5e5e5",
          }}
        >
          Resolution: {resolutionLabel}
        </span>

        <span
          style={{
            padding: "6px 10px",
            borderRadius: 999,
            background: "#f5f5f5",
            border: "1px solid #e5e5e5",
          }}
        >
          Points: {pointCountText}
        </span>

        <span
          style={{
            padding: "6px 10px",
            borderRadius: 999,
            background: "#f5f5f5",
            border: "1px solid #e5e5e5",
          }}
        >
          Last seen: {lastSeenText}
        </span>
      </div>

      {latest ? (
        <div style={{ display: "flex", gap: 12, flexWrap: "wrap", marginBottom: 22 }}>
          <div style={metricBox()}>
            <div style={{ fontSize: 12, color: "#666" }}>CO₂</div>
            <div style={{ display: "flex", alignItems: "baseline", gap: 6 }}>
              <div style={{ fontSize: 24, fontWeight: 700 }}>{latest.co2_ppm ?? "-"}</div>
              <div style={{ fontSize: 14, color: "#666" }}>ppm</div>
            </div>
            <StatusBadge {...co2Status} />
          </div>

          <div style={metricBox()}>
            <div style={{ fontSize: 12, color: "#666" }}>Temperature</div>
            <div style={{ display: "flex", alignItems: "baseline", gap: 6 }}>
              <div style={{ fontSize: 24, fontWeight: 700 }}>{formatMetric(latest.temp_c, 1)}</div>
              <div style={{ fontSize: 14, color: "#666" }}>°C</div>
            </div>
          </div>

          <div style={metricBox()}>
            <div style={{ fontSize: 12, color: "#666" }}>Humidity</div>
            <div style={{ display: "flex", alignItems: "baseline", gap: 6 }}>
              <div style={{ fontSize: 24, fontWeight: 700 }}>{formatMetric(latest.rh, 1)}</div>
              <div style={{ fontSize: 14, color: "#666" }}>% RH</div>
            </div>
          </div>

          <div style={metricBox()}>
            <div style={{ fontSize: 12, color: "#666" }}>Battery</div>
            <div style={{ display: "flex", alignItems: "baseline", gap: 6 }}>
              <div style={{ fontSize: 24, fontWeight: 700 }}>{formatMetric(latest.battery_v, 2)}</div>
              <div style={{ fontSize: 14, color: "#666" }}>V</div>
            </div>
            <StatusBadge {...batteryStatus} />
          </div>

          <div style={metricBox()}>
            <div style={{ fontSize: 12, color: "#666" }}>RSSI</div>
            <div style={{ display: "flex", alignItems: "baseline", gap: 6 }}>
              <div style={{ fontSize: 24, fontWeight: 700 }}>{formatMetric(latest.rssi, 0)}</div>
              <div style={{ fontSize: 14, color: "#666" }}>dBm</div>
            </div>
            <StatusBadge {...radioStatus} />
          </div>

          <div style={metricBox()}>
            <div style={{ fontSize: 12, color: "#666" }}>SNR</div>
            <div style={{ display: "flex", alignItems: "baseline", gap: 6 }}>
              <div style={{ fontSize: 24, fontWeight: 700 }}>{formatMetric(latest.snr, 1)}</div>
              <div style={{ fontSize: 14, color: "#666" }}>dB</div>
            </div>
          </div>
        </div>
      ) : (
        <div style={{ marginBottom: 20 }}>Device not found.</div>
      )}
   
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))",
          gap: 12,
          marginBottom: 18,
        }}
      >
        <section style={chartSectionStyle()}>
          <div style={{ fontSize: 13, color: "#666", marginBottom: 8 }}>CO₂ Range Stats</div>
          <div style={{ fontSize: 13, color: "#555", lineHeight: 1.7 }}>
            <div>Latest: {formatMetric(co2Stats.latest, 0)} ppm</div>
            <div>Min: {formatMetric(co2Stats.min, 0)} ppm</div>
            <div>Avg: {formatMetric(co2Stats.avg, 0)} ppm</div>
            <div>Max: {formatMetric(co2Stats.max, 0)} ppm</div>
          </div>
        </section>

        <section style={chartSectionStyle()}>
          <div style={{ fontSize: 13, color: "#666", marginBottom: 8 }}>Temperature Range Stats</div>
          <div style={{ fontSize: 13, color: "#555", lineHeight: 1.7 }}>
            <div>Latest: {formatMetric(tempStats.latest, 1)} °C</div>
            <div>Min: {formatMetric(tempStats.min, 1)} °C</div>
            <div>Avg: {formatMetric(tempStats.avg, 1)} °C</div>
            <div>Max: {formatMetric(tempStats.max, 1)} °C</div>
          </div>
        </section>

        <section style={chartSectionStyle()}>
          <div style={{ fontSize: 13, color: "#666", marginBottom: 8 }}>Humidity Range Stats</div>
          <div style={{ fontSize: 13, color: "#555", lineHeight: 1.7 }}>
            <div>Latest: {formatMetric(rhStats.latest, 1)} % RH</div>
            <div>Min: {formatMetric(rhStats.min, 1)} % RH</div>
            <div>Avg: {formatMetric(rhStats.avg, 1)} % RH</div>
            <div>Max: {formatMetric(rhStats.max, 1)} % RH</div>
          </div>
        </section>

        <section style={chartSectionStyle()}>
          <div style={{ fontSize: 13, color: "#666", marginBottom: 8 }}>Battery Range Stats</div>
          <div style={{ fontSize: 13, color: "#555", lineHeight: 1.7 }}>
            <div>Latest: {formatMetric(batteryStats.latest, 2)} V</div>
            <div>Min: {formatMetric(batteryStats.min, 2)} V</div>
            <div>Avg: {formatMetric(batteryStats.avg, 2)} V</div>
            <div>Max: {formatMetric(batteryStats.max, 2)} V</div>
          </div>
        </section>

        <section style={chartSectionStyle()}>
          <div style={{ fontSize: 13, color: "#666", marginBottom: 8 }}>RSSI Range Stats</div>
          <div style={{ fontSize: 13, color: "#555", lineHeight: 1.7 }}>
            <div>Latest: {formatMetric(rssiStats.latest, 0)} dBm</div>
            <div>Min: {formatMetric(rssiStats.min, 0)} dBm</div>
            <div>Avg: {formatMetric(rssiStats.avg, 0)} dBm</div>
            <div>Max: {formatMetric(rssiStats.max, 0)} dBm</div>
          </div>
        </section>

        <section style={chartSectionStyle()}>
          <div style={{ fontSize: 13, color: "#666", marginBottom: 8 }}>SNR Range Stats</div>
          <div style={{ fontSize: 13, color: "#555", lineHeight: 1.7 }}>
            <div>Latest: {formatMetric(snrStats.latest, 1)} dB</div>
            <div>Min: {formatMetric(snrStats.min, 1)} dB</div>
            <div>Avg: {formatMetric(snrStats.avg, 1)} dB</div>
            <div>Max: {formatMetric(snrStats.max, 1)} dB</div>
          </div>
        </section>
      </div>
   
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(340px, 1fr))",
          gap: 18,
          marginBottom: 18,
        }}
      >
        <ChartCard
          title={`CO₂ History (${selectedRangeLabel})`}
          values={co2Values}
          unit="ppm"
          color="#2563eb"
          threshold={1000}
          thresholdLabel="Target"
        />

        <ChartCard
          title={`Temperature History (${selectedRangeLabel})`}
          values={tempValues}
          unit="°C"
          color="#dc2626"
          decimals={1}
        />

        <ChartCard
          title={`Humidity History (${selectedRangeLabel})`}
          values={rhValues}
          unit="% RH"
          color="#0891b2"
          decimals={1}
        />

        <ChartCard
          title={`Battery History (${selectedRangeLabel})`}
          values={batteryValues}
          unit="V"
          color="#16a34a"
          decimals={2}
          threshold={3.4}
          thresholdLabel="Warning line"
        />

        <ChartCard
          title={`RSSI History (${selectedRangeLabel})`}
          values={rssiValues}
          unit="dBm"
          color="#7c3aed"
          threshold={-95}
          thresholdLabel="Weak radio"
        />

        <ChartCard
          title={`SNR History (${selectedRangeLabel})`}
          values={snrValues}
          unit="dB"
          color="#ea580c"
          decimals={1}
          threshold={5}
          thresholdLabel="Weak radio"
        />
      </div>      
      
      <section
        style={{
          border: "1px solid #ddd",
          borderRadius: 14,
          padding: 14,
          background: "#fff",
        }}
      >
        <h2 style={{ marginTop: 0 }}>Recent History</h2>

        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 14 }}>
            <thead>
              <tr style={{ textAlign: "left", borderBottom: "1px solid #ddd" }}>
                <th style={{ padding: "8px 6px" }}>Time</th>
                <th style={{ padding: "8px 6px" }}>CO₂</th>
                <th style={{ padding: "8px 6px" }}>Temp</th>
                <th style={{ padding: "8px 6px" }}>RH</th>
                <th style={{ padding: "8px 6px" }}>Battery</th>
                <th style={{ padding: "8px 6px" }}>RSSI</th>
                <th style={{ padding: "8px 6px" }}>SNR</th>
              </tr>
            </thead>
            <tbody>
              {history.slice(0, 30).map((row, idx) => (
                <tr key={idx} style={{ borderBottom: "1px solid #f0f0f0" }}>
                  <td style={{ padding: "8px 6px" }}>{formatLocalTs(row.time)}</td>
                  <td style={{ padding: "8px 6px" }}>{row.co2_ppm ?? "-"}</td>
                  <td style={{ padding: "8px 6px" }}>{formatMetric(row.temp_c, 1)}</td>
                  <td style={{ padding: "8px 6px" }}>{formatMetric(row.rh, 1)}</td>
                  <td style={{ padding: "8px 6px" }}>{formatMetric(row.battery_v, 2)}</td>
                  <td style={{ padding: "8px 6px" }}>{formatMetric(row.rssi, 0)}</td>
                  <td style={{ padding: "8px 6px" }}>{formatMetric(row.snr, 1)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </main>
  );
}