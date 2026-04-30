import AutoRefresh from "../AutoRefresh";

export const dynamic = "force-dynamic";

type SecuritySummary = {
  total_events?: number;
  join_events?: number;
  up_events?: number;
  log_events?: number;
  status_events?: number;
  ack_events?: number;
  txack_events?: number;
  mic_failures?: number;
  replay_flags?: number;
  error_events?: number;
  warning_events?: number;
  devices_seen?: number;
};

type SecurityDevice = {
  dev_eui: string;
  device_name?: string | null;
  tenant_name?: string | null;
  application_name?: string | null;
  join_count?: number;
  up_count?: number;
  ack_count?: number;
  txack_count?: number;
  status_count?: number;
  log_count?: number;
  error_count?: number;
  warning_count?: number;
  mic_error_count?: number;
  replay_suspected_count?: number;
  last_battery_level?: number | null;
  last_margin?: number | null;
  last_rssi?: number | null;
  last_snr?: number | null;
  updated_at: string;
};

type SecurityEvent = {
  id: number;
  observed_at: string;
  event_type: string;
  tenant_name?: string | null;
  application_name?: string | null;
  device_name?: string | null;
  dev_eui?: string | null;
  gateway_id?: string | null;
  code?: string | null;
  description?: string | null;
  event_level?: string | null;
  failure_class?: string | null;
  replay_suspected?: boolean;
  mic_status?: string;
  battery_level?: number | null;
  margin?: number | null;
  rssi?: number | null;
  snr?: number | null;
};

async function getSecuritySummary(): Promise<{ summary: SecuritySummary; devices: SecurityDevice[] }> {
  const base = process.env.API_INTERNAL_URL || "http://vb-api:8000";
  try {
    const res = await fetch(`${base}/security/summary`, { cache: "no-store" });
    if (!res.ok) return { summary: {}, devices: [] };
    return res.json();
  } catch {
    return { summary: {}, devices: [] };
  }
}

async function getSecurityEvents(): Promise<SecurityEvent[]> {
  const base = process.env.API_INTERNAL_URL || "http://vb-api:8000";
  try {
    const res = await fetch(`${base}/security/events?limit=50`, { cache: "no-store" });
    if (!res.ok) return [];
    return res.json();
  } catch {
    return [];
  }
}

function cardStyle() {
  return {
    background: "#fff",
    border: "1px solid #e5e7eb",
    borderRadius: 14,
    padding: 16,
  } as const;
}

function metricCard(label: string, value: string | number | undefined) {
  return (
    <div style={{ ...cardStyle(), minWidth: 150 }}>
      <div style={{ fontSize: 12, color: "#6b7280", marginBottom: 6 }}>{label}</div>
      <div style={{ fontSize: 28, fontWeight: 800 }}>{value ?? 0}</div>
    </div>
  );
}

function formatTs(ts: string) {
  if (!ts) return "-";
  return ts.replace("T", " ").replace("+00:00", " UTC").slice(0, 23);
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

function displayDevice(primary?: string | null, fallback?: string | null) {
  return compactDisplayId(primary || fallback);
}

function badge(text: string, bg: string, fg = "#111827") {
  return (
    <span
      style={{
        display: "inline-block",
        padding: "2px 8px",
        borderRadius: 999,
        background: bg,
        color: fg,
        fontSize: 12,
        fontWeight: 700,
      }}
    >
      {text}
    </span>
  );
}

function failureBadge(kind?: string | null) {
  if (!kind) return "-";
  if (kind === "mic") return badge("MIC", "#fee2e2", "#991b1b");
  if (kind === "replay") return badge("Replay", "#ffedd5", "#9a3412");
  if (kind === "downlink_nack") return badge("Downlink NACK", "#ede9fe", "#5b21b6");
  if (kind === "error") return badge("Error", "#fee2e2", "#991b1b");
  if (kind === "warning") return badge("Warning", "#fef3c7", "#92400e");
  return badge(kind, "#e5e7eb");
}

export default async function SecurityPage() {
  const [{ summary, devices }, events] = await Promise.all([getSecuritySummary(), getSecurityEvents()]);

  return (
    <div style={{ padding: 20, display: "grid", gap: 18 }}>
      <AutoRefresh intervalMs={10000} />


      <section style={{ display: "flex", gap: 12, flexWrap: "wrap" }}>
        {metricCard("Total security events", summary.total_events)}
        {metricCard("Join events", summary.join_events)}
        {metricCard("Uplink events", summary.up_events)}
        {metricCard("Log events", summary.log_events)}
        {metricCard("Status events", summary.status_events)}
        {metricCard("ACK / TXACK", `${summary.ack_events ?? 0} / ${summary.txack_events ?? 0}`)}
        {metricCard("MIC failures", summary.mic_failures)}
        {metricCard("Replay flags", summary.replay_flags)}
        {metricCard("Error / Warning", `${summary.error_events ?? 0} / ${summary.warning_events ?? 0}`)}
        {metricCard("Devices seen", summary.devices_seen)}
      </section>

      <section style={cardStyle()}>
        <h2 style={{ marginTop: 0 }}>Security state by device</h2>
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 14 }}>
            <thead>
              <tr style={{ textAlign: "left", borderBottom: "1px solid #e5e7eb" }}>
                <th style={{ padding: "10px 8px" }}>Device</th>
                <th style={{ padding: "10px 8px" }}>Application</th>
                <th style={{ padding: "10px 8px" }}>Join</th>
                <th style={{ padding: "10px 8px" }}>Up</th>
                <th style={{ padding: "10px 8px" }}>Status</th>
                <th style={{ padding: "10px 8px" }}>Ack / TxAck</th>
                <th style={{ padding: "10px 8px" }}>Log</th>
                <th style={{ padding: "10px 8px" }}>Err / Warn</th>
                <th style={{ padding: "10px 8px" }}>MIC errors</th>
                <th style={{ padding: "10px 8px" }}>Replay flags</th>
                <th style={{ padding: "10px 8px" }}>Battery / Margin</th>
                <th style={{ padding: "10px 8px" }}>Last RSSI / SNR</th>
                <th style={{ padding: "10px 8px" }}>Updated</th>
              </tr>
            </thead>
            <tbody>
              {devices.map((d) => (
                <tr key={d.dev_eui} style={{ borderBottom: "1px solid #f3f4f6" }}>
                  <td style={{ padding: "10px 8px", fontWeight: 700 }} title={d.device_name || d.dev_eui || ""}>
                    {displayDevice(d.device_name, d.dev_eui)}
                  </td>
                  <td style={{ padding: "10px 8px" }}>{d.application_name || "-"}</td>
                  <td style={{ padding: "10px 8px" }}>{d.join_count ?? 0}</td>
                  <td style={{ padding: "10px 8px" }}>{d.up_count ?? 0}</td>
                  <td style={{ padding: "10px 8px" }}>{d.status_count ?? 0}</td>
                  <td style={{ padding: "10px 8px" }}>{d.ack_count ?? 0} / {d.txack_count ?? 0}</td>
                  <td style={{ padding: "10px 8px" }}>{d.log_count ?? 0}</td>
                  <td style={{ padding: "10px 8px" }}>{d.error_count ?? 0} / {d.warning_count ?? 0}</td>
                  <td style={{ padding: "10px 8px" }}>{d.mic_error_count ?? 0}</td>
                  <td style={{ padding: "10px 8px" }}>{d.replay_suspected_count ?? 0}</td>
                  <td style={{ padding: "10px 8px" }}>{d.last_battery_level ?? "-"} / {d.last_margin ?? "-"}</td>
                  <td style={{ padding: "10px 8px" }}>{d.last_rssi ?? "-"} / {d.last_snr ?? "-"}</td>
                  <td style={{ padding: "10px 8px" }}>{formatTs(d.updated_at)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section style={cardStyle()}>
        <h2 style={{ marginTop: 0 }}>Recent ChirpStack events</h2>
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 14 }}>
            <thead>
              <tr style={{ textAlign: "left", borderBottom: "1px solid #e5e7eb" }}>
                <th style={{ padding: "10px 8px" }}>Time</th>
                <th style={{ padding: "10px 8px" }}>Type</th>
                <th style={{ padding: "10px 8px" }}>Level</th>
                <th style={{ padding: "10px 8px" }}>Failure</th>
                <th style={{ padding: "10px 8px" }}>Device</th>
                <th style={{ padding: "10px 8px" }}>Gateway</th>
                <th style={{ padding: "10px 8px" }}>MIC</th>
                <th style={{ padding: "10px 8px" }}>Replay</th>
                <th style={{ padding: "10px 8px" }}>Battery / Margin</th>
                <th style={{ padding: "10px 8px" }}>Code</th>
                <th style={{ padding: "10px 8px" }}>Description</th>
              </tr>
            </thead>
            <tbody>
              {events.map((e) => (
                <tr key={e.id} style={{ borderBottom: "1px solid #f3f4f6" }}>
                  <td style={{ padding: "10px 8px", whiteSpace: "nowrap" }}>{formatTs(e.observed_at)}</td>
                  <td style={{ padding: "10px 8px", fontWeight: 700 }}>{e.event_type}</td>
                  <td style={{ padding: "10px 8px" }}>{e.event_level || "-"}</td>
                  <td style={{ padding: "10px 8px" }}>{failureBadge(e.failure_class)}</td>
                  <td style={{ padding: "10px 8px" }}>
                    <div title={e.device_name || e.dev_eui || ""}>{displayDevice(e.device_name, e.dev_eui)}</div>
                    <div style={{ color: "#6b7280", fontSize: 12 }}>{e.application_name || "-"}</div>
                  </td>
                  <td style={{ padding: "10px 8px" }} title={e.gateway_id || ""}>{compactDisplayId(e.gateway_id)}</td>
                  <td style={{ padding: "10px 8px" }}>{e.mic_status || "unknown"}</td>
                  <td style={{ padding: "10px 8px" }}>{e.replay_suspected ? "yes" : "no"}</td>
                  <td style={{ padding: "10px 8px" }}>{e.battery_level ?? "-"} / {e.margin ?? "-"}</td>
                  <td style={{ padding: "10px 8px" }}>{e.code || "-"}</td>
                  <td style={{ padding: "10px 8px", maxWidth: 420 }}>{e.description || "-"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}
