export const metadata = { title: "Demo LoRaWAN Security Lab" };

function navLink(href: string) {
  return {
    color: "#111827",
    textDecoration: "none",
    padding: "8px 12px",
    borderRadius: 10,
    border: "1px solid #e5e7eb",
    background: "#ffffff",
    fontSize: 14,
    fontWeight: 600,
  } as const;
}

export default function RootLayout({ children }) {
  return (
    <html lang="en">
      <body style={{ margin: 0, fontFamily: "Arial, sans-serif", background: "#f6f7fb", color: "#111827" }}>
        <header
          style={{
            position: "sticky",
            top: 0,
            zIndex: 10,
            background: "#ffffff",
            borderBottom: "1px solid #e5e7eb",
            padding: "14px 20px",
          }}
        >
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 16, flexWrap: "wrap" }}>
            <div>
              <div style={{ fontSize: 20, fontWeight: 800 }}>Demo LoRaWAN Security Lab</div>
              <div style={{ fontSize: 13, color: "#6b7280" }}>Dashboard + ChirpStack reference + virtual security test bench</div>
            </div>
            <nav style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
              <a href="/" style={navLink("/")}>Dashboard</a>
              <a href="/security" style={navLink("/security")}>Security</a>
            </nav>
          </div>
        </header>
        <main>{children}</main>
      </body>
    </html>
  );
}
