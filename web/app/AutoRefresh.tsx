"use client";

import { useEffect } from "react";
import { useRouter } from "next/navigation";

export default function AutoRefresh({ intervalMs = 10000 }: { intervalMs?: number }) {
  const router = useRouter();

  useEffect(() => {
    const id = setInterval(() => {
      router.refresh();
    }, intervalMs);

    return () => clearInterval(id);
  }, [router, intervalMs]);

  return (
    <p style={{ fontSize: 12, color: "#777", marginTop: 8 }}>
      Auto-refresh every {Math.round(intervalMs / 1000)} seconds
    </p>
  );
}