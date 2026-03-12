"use client";

import { useState, useEffect, useCallback } from "react";
import Card from "./card";
import { eventualWrite, eventualRead, type CrdtValueJson } from "@/lib/asteroidb";

export default function CounterPanel() {
  const [key, setKey] = useState("telemetry-packets");
  const [value, setValue] = useState<number | null>(null);
  const [raw, setRaw] = useState<string>("");
  const [error, setError] = useState<string>("");
  const [loading, setLoading] = useState(false);

  const readValue = useCallback(async () => {
    try {
      const res = await eventualRead(key);
      setRaw(JSON.stringify(res, null, 2));
      if (res.value && res.value.type === "counter") {
        setValue(res.value.value);
      } else {
        setValue(null);
      }
      setError("");
    } catch (e) {
      setError(String(e));
    }
  }, [key]);

  useEffect(() => {
    readValue();
    const id = setInterval(readValue, 2000);
    return () => clearInterval(id);
  }, [readValue]);

  const handleWrite = async (type: "counter_inc" | "counter_dec") => {
    setLoading(true);
    try {
      await eventualWrite({ type, key });
      await readValue();
      setError("");
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  };

  return (
    <Card
      title="PN-Counter"
      subtitle="Increment / decrement counter with conflict-free merge"
    >
      <div className="space-y-3">
        <div>
          <label className="block text-xs mb-1" style={{ color: "var(--text-secondary)" }}>Key</label>
          <input
            value={key}
            onChange={(e) => setKey(e.target.value)}
            className="w-full px-3 py-1.5 rounded-lg text-sm border outline-none focus:ring-1"
            style={{
              background: "var(--bg-primary)",
              borderColor: "var(--border-color)",
              color: "var(--text-primary)",
            }}
          />
        </div>
        <div className="flex items-center gap-4">
          <div className="text-3xl font-mono font-bold" style={{ color: "var(--accent-blue)" }}>
            {value ?? "—"}
          </div>
          <div className="flex gap-2 ml-auto">
            <button
              onClick={() => handleWrite("counter_dec")}
              disabled={loading}
              className="px-4 py-1.5 rounded-lg text-sm font-medium border transition-colors hover:opacity-80 disabled:opacity-40"
              style={{ borderColor: "var(--border-color)", color: "var(--text-primary)" }}
            >
              −
            </button>
            <button
              onClick={() => handleWrite("counter_inc")}
              disabled={loading}
              className="px-4 py-1.5 rounded-lg text-sm font-medium transition-colors hover:opacity-80 disabled:opacity-40"
              style={{ background: "var(--accent-blue)", color: "#fff" }}
            >
              +
            </button>
          </div>
        </div>
        {error && <p className="text-xs" style={{ color: "var(--accent-red)" }}>{error}</p>}
        <details>
          <summary className="text-xs cursor-pointer" style={{ color: "var(--text-secondary)" }}>
            Raw JSON
          </summary>
          <pre className="mt-1 p-2 rounded text-xs overflow-auto" style={{ background: "var(--bg-primary)", color: "var(--text-secondary)" }}>
            {raw || "No data"}
          </pre>
        </details>
      </div>
    </Card>
  );
}
