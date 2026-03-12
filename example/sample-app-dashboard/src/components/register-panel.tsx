"use client";

import { useState, useEffect, useCallback } from "react";
import Card from "./card";
import { eventualWrite, eventualRead } from "@/lib/asteroidb";

export default function RegisterPanel() {
  const [key, setKey] = useState("sensor-temperature");
  const [currentValue, setCurrentValue] = useState<string | null>(null);
  const [inputValue, setInputValue] = useState("");
  const [raw, setRaw] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);

  const readValue = useCallback(async () => {
    try {
      const res = await eventualRead(key);
      setRaw(JSON.stringify(res, null, 2));
      if (res.value && res.value.type === "register") {
        setCurrentValue(res.value.value);
      } else {
        setCurrentValue(null);
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

  const handleSet = async () => {
    if (!inputValue) return;
    setLoading(true);
    try {
      await eventualWrite({ type: "register_set", key, value: inputValue });
      setInputValue("");
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
      title="LWW-Register"
      subtitle="Last-Writer-Wins single value register"
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
        <div className="p-3 rounded-lg" style={{ background: "var(--bg-primary)" }}>
          <div className="text-xs mb-1" style={{ color: "var(--text-secondary)" }}>Current value</div>
          <div className="text-lg font-mono" style={{ color: "var(--accent-green)" }}>
            {currentValue ?? "—"}
          </div>
        </div>
        <div className="flex gap-2">
          <input
            value={inputValue}
            onChange={(e) => setInputValue(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && handleSet()}
            placeholder="New value..."
            className="flex-1 px-3 py-1.5 rounded-lg text-sm border outline-none focus:ring-1"
            style={{
              background: "var(--bg-primary)",
              borderColor: "var(--border-color)",
              color: "var(--text-primary)",
            }}
          />
          <button
            onClick={handleSet}
            disabled={loading || !inputValue}
            className="px-4 py-1.5 rounded-lg text-sm font-medium transition-colors hover:opacity-80 disabled:opacity-40"
            style={{ background: "var(--accent-green)", color: "#fff" }}
          >
            Set
          </button>
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
