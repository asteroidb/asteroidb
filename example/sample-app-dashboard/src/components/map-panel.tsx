"use client";

import { useState, useEffect, useCallback } from "react";
import Card from "./card";
import { eventualWrite, eventualRead } from "@/lib/asteroidb";

export default function MapPanel() {
  const [key, setKey] = useState("sensor-config");
  const [entries, setEntries] = useState<Record<string, string>>({});
  const [mapKey, setMapKey] = useState("");
  const [mapValue, setMapValue] = useState("");
  const [raw, setRaw] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);

  const readValue = useCallback(async () => {
    try {
      const res = await eventualRead(key);
      setRaw(JSON.stringify(res, null, 2));
      if (res.value && res.value.type === "map") {
        setEntries(res.value.entries);
      } else {
        setEntries({});
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
    if (!mapKey) return;
    setLoading(true);
    try {
      await eventualWrite({ type: "map_set", key, map_key: mapKey, map_value: mapValue });
      setMapKey("");
      setMapValue("");
      await readValue();
      setError("");
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  };

  const handleDelete = async (deleteKey: string) => {
    setLoading(true);
    try {
      await eventualWrite({ type: "map_delete", key, map_key: deleteKey });
      await readValue();
      setError("");
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  };

  const sortedEntries = Object.entries(entries).sort(([a], [b]) => a.localeCompare(b));

  return (
    <Card
      title="OR-Map"
      subtitle="Observed-Remove map with LWW values"
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
        <div className="rounded-lg overflow-hidden border" style={{ borderColor: "var(--border-color)" }}>
          <table className="w-full text-sm">
            <thead>
              <tr style={{ background: "var(--bg-primary)" }}>
                <th className="text-left px-3 py-1.5 text-xs font-medium" style={{ color: "var(--text-secondary)" }}>Key</th>
                <th className="text-left px-3 py-1.5 text-xs font-medium" style={{ color: "var(--text-secondary)" }}>Value</th>
                <th className="w-8"></th>
              </tr>
            </thead>
            <tbody>
              {sortedEntries.length === 0 && (
                <tr>
                  <td colSpan={3} className="px-3 py-2 text-xs" style={{ color: "var(--text-secondary)" }}>
                    Empty map
                  </td>
                </tr>
              )}
              {sortedEntries.map(([k, v]) => (
                <tr key={k} className="border-t" style={{ borderColor: "var(--border-color)" }}>
                  <td className="px-3 py-1.5 font-mono text-xs" style={{ color: "var(--accent-yellow)" }}>{k}</td>
                  <td className="px-3 py-1.5 font-mono text-xs" style={{ color: "var(--text-primary)" }}>{v}</td>
                  <td className="px-2">
                    <button
                      onClick={() => handleDelete(k)}
                      disabled={loading}
                      className="text-xs hover:opacity-60"
                      style={{ color: "var(--accent-red)" }}
                    >
                      x
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <div className="flex gap-2">
          <input
            value={mapKey}
            onChange={(e) => setMapKey(e.target.value)}
            placeholder="Key"
            className="flex-1 px-3 py-1.5 rounded-lg text-sm border outline-none focus:ring-1"
            style={{
              background: "var(--bg-primary)",
              borderColor: "var(--border-color)",
              color: "var(--text-primary)",
            }}
          />
          <input
            value={mapValue}
            onChange={(e) => setMapValue(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && handleSet()}
            placeholder="Value"
            className="flex-1 px-3 py-1.5 rounded-lg text-sm border outline-none focus:ring-1"
            style={{
              background: "var(--bg-primary)",
              borderColor: "var(--border-color)",
              color: "var(--text-primary)",
            }}
          />
          <button
            onClick={handleSet}
            disabled={loading || !mapKey}
            className="px-4 py-1.5 rounded-lg text-sm font-medium transition-colors hover:opacity-80 disabled:opacity-40"
            style={{ background: "var(--accent-yellow)", color: "#000" }}
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
