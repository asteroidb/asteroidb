"use client";

import { useState, useEffect, useCallback } from "react";
import Card from "./card";
import { eventualWrite, eventualRead } from "@/lib/asteroidb";

export default function SetPanel() {
  const [key, setKey] = useState("active-alerts");
  const [elements, setElements] = useState<string[]>([]);
  const [inputValue, setInputValue] = useState("");
  const [raw, setRaw] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);

  const readValue = useCallback(async () => {
    try {
      const res = await eventualRead(key);
      setRaw(JSON.stringify(res, null, 2));
      if (res.value && res.value.type === "set") {
        setElements(res.value.elements);
      } else {
        setElements([]);
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

  const handleAdd = async () => {
    if (!inputValue) return;
    setLoading(true);
    try {
      await eventualWrite({ type: "set_add", key, element: inputValue });
      setInputValue("");
      await readValue();
      setError("");
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  };

  const handleRemove = async (element: string) => {
    setLoading(true);
    try {
      await eventualWrite({ type: "set_remove", key, element });
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
      title="OR-Set"
      subtitle="Observed-Remove set with add-wins semantics"
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
        <div className="flex flex-wrap gap-1.5 min-h-[2rem] p-2 rounded-lg" style={{ background: "var(--bg-primary)" }}>
          {elements.length === 0 && (
            <span className="text-xs" style={{ color: "var(--text-secondary)" }}>Empty set</span>
          )}
          {elements.map((el) => (
            <span
              key={el}
              className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs"
              style={{ background: "rgba(168, 85, 247, 0.15)", color: "var(--accent-purple)" }}
            >
              {el}
              <button
                onClick={() => handleRemove(el)}
                className="hover:opacity-60 ml-0.5"
                disabled={loading}
              >
                x
              </button>
            </span>
          ))}
        </div>
        <div className="flex gap-2">
          <input
            value={inputValue}
            onChange={(e) => setInputValue(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && handleAdd()}
            placeholder="Add element..."
            className="flex-1 px-3 py-1.5 rounded-lg text-sm border outline-none focus:ring-1"
            style={{
              background: "var(--bg-primary)",
              borderColor: "var(--border-color)",
              color: "var(--text-primary)",
            }}
          />
          <button
            onClick={handleAdd}
            disabled={loading || !inputValue}
            className="px-4 py-1.5 rounded-lg text-sm font-medium transition-colors hover:opacity-80 disabled:opacity-40"
            style={{ background: "var(--accent-purple)", color: "#fff" }}
          >
            Add
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
