"use client";

import CounterPanel from "@/components/counter-panel";
import RegisterPanel from "@/components/register-panel";
import SetPanel from "@/components/set-panel";
import MapPanel from "@/components/map-panel";

export default function TelemetryPage() {
  return (
    <div>
      <div className="mb-6">
        <h1 className="text-xl font-bold" style={{ color: "var(--text-primary)" }}>
          Telemetry Playground
        </h1>
        <p className="text-sm mt-1" style={{ color: "var(--text-secondary)" }}>
          Interactive exploration of AsteroidDB&apos;s 4 CRDT types via Eventual consistency.
          Writes are locally accepted and propagated asynchronously via CRDT merge.
        </p>
      </div>
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-5">
        <CounterPanel />
        <RegisterPanel />
        <SetPanel />
        <MapPanel />
      </div>
    </div>
  );
}
