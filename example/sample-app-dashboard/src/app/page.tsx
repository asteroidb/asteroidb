"use client";

import { useState, useEffect, useCallback } from "react";
import Link from "next/link";
import Card from "@/components/card";
import {
  healthCheck,
  getMetrics,
  getSlo,
  eventualRead,
  type MetricsSnapshot,
  type SloSnapshot,
} from "@/lib/asteroidb";

function useRefresh<T>(fn: () => Promise<T>, intervalMs: number) {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState("");

  const refresh = useCallback(async () => {
    try {
      setData(await fn());
      setError("");
    } catch (e) {
      setError(String(e));
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    refresh();
    const id = setInterval(refresh, intervalMs);
    return () => clearInterval(id);
  }, [refresh, intervalMs]);

  return { data, error };
}

export default function DashboardPage() {
  const health = useRefresh(healthCheck, 5000);
  const metrics = useRefresh(getMetrics, 5000);
  const slo = useRefresh(getSlo, 10000);
  const events = useRefresh(() => eventualRead("telemetry-packets"), 5000);

  const m = metrics.data as MetricsSnapshot | null;
  const s = slo.data as SloSnapshot | null;
  const isHealthy = health.data?.status === "ok";

  const worstBudget = s
    ? Object.values(s.budgets).reduce(
        (worst, b) => (b.budget_remaining < worst ? b.budget_remaining : worst),
        100
      )
    : null;

  const eventCount =
    events.data?.value && events.data.value.type === "counter"
      ? events.data.value.value
      : 0;

  return (
    <div>
      <div className="mb-6">
        <h1 className="text-xl font-bold" style={{ color: "var(--text-primary)" }}>
          Mission Control
        </h1>
        <p className="text-sm mt-1" style={{ color: "var(--text-secondary)" }}>
          AsteroidDB distributed key-value store — dual consistency with CRDT merge
        </p>
      </div>

      {/* Summary tiles */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
        <SummaryTile
          label="Cluster Health"
          value={isHealthy ? "Healthy" : health.error ? "Down" : "..."}
          color={isHealthy ? "var(--accent-green)" : "var(--accent-red)"}
          dot
        />
        <SummaryTile
          label="Event Counter"
          value={eventCount}
          color="var(--accent-blue)"
        />
        <SummaryTile
          label="Pending Certs"
          value={m?.pending_count ?? "—"}
          color="var(--accent-yellow)"
        />
        <SummaryTile
          label="SLO Budget"
          value={worstBudget != null ? `${worstBudget.toFixed(1)}%` : "—"}
          color={
            worstBudget != null && worstBudget < 20
              ? "var(--accent-red)"
              : worstBudget != null && worstBudget < 50
              ? "var(--accent-yellow)"
              : "var(--accent-green)"
          }
        />
      </div>

      {/* Feature cards */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-5">
        <FeatureCard
          href="/telemetry"
          title="Telemetry"
          description="Explore all 4 CRDT types: PN-Counter, LWW-Register, OR-Set, and OR-Map. Write data with eventual consistency and watch CRDT merge in real-time."
          color="var(--accent-blue)"
          stats={m ? `${m.certified_total + (m.sync_attempt_total || 0)} operations` : undefined}
        />
        <FeatureCard
          href="/certified"
          title="Certified Writes"
          description="Submit writes requiring authority-consensus certification. View proof bundles with Ed25519 signatures and verify them independently."
          color="var(--accent-green)"
          stats={m ? `${m.certified_total} certified` : undefined}
        />
        <FeatureCard
          href="/cluster"
          title="Cluster Operations"
          description="Monitor cluster topology, per-peer sync metrics, certification latency, frontier skew, and SLO error budgets."
          color="var(--accent-purple)"
          stats={m ? `${m.frontier_skew_ms}ms frontier skew` : undefined}
        />
      </div>

      {/* About */}
      <div className="mt-8 p-5 rounded-xl border" style={{ background: "var(--bg-card)", borderColor: "var(--border-color)" }}>
        <h2 className="text-sm font-semibold mb-3" style={{ color: "var(--text-primary)" }}>
          About AsteroidDB
        </h2>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-xs" style={{ color: "var(--text-secondary)" }}>
          <div>
            <p className="mb-2">
              AsteroidDB is a distributed key-value store that unifies <strong style={{ color: "var(--text-primary)" }}>eventual</strong> and{" "}
              <strong style={{ color: "var(--text-primary)" }}>certified</strong> consistency in a single cluster.
            </p>
            <p>
              Applications choose per-operation between fast CRDT-based eventual writes
              and authority-consensus certified writes with cryptographic proof bundles.
            </p>
          </div>
          <div>
            <p className="mb-2">Key features:</p>
            <ul className="space-y-1 list-disc list-inside">
              <li>4 CRDT types with automatic conflict resolution</li>
              <li>Hybrid Logical Clock (HLC) ordering</li>
              <li>Ed25519 / BLS12-381 dual-mode signatures</li>
              <li>Tag-based placement (no fixed hierarchy)</li>
              <li>Built-in SLO monitoring</li>
            </ul>
          </div>
        </div>
      </div>
    </div>
  );
}

function SummaryTile({
  label,
  value,
  color,
  dot,
}: {
  label: string;
  value: string | number;
  color: string;
  dot?: boolean;
}) {
  return (
    <div className="p-4 rounded-xl border" style={{ background: "var(--bg-card)", borderColor: "var(--border-color)" }}>
      <div className="text-xs mb-2" style={{ color: "var(--text-secondary)" }}>{label}</div>
      <div className="flex items-center gap-2">
        {dot && <div className="w-2.5 h-2.5 rounded-full" style={{ background: color }} />}
        <span className="text-2xl font-mono font-bold" style={{ color }}>{value}</span>
      </div>
    </div>
  );
}

function FeatureCard({
  href,
  title,
  description,
  color,
  stats,
}: {
  href: string;
  title: string;
  description: string;
  color: string;
  stats?: string;
}) {
  return (
    <Link href={href}>
      <div className="p-5 rounded-xl border transition-colors hover:border-opacity-60 cursor-pointer h-full"
        style={{ background: "var(--bg-card)", borderColor: "var(--border-color)" }}>
        <div className="flex items-center gap-2 mb-2">
          <div className="w-2 h-2 rounded-full" style={{ background: color }} />
          <h3 className="text-sm font-semibold" style={{ color: "var(--text-primary)" }}>{title}</h3>
        </div>
        <p className="text-xs mb-3" style={{ color: "var(--text-secondary)" }}>{description}</p>
        {stats && (
          <div className="text-xs font-mono" style={{ color }}>{stats}</div>
        )}
      </div>
    </Link>
  );
}
