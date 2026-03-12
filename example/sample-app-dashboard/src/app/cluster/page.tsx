"use client";

import { useState, useEffect, useCallback } from "react";
import Card from "@/components/card";
import {
  healthCheck,
  getTopology,
  getMetrics,
  getSlo,
  type TopologyView,
  type MetricsSnapshot,
  type SloSnapshot,
} from "@/lib/asteroidb";

function HealthSection() {
  const [status, setStatus] = useState<string | null>(null);
  const [error, setError] = useState("");

  const refresh = useCallback(async () => {
    try {
      const res = await healthCheck();
      setStatus(res.status);
      setError("");
    } catch (e) {
      setStatus(null);
      setError(String(e));
    }
  }, []);

  useEffect(() => {
    refresh();
    const id = setInterval(refresh, 5000);
    return () => clearInterval(id);
  }, [refresh]);

  return (
    <Card title="Health" subtitle="Node health check (auto-refresh 5s)">
      <div className="flex items-center gap-3">
        <div
          className="w-4 h-4 rounded-full"
          style={{ background: status === "ok" ? "var(--accent-green)" : error ? "var(--accent-red)" : "var(--accent-yellow)" }}
        />
        <span className="text-sm font-mono" style={{ color: "var(--text-primary)" }}>
          {status === "ok" ? "Healthy" : error ? "Unreachable" : "Checking..."}
        </span>
        {error && <span className="text-xs ml-2" style={{ color: "var(--accent-red)" }}>{error}</span>}
      </div>
    </Card>
  );
}

function TopologySection() {
  const [topo, setTopo] = useState<TopologyView | null>(null);
  const [error, setError] = useState("");

  const refresh = useCallback(async () => {
    try {
      const res = await getTopology();
      setTopo(res);
      setError("");
    } catch (e) {
      setError(String(e));
    }
  }, []);

  useEffect(() => {
    refresh();
    const id = setInterval(refresh, 10000);
    return () => clearInterval(id);
  }, [refresh]);

  return (
    <Card title="Topology" subtitle={`Cluster topology by region (${topo?.total_nodes ?? 0} nodes)`}>
      {error && <p className="text-xs" style={{ color: "var(--accent-red)" }}>{error}</p>}
      {topo && topo.regions.length === 0 && (
        <p className="text-xs" style={{ color: "var(--text-secondary)" }}>No regions available</p>
      )}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
        {topo?.regions.map((region) => (
          <div key={region.name} className="p-3 rounded-lg border" style={{ background: "var(--bg-primary)", borderColor: "var(--border-color)" }}>
            <div className="flex items-center gap-2 mb-2">
              <div className="w-2 h-2 rounded-full" style={{ background: "var(--accent-blue)" }} />
              <span className="text-sm font-medium" style={{ color: "var(--text-primary)" }}>{region.name}</span>
              <span className="text-xs ml-auto" style={{ color: "var(--text-secondary)" }}>
                {region.node_count} node{region.node_count !== 1 ? "s" : ""}
              </span>
            </div>
            <div className="flex flex-wrap gap-1 mb-2">
              {region.node_ids.map((id) => (
                <span key={id} className="px-1.5 py-0.5 rounded text-xs font-mono"
                  style={{ background: "rgba(59, 130, 246, 0.1)", color: "var(--accent-blue)" }}>
                  {id}
                </span>
              ))}
            </div>
            {Object.keys(region.inter_region_latency_ms).length > 0 && (
              <div className="text-xs" style={{ color: "var(--text-secondary)" }}>
                {Object.entries(region.inter_region_latency_ms).map(([target, ms]) => (
                  <div key={target} className="flex justify-between">
                    <span>to {target}</span>
                    <span className="font-mono">{ms.toFixed(1)}ms</span>
                  </div>
                ))}
              </div>
            )}
          </div>
        ))}
      </div>
    </Card>
  );
}

function MetricsSection() {
  const [metrics, setMetrics] = useState<MetricsSnapshot | null>(null);
  const [error, setError] = useState("");

  const refresh = useCallback(async () => {
    try {
      const res = await getMetrics();
      setMetrics(res);
      setError("");
    } catch (e) {
      setError(String(e));
    }
  }, []);

  useEffect(() => {
    refresh();
    const id = setInterval(refresh, 5000);
    return () => clearInterval(id);
  }, [refresh]);

  if (error) {
    return (
      <Card title="Metrics" subtitle="Runtime operational metrics">
        <p className="text-xs" style={{ color: "var(--accent-red)" }}>{error}</p>
      </Card>
    );
  }

  const m = metrics;

  return (
    <Card title="Metrics" subtitle="Runtime operational metrics (auto-refresh 5s)">
      {!m ? (
        <p className="text-xs" style={{ color: "var(--text-secondary)" }}>Loading...</p>
      ) : (
        <div className="space-y-4">
          <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-3">
            <MetricTile label="Pending" value={m.pending_count} color="var(--accent-yellow)" />
            <MetricTile label="Certified Total" value={m.certified_total} color="var(--accent-green)" />
            <MetricTile label="Cert. Latency (mean)" value={`${(m.certification_latency_mean_us / 1000).toFixed(1)}ms`} color="var(--accent-blue)" />
            <MetricTile label="Frontier Skew" value={`${m.frontier_skew_ms}ms`} color="var(--accent-purple)" />
            <MetricTile label="Sync Failure Rate" value={`${(m.sync_failure_rate * 100).toFixed(1)}%`}
              color={m.sync_failure_rate > 0.1 ? "var(--accent-red)" : "var(--accent-green)"} />
            <MetricTile label="Sync Attempts" value={m.sync_attempt_total} color="var(--text-secondary)" />
            <MetricTile label="Key Rotations" value={m.key_rotation_total} color="var(--text-secondary)" />
            <MetricTile label="Rebalance Ops" value={m.rebalance_complete_total} color="var(--text-secondary)" />
          </div>

          {m.certification_latency_window.sample_count > 0 && (
            <div className="p-3 rounded-lg" style={{ background: "var(--bg-primary)" }}>
              <div className="text-xs font-medium mb-2" style={{ color: "var(--text-secondary)" }}>
                Certification Latency Window ({m.certification_latency_window.sample_count} samples)
              </div>
              <div className="flex gap-6 text-sm font-mono">
                <span style={{ color: "var(--accent-blue)" }}>
                  mean: {(m.certification_latency_window.mean_us / 1000).toFixed(1)}ms
                </span>
                <span style={{ color: "var(--accent-purple)" }}>
                  p99: {(m.certification_latency_window.p99_us / 1000).toFixed(1)}ms
                </span>
              </div>
            </div>
          )}

          {Object.keys(m.peer_sync).length > 0 && (
            <div>
              <div className="text-xs font-medium mb-2" style={{ color: "var(--text-secondary)" }}>Per-Peer Sync</div>
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
                {Object.entries(m.peer_sync).map(([peer, stats]) => (
                  <div key={peer} className="p-2 rounded text-xs font-mono"
                    style={{ background: "var(--bg-primary)" }}>
                    <span style={{ color: "var(--accent-blue)" }}>{peer}</span>
                    <div className="flex gap-4 mt-1" style={{ color: "var(--text-secondary)" }}>
                      <span>mean: {(stats.mean_latency_us / 1000).toFixed(1)}ms</span>
                      <span>ok: {stats.success_count}</span>
                      <span style={{ color: stats.failure_count > 0 ? "var(--accent-red)" : "inherit" }}>
                        fail: {stats.failure_count}
                      </span>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      )}
    </Card>
  );
}

function MetricTile({ label, value, color }: { label: string; value: string | number; color: string }) {
  return (
    <div className="p-3 rounded-lg" style={{ background: "var(--bg-primary)" }}>
      <div className="text-xs mb-1" style={{ color: "var(--text-secondary)" }}>{label}</div>
      <div className="text-lg font-mono font-bold" style={{ color }}>{value}</div>
    </div>
  );
}

function SloSection() {
  const [slo, setSlo] = useState<SloSnapshot | null>(null);
  const [error, setError] = useState("");

  const refresh = useCallback(async () => {
    try {
      const res = await getSlo();
      setSlo(res);
      setError("");
    } catch (e) {
      setError(String(e));
    }
  }, []);

  useEffect(() => {
    refresh();
    const id = setInterval(refresh, 10000);
    return () => clearInterval(id);
  }, [refresh]);

  return (
    <Card title="SLO Budgets" subtitle="Service Level Objective error budgets (auto-refresh 10s)">
      {error && <p className="text-xs" style={{ color: "var(--accent-red)" }}>{error}</p>}
      {!slo ? (
        <p className="text-xs" style={{ color: "var(--text-secondary)" }}>Loading...</p>
      ) : Object.keys(slo.budgets).length === 0 ? (
        <p className="text-xs" style={{ color: "var(--text-secondary)" }}>No SLO data yet</p>
      ) : (
        <div className="space-y-3">
          {Object.entries(slo.budgets).map(([name, budget]) => {
            const pct = Math.max(0, Math.min(100, budget.budget_remaining));
            const barColor = budget.is_critical
              ? "var(--accent-red)"
              : budget.is_warning
              ? "var(--accent-yellow)"
              : "var(--accent-green)";
            return (
              <div key={name}>
                <div className="flex items-center justify-between mb-1">
                  <span className="text-xs font-mono" style={{ color: "var(--text-primary)" }}>{name}</span>
                  <div className="flex items-center gap-2">
                    {budget.is_critical && (
                      <span className="text-xs px-1.5 py-0.5 rounded"
                        style={{ background: "rgba(239, 68, 68, 0.15)", color: "var(--accent-red)" }}>CRITICAL</span>
                    )}
                    {budget.is_warning && !budget.is_critical && (
                      <span className="text-xs px-1.5 py-0.5 rounded"
                        style={{ background: "rgba(234, 179, 8, 0.15)", color: "var(--accent-yellow)" }}>WARNING</span>
                    )}
                    <span className="text-xs font-mono" style={{ color: "var(--text-secondary)" }}>
                      {pct.toFixed(1)}% remaining
                    </span>
                  </div>
                </div>
                <div className="h-2 rounded-full overflow-hidden" style={{ background: "var(--bg-primary)" }}>
                  <div
                    className="h-full rounded-full transition-all"
                    style={{ width: `${pct}%`, background: barColor }}
                  />
                </div>
                <div className="flex justify-between mt-0.5 text-xs" style={{ color: "var(--text-secondary)" }}>
                  <span>
                    {budget.target.kind === "LessThan" ? "<" : ">"} {budget.target.target_value}
                    {" "}@ {budget.target.target_percentage}%
                  </span>
                  <span>{budget.violations}/{budget.total_requests} violations</span>
                </div>
              </div>
            );
          })}
        </div>
      )}
    </Card>
  );
}

export default function ClusterPage() {
  return (
    <div>
      <div className="mb-6">
        <h1 className="text-xl font-bold" style={{ color: "var(--text-primary)" }}>
          Cluster Operations
        </h1>
        <p className="text-sm mt-1" style={{ color: "var(--text-secondary)" }}>
          Monitor cluster health, topology, runtime metrics, and SLO error budgets.
        </p>
      </div>
      <div className="space-y-5">
        <HealthSection />
        <TopologySection />
        <MetricsSection />
        <SloSection />
      </div>
    </div>
  );
}
