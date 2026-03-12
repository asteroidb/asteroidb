// AsteroidDB HTTP API client
// All requests go through the Next.js rewrite proxy at /api/asteroidb/*

const BASE = "/api/asteroidb";

// ---------------------------------------------------------------
// CRDT value types (mirrors src/http/types.rs CrdtValueJson)
// ---------------------------------------------------------------

export type CrdtValueJson =
  | { type: "counter"; value: number }
  | { type: "set"; elements: string[] }
  | { type: "map"; entries: Record<string, string> }
  | { type: "register"; value: string | null };

export type CertificationStatus =
  | "pending"
  | "certified"
  | "rejected"
  | "timeout";

// ---------------------------------------------------------------
// Request types
// ---------------------------------------------------------------

export type EventualWriteRequest =
  | { type: "counter_inc"; key: string }
  | { type: "counter_dec"; key: string }
  | { type: "set_add"; key: string; element: string }
  | { type: "set_remove"; key: string; element: string }
  | { type: "map_set"; key: string; map_key: string; map_value: string }
  | { type: "map_delete"; key: string; map_key: string }
  | { type: "register_set"; key: string; value: string };

export interface CertifiedWriteRequest {
  key: string;
  value: CrdtValueJson;
  on_timeout?: "pending" | "error";
}

// ---------------------------------------------------------------
// Response types
// ---------------------------------------------------------------

export interface EventualReadResponse {
  key: string;
  value: CrdtValueJson | null;
}

export interface WriteResponse {
  ok: boolean;
}

export interface FrontierJson {
  physical: number;
  logical: number;
  node_id: string;
}

export interface AuthoritySignatureJson {
  authority_id: string;
  public_key: string;
  signature: string;
  keyset_version: number;
}

export interface CertificateJson {
  keyset_version: number;
  signatures: AuthoritySignatureJson[];
}

export interface ProofBundleJson {
  key_range_prefix: string;
  frontier: FrontierJson;
  policy_version: number;
  contributing_authorities: string[];
  total_authorities: number;
  certificate?: CertificateJson;
}

export interface CertifiedReadResponse {
  key: string;
  value: CrdtValueJson | null;
  status: CertificationStatus;
  frontier: FrontierJson | null;
  proof: ProofBundleJson | null;
}

export interface CertifiedWriteResponse {
  status: CertificationStatus;
}

export interface VerifyProofResponse {
  valid: boolean;
  has_majority: boolean;
  contributing_count: number;
  required_count: number;
}

export interface StatusResponse {
  key: string;
  status: CertificationStatus;
}

// Metrics
export interface PeerSyncSnapshot {
  mean_latency_us: number;
  p99_latency_us: number;
  success_count: number;
  failure_count: number;
}

export interface CertificationLatencySnapshot {
  sample_count: number;
  mean_us: number;
  p99_us: number;
}

export interface MetricsSnapshot {
  pending_count: number;
  certified_total: number;
  certification_latency_mean_us: number;
  frontier_skew_ms: number;
  sync_failure_rate: number;
  sync_attempt_total: number;
  sync_failure_total: number;
  peer_sync: Record<string, PeerSyncSnapshot>;
  certification_latency_window: CertificationLatencySnapshot;
  rebalance_start_total: number;
  rebalance_keys_migrated: number;
  rebalance_keys_failed: number;
  rebalance_complete_total: number;
  rebalance_duration_sum_us: number;
  key_rotation_total: number;
}

// SLO
export interface SloTarget {
  name: string;
  kind: string;
  target_value: number;
  target_percentage: number;
  window_secs: number;
}

export interface SloBudget {
  target: SloTarget;
  total_requests: number;
  violations: number;
  budget_remaining: number;
  is_warning: boolean;
  is_critical: boolean;
}

export interface SloSnapshot {
  budgets: Record<string, SloBudget>;
}

// Topology
export interface RegionInfo {
  name: string;
  node_count: number;
  node_ids: string[];
  inter_region_latency_ms: Record<string, number>;
}

export interface TopologyView {
  regions: RegionInfo[];
  total_nodes: number;
}

// ---------------------------------------------------------------
// API functions
// ---------------------------------------------------------------

async function fetchJson<T>(url: string, init?: RequestInit): Promise<T> {
  const res = await fetch(url, init);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`${res.status}: ${text}`);
  }
  return res.json();
}

// Eventual API

export async function eventualWrite(
  body: EventualWriteRequest
): Promise<WriteResponse> {
  return fetchJson(`${BASE}/eventual/write`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

export async function eventualRead(
  key: string
): Promise<EventualReadResponse> {
  return fetchJson(`${BASE}/eventual/${key}`);
}

// Certified API

export async function certifiedWrite(
  body: CertifiedWriteRequest
): Promise<CertifiedWriteResponse> {
  return fetchJson(`${BASE}/certified/write`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

export async function certifiedRead(
  key: string
): Promise<CertifiedReadResponse> {
  return fetchJson(`${BASE}/certified/${key}`);
}

export async function getStatus(key: string): Promise<StatusResponse> {
  return fetchJson(`${BASE}/status/${key}`);
}

export async function verifyProof(
  proof: ProofBundleJson
): Promise<VerifyProofResponse> {
  return fetchJson(`${BASE}/certified/verify`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(proof),
  });
}

// Operational API

export async function getMetrics(): Promise<MetricsSnapshot> {
  return fetchJson(`${BASE}/metrics`);
}

export async function getSlo(): Promise<SloSnapshot> {
  return fetchJson(`${BASE}/slo`);
}

export async function getTopology(): Promise<TopologyView> {
  return fetchJson(`${BASE}/topology`);
}

export async function healthCheck(): Promise<{ status: string }> {
  return fetchJson("/healthz");
}
