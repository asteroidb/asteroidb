"use client";

import { useState, useEffect, useRef } from "react";
import Card from "@/components/card";
import StatusBadge from "@/components/status-badge";
import ProofViewer from "@/components/proof-viewer";
import {
  certifiedWrite,
  certifiedRead,
  type CrdtValueJson,
  type CertifiedReadResponse,
  type CertificationStatus,
} from "@/lib/asteroidb";

type CrdtType = "counter" | "register";

export default function CertifiedPage() {
  // Write form
  const [writeKey, setWriteKey] = useState("mission/thrust-cmd");
  const [crdtType, setCrdtType] = useState<CrdtType>("counter");
  const [writeValue, setWriteValue] = useState("42");
  const [onTimeout, setOnTimeout] = useState<"pending" | "error">("pending");
  const [writeResult, setWriteResult] = useState<{ status: CertificationStatus } | null>(null);
  const [writeError, setWriteError] = useState("");
  const [writing, setWriting] = useState(false);

  // Read & verify
  const [readKey, setReadKey] = useState("mission/thrust-cmd");
  const [readResult, setReadResult] = useState<CertifiedReadResponse | null>(null);
  const [readRaw, setReadRaw] = useState("");
  const [readError, setReadError] = useState("");
  const [polling, setPolling] = useState(false);
  const pollingRef = useRef(false);

  const buildValue = (): CrdtValueJson => {
    if (crdtType === "counter") {
      return { type: "counter", value: parseInt(writeValue) || 0 };
    }
    return { type: "register", value: writeValue };
  };

  const handleWrite = async () => {
    setWriting(true);
    setWriteError("");
    setWriteResult(null);
    try {
      const res = await certifiedWrite({
        key: writeKey,
        value: buildValue(),
        on_timeout: onTimeout,
      });
      setWriteResult(res);
      setReadKey(writeKey);
    } catch (e) {
      setWriteError(String(e));
    } finally {
      setWriting(false);
    }
  };

  const handleRead = async () => {
    try {
      const res = await certifiedRead(readKey);
      setReadResult(res);
      setReadRaw(JSON.stringify(res, null, 2));
      setReadError("");
      return res;
    } catch (e) {
      setReadError(String(e));
      return null;
    }
  };

  useEffect(() => {
    pollingRef.current = polling;
  }, [polling]);

  useEffect(() => {
    if (!polling) return;
    const id = setInterval(async () => {
      if (!pollingRef.current) return;
      const res = await handleRead();
      if (res && res.status === "certified") {
        setPolling(false);
      }
    }, 2000);
    return () => clearInterval(id);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [polling, readKey]);

  return (
    <div>
      <div className="mb-6">
        <h1 className="text-xl font-bold" style={{ color: "var(--text-primary)" }}>
          Certified Writes
        </h1>
        <p className="text-sm mt-1" style={{ color: "var(--text-secondary)" }}>
          Write data with authority-consensus certification and cryptographic proof bundles.
          Unlike eventual writes, certified writes require majority agreement from authority nodes.
        </p>
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-2 gap-5">
        {/* Write form */}
        <Card title="Submit Certified Write" subtitle="Send a write request requiring authority consensus">
          <div className="space-y-3">
            <div>
              <label className="block text-xs mb-1" style={{ color: "var(--text-secondary)" }}>Key</label>
              <input
                value={writeKey}
                onChange={(e) => setWriteKey(e.target.value)}
                className="w-full px-3 py-1.5 rounded-lg text-sm border outline-none"
                style={{ background: "var(--bg-primary)", borderColor: "var(--border-color)", color: "var(--text-primary)" }}
              />
            </div>
            <div className="grid grid-cols-2 gap-3">
              <div>
                <label className="block text-xs mb-1" style={{ color: "var(--text-secondary)" }}>CRDT Type</label>
                <select
                  value={crdtType}
                  onChange={(e) => setCrdtType(e.target.value as CrdtType)}
                  className="w-full px-3 py-1.5 rounded-lg text-sm border outline-none"
                  style={{ background: "var(--bg-primary)", borderColor: "var(--border-color)", color: "var(--text-primary)" }}
                >
                  <option value="counter">Counter</option>
                  <option value="register">Register</option>
                </select>
              </div>
              <div>
                <label className="block text-xs mb-1" style={{ color: "var(--text-secondary)" }}>On Timeout</label>
                <select
                  value={onTimeout}
                  onChange={(e) => setOnTimeout(e.target.value as "pending" | "error")}
                  className="w-full px-3 py-1.5 rounded-lg text-sm border outline-none"
                  style={{ background: "var(--bg-primary)", borderColor: "var(--border-color)", color: "var(--text-primary)" }}
                >
                  <option value="pending">Pending (keep polling)</option>
                  <option value="error">Error (504 on timeout)</option>
                </select>
              </div>
            </div>
            <div>
              <label className="block text-xs mb-1" style={{ color: "var(--text-secondary)" }}>
                Value {crdtType === "counter" ? "(integer)" : "(string)"}
              </label>
              <input
                value={writeValue}
                onChange={(e) => setWriteValue(e.target.value)}
                className="w-full px-3 py-1.5 rounded-lg text-sm border outline-none"
                style={{ background: "var(--bg-primary)", borderColor: "var(--border-color)", color: "var(--text-primary)" }}
              />
            </div>
            <button
              onClick={handleWrite}
              disabled={writing}
              className="w-full py-2 rounded-lg text-sm font-medium transition-colors hover:opacity-80 disabled:opacity-40"
              style={{ background: "var(--accent-blue)", color: "#fff" }}
            >
              {writing ? "Submitting..." : "Submit Certified Write"}
            </button>
            {writeResult && (
              <div className="flex items-center gap-2 p-3 rounded-lg" style={{ background: "var(--bg-primary)" }}>
                <span className="text-xs" style={{ color: "var(--text-secondary)" }}>Result:</span>
                <StatusBadge status={writeResult.status} />
              </div>
            )}
            {writeError && <p className="text-xs" style={{ color: "var(--accent-red)" }}>{writeError}</p>}
          </div>
        </Card>

        {/* Read & verify */}
        <Card title="Certified Read & Verify" subtitle="Read with certification status and proof bundle">
          <div className="space-y-3">
            <div className="flex gap-2">
              <input
                value={readKey}
                onChange={(e) => setReadKey(e.target.value)}
                placeholder="Key to read..."
                className="flex-1 px-3 py-1.5 rounded-lg text-sm border outline-none"
                style={{ background: "var(--bg-primary)", borderColor: "var(--border-color)", color: "var(--text-primary)" }}
              />
              <button
                onClick={handleRead}
                className="px-4 py-1.5 rounded-lg text-sm font-medium border transition-colors hover:opacity-80"
                style={{ borderColor: "var(--border-color)", color: "var(--text-primary)" }}
              >
                Read
              </button>
              <button
                onClick={() => { setPolling(!polling); if (!polling) handleRead(); }}
                className="px-4 py-1.5 rounded-lg text-sm font-medium transition-colors hover:opacity-80"
                style={{
                  background: polling ? "var(--accent-yellow)" : "transparent",
                  color: polling ? "#000" : "var(--accent-yellow)",
                  border: polling ? "none" : "1px solid var(--accent-yellow)",
                }}
              >
                {polling ? "Stop" : "Poll"}
              </button>
            </div>

            {readResult && (
              <>
                <div className="flex items-center gap-3 p-3 rounded-lg" style={{ background: "var(--bg-primary)" }}>
                  <StatusBadge status={readResult.status} />
                  <span className="text-sm font-mono" style={{ color: "var(--text-primary)" }}>
                    {readResult.value
                      ? readResult.value.type === "counter"
                        ? `counter = ${readResult.value.value}`
                        : readResult.value.type === "register"
                        ? `register = "${readResult.value.value}"`
                        : JSON.stringify(readResult.value)
                      : "null"}
                  </span>
                </div>

                {readResult.proof && <ProofViewer proof={readResult.proof} />}
              </>
            )}

            {readError && <p className="text-xs" style={{ color: "var(--accent-red)" }}>{readError}</p>}

            {readRaw && (
              <details>
                <summary className="text-xs cursor-pointer" style={{ color: "var(--text-secondary)" }}>
                  Raw JSON
                </summary>
                <pre className="mt-1 p-2 rounded text-xs overflow-auto max-h-64"
                  style={{ background: "var(--bg-primary)", color: "var(--text-secondary)" }}>
                  {readRaw}
                </pre>
              </details>
            )}
          </div>
        </Card>
      </div>

      {/* Explanation */}
      <div className="mt-6 p-5 rounded-xl border" style={{ background: "var(--bg-card)", borderColor: "var(--border-color)" }}>
        <h2 className="text-sm font-semibold mb-2" style={{ color: "var(--text-primary)" }}>
          How Certified Writes Work
        </h2>
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4 text-xs" style={{ color: "var(--text-secondary)" }}>
          <div className="p-3 rounded-lg" style={{ background: "var(--bg-primary)" }}>
            <div className="font-medium mb-1" style={{ color: "var(--accent-blue)" }}>1. Submit</div>
            Client sends a certified write request with the key, CRDT value, and timeout behavior.
          </div>
          <div className="p-3 rounded-lg" style={{ background: "var(--bg-primary)" }}>
            <div className="font-medium mb-1" style={{ color: "var(--accent-yellow)" }}>2. Pending</div>
            The write enters &quot;pending&quot; state. Authority nodes exchange ack_frontier updates via HLC timestamps.
          </div>
          <div className="p-3 rounded-lg" style={{ background: "var(--bg-primary)" }}>
            <div className="font-medium mb-1" style={{ color: "var(--accent-green)" }}>3. Certified</div>
            When majority of authorities acknowledge the frontier past the write timestamp, a certificate with Ed25519 signatures is created.
          </div>
          <div className="p-3 rounded-lg" style={{ background: "var(--bg-primary)" }}>
            <div className="font-medium mb-1" style={{ color: "var(--accent-purple)" }}>4. Verify</div>
            Clients can independently verify the proof bundle containing the majority certificate and authority signatures.
          </div>
        </div>
      </div>
    </div>
  );
}
