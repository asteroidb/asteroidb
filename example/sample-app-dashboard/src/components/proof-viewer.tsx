"use client";

import { useState } from "react";
import type { ProofBundleJson, VerifyProofResponse } from "@/lib/asteroidb";
import { verifyProof } from "@/lib/asteroidb";

interface Props {
  proof: ProofBundleJson;
}

export default function ProofViewer({ proof }: Props) {
  const [verifyResult, setVerifyResult] = useState<VerifyProofResponse | null>(null);
  const [verifying, setVerifying] = useState(false);
  const [error, setError] = useState("");

  const handleVerify = async () => {
    setVerifying(true);
    setError("");
    try {
      const res = await verifyProof(proof);
      setVerifyResult(res);
    } catch (e) {
      setError(String(e));
    } finally {
      setVerifying(false);
    }
  };

  return (
    <div className="space-y-3">
      <div className="grid grid-cols-2 gap-3 text-xs">
        <div className="p-2 rounded" style={{ background: "var(--bg-primary)" }}>
          <div style={{ color: "var(--text-secondary)" }}>Key Range Prefix</div>
          <div className="font-mono mt-0.5" style={{ color: "var(--text-primary)" }}>
            {proof.key_range_prefix || "(default)"}
          </div>
        </div>
        <div className="p-2 rounded" style={{ background: "var(--bg-primary)" }}>
          <div style={{ color: "var(--text-secondary)" }}>Policy Version</div>
          <div className="font-mono mt-0.5" style={{ color: "var(--text-primary)" }}>
            {proof.policy_version}
          </div>
        </div>
        <div className="p-2 rounded" style={{ background: "var(--bg-primary)" }}>
          <div style={{ color: "var(--text-secondary)" }}>Frontier</div>
          <div className="font-mono mt-0.5" style={{ color: "var(--text-primary)" }}>
            {proof.frontier.node_id} @ {proof.frontier.physical}:{proof.frontier.logical}
          </div>
        </div>
        <div className="p-2 rounded" style={{ background: "var(--bg-primary)" }}>
          <div style={{ color: "var(--text-secondary)" }}>Authorities</div>
          <div className="font-mono mt-0.5" style={{ color: "var(--text-primary)" }}>
            {proof.contributing_authorities.length} / {proof.total_authorities}
          </div>
        </div>
      </div>

      {proof.contributing_authorities.length > 0 && (
        <div className="p-2 rounded text-xs" style={{ background: "var(--bg-primary)" }}>
          <div style={{ color: "var(--text-secondary)" }}>Contributing Authorities</div>
          <div className="flex flex-wrap gap-1 mt-1">
            {proof.contributing_authorities.map((a) => (
              <span key={a} className="px-1.5 py-0.5 rounded text-xs font-mono"
                style={{ background: "rgba(34, 197, 94, 0.15)", color: "var(--accent-green)" }}>
                {a}
              </span>
            ))}
          </div>
        </div>
      )}

      {proof.certificate && (
        <details>
          <summary className="text-xs cursor-pointer" style={{ color: "var(--text-secondary)" }}>
            Certificate Signatures ({proof.certificate.signatures.length})
          </summary>
          <div className="mt-1 space-y-1">
            {proof.certificate.signatures.map((sig, i) => (
              <div key={i} className="p-2 rounded text-xs font-mono" style={{ background: "var(--bg-primary)" }}>
                <div style={{ color: "var(--accent-blue)" }}>{sig.authority_id}</div>
                <div className="mt-0.5 truncate" style={{ color: "var(--text-secondary)" }}>
                  pub: {sig.public_key.slice(0, 16)}...
                </div>
                <div className="truncate" style={{ color: "var(--text-secondary)" }}>
                  sig: {sig.signature.slice(0, 16)}...
                </div>
              </div>
            ))}
          </div>
        </details>
      )}

      <div className="flex items-center gap-3">
        <button
          onClick={handleVerify}
          disabled={verifying}
          className="px-4 py-1.5 rounded-lg text-xs font-medium transition-colors hover:opacity-80 disabled:opacity-40"
          style={{ background: "var(--accent-green)", color: "#fff" }}
        >
          {verifying ? "Verifying..." : "Verify Proof"}
        </button>
        {verifyResult && (
          <span className="text-xs font-mono" style={{
            color: verifyResult.valid ? "var(--accent-green)" : "var(--accent-red)"
          }}>
            {verifyResult.valid ? "VALID" : "INVALID"} — {verifyResult.contributing_count}/{verifyResult.required_count} required
          </span>
        )}
        {error && <span className="text-xs" style={{ color: "var(--accent-red)" }}>{error}</span>}
      </div>
    </div>
  );
}
