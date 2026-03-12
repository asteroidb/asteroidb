import type { CertificationStatus } from "@/lib/asteroidb";

const STATUS_STYLES: Record<CertificationStatus, { bg: string; text: string; label: string }> = {
  pending: { bg: "rgba(234, 179, 8, 0.15)", text: "var(--accent-yellow)", label: "Pending" },
  certified: { bg: "rgba(34, 197, 94, 0.15)", text: "var(--accent-green)", label: "Certified" },
  rejected: { bg: "rgba(239, 68, 68, 0.15)", text: "var(--accent-red)", label: "Rejected" },
  timeout: { bg: "rgba(148, 163, 184, 0.15)", text: "var(--text-secondary)", label: "Timeout" },
};

export default function StatusBadge({ status }: { status: CertificationStatus }) {
  const s = STATUS_STYLES[status] ?? STATUS_STYLES.timeout;
  return (
    <span
      className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium"
      style={{ background: s.bg, color: s.text }}
    >
      {s.label}
    </span>
  );
}
