import { clsx } from "clsx";

const labels: Record<string, string> = {
  queued: "排队中",
  processing: "处理中",
  ready: "已完成",
  failed: "失败",
  running: "生成中",
  new: "新词",
  known: "认识",
  learning: "学习中",
  mastered: "已掌握",
};

export function StatusPill({ status }: { status: string }) {
  return (
    <span
      className={clsx(
        "inline-flex h-7 items-center rounded-full px-2.5 text-xs font-medium",
        status === "ready" || status === "mastered"
          ? "bg-[rgba(52,199,89,0.16)] text-[#248a3d]"
          : status === "failed"
            ? "bg-[rgba(255,59,48,0.14)] text-[var(--red)]"
            : status === "processing" || status === "running"
              ? "bg-[var(--accent-soft)] text-[var(--accent)]"
              : "bg-[var(--surface-strong)] text-[var(--muted)]",
      )}
    >
      {labels[status] ?? status}
    </span>
  );
}
