"use client";

import { useState, useTransition } from "react";
import { useRouter } from "next/navigation";
import { Download, Loader2 } from "lucide-react";

export function ExportActions({ documentId }: { documentId: string }) {
  const router = useRouter();
  const [error, setError] = useState<string | null>(null);
  const [isPending, startTransition] = useTransition();

  function requestExport(format: "markdown" | "pdf") {
    setError(null);
    startTransition(async () => {
      const response = await fetch(`/api/documents/${documentId}/export`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ format }),
      });
      const payload = await response.json();
      if (!response.ok) {
        setError(payload.error?.message ?? "导出失败");
        return;
      }
      router.push("/exports");
    });
  }

  return (
    <div>
      <div className="flex flex-wrap gap-2">
        <button
          type="button"
          disabled={isPending}
          onClick={() => requestExport("markdown")}
          className="focus-ring apple-spring inline-flex h-10 items-center gap-2 rounded-[10px] bg-[var(--paper)] px-4 text-sm font-semibold text-[var(--accent)] transition duration-200 hover:bg-[var(--surface-strong)] active:opacity-60 disabled:opacity-60"
        >
          {isPending ? (
            <Loader2 className="h-4 w-4 animate-spin" />
          ) : (
            <Download className="h-4 w-4" />
          )}
          Markdown
        </button>
        <button
          type="button"
          disabled={isPending}
          onClick={() => requestExport("pdf")}
          className="focus-ring apple-spring inline-flex h-10 items-center gap-2 rounded-[10px] bg-[var(--accent)] px-4 text-sm font-semibold text-white transition duration-200 hover:bg-[var(--accent-strong)] active:opacity-60 disabled:opacity-60"
        >
          PDF
        </button>
      </div>
      {error ? <p className="mt-2 text-sm text-red-700">{error}</p> : null}
    </div>
  );
}
