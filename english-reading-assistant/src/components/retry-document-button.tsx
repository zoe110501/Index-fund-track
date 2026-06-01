"use client";

import { useState, useTransition } from "react";
import { useRouter } from "next/navigation";
import { Loader2, RefreshCw } from "lucide-react";

export function RetryDocumentButton({ documentId }: { documentId: string }) {
  const router = useRouter();
  const [error, setError] = useState<string | null>(null);
  const [isPending, startTransition] = useTransition();

  return (
    <div className="flex flex-col items-start gap-2">
      <button
        type="button"
        disabled={isPending}
        onClick={() => {
          setError(null);
          startTransition(async () => {
            const response = await fetch(`/api/documents/${documentId}/retry`, {
              method: "POST",
            });
            const payload = await response.json();
            if (!response.ok) {
              setError(payload.error?.message ?? "重新处理失败");
              router.refresh();
              return;
            }
            router.refresh();
          });
        }}
        className="focus-ring apple-spring inline-flex h-10 items-center gap-2 rounded-full bg-[var(--accent)] px-4 text-sm font-semibold text-white transition duration-200 hover:bg-[var(--accent-strong)] active:opacity-60 disabled:opacity-60"
      >
        {isPending ? (
          <Loader2 className="h-4 w-4 animate-spin" aria-hidden />
        ) : (
          <RefreshCw className="h-4 w-4" aria-hidden />
        )}
        重新处理
      </button>
      {error ? <p className="max-w-sm text-xs text-red-700">{error}</p> : null}
    </div>
  );
}
