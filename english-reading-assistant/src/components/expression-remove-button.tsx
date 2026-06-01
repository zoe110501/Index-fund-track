"use client";

import { useRouter } from "next/navigation";
import { useState, useTransition } from "react";
import { BookmarkX } from "lucide-react";

export function ExpressionRemoveButton({ id }: { id: string }) {
  const router = useRouter();
  const [error, setError] = useState<string | null>(null);
  const [isPending, startTransition] = useTransition();

  function removeExpression() {
    const confirmed = window.confirm(
      "确定要从表达本取消记录这条表达吗？这会从你的表达本删除。",
    );
    if (!confirmed) return;

    setError(null);
    startTransition(async () => {
      const response = await fetch(`/api/expressions/${id}`, {
        method: "DELETE",
      });

      if (!response.ok) {
        const payload = await response.json().catch(() => null);
        setError(payload?.error?.message ?? "取消记录失败");
        return;
      }

      router.refresh();
    });
  }

  return (
    <div className="flex flex-col items-end gap-1">
      <button
        type="button"
        disabled={isPending}
        onClick={removeExpression}
        className="focus-ring apple-spring inline-flex h-8 items-center gap-1.5 rounded-full bg-[var(--surface)] px-3 text-xs font-semibold text-[var(--muted)] transition duration-200 hover:bg-[rgba(255,59,48,0.12)] hover:text-[var(--red)] active:opacity-60 disabled:opacity-50"
        title="取消记录"
      >
        <BookmarkX className="h-3.5 w-3.5" aria-hidden />
        取消记录
      </button>
      {error ? (
        <p className="max-w-40 text-right text-xs leading-5 text-[var(--red)]">
          {error}
        </p>
      ) : null}
    </div>
  );
}
