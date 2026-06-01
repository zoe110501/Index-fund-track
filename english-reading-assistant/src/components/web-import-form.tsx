"use client";

import { useState, useTransition } from "react";
import { useRouter } from "next/navigation";
import { Globe2, Loader2 } from "lucide-react";

export function WebImportForm() {
  const router = useRouter();
  const [error, setError] = useState<string | null>(null);
  const [isPending, startTransition] = useTransition();

  return (
    <form
      className="soft-panel rounded-[14px] p-5"
      onSubmit={(event) => {
        event.preventDefault();
        setError(null);
        const form = event.currentTarget;
        const formData = new FormData(form);
        startTransition(async () => {
          const response = await fetch("/api/documents/from-url", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              title: formData.get("title"),
              url: formData.get("url"),
              text: formData.get("text"),
            }),
          });
          const payload = await response.json();
          if (!response.ok) {
            setError(payload.error?.message ?? "导入失败");
            return;
          }
          router.push(`/documents/${payload.id}`);
        });
      }}
    >
      <div className="flex items-center gap-3 text-base font-semibold">
        <span className="grid h-10 w-10 place-items-center rounded-[10px] bg-[var(--accent-soft)] text-[var(--accent)]">
          <Globe2 className="h-5 w-5" aria-hidden />
        </span>
        粘贴网页正文
      </div>
      <label className="mt-4 block text-xs font-medium text-[var(--muted)]">
        标题
        <input
          required
          name="title"
          className="focus-ring mt-2 h-12 w-full rounded-[10px] border border-[var(--line)] bg-[var(--surface)] px-4 text-sm text-[var(--foreground)]"
          placeholder="文章标题"
        />
      </label>
      <label className="mt-3 block text-xs font-medium text-[var(--muted)]">
        URL
        <input
          name="url"
          type="url"
          className="focus-ring mt-2 h-12 w-full rounded-[10px] border border-[var(--line)] bg-[var(--surface)] px-4 text-sm text-[var(--foreground)]"
          placeholder="https://example.com/article"
        />
      </label>
      <label className="mt-3 block text-xs font-medium text-[var(--muted)]">
        正文
        <textarea
          required
          name="text"
          rows={9}
          className="focus-ring mt-2 w-full rounded-[10px] border border-[var(--line)] bg-[var(--surface)] px-4 py-3 text-sm leading-6 text-[var(--foreground)]"
          placeholder="把英文正文粘贴到这里"
        />
      </label>
      {error ? <p className="mt-3 text-sm text-red-700">{error}</p> : null}
      <button
        type="submit"
        disabled={isPending}
        className="focus-ring apple-spring mt-5 inline-flex h-11 items-center gap-2 rounded-full bg-[var(--accent)] px-5 text-sm font-semibold text-white transition duration-200 hover:bg-[var(--accent-strong)] active:opacity-60 disabled:opacity-60"
      >
        {isPending ? <Loader2 className="h-4 w-4 animate-spin" /> : null}
        导入网页
      </button>
    </form>
  );
}
