"use client";

import { useState, useTransition } from "react";
import { useRouter } from "next/navigation";
import { FileUp, Loader2 } from "lucide-react";

export function UploadForm() {
  const router = useRouter();
  const [error, setError] = useState<string | null>(null);
  const [isPending, startTransition] = useTransition();

  return (
    <form
      className="soft-panel rounded-[14px] p-5"
      onSubmit={(event) => {
        event.preventDefault();
        setError(null);
        const formData = new FormData(event.currentTarget);
        startTransition(async () => {
          const response = await fetch("/api/documents/upload", {
            method: "POST",
            body: formData,
          });
          const payload = await response.json();
          if (!response.ok) {
            setError(payload.error?.message ?? "上传失败");
            return;
          }
          router.push(`/documents/${payload.id}`);
        });
      }}
    >
      <div className="flex items-center gap-3 text-base font-semibold">
        <span className="grid h-10 w-10 place-items-center rounded-[10px] bg-[var(--accent-soft)] text-[var(--accent)]">
          <FileUp className="h-5 w-5" aria-hidden />
        </span>
        上传 PDF / Word
      </div>
      <label className="mt-4 block text-xs font-medium text-[var(--muted)]">
        标题
        <input
          name="title"
          className="focus-ring mt-2 h-12 w-full rounded-[10px] border border-[var(--line)] bg-[var(--surface)] px-4 text-sm text-[var(--foreground)]"
          placeholder="留空则使用文件名"
        />
      </label>
      <label className="mt-3 block text-xs font-medium text-[var(--muted)]">
        文件
        <input
          required
          name="file"
          type="file"
          accept=".pdf,.docx,application/pdf,application/vnd.openxmlformats-officedocument.wordprocessingml.document"
          className="focus-ring mt-2 w-full rounded-[10px] border border-dashed border-[var(--line)] bg-[var(--paper)] px-4 py-4 text-sm"
        />
      </label>
      {error ? <p className="mt-3 text-sm text-red-700">{error}</p> : null}
      <button
        type="submit"
        disabled={isPending}
        className="focus-ring apple-spring mt-5 inline-flex h-11 items-center gap-2 rounded-full bg-[var(--accent)] px-5 text-sm font-semibold text-white transition duration-200 hover:bg-[var(--accent-strong)] active:opacity-60 disabled:opacity-60"
      >
        {isPending ? <Loader2 className="h-4 w-4 animate-spin" /> : null}
        开始处理
      </button>
    </form>
  );
}
