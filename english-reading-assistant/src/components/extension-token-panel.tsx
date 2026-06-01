"use client";

import { useState, useTransition } from "react";
import { Copy, KeyRound, Loader2 } from "lucide-react";

export function ExtensionTokenPanel() {
  const [token, setToken] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [isPending, startTransition] = useTransition();

  return (
    <div className="rounded border border-[var(--line)] bg-white p-4">
      <div className="flex items-center gap-2 text-sm font-semibold">
        <KeyRound className="h-4 w-4 text-[var(--coral)]" aria-hidden />
        浏览器插件 Token
      </div>
      <p className="mt-3 text-sm leading-6 text-[var(--muted)]">
        生成后粘贴到 Chrome/Edge 插件弹窗里。Token 只显示一次。
      </p>
      {token ? (
        <div className="mt-3 rounded border border-[var(--line)] bg-[#fbfbf8] p-3 font-mono text-xs break-all">
          {token}
        </div>
      ) : null}
      {error ? <p className="mt-3 text-sm text-red-700">{error}</p> : null}
      <div className="mt-4 flex gap-2">
        <button
          type="button"
          disabled={isPending}
          className="focus-ring inline-flex h-10 items-center gap-2 rounded bg-stone-950 px-4 text-sm font-semibold text-white transition hover:bg-stone-800 disabled:opacity-60"
          onClick={() => {
            setError(null);
            startTransition(async () => {
              const response = await fetch("/api/extension-token", {
                method: "POST",
              });
              const payload = await response.json();
              if (!response.ok) {
                setError(payload.error?.message ?? "生成失败");
                return;
              }
              setToken(payload.token);
            });
          }}
        >
          {isPending ? <Loader2 className="h-4 w-4 animate-spin" /> : null}
          生成 Token
        </button>
        {token ? (
          <button
            type="button"
            className="focus-ring inline-flex h-10 items-center gap-2 rounded border border-[var(--line)] bg-white px-4 text-sm font-semibold transition hover:bg-[var(--surface-strong)]"
            onClick={() => navigator.clipboard.writeText(token)}
          >
            <Copy className="h-4 w-4" aria-hidden />
            复制
          </button>
        ) : null}
      </div>
    </div>
  );
}
