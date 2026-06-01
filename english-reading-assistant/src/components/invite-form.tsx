"use client";

import { useState, useTransition } from "react";
import { Send } from "lucide-react";

export function InviteForm() {
  const [message, setMessage] = useState<string | null>(null);
  const [isPending, startTransition] = useTransition();

  return (
    <form
      className="flex flex-col gap-3 rounded border border-[var(--line)] bg-white p-4 sm:flex-row"
      onSubmit={(event) => {
        event.preventDefault();
        setMessage(null);
        const formData = new FormData(event.currentTarget);
        const email = formData.get("email")?.toString();
        startTransition(async () => {
          const response = await fetch("/api/admin/invites", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ email }),
          });
          const payload = await response.json();
          setMessage(
            response.ok
              ? `已邀请 ${payload.invite.email}`
              : (payload.error?.message ?? "邀请失败"),
          );
        });
      }}
    >
      <input
        required
        name="email"
        type="email"
        className="focus-ring h-10 flex-1 rounded border border-[var(--line)] bg-white px-3 text-sm"
        placeholder="user@example.com"
      />
      <button
        type="submit"
        disabled={isPending}
        className="focus-ring inline-flex h-10 items-center justify-center gap-2 rounded bg-stone-950 px-4 text-sm font-semibold text-white transition hover:bg-stone-800 disabled:opacity-60"
      >
        <Send className="h-4 w-4" aria-hidden />
        发送邀请
      </button>
      {message ? (
        <p className="self-center text-sm text-[var(--muted)]">{message}</p>
      ) : null}
    </form>
  );
}
