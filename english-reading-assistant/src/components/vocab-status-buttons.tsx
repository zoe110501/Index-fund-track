"use client";

import { useState, useTransition } from "react";
import { Check, GraduationCap } from "lucide-react";

type VocabularyStatus = "new" | "known" | "learning" | "mastered";

export function VocabStatusButtons({
  id,
  initialStatus,
}: {
  id: string;
  initialStatus: VocabularyStatus;
}) {
  const [status, setStatus] = useState<VocabularyStatus>(initialStatus);
  const [isPending, startTransition] = useTransition();

  function updateStatus(nextStatus: VocabularyStatus) {
    startTransition(async () => {
      const response = await fetch(`/api/vocabulary/${id}/status`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ status: nextStatus }),
      });
      if (response.ok) {
        setStatus(nextStatus);
      }
    });
  }

  return (
    <div className="flex gap-1">
      <button
        type="button"
        disabled={isPending}
        onClick={() => updateStatus("known")}
        className="focus-ring apple-spring grid h-8 w-8 place-items-center rounded-full bg-[var(--surface)] text-[var(--accent)] transition duration-200 hover:bg-[var(--paper)] active:opacity-60 disabled:opacity-50"
        title="标记认识"
      >
        <Check className="h-4 w-4" aria-hidden />
      </button>
      <button
        type="button"
        disabled={isPending}
        onClick={() => updateStatus("learning")}
        className="focus-ring apple-spring grid h-8 w-8 place-items-center rounded-full bg-[var(--surface)] text-[var(--accent)] transition duration-200 hover:bg-[var(--paper)] active:opacity-60 disabled:opacity-50"
        title={status === "learning" ? "学习中" : "加入学习"}
      >
        <GraduationCap className="h-4 w-4" aria-hidden />
      </button>
    </div>
  );
}
