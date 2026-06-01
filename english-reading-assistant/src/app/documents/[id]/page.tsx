import { notFound } from "next/navigation";

import { AppShell } from "@/components/app-shell";
import { PageHeader } from "@/components/page-header";
import { ReadingWorkbench } from "@/components/reading-workbench";
import { RetryDocumentButton } from "@/components/retry-document-button";
import { StatusPill } from "@/components/status-pill";
import { requireUser } from "@/lib/auth";
import { getAiProviderLabel, hasAiEnv } from "@/lib/env";
import { createSupabaseAdminClient } from "@/lib/supabase/admin";

export default async function DocumentPage({
  params,
}: {
  params: Promise<{ id: string }>;
}) {
  const { id } = await params;
  const { profile } = await requireUser();
  const admin = createSupabaseAdminClient();
  const [documentResult, segmentResult, vocabResult, expressionResult] =
    await Promise.all([
      admin
        .from("documents")
        .select("*")
        .eq("id", id)
        .eq("user_id", profile.id)
        .single(),
      admin
        .from("segments")
        .select("*")
        .eq("document_id", id)
        .eq("user_id", profile.id)
        .order("order_index", { ascending: true }),
      admin
        .from("vocabulary_items")
        .select("*")
        .eq("document_id", id)
        .eq("user_id", profile.id)
        .order("created_at", { ascending: true }),
      admin
        .from("expression_items")
        .select("*")
        .eq("document_id", id)
        .eq("user_id", profile.id)
        .order("created_at", { ascending: true }),
    ]);

  if (documentResult.error) notFound();
  if (segmentResult.error) throw segmentResult.error;
  if (vocabResult.error) throw vocabResult.error;
  if (expressionResult.error) throw expressionResult.error;

  const document = documentResult.data;
  const canRetry =
    document.status === "failed" || document.status === "queued";
  const aiConfigured = hasAiEnv();
  const aiProviderLabel = getAiProviderLabel();
  const hasReadableSegments = segmentResult.data.length > 0;

  return (
    <AppShell profile={profile}>
      {!hasReadableSegments ? (
        <PageHeader
          title={document.title}
          description={
            document.source_url ?? `${document.source_type.toUpperCase()} 文档`
          }
          actions={
            <div className="flex flex-wrap items-start gap-2">
              <StatusPill status={document.status} />
              {canRetry ? <RetryDocumentButton documentId={document.id} /> : null}
            </div>
          }
        />
      ) : null}

      {document.status !== "ready" ? (
        <div className="soft-panel mb-5 rounded-lg p-6 text-sm leading-6 text-[var(--muted)]">
          <p className="font-semibold text-[var(--foreground)]">
            {document.status === "queued"
              ? "这篇文档还没有开始处理。"
              : document.status === "processing"
                ? "这篇文档正在生成精读内容。"
                : "这篇文档处理失败。"}
          </p>
          <p className="mt-2">
            {document.status === "failed"
              ? (document.error_message ?? "处理失败，请重新导入或重新处理。")
              : "稍后刷新页面即可看到双语版本、生词表和地道表达表。"}
          </p>
          {!aiConfigured ? (
            <div className="mt-4 rounded-lg border border-amber-200 bg-amber-50 p-4 text-amber-950">
              <p className="font-semibold">还缺少 {aiProviderLabel} API Key。</p>
              <p className="mt-1">
                请在项目根目录的 `.env.local` 填入对应 Key，重启 `auto-start.bat`，
                再点击“重新处理”。
              </p>
            </div>
          ) : null}
        </div>
      ) : null}

      {document.status === "ready" || hasReadableSegments ? (
        <ReadingWorkbench
          document={document}
          segments={segmentResult.data}
          vocabulary={vocabResult.data}
          expressions={expressionResult.data}
        />
      ) : null}
    </AppShell>
  );
}
