import Link from "next/link";
import { Plus } from "lucide-react";

import { AppShell } from "@/components/app-shell";
import { PageHeader } from "@/components/page-header";
import { StatusPill } from "@/components/status-pill";
import { requireUser } from "@/lib/auth";
import { createSupabaseAdminClient } from "@/lib/supabase/admin";
import { getQuotaSnapshot } from "@/lib/usage/quota";

export default async function DocumentsPage() {
  const { profile } = await requireUser();
  const admin = createSupabaseAdminClient();
  const [documentsResult, usageResult] = await Promise.all([
    admin
      .from("documents")
      .select("id,title,source_type,source_url,status,character_count,created_at,error_message")
      .eq("user_id", profile.id)
      .order("created_at", { ascending: false }),
    admin
      .from("usage_events")
      .select("kind,quantity")
      .eq("user_id", profile.id)
      .gte("created_at", monthStartIso()),
  ]);

  if (documentsResult.error) throw documentsResult.error;
  if (usageResult.error) throw usageResult.error;

  const quota = getQuotaSnapshot({
    monthlyCharacterQuota: profile.monthly_character_quota,
    events: usageResult.data,
  });

  return (
    <AppShell profile={profile}>
      <PageHeader
        title="文章库"
        description="保存网页、PDF 和 Word 文档，后台会自动生成双语精读、生词表和地道表达表。"
        actions={
          <Link
            href="/documents/import"
            className="focus-ring apple-spring inline-flex h-11 items-center gap-2 rounded-full bg-[var(--accent)] px-4 text-sm font-semibold text-white transition duration-200 hover:bg-[var(--accent-strong)] active:opacity-60"
          >
            <Plus className="h-4 w-4" aria-hidden />
            导入材料
          </Link>
        }
      />

      <section className="mb-6 grid gap-3 sm:grid-cols-3">
        <Metric label="本月已处理" value={quota.used.toLocaleString()} />
        <Metric label="剩余额度" value={quota.remaining.toLocaleString()} />
        <Metric label="文章数" value={documentsResult.data.length.toString()} />
      </section>

      <section className="soft-panel overflow-hidden rounded-lg">
        <div className="grid grid-cols-[1fr_110px_110px] border-b border-[var(--line)] px-5 py-4 text-xs font-semibold uppercase tracking-[0.16em] text-[var(--muted)] max-sm:hidden">
          <span>标题</span>
          <span>来源</span>
          <span>状态</span>
        </div>
        {documentsResult.data.length === 0 ? (
          <div className="grid min-h-72 place-items-center p-8 text-center">
            <div>
              <p className="text-lg font-semibold text-[var(--foreground)]">
                还没有文章
              </p>
              <p className="mt-2 max-w-sm text-sm leading-6 text-[var(--muted)]">
                导入一篇英文网页、PDF 或 Word，系统会自动生成双语精读、生词表和地道表达。
              </p>
              <Link
                href="/documents/import"
                className="focus-ring apple-spring mt-5 inline-flex h-11 items-center gap-2 rounded-full bg-[var(--accent)] px-5 text-sm font-semibold text-white transition duration-200 hover:bg-[var(--accent-strong)] active:opacity-60"
              >
                <Plus className="h-4 w-4" aria-hidden />
                导入第一篇材料
              </Link>
            </div>
          </div>
        ) : (
          documentsResult.data.map((document) => (
            <Link
              key={document.id}
              href={`/documents/${document.id}`}
              className="apple-spring grid gap-2 border-b border-[var(--line)] px-5 py-5 transition duration-200 last:border-b-0 hover:bg-[var(--accent-soft)] active:opacity-60 sm:grid-cols-[1fr_110px_110px] sm:items-center"
            >
              <span>
                <span className="block text-base font-semibold text-[var(--foreground)]">
                  {document.title}
                </span>
                <span className="mt-1 block truncate text-xs text-[var(--muted)]">
                  {document.source_url ?? `${document.character_count} chars`}
                </span>
              </span>
              <span className="text-xs uppercase tracking-wide text-[var(--muted)]">
                {document.source_type}
              </span>
              <StatusPill status={document.status} />
            </Link>
          ))
        )}
      </section>
    </AppShell>
  );
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div className="soft-panel rounded-lg p-5">
      <p className="text-xs font-medium text-[var(--muted)]">{label}</p>
      <p className="mt-2 text-3xl font-semibold text-[var(--foreground)]">
        {value}
      </p>
    </div>
  );
}

function monthStartIso(): string {
  const now = new Date();
  return new Date(now.getFullYear(), now.getMonth(), 1).toISOString();
}
