import { Download } from "lucide-react";

import { AppShell } from "@/components/app-shell";
import { PageHeader } from "@/components/page-header";
import { StatusPill } from "@/components/status-pill";
import { requireUser } from "@/lib/auth";
import { createSupabaseAdminClient } from "@/lib/supabase/admin";

type ExportRow = {
  id: string;
  format: string;
  status: string;
  created_at: string;
  documents: { title: string } | { title: string }[] | null;
};

export default async function ExportsPage() {
  const { profile } = await requireUser();
  const admin = createSupabaseAdminClient();
  const { data, error } = await admin
    .from("exports")
    .select("id,format,status,file_path,created_at,documents(title)")
    .eq("user_id", profile.id)
    .order("created_at", { ascending: false });

  if (error) throw error;
  const rows = data as ExportRow[];

  return (
    <AppShell profile={profile}>
      <PageHeader
        title="导出中心"
        description="导出任务完成后会生成 10 分钟有效的私有下载链接。"
      />
      <section className="soft-panel overflow-hidden rounded-[14px]">
        {rows.length === 0 ? (
          <div className="p-8 text-center text-sm text-[var(--muted)]">
            在文章阅读页创建 Markdown 或 PDF 导出。
          </div>
        ) : (
          rows.map((item) => (
            <div
              key={item.id}
              className="grid gap-3 border-b border-[var(--line)] px-4 py-4 last:border-b-0 sm:grid-cols-[1fr_100px_110px_120px] sm:items-center"
            >
              <div>
                <p className="text-sm font-semibold text-[var(--foreground)]">
                  {getExportDocumentTitle(item.documents)}
                </p>
                <p className="mt-1 text-xs uppercase tracking-wide text-[var(--muted)]">
                  {item.format}
                </p>
              </div>
              <StatusPill status={item.status} />
              <span className="text-xs text-[var(--muted)]">
                {new Date(item.created_at).toLocaleString("zh-CN")}
              </span>
              {item.status === "ready" ? (
                <a
                  href={`/api/exports/${item.id}/download`}
                  className="focus-ring apple-spring inline-flex h-10 items-center justify-center gap-2 rounded-full bg-[var(--surface)] px-4 text-sm font-semibold text-[var(--accent)] transition duration-200 hover:bg-[var(--paper)] active:opacity-60"
                >
                  <Download className="h-4 w-4" aria-hidden />
                  获取链接
                </a>
              ) : (
                <span className="text-sm text-[var(--muted)]">等待中</span>
              )}
            </div>
          ))
        )}
      </section>
    </AppShell>
  );
}

function getExportDocumentTitle(
  documents: ExportRow["documents"],
): string {
  if (Array.isArray(documents)) {
    return documents[0]?.title ?? "Untitled document";
  }
  return documents?.title ?? "Untitled document";
}
