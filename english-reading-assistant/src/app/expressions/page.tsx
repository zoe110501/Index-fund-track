import Link from "next/link";

import { AppShell } from "@/components/app-shell";
import { ExpressionRemoveButton } from "@/components/expression-remove-button";
import { PageHeader } from "@/components/page-header";
import { requireUser } from "@/lib/auth";
import { createSupabaseAdminClient } from "@/lib/supabase/admin";

export default async function ExpressionsPage() {
  const { profile } = await requireUser();
  const admin = createSupabaseAdminClient();
  const { data, error } = await admin
    .from("expression_items")
    .select("id,expression,chinese_meaning,usage_note,example_sentence,created_at,documents(id,title)")
    .eq("user_id", profile.id)
    .order("created_at", { ascending: false })
    .limit(200);

  if (error) throw error;

  return (
    <AppShell profile={profile}>
      <PageHeader
        title="表达本"
        description="沉淀搭配、句式、习惯表达和你划选保存的短语。"
      />
      <section className="grid gap-3 lg:grid-cols-2">
        {data.length === 0 ? (
          <div className="soft-panel col-span-full rounded-lg p-8 text-center text-sm text-[var(--muted)]">
            处理完成的文章和你划选的短语会在这里沉淀。
          </div>
        ) : (
          data.map((item) => {
            const sourceDocument = Array.isArray(item.documents)
              ? item.documents[0]
              : item.documents;

            return (
              <article key={item.id} className="soft-panel rounded-[14px] p-4">
                <div className="flex items-start justify-between gap-3">
                  <h2 className="min-w-0 text-[17px] font-semibold text-[var(--foreground)]">
                    {item.expression}
                  </h2>
                  <ExpressionRemoveButton id={item.id} />
                </div>
                <p className="mt-2 text-sm leading-6 text-[var(--foreground)]">
                  {item.chinese_meaning}
                </p>
                {item.usage_note ? (
                  <p className="mt-3 rounded-[10px] bg-[var(--paper)] p-3 text-xs leading-5 text-[var(--muted)]">
                    {item.usage_note}
                  </p>
                ) : null}
                {item.example_sentence ? (
                  <p className="mt-2 text-xs leading-5 text-[var(--muted)]">
                    {item.example_sentence}
                  </p>
                ) : null}
                {sourceDocument ? (
                  <Link
                    href={`/documents/${sourceDocument.id}`}
                    className="mt-4 inline-flex text-xs font-semibold text-[var(--accent)]"
                  >
                    来自：{sourceDocument.title}
                  </Link>
                ) : null}
              </article>
            );
          })
        )}
      </section>
    </AppShell>
  );
}
