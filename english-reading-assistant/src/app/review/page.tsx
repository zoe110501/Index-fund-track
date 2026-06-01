import { AppShell } from "@/components/app-shell";
import { PageHeader } from "@/components/page-header";
import { StatusPill } from "@/components/status-pill";
import { requireUser } from "@/lib/auth";
import { createSupabaseAdminClient } from "@/lib/supabase/admin";

export default async function ReviewPage() {
  const { profile } = await requireUser();
  const admin = createSupabaseAdminClient();
  const { data, error } = await admin
    .from("vocabulary_items")
    .select("id,term,chinese_definition,example_sentence,difficulty,status,created_at")
    .eq("user_id", profile.id)
    .in("status", ["new", "learning"])
    .order("created_at", { ascending: true })
    .limit(24);

  if (error) throw error;

  return (
    <AppShell profile={profile}>
      <PageHeader
        title="复习"
        description="先把新词和学习中的词集中起来，后续可以接入间隔重复和闪卡。"
      />
      <section className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
        {data.length === 0 ? (
          <div className="soft-panel col-span-full rounded-[14px] p-8 text-center text-sm text-[var(--muted)]">
            暂时没有需要复习的词。继续精读一篇文章吧。
          </div>
        ) : (
          data.map((item) => (
            <article key={item.id} className="soft-panel rounded-[14px] p-4">
              <div className="flex items-start justify-between gap-3">
                <div>
                  <h2 className="text-[17px] font-semibold text-[var(--foreground)]">
                    {item.term}
                  </h2>
                  {item.difficulty ? (
                    <p className="mt-1 text-xs text-[var(--muted)]">
                      难度 {item.difficulty}
                    </p>
                  ) : null}
                </div>
                <StatusPill status={item.status} />
              </div>
              <p className="mt-3 text-sm leading-6 text-[var(--foreground)]">
                {item.chinese_definition}
              </p>
              {item.example_sentence ? (
                <p className="mt-3 rounded-[10px] bg-[var(--paper)] p-3 text-xs leading-5 text-[var(--muted)]">
                  {item.example_sentence}
                </p>
              ) : null}
            </article>
          ))
        )}
      </section>
    </AppShell>
  );
}
