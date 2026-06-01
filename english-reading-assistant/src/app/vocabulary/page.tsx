import { AppShell } from "@/components/app-shell";
import { PageHeader } from "@/components/page-header";
import { StatusPill } from "@/components/status-pill";
import { VocabStatusButtons } from "@/components/vocab-status-buttons";
import { requireUser } from "@/lib/auth";
import { createSupabaseAdminClient } from "@/lib/supabase/admin";

export default async function VocabularyPage() {
  const { profile } = await requireUser();
  const admin = createSupabaseAdminClient();
  const { data, error } = await admin
    .from("vocabulary_items")
    .select("id,term,phonetic,part_of_speech,chinese_definition,example_sentence,difficulty,status,created_at,documents(title)")
    .eq("user_id", profile.id)
    .order("created_at", { ascending: false })
    .limit(200);

  if (error) throw error;

  return (
    <AppShell profile={profile}>
      <PageHeader
        title="生词本"
        description="系统按你的水平初筛生词，你可以把词标记为认识或加入学习。"
      />
      <section className="soft-panel overflow-hidden rounded-[14px]">
        {data.length === 0 ? (
          <div className="p-8 text-center text-sm text-[var(--muted)]">
            处理完成的文章会在这里沉淀生词。
          </div>
        ) : (
          data.map((item) => (
            <div
              key={item.id}
              className="grid gap-3 border-b border-[var(--line)] px-4 py-4 last:border-b-0 lg:grid-cols-[1fr_160px_90px]"
            >
              <div>
                <div className="flex flex-wrap items-center gap-2">
                  <h2 className="text-[17px] font-semibold text-[var(--foreground)]">
                    {item.term}
                  </h2>
                  <span className="text-xs text-[var(--muted)]">
                    {[item.part_of_speech, item.phonetic, item.difficulty]
                      .filter(Boolean)
                      .join(" · ")}
                  </span>
                </div>
                <p className="mt-1 text-sm leading-6 text-[var(--foreground)]">
                  {item.chinese_definition}
                </p>
                {item.example_sentence ? (
                  <p className="mt-1 text-xs leading-5 text-[var(--muted)]">
                    {item.example_sentence}
                  </p>
                ) : null}
              </div>
              <StatusPill status={item.status} />
              <VocabStatusButtons id={item.id} initialStatus={item.status} />
            </div>
          ))
        )}
      </section>
    </AppShell>
  );
}
