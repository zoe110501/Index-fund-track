import { AppShell } from "@/components/app-shell";
import { InviteForm } from "@/components/invite-form";
import { PageHeader } from "@/components/page-header";
import { StatusPill } from "@/components/status-pill";
import { requireAdmin } from "@/lib/auth";
import { createSupabaseAdminClient } from "@/lib/supabase/admin";

export default async function AdminPage() {
  const { profile } = await requireAdmin();
  const admin = createSupabaseAdminClient();
  const [profilesResult, invitesResult] = await Promise.all([
    admin
      .from("profiles")
      .select("id,email,role,status,monthly_character_quota,created_at")
      .order("created_at", { ascending: false })
      .limit(100),
    admin
      .from("invites")
      .select("id,email,status,expires_at,created_at")
      .order("created_at", { ascending: false })
      .limit(100),
  ]);

  if (profilesResult.error) throw profilesResult.error;
  if (invitesResult.error) throw invitesResult.error;

  return (
    <AppShell profile={profile}>
      <PageHeader
        title="管理员"
        description="邀请用户、查看账号状态和默认额度。"
      />
      <InviteForm />

      <section className="mt-5 grid gap-5 xl:grid-cols-2">
        <div className="rounded border border-[var(--line)] bg-white">
          <div className="border-b border-[var(--line)] px-4 py-3 text-sm font-semibold">
            用户
          </div>
          {profilesResult.data.map((user) => (
            <div
              key={user.id}
              className="flex items-center justify-between gap-3 border-b border-[var(--line)] px-4 py-3 last:border-0"
            >
              <div>
                <p className="text-sm font-semibold">{user.email}</p>
                <p className="text-xs text-[var(--muted)]">
                  {user.role} · {user.monthly_character_quota.toLocaleString()} chars
                </p>
              </div>
              <StatusPill status={user.status} />
            </div>
          ))}
        </div>

        <div className="rounded border border-[var(--line)] bg-white">
          <div className="border-b border-[var(--line)] px-4 py-3 text-sm font-semibold">
            邀请
          </div>
          {invitesResult.data.map((invite) => (
            <div
              key={invite.id}
              className="flex items-center justify-between gap-3 border-b border-[var(--line)] px-4 py-3 last:border-0"
            >
              <div>
                <p className="text-sm font-semibold">{invite.email}</p>
                <p className="text-xs text-[var(--muted)]">
                  过期：{new Date(invite.expires_at).toLocaleDateString("zh-CN")}
                </p>
              </div>
              <StatusPill status={invite.status} />
            </div>
          ))}
        </div>
      </section>
    </AppShell>
  );
}
