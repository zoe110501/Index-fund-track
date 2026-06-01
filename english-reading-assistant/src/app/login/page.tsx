import { ArrowRight, BookOpen, Mail, Sparkles } from "lucide-react";

import { requestMagicLink } from "./actions";
import { getAdminEmails, isLocalAdminLoginEnabled } from "@/lib/env";
import { getSetupStatus } from "@/lib/setup-status";

export default async function LoginPage({
  searchParams,
}: {
  searchParams: Promise<{
    error?: string;
    sent?: string;
    email?: string;
    reason?: string;
  }>;
}) {
  const params = await searchParams;
  const setupStatus = await getSetupStatus();
  const missingEnv = setupStatus.missingEnv;
  const isConfigMissing = missingEnv.length > 0;
  const isDatabaseMissing = !isConfigMissing && !setupStatus.databaseReady;
  const localLoginEnabled = isLocalAdminLoginEnabled();
  const [adminEmail] = getAdminEmails();

  return (
    <main className="login-stage min-h-screen bg-[var(--background)] px-5 py-6 sm:px-8">
      <div className="mx-auto grid min-h-[calc(100vh-48px)] w-full max-w-6xl overflow-hidden rounded-lg border border-[var(--line)] bg-[var(--surface)] shadow-[0_24px_80px_rgba(20,32,31,0.12)] lg:grid-cols-[1.08fr_0.92fr]">
        <section className="relative hidden min-h-[720px] flex-col justify-between overflow-hidden bg-[var(--ink)] p-10 text-white lg:flex">
          <div className="absolute inset-0 opacity-55">
            <div className="absolute inset-0 bg-[linear-gradient(135deg,rgba(20,125,115,0.26),transparent_34%),linear-gradient(315deg,rgba(201,135,42,0.18),transparent_42%)]" />
            <div className="absolute inset-0 bg-[linear-gradient(rgba(255,255,255,0.055)_1px,transparent_1px),linear-gradient(90deg,rgba(255,255,255,0.05)_1px,transparent_1px)] bg-[size:34px_34px]" />
          </div>

          <div className="relative z-10 flex items-center gap-3">
            <span className="grid h-12 w-12 place-items-center rounded-lg bg-[var(--accent)] text-xl font-black text-white shadow-[0_12px_30px_rgba(20,184,166,0.28)]">
              英
            </span>
            <div>
              <p className="text-lg font-semibold">英读助手</p>
              <p className="text-xs uppercase tracking-[0.32em] text-white/45">
                Deep Reading OS
              </p>
            </div>
          </div>

          <div className="relative z-10 max-w-xl">
            <div className="mb-8 inline-flex items-center gap-2 rounded-full border border-white/12 bg-white/8 px-3 py-1.5 text-xs font-medium text-white/80">
              <Sparkles className="h-3.5 w-3.5 text-[var(--gold)]" />
              网页、PDF、Word，一处沉淀
            </div>
            <h1 className="text-5xl font-semibold leading-[1.05]">
              把真实英文材料变成你的私人精读课。
            </h1>
            <p className="mt-6 max-w-lg text-base leading-8 text-white/68">
              逐段双语、个人生词、地道表达、可导出的阅读笔记。少一点工具感，多一点坐下来读懂的秩序感。
            </p>
          </div>

          <div className="relative z-10 grid grid-cols-3 gap-3">
            {[
              ["01", "导入材料"],
              ["02", "生成精读"],
              ["03", "沉淀词库"],
            ].map(([index, label]) => (
              <div
                key={index}
              className="rounded-lg border border-white/10 bg-white/8 p-4 backdrop-blur"
              >
                <p className="font-mono text-xs text-[var(--gold)]">{index}</p>
                <p className="mt-3 text-sm font-medium text-white/86">
                  {label}
                </p>
              </div>
            ))}
          </div>
        </section>

        <section className="flex min-h-[680px] items-center justify-center px-5 py-10 sm:px-10">
          <div className="w-full max-w-md">
            <div className="mb-10 flex items-center gap-3 lg:hidden">
              <span className="grid h-12 w-12 place-items-center rounded-lg bg-[var(--accent)] text-xl font-black text-white">
                英
              </span>
              <div>
                <p className="text-lg font-semibold">英读助手</p>
                <p className="text-sm text-[var(--muted)]">Deep Reading OS</p>
              </div>
            </div>

            <div className="mb-8">
              <p className="mb-3 inline-flex items-center gap-2 text-xs font-semibold uppercase tracking-[0.22em] text-[var(--accent-strong)]">
                <BookOpen className="h-4 w-4" />
                Reader Login
              </p>
              <h2 className="text-3xl font-semibold text-[var(--foreground)]">
                继续你的英语精读
              </h2>
              <p className="mt-3 text-sm leading-6 text-[var(--muted)]">
                本机使用建议直接进入，刷新页面会保持登录状态；邮件链接只作为备用入口。
              </p>
            </div>

            {isConfigMissing ? (
              <SetupWarning
                title="还需要配置 Supabase 才能登录。"
                body="请在项目根目录创建 `.env.local`，补齐："
                detail={missingEnv.join("\n")}
              />
            ) : null}

            {isDatabaseMissing ? (
              <SetupWarning
                title="还需要初始化 Supabase 数据库。"
                body="请在 Supabase SQL Editor 运行项目里的迁移文件："
                detail={`supabase/migrations/0001_initial.sql${
                  setupStatus.databaseError
                    ? `\n\n当前错误：${setupStatus.databaseError}`
                    : ""
                }`}
              />
            ) : null}

            {params.sent ? (
              <div className="rounded-lg border border-emerald-200 bg-emerald-50 p-4 text-sm leading-6 text-emerald-900">
                登录链接已发送到 {params.email}。请在同一台电脑的浏览器里打开邮件链接。
              </div>
            ) : (
              <div className="space-y-4">
                {localLoginEnabled ? (
                  <div>
                    <a
                      aria-disabled={
                        isConfigMissing || isDatabaseMissing || !adminEmail
                      }
                      href={
                        isConfigMissing || isDatabaseMissing || !adminEmail
                          ? "#"
                          : "/api/local-admin-login"
                      }
                      className="focus-ring group inline-flex h-14 w-full items-center justify-between rounded-lg bg-[var(--ink)] px-5 text-base font-semibold text-white shadow-[0_18px_40px_rgba(20,32,31,0.18)] transition hover:translate-y-[-1px] hover:bg-[#0b2523] aria-disabled:pointer-events-none aria-disabled:opacity-50"
                    >
                      <span>本机一键进入</span>
                      <ArrowRight className="h-5 w-5 transition group-hover:translate-x-1" />
                    </a>
                    <p className="mt-2 text-xs leading-5 text-[var(--muted)]">
                      使用管理员邮箱 {adminEmail || "未设置"} 创建本地会话，不消耗 Supabase 邮件额度。
                    </p>
                  </div>
                ) : null}

                <form
                  action={requestMagicLink}
                  className="rounded-lg border border-[var(--line)] bg-[var(--paper)] p-4"
                >
                  <label className="block text-xs font-semibold uppercase tracking-[0.16em] text-[var(--muted)]">
                    邮箱登录备用
                    <input
                      required
                      name="email"
                      type="email"
                      defaultValue={adminEmail}
                      className="focus-ring mt-3 h-12 w-full rounded-lg border border-[var(--line)] bg-white px-4 text-sm text-stone-950 shadow-inner shadow-stone-100"
                      placeholder="you@example.com"
                    />
                  </label>
                  {params.error ? (
                    <p className="mt-3 text-sm leading-6 text-red-700">
                      {toLoginErrorMessage(params.error)}
                      {params.reason ? `（${params.reason}）` : ""}
                    </p>
                  ) : null}
                  <button
                    type="submit"
                    disabled={isConfigMissing || isDatabaseMissing}
                    className="focus-ring mt-4 inline-flex h-11 w-full items-center justify-center gap-2 rounded-lg border border-[var(--line)] bg-white px-4 text-sm font-semibold text-[var(--foreground)] transition hover:bg-[var(--surface-strong)] disabled:cursor-not-allowed disabled:opacity-50"
                  >
                    <Mail className="h-4 w-4" aria-hidden />
                    发送邮件登录链接
                  </button>
                </form>
              </div>
            )}
          </div>
        </section>
      </div>
    </main>
  );
}

function SetupWarning({
  title,
  body,
  detail,
}: {
  title: string;
  body: string;
  detail: string;
}) {
  return (
    <div className="mb-4 rounded-lg border border-amber-200 bg-amber-50 p-4 text-sm leading-6 text-amber-950">
      <p className="font-semibold">{title}</p>
      <p className="mt-1">{body}</p>
      <pre className="mt-3 max-h-48 overflow-auto rounded-lg bg-white p-3 text-xs leading-5 text-stone-800">
        {detail}
      </pre>
    </div>
  );
}

function toLoginErrorMessage(error: string): string {
  const messages: Record<string, string> = {
    "missing-email": "请输入邮箱。",
    "not-invited": "这个邮箱还没有被邀请。",
    "magic-link-failed": "登录链接发送失败，请稍后再试。",
    "rate-limited":
      "Supabase 邮件额度暂时限流。请优先使用“本机一键进入”。",
    "config-missing": "还没有配置 Supabase，暂时不能登录。",
    "database-missing": "还没有初始化 Supabase 数据库，请先运行迁移 SQL。",
    "callback-failed": "登录链接验证失败，请重新生成一个新的登录入口。",
    "admin-email-missing": "请先在 `.env.local` 设置 ADMIN_EMAILS。",
    "local-login-disabled": "当前环境未启用本机一键登录。",
    "local-login-failed": "本机一键登录失败，请检查 Supabase service key。",
    paused: "这个账号已暂停使用。",
  };
  return messages[error] ?? "登录失败，请稍后再试。";
}
