"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import {
  Bookmark,
  Download,
  KeyRound,
  Library,
  Quote,
  RefreshCcw,
  ShieldCheck,
  Upload,
} from "lucide-react";

import type { AppProfile } from "@/lib/auth";

const navItems = [
  { href: "/documents", label: "我的文章", icon: Library, group: "学习" },
  { href: "/vocabulary", label: "生词本", icon: Bookmark, group: "学习" },
  { href: "/expressions", label: "表达本", icon: Quote, group: "学习" },
  { href: "/review", label: "复习", icon: RefreshCcw, group: "学习" },
  { href: "/documents/import", label: "导入", icon: Upload, group: "工具" },
  { href: "/exports", label: "导出", icon: Download, group: "工具" },
];

export function AppShell({
  profile,
  children,
}: {
  profile: AppProfile;
  children: React.ReactNode;
}) {
  const pathname = usePathname();

  function isActive(href: string) {
    if (href === "/documents") {
      return (
        pathname === "/documents" ||
        (pathname.startsWith("/documents/") && !pathname.startsWith("/documents/import"))
      );
    }
    return pathname === href || pathname.startsWith(`${href}/`);
  }

  return (
    <div className="reading-shell-bg min-h-screen">
      <aside className="toolbar-material fixed inset-y-0 left-0 hidden w-72 border-r border-[var(--line)] px-5 py-6 text-[var(--foreground)] lg:block">
        <Link href="/documents" className="flex items-center gap-2 px-2">
          <span className="grid h-7 w-7 place-items-center rounded-[8px] bg-[var(--accent)] text-sm font-bold text-white">
            英
          </span>
          <span className="text-[28px] font-bold leading-tight tracking-normal">
            英读助手
          </span>
        </Link>

        <nav className="mt-8 space-y-5">
          {["学习", "工具"].map((group) => (
            <div key={group}>
              <p className="mb-2 px-3 text-[13px] font-normal uppercase tracking-[0.04em] text-[var(--muted)]">
                {group}
              </p>
              <div className="space-y-1">
                {navItems
                  .filter((item) => item.group === group)
                  .map((item) => {
                    const active = isActive(item.href);
                    return (
                      <Link
                        key={item.href}
                        href={item.href}
                        className={`apple-spring flex h-11 items-center gap-3 rounded-[10px] px-3 text-[15px] font-medium transition duration-200 active:opacity-60 ${
                          active
                            ? "bg-[var(--accent-soft)] text-[var(--accent)]"
                            : "text-[var(--muted)] hover:bg-[var(--accent-soft)] hover:text-[var(--accent)]"
                        }`}
                      >
                        <item.icon className="h-4 w-4 stroke-[1.7]" aria-hidden />
                        {item.label}
                      </Link>
                    );
                  })}
              </div>
            </div>
          ))}
          {profile.role === "admin" ? (
            <Link
              href="/admin"
              className={`apple-spring flex h-11 items-center gap-3 rounded-[10px] px-3 text-[15px] font-medium transition duration-200 active:opacity-60 ${
                isActive("/admin")
                  ? "bg-[var(--accent-soft)] text-[var(--accent)]"
                  : "text-[var(--muted)] hover:bg-[var(--accent-soft)] hover:text-[var(--accent)]"
              }`}
            >
              <ShieldCheck className="h-4 w-4 stroke-[1.7]" aria-hidden />
              管理员
            </Link>
          ) : null}
        </nav>

        <div className="apple-spring absolute bottom-5 left-5 right-5 rounded-[14px] p-3 transition duration-200 hover:bg-[var(--paper)]">
          <div className="flex items-center gap-3">
            <span className="grid h-9 w-9 shrink-0 place-items-center rounded-full bg-[var(--accent-soft)] text-sm font-semibold text-[var(--accent)]">
              {profile.email.slice(0, 1).toUpperCase()}
            </span>
            <div className="min-w-0">
              <p className="truncate text-[15px] font-semibold text-[var(--foreground)]">
                {profile.email}
              </p>
              <p className="mt-0.5 flex items-center gap-1.5 text-xs text-[var(--muted)]">
                <KeyRound className="h-3.5 w-3.5" aria-hidden />
                Level {profile.learner_level}
              </p>
            </div>
          </div>
        </div>
      </aside>

      <div className="lg:pl-72">
        <header className="toolbar-material sticky top-0 z-20 border-b border-[var(--line)] px-4 py-3 lg:hidden">
          <Link href="/documents" className="flex items-center gap-2">
            <span className="grid h-8 w-8 place-items-center rounded-[10px] bg-[var(--accent)] font-semibold text-white">
              英
            </span>
            <span className="font-semibold">英读助手</span>
          </Link>
          <nav className="mt-3 grid grid-cols-4 gap-1 text-xs">
            {navItems.slice(0, 4).map((item) => (
              <Link
                key={item.href}
                href={item.href}
                className={`rounded-[10px] px-2 py-2 text-center ${
                  isActive(item.href)
                    ? "bg-[var(--accent-soft)] text-[var(--accent)]"
                    : "bg-white/70 text-[var(--muted)]"
                }`}
              >
                {item.label}
              </Link>
            ))}
          </nav>
        </header>
        <main className="mx-auto w-full max-w-7xl px-4 py-7 sm:px-6 lg:px-10">
          {children}
        </main>
      </div>
    </div>
  );
}
