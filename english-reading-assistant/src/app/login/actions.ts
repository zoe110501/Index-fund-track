"use server";

import { redirect } from "next/navigation";

import { isEmailInvited } from "@/lib/auth";
import {
  getAdminEmails,
  getAppUrl,
  hasSupabaseEnv,
  isLocalAdminLoginEnabled,
} from "@/lib/env";
import { getSetupStatus } from "@/lib/setup-status";
import { createSupabaseAdminClient } from "@/lib/supabase/admin";
import { createSupabaseServerClient } from "@/lib/supabase/server";

export async function requestMagicLink(formData: FormData) {
  const email = formData.get("email")?.toString().trim().toLowerCase();
  if (!email) {
    redirect("/login?error=missing-email");
  }

  if (!hasSupabaseEnv()) {
    redirect("/login?error=config-missing");
  }

  const setupStatus = await getSetupStatus();
  if (!setupStatus.databaseReady) {
    redirect("/login?error=database-missing");
  }

  const allowed = await isEmailInvited(email);
  if (!allowed) {
    redirect("/login?error=not-invited");
  }

  const supabase = await createSupabaseServerClient();
  const { error } = await supabase.auth.signInWithOtp({
    email,
    options: {
      emailRedirectTo: `${getAppUrl()}/auth/callback`,
    },
  });

  if (error) {
    if (error.status === 429) {
      redirect("/login?error=rate-limited");
    }
    redirect("/login?error=magic-link-failed");
  }

  redirect(`/login?sent=1&email=${encodeURIComponent(email)}`);
}

export async function localAdminLogin() {
  if (!isLocalAdminLoginEnabled()) {
    redirect("/login?error=local-login-disabled");
  }

  if (!hasSupabaseEnv()) {
    redirect("/login?error=config-missing");
  }

  const setupStatus = await getSetupStatus();
  if (!setupStatus.databaseReady) {
    redirect("/login?error=database-missing");
  }

  const [email] = getAdminEmails();
  if (!email) {
    redirect("/login?error=admin-email-missing");
  }

  const admin = createSupabaseAdminClient();
  const { data, error } = await admin.auth.admin.generateLink({
    type: "magiclink",
    email,
    options: {
      redirectTo: `${getAppUrl()}/auth/token`,
    },
  });

  if (error || !data.properties?.hashed_token) {
    redirect("/login?error=local-login-failed");
  }

  redirect(
    `/auth/token?type=magiclink&token_hash=${encodeURIComponent(
      data.properties.hashed_token,
    )}`,
  );
}
