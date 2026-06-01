import { NextResponse } from "next/server";

import {
  getAdminEmails,
  getAppUrl,
  hasSupabaseEnv,
  isLocalAdminLoginEnabled,
} from "@/lib/env";
import { getSetupStatus } from "@/lib/setup-status";
import { createSupabaseAdminClient } from "@/lib/supabase/admin";

export const runtime = "nodejs";

export async function GET() {
  if (!isLocalAdminLoginEnabled()) {
    return redirectToLoginError("local-login-disabled");
  }

  if (!hasSupabaseEnv()) {
    return redirectToLoginError("config-missing");
  }

  const setupStatus = await getSetupStatus();
  if (!setupStatus.databaseReady) {
    return redirectToLoginError("database-missing");
  }

  const [email] = getAdminEmails();
  if (!email) {
    return redirectToLoginError("admin-email-missing");
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
    return redirectToLoginError("local-login-failed");
  }

  const url = new URL("/auth/token", getAppUrl());
  url.searchParams.set("type", "magiclink");
  url.searchParams.set("token_hash", data.properties.hashed_token);
  return NextResponse.redirect(url);
}

function redirectToLoginError(error: string) {
  const url = new URL("/login", getAppUrl());
  url.searchParams.set("error", error);
  return NextResponse.redirect(url);
}
