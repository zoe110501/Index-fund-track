import { NextRequest, NextResponse } from "next/server";

import { getAppUrl } from "@/lib/env";
import { createSupabaseServerClient } from "@/lib/supabase/server";

export async function GET(request: NextRequest) {
  const requestUrl = new URL(request.url);
  const tokenHash = requestUrl.searchParams.get("token_hash");
  const type = requestUrl.searchParams.get("type") ?? "magiclink";

  if (!tokenHash || type !== "magiclink") {
    const url = new URL("/login", getAppUrl());
    url.searchParams.set("error", "callback-failed");
    url.searchParams.set("reason", "Missing magic link token.");
    return NextResponse.redirect(url);
  }

  const supabase = await createSupabaseServerClient();
  const { error } = await supabase.auth.verifyOtp({
    token_hash: tokenHash,
    type: "magiclink",
  });

  if (error) {
    const url = new URL("/login", getAppUrl());
    url.searchParams.set("error", "callback-failed");
    url.searchParams.set("reason", error.message);
    return NextResponse.redirect(url);
  }

  return NextResponse.redirect(`${getAppUrl()}/documents`);
}
