import { NextRequest, NextResponse } from "next/server";

import { getAppUrl } from "@/lib/env";
import { createSupabaseServerClient } from "@/lib/supabase/server";

export async function GET(request: NextRequest) {
  const requestUrl = new URL(request.url);
  const code = requestUrl.searchParams.get("code");
  const error = requestUrl.searchParams.get("error");
  const errorDescription = requestUrl.searchParams.get("error_description");

  if (error) {
    const url = new URL("/login", getAppUrl());
    url.searchParams.set("error", "callback-failed");
    if (errorDescription) {
      url.searchParams.set("reason", errorDescription);
    }
    return NextResponse.redirect(url);
  }

  if (code) {
    const supabase = await createSupabaseServerClient();
    const { error: exchangeError } =
      await supabase.auth.exchangeCodeForSession(code);
    if (exchangeError) {
      const url = new URL("/login", getAppUrl());
      url.searchParams.set("error", "callback-failed");
      url.searchParams.set("reason", exchangeError.message);
      return NextResponse.redirect(url);
    }
  }

  return NextResponse.redirect(`${getAppUrl()}/documents`);
}
