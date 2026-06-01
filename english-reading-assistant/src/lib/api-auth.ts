import { NextRequest } from "next/server";

import { ensureProfile } from "@/lib/auth";
import { resolveExtensionToken } from "@/lib/extension-tokens";
import { createSupabaseAdminClient } from "@/lib/supabase/admin";
import { createSupabaseServerClient } from "@/lib/supabase/server";

export type ApiActor = {
  userId: string;
  role: "learner" | "admin";
  learnerLevel: string;
  monthlyCharacterQuota: number;
};

export async function resolveApiActor(
  request: NextRequest,
): Promise<ApiActor | null> {
  const authorization = request.headers.get("authorization");
  const token = authorization?.startsWith("Bearer ")
    ? authorization.slice("Bearer ".length).trim()
    : null;

  if (token) {
    const tokenUser = await resolveExtensionToken(token);
    if (!tokenUser) return null;
    return loadActorProfile(tokenUser.userId);
  }

  const supabase = await createSupabaseServerClient();
  const {
    data: { user },
  } = await supabase.auth.getUser();

  if (!user) return null;
  const profile = await ensureProfile(user);

  return {
    userId: profile.id,
    role: profile.role,
    learnerLevel: profile.learner_level,
    monthlyCharacterQuota: profile.monthly_character_quota,
  };
}

export async function loadActorProfile(userId: string): Promise<ApiActor | null> {
  const admin = createSupabaseAdminClient();
  const { data, error } = await admin
    .from("profiles")
    .select("id,role,status,learner_level,monthly_character_quota")
    .eq("id", userId)
    .single();

  if (error || !data || data.status !== "active") {
    return null;
  }

  return {
    userId: data.id,
    role: data.role,
    learnerLevel: data.learner_level,
    monthlyCharacterQuota: data.monthly_character_quota,
  };
}
