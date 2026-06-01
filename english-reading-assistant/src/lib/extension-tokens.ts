import crypto from "node:crypto";

import { createSupabaseAdminClient } from "@/lib/supabase/admin";

const TOKEN_PREFIX = "era_";

export function generateExtensionToken(): string {
  return `${TOKEN_PREFIX}${crypto.randomBytes(32).toString("base64url")}`;
}

export function hashExtensionToken(token: string): string {
  return crypto.createHash("sha256").update(token).digest("hex");
}

export async function createExtensionToken(input: {
  userId: string;
  name?: string;
}): Promise<{ token: string; id: string }> {
  const token = generateExtensionToken();
  const admin = createSupabaseAdminClient();
  const { data, error } = await admin
    .from("extension_tokens")
    .insert({
      user_id: input.userId,
      name: input.name ?? "Browser extension",
      token_hash: hashExtensionToken(token),
    })
    .select("id")
    .single();

  if (error) throw error;
  return { token, id: data.id };
}

export async function resolveExtensionToken(token: string): Promise<{
  userId: string;
} | null> {
  if (!token.startsWith(TOKEN_PREFIX)) {
    return null;
  }

  const admin = createSupabaseAdminClient();
  const { data, error } = await admin
    .from("extension_tokens")
    .select("id,user_id,revoked_at")
    .eq("token_hash", hashExtensionToken(token))
    .is("revoked_at", null)
    .maybeSingle();

  if (error) throw error;
  if (!data) return null;

  await admin
    .from("extension_tokens")
    .update({ last_used_at: new Date().toISOString() })
    .eq("id", data.id);

  return { userId: data.user_id };
}
