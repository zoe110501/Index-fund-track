import { redirect } from "next/navigation";
import type { User } from "@supabase/supabase-js";

import { getAdminEmails } from "@/lib/env";
import { createSupabaseAdminClient } from "@/lib/supabase/admin";
import { createSupabaseServerClient } from "@/lib/supabase/server";

export type AppProfile = {
  id: string;
  email: string;
  role: "learner" | "admin";
  status: "active" | "paused";
  learner_level: string;
  monthly_character_quota: number;
};

export async function requireUser(): Promise<{
  user: User;
  profile: AppProfile;
}> {
  const supabase = await createSupabaseServerClient();
  const {
    data: { user },
  } = await supabase.auth.getUser();

  if (!user) {
    redirect("/login");
  }

  const profile = await ensureProfile(user);
  if (profile.status !== "active") {
    redirect("/login?error=paused");
  }

  return { user, profile };
}

export async function requireAdmin() {
  const session = await requireUser();
  if (session.profile.role !== "admin") {
    redirect("/documents");
  }
  return session;
}

export async function ensureProfile(user: User): Promise<AppProfile> {
  const admin = createSupabaseAdminClient();
  const email = user.email?.toLowerCase();
  if (!email) {
    throw new Error("Authenticated user does not have an email address.");
  }

  const { data: existing, error: existingError } = await admin
    .from("profiles")
    .select("id,email,role,status,learner_level,monthly_character_quota")
    .eq("id", user.id)
    .maybeSingle();

  if (existingError) {
    throw existingError;
  }

  if (existing) {
    return existing as AppProfile;
  }

  const adminEmails = getAdminEmails();
  const isBootstrapAdmin = adminEmails.includes(email);

  const { data: invite, error: inviteError } = await admin
    .from("invites")
    .select("id,status,expires_at")
    .eq("email", email)
    .eq("status", "pending")
    .gt("expires_at", new Date().toISOString())
    .maybeSingle();

  if (inviteError) {
    throw inviteError;
  }

  if (!invite && !isBootstrapAdmin) {
    throw new Error("This email has not been invited.");
  }

  const { data: inserted, error: insertError } = await admin
    .from("profiles")
    .insert({
      id: user.id,
      email,
      display_name: user.user_metadata?.name ?? null,
      role: isBootstrapAdmin ? "admin" : "learner",
    })
    .select("id,email,role,status,learner_level,monthly_character_quota")
    .single();

  if (insertError) {
    throw insertError;
  }

  if (invite) {
    await admin
      .from("invites")
      .update({
        status: "accepted",
        accepted_by: user.id,
        accepted_at: new Date().toISOString(),
      })
      .eq("id", invite.id);
  }

  return inserted as AppProfile;
}

export async function isEmailInvited(email: string): Promise<boolean> {
  const normalizedEmail = email.trim().toLowerCase();
  if (!normalizedEmail) {
    return false;
  }

  if (getAdminEmails().includes(normalizedEmail)) {
    return true;
  }

  const admin = createSupabaseAdminClient();
  const { data, error } = await admin
    .from("invites")
    .select("id")
    .eq("email", normalizedEmail)
    .eq("status", "pending")
    .gt("expires_at", new Date().toISOString())
    .maybeSingle();

  if (error) {
    throw error;
  }

  return Boolean(data);
}
