import crypto from "node:crypto";

import { NextRequest, NextResponse } from "next/server";
import { z } from "zod";

import { requireAdmin } from "@/lib/auth";
import { getErrorMessage, jsonError } from "@/lib/http";
import { createSupabaseAdminClient } from "@/lib/supabase/admin";

export const runtime = "nodejs";

const requestSchema = z.object({
  email: z.string().email(),
});

export async function POST(request: NextRequest) {
  try {
    const { profile } = await requireAdmin();
    const { email } = requestSchema.parse(await request.json());
    const normalizedEmail = email.trim().toLowerCase();
    const codeHash = crypto
      .createHash("sha256")
      .update(`${normalizedEmail}:${crypto.randomUUID()}`)
      .digest("hex");

    const admin = createSupabaseAdminClient();
    const { data, error } = await admin
      .from("invites")
      .insert({
        email: normalizedEmail,
        code_hash: codeHash,
        created_by: profile.id,
      })
      .select("id,email,status,expires_at")
      .single();
    if (error) throw error;

    return NextResponse.json({ invite: data });
  } catch (error) {
    return jsonError(500, "invite_create_failed", getErrorMessage(error));
  }
}
