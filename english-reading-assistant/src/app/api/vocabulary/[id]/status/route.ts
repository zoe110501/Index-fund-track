import { NextRequest, NextResponse } from "next/server";
import { z } from "zod";

import { resolveApiActor } from "@/lib/api-auth";
import { getErrorMessage, jsonError } from "@/lib/http";
import { createSupabaseAdminClient } from "@/lib/supabase/admin";

export const runtime = "nodejs";

const requestSchema = z.object({
  status: z.enum(["new", "known", "learning", "mastered"]),
});

export async function PATCH(
  request: NextRequest,
  context: { params: Promise<{ id: string }> },
) {
  try {
    const actor = await resolveApiActor(request);
    if (!actor) {
      return jsonError(401, "unauthorized", "请先登录。");
    }

    const { id } = await context.params;
    const { status } = requestSchema.parse(await request.json());
    const admin = createSupabaseAdminClient();
    const { data, error } = await admin
      .from("vocabulary_items")
      .update({ status })
      .eq("id", id)
      .eq("user_id", actor.userId)
      .select("*")
      .single();

    if (error) throw error;
    return NextResponse.json({ item: data });
  } catch (error) {
    return jsonError(500, "vocabulary_update_failed", getErrorMessage(error));
  }
}
