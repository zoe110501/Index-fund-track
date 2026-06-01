import { NextRequest, NextResponse } from "next/server";

import { resolveApiActor } from "@/lib/api-auth";
import { getErrorMessage, jsonError } from "@/lib/http";
import { createSupabaseAdminClient } from "@/lib/supabase/admin";

export const runtime = "nodejs";

export async function GET(
  request: NextRequest,
  context: { params: Promise<{ id: string }> },
) {
  try {
    const actor = await resolveApiActor(request);
    if (!actor) {
      return jsonError(401, "unauthorized", "请先登录。");
    }

    const { id } = await context.params;
    const admin = createSupabaseAdminClient();
    const [documentResult, segmentsResult, vocabResult, expressionResult] =
      await Promise.all([
        admin
          .from("documents")
          .select("*")
          .eq("id", id)
          .eq("user_id", actor.userId)
          .single(),
        admin
          .from("segments")
          .select("*")
          .eq("document_id", id)
          .eq("user_id", actor.userId)
          .order("order_index", { ascending: true }),
        admin
          .from("vocabulary_items")
          .select("*")
          .eq("document_id", id)
          .eq("user_id", actor.userId)
          .order("created_at", { ascending: true }),
        admin
          .from("expression_items")
          .select("*")
          .eq("document_id", id)
          .eq("user_id", actor.userId)
          .order("created_at", { ascending: true }),
      ]);

    if (documentResult.error) throw documentResult.error;
    if (segmentsResult.error) throw segmentsResult.error;
    if (vocabResult.error) throw vocabResult.error;
    if (expressionResult.error) throw expressionResult.error;

    return NextResponse.json({
      document: documentResult.data,
      segments: segmentsResult.data,
      vocabulary: vocabResult.data,
      expressions: expressionResult.data,
    });
  } catch (error) {
    return jsonError(500, "document_fetch_failed", getErrorMessage(error));
  }
}
