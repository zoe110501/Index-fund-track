import { NextRequest, NextResponse } from "next/server";

import { resolveApiActor } from "@/lib/api-auth";
import { getAiProviderLabel, hasAiEnv } from "@/lib/env";
import { getErrorMessage, jsonError } from "@/lib/http";
import { dispatchDocumentProcessing } from "@/lib/jobs/dispatch";
import { createSupabaseAdminClient } from "@/lib/supabase/admin";

export const runtime = "nodejs";

export async function POST(
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
    const { data: document, error: documentError } = await admin
      .from("documents")
      .select("id,status")
      .eq("id", id)
      .eq("user_id", actor.userId)
      .single();
    if (documentError) throw documentError;

    if (document.status === "ready") {
      return NextResponse.json({ id, status: "ready" });
    }

    if (!hasAiEnv()) {
      const providerLabel = getAiProviderLabel();
      const missingAiMessage = `${providerLabel} API key is not configured. Add it to .env.local, restart the app, then retry processing.`;
      await Promise.all([
        admin
          .from("documents")
          .update({ status: "failed", error_message: missingAiMessage })
          .eq("id", id)
          .eq("user_id", actor.userId),
        admin
          .from("processing_jobs")
          .update({ status: "failed", error_message: missingAiMessage })
          .eq("document_id", id)
          .eq("kind", "process_document"),
      ]);

      return jsonError(
        503,
        "ai_not_configured",
        `还没有配置 ${providerLabel} API Key，暂时不能生成精读内容。`,
      );
    }

    await Promise.all([
      admin
        .from("documents")
        .update({ status: "queued", error_message: null })
        .eq("id", id)
        .eq("user_id", actor.userId),
      admin
        .from("processing_jobs")
        .update({ status: "queued", attempts: 0, error_message: null })
        .eq("document_id", id)
        .eq("kind", "process_document"),
    ]);

    const dispatch = await dispatchDocumentProcessing({
      documentId: id,
      userId: actor.userId,
    });

    return NextResponse.json({
      id,
      status: dispatch.mode === "blocked" ? "failed" : "queued",
      processingMode: dispatch.mode,
    });
  } catch (error) {
    return jsonError(500, "retry_failed", getErrorMessage(error));
  }
}
