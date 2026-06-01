import { NextRequest, NextResponse } from "next/server";
import { z } from "zod";

import { resolveApiActor } from "@/lib/api-auth";
import { validateWebImport } from "@/lib/documents/limits";
import { extractReadableTextFromHtml } from "@/lib/documents/parse";
import { getErrorMessage, jsonError } from "@/lib/http";
import { dispatchDocumentProcessing } from "@/lib/jobs/dispatch";
import { createSupabaseAdminClient } from "@/lib/supabase/admin";

export const runtime = "nodejs";

const requestSchema = z.object({
  title: z.string().trim().min(1).max(300),
  url: z.string().url().optional().nullable(),
  text: z.string().optional().nullable(),
  html: z.string().optional().nullable(),
});

export async function POST(request: NextRequest) {
  try {
    const actor = await resolveApiActor(request);
    if (!actor) {
      return jsonError(401, "unauthorized", "请先登录或配置插件导入 Token。");
    }

    const body = requestSchema.parse(await request.json());
    const text = body.text?.trim() || extractReadableTextFromHtml(body.html ?? "");
    const validation = validateWebImport(text);
    if (!validation.ok) {
      return jsonError(400, validation.reason, validation.message);
    }

    const admin = createSupabaseAdminClient();
    const { data: document, error } = await admin
      .from("documents")
      .insert({
        user_id: actor.userId,
        title: body.title,
        source_type: "web",
        source_url: body.url ?? null,
        raw_text: text,
        status: "queued",
        character_count: text.length,
      })
      .select("id")
      .single();
    if (error) throw error;

    await admin.from("processing_jobs").insert({
      user_id: actor.userId,
      document_id: document.id,
      kind: "process_document",
      status: "queued",
    });
    await admin.from("usage_events").insert({
      user_id: actor.userId,
      document_id: document.id,
      kind: "document_imported",
      quantity: text.length,
      metadata: { source: "web" },
    });

    const dispatch = await dispatchDocumentProcessing({
      documentId: document.id,
      userId: actor.userId,
    });

    return NextResponse.json({
      id: document.id,
      status: "queued",
      processingMode: dispatch.mode,
    });
  } catch (error) {
    return jsonError(500, "from_url_failed", getErrorMessage(error));
  }
}
