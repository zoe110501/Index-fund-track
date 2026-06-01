import { NextRequest, NextResponse } from "next/server";
import { z } from "zod";

import { resolveApiActor } from "@/lib/api-auth";
import { getErrorMessage, jsonError } from "@/lib/http";
import { dispatchDocumentExport } from "@/lib/jobs/dispatch";
import { createSupabaseAdminClient } from "@/lib/supabase/admin";

export const runtime = "nodejs";

const requestSchema = z.object({
  format: z.enum(["markdown", "pdf"]),
});

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
    const { format } = requestSchema.parse(await request.json());
    const admin = createSupabaseAdminClient();

    const { data: document, error: documentError } = await admin
      .from("documents")
      .select("id,status")
      .eq("id", id)
      .eq("user_id", actor.userId)
      .single();
    if (documentError) throw documentError;
    if (document.status !== "ready") {
      return jsonError(409, "document_not_ready", "文档还没有处理完成。");
    }

    const { data: exportRow, error: exportError } = await admin
      .from("exports")
      .insert({
        user_id: actor.userId,
        document_id: id,
        format,
        status: "queued",
      })
      .select("id")
      .single();
    if (exportError) throw exportError;

    await admin.from("processing_jobs").insert({
      user_id: actor.userId,
      document_id: id,
      kind: "export_document",
      status: "queued",
    });

    const dispatch = await dispatchDocumentExport({
      exportId: exportRow.id,
      documentId: id,
      userId: actor.userId,
      format,
    });

    return NextResponse.json({
      id: exportRow.id,
      status: "queued",
      processingMode: dispatch.mode,
    });
  } catch (error) {
    return jsonError(500, "export_failed", getErrorMessage(error));
  }
}
