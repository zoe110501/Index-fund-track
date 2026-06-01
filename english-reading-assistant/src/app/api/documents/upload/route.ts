import { NextRequest, NextResponse } from "next/server";

import { resolveApiActor } from "@/lib/api-auth";
import { validateUpload } from "@/lib/documents/limits";
import { getErrorMessage, jsonError } from "@/lib/http";
import { dispatchDocumentProcessing } from "@/lib/jobs/dispatch";
import { createSupabaseAdminClient } from "@/lib/supabase/admin";

export const runtime = "nodejs";

export async function POST(request: NextRequest) {
  try {
    const actor = await resolveApiActor(request);
    if (!actor) {
      return jsonError(401, "unauthorized", "请先登录后再上传文档。");
    }

    const formData = await request.formData();
    const file = formData.get("file");
    if (!(file instanceof File)) {
      return jsonError(400, "missing_file", "请选择 PDF 或 DOCX 文件。");
    }

    const validation = validateUpload({
      name: file.name,
      type: file.type,
      size: file.size,
    });
    if (!validation.ok) {
      return jsonError(400, validation.reason, validation.message);
    }

    const admin = createSupabaseAdminClient();
    const { data: document, error: insertError } = await admin
      .from("documents")
      .insert({
        user_id: actor.userId,
        title: formData.get("title")?.toString() || file.name,
        source_type: validation.kind,
        mime_type: file.type,
        status: "queued",
      })
      .select("id")
      .single();
    if (insertError) throw insertError;

    const rawFilePath = `${actor.userId}/${document.id}/${file.name}`;
    const { error: uploadError } = await admin.storage
      .from("raw-documents")
      .upload(rawFilePath, file, {
        contentType: file.type || inferContentType(validation.kind),
        upsert: true,
      });
    if (uploadError) throw uploadError;

    await admin
      .from("documents")
      .update({ raw_file_path: rawFilePath })
      .eq("id", document.id);
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
      quantity: file.size,
      metadata: { source: validation.kind },
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
    return jsonError(500, "upload_failed", getErrorMessage(error));
  }
}

function inferContentType(kind: "pdf" | "docx"): string {
  return kind === "pdf"
    ? "application/pdf"
    : "application/vnd.openxmlformats-officedocument.wordprocessingml.document";
}
