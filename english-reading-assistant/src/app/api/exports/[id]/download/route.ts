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
    const { data: exportRow, error } = await admin
      .from("exports")
      .select("file_path,status")
      .eq("id", id)
      .eq("user_id", actor.userId)
      .single();

    if (error) throw error;
    if (exportRow.status !== "ready" || !exportRow.file_path) {
      return jsonError(409, "export_not_ready", "导出文件还没有生成完成。");
    }

    const { data, error: signedUrlError } = await admin.storage
      .from("exports")
      .createSignedUrl(exportRow.file_path, 60 * 10);
    if (signedUrlError) throw signedUrlError;

    return NextResponse.redirect(data.signedUrl);
  } catch (error) {
    return jsonError(500, "export_download_failed", getErrorMessage(error));
  }
}
