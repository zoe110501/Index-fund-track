import { NextRequest, NextResponse } from "next/server";

import { resolveApiActor } from "@/lib/api-auth";
import { deleteExpressionItem } from "@/lib/expressions/remove";
import { getErrorMessage, jsonError } from "@/lib/http";
import { createSupabaseAdminClient } from "@/lib/supabase/admin";

export const runtime = "nodejs";

export async function DELETE(
  request: NextRequest,
  context: { params: Promise<{ id: string }> },
) {
  try {
    const actor = await resolveApiActor(request);
    if (!actor) {
      return jsonError(401, "unauthorized", "请先登录。");
    }

    const { id } = await context.params;
    const deleted = await deleteExpressionItem(createSupabaseAdminClient(), {
      expressionId: id,
      userId: actor.userId,
    });

    if (!deleted) {
      return jsonError(404, "expression_not_found", "没有找到这条表达记录。");
    }

    return NextResponse.json({ deleted: true });
  } catch (error) {
    return jsonError(500, "expression_delete_failed", getErrorMessage(error));
  }
}
