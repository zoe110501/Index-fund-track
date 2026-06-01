import { NextRequest, NextResponse } from "next/server";
import { z } from "zod";

import { resolveApiActor } from "@/lib/api-auth";
import { getErrorMessage, jsonError } from "@/lib/http";
import { createSupabaseAdminClient } from "@/lib/supabase/admin";

export const runtime = "nodejs";

const requestSchema = z.object({
  type: z.enum(["vocabulary", "expression"]),
  text: z.string().trim().min(1).max(240),
  context: z.string().trim().max(1200).optional(),
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
    const body = requestSchema.parse(await request.json());
    const admin = createSupabaseAdminClient();
    const { data: document, error: documentError } = await admin
      .from("documents")
      .select("id")
      .eq("id", id)
      .eq("user_id", actor.userId)
      .single();
    if (documentError || !document) throw documentError;

    if (body.type === "vocabulary") {
      const { data, error } = await admin
        .from("vocabulary_items")
        .insert({
          document_id: id,
          user_id: actor.userId,
          term: body.text,
          chinese_definition: "待补充释义",
          example_sentence: body.context ?? null,
          difficulty: actor.learnerLevel,
          status: "learning",
        })
        .select("id")
        .single();
      if (error) throw error;
      return NextResponse.json({ id: data.id, type: body.type });
    }

    const { data, error } = await admin
      .from("expression_items")
      .insert({
        document_id: id,
        user_id: actor.userId,
        expression: body.text,
        chinese_meaning: "待补充讲解",
        usage_note: body.context ? `来自原文：${body.context}` : "手动划选",
      })
      .select("id")
      .single();
    if (error) throw error;

    return NextResponse.json({ id: data.id, type: body.type });
  } catch (error) {
    return jsonError(500, "selection_save_failed", getErrorMessage(error));
  }
}
