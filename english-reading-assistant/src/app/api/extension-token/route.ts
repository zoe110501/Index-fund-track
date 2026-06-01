import { NextResponse } from "next/server";

import { requireUser } from "@/lib/auth";
import { createExtensionToken } from "@/lib/extension-tokens";
import { getErrorMessage, jsonError } from "@/lib/http";

export const runtime = "nodejs";

export async function POST() {
  try {
    const { profile } = await requireUser();
    const token = await createExtensionToken({
      userId: profile.id,
      name: "Chrome/Edge extension",
    });

    return NextResponse.json(token);
  } catch (error) {
    return jsonError(500, "extension_token_failed", getErrorMessage(error));
  }
}
