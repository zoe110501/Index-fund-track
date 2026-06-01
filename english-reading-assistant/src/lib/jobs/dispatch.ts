import { inngest } from "@/inngest/client";
import { getAiProviderLabel, hasAiEnv } from "@/lib/env";
import {
  exportDocumentLocally,
  processDocumentLocally,
} from "@/lib/jobs/local";
import { createSupabaseAdminClient } from "@/lib/supabase/admin";

export async function dispatchDocumentProcessing(input: {
  documentId: string;
  userId: string;
}) {
  if (!hasAiEnv()) {
    const providerLabel = getAiProviderLabel();
    const message =
      `${providerLabel} API key is not configured. Add it to .env.local, restart the app, then retry processing.`;
    const admin = createSupabaseAdminClient();
    await Promise.all([
      admin
        .from("documents")
        .update({ status: "failed", error_message: message })
        .eq("id", input.documentId)
        .eq("user_id", input.userId),
      admin
        .from("processing_jobs")
        .update({ status: "failed", error_message: message })
        .eq("document_id", input.documentId)
        .eq("kind", "process_document"),
    ]);
    return { mode: "blocked" as const };
  }

  if (process.env.INNGEST_EVENT_KEY) {
    await inngest.send({
      name: "document/process.requested",
      data: input,
    });
    return { mode: "inngest" as const };
  }

  processDocumentLocally(input).catch((error) => {
    console.error("Local document processing failed", error);
  });
  return { mode: "local" as const };
}

export async function dispatchDocumentExport(input: {
  exportId: string;
  documentId: string;
  userId: string;
  format: "markdown" | "pdf";
}) {
  if (process.env.INNGEST_EVENT_KEY) {
    await inngest.send({
      name: "document/export.requested",
      data: input,
    });
    return { mode: "inngest" as const };
  }

  exportDocumentLocally(input).catch((error) => {
    console.error("Local document export failed", error);
  });
  return { mode: "local" as const };
}
