import { analyzeSegmentsWithOpenAI } from "@/lib/ai/client";
import { parseUploadedDocument } from "@/lib/documents/parse";
import { renderMarkdownExport } from "@/lib/exports/markdown";
import { renderPdfExport } from "@/lib/exports/pdf";
import { persistAnalyzedBatch } from "@/lib/jobs/persist-analysis";
import { createSupabaseAdminClient } from "@/lib/supabase/admin";
import { splitIntoSegments } from "@/lib/text/segment";

export async function processDocumentLocally(input: {
  documentId: string;
  userId: string;
}) {
  const admin = createSupabaseAdminClient();

  try {
    await admin
      .from("documents")
      .update({ status: "processing", error_message: null })
      .eq("id", input.documentId)
      .eq("user_id", input.userId);
    await admin
      .from("processing_jobs")
      .update({ status: "running", attempts: 1 })
      .eq("document_id", input.documentId)
      .eq("kind", "process_document");

    const { data: document, error: documentError } = await admin
      .from("documents")
      .select("id,user_id,title,source_type,raw_text,raw_file_path,mime_type")
      .eq("id", input.documentId)
      .eq("user_id", input.userId)
      .single();
    if (documentError) throw documentError;

    const { data: profile, error: profileError } = await admin
      .from("profiles")
      .select("learner_level")
      .eq("id", input.userId)
      .single();
    if (profileError) throw profileError;

    let rawText = document.raw_text ?? "";
    if (document.source_type !== "web") {
      if (!document.raw_file_path) {
        throw new Error("Document has no uploaded file path.");
      }

      const { data, error } = await admin.storage
        .from("raw-documents")
        .download(document.raw_file_path);
      if (error) throw error;

      rawText = await parseUploadedDocument({
        kind: document.source_type as "pdf" | "docx",
        buffer: Buffer.from(await data.arrayBuffer()),
      });
    }

    const segmentDrafts = splitIntoSegments(rawText);
    if (segmentDrafts.length === 0) {
      throw new Error("No readable English text was found.");
    }

    await admin.from("segments").delete().eq("document_id", input.documentId);
    await admin
      .from("vocabulary_items")
      .delete()
      .eq("document_id", input.documentId);
    await admin
      .from("expression_items")
      .delete()
      .eq("document_id", input.documentId);

    const seenVocabulary = new Set<string>();
    const seenExpressions = new Set<string>();
    await analyzeSegmentsWithOpenAI({
      segments: segmentDrafts,
      userLevel: profile.learner_level,
      onBatchAnalyzed: async ({ segments, analysis, batchIndex, batchCount }) => {
        await persistAnalyzedBatch({
          admin,
          documentId: input.documentId,
          userId: input.userId,
          segments,
          analysis,
          seenVocabulary,
          seenExpressions,
        });
        await admin
          .from("processing_jobs")
          .update({
            error_message: `已完成 ${batchIndex + 1}/${batchCount} 批`,
          })
          .eq("document_id", input.documentId)
          .eq("kind", "process_document");
      },
    });

    await admin
      .from("documents")
      .update({
        status: "ready",
        character_count: rawText.length,
        raw_text: document.source_type === "web" ? rawText : null,
      })
      .eq("id", input.documentId);
    await admin
      .from("processing_jobs")
      .update({ status: "succeeded" })
      .eq("document_id", input.documentId)
      .eq("kind", "process_document");
    await admin.from("usage_events").insert({
      user_id: input.userId,
      document_id: input.documentId,
      kind: "document_processed",
      quantity: rawText.length,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown error";
    await admin
      .from("documents")
      .update({ status: "failed", error_message: message })
      .eq("id", input.documentId)
      .eq("user_id", input.userId);
    await admin
      .from("processing_jobs")
      .update({ status: "failed", error_message: message })
      .eq("document_id", input.documentId)
      .eq("kind", "process_document");
    throw error;
  }
}

export async function exportDocumentLocally(input: {
  exportId: string;
  documentId: string;
  userId: string;
  format: "markdown" | "pdf";
}) {
  const admin = createSupabaseAdminClient();

  try {
    await admin
      .from("exports")
      .update({ status: "running", error_message: null })
      .eq("id", input.exportId)
      .eq("user_id", input.userId);

    const [documentResult, segmentsResult, vocabResult, expressionResult] =
      await Promise.all([
        admin
          .from("documents")
          .select("id,title,source_url")
          .eq("id", input.documentId)
          .eq("user_id", input.userId)
          .single(),
        admin
          .from("segments")
          .select("order_index,original_text,translated_text")
          .eq("document_id", input.documentId)
          .order("order_index", { ascending: true }),
        admin
          .from("vocabulary_items")
          .select(
            "term,phonetic,part_of_speech,chinese_definition,example_sentence,difficulty",
          )
          .eq("document_id", input.documentId),
        admin
          .from("expression_items")
          .select(
            "expression,chinese_meaning,usage_note,example_sentence,rewrite_template",
          )
          .eq("document_id", input.documentId),
      ]);

    if (documentResult.error) throw documentResult.error;
    if (segmentsResult.error) throw segmentsResult.error;
    if (vocabResult.error) throw vocabResult.error;
    if (expressionResult.error) throw expressionResult.error;

    const output =
      input.format === "markdown"
        ? {
            contentType: "text/markdown; charset=utf-8",
            extension: "md",
            buffer: Buffer.from(
              renderMarkdownExport({
                document: {
                  title: documentResult.data.title,
                  sourceUrl: documentResult.data.source_url,
                },
                segments: segmentsResult.data.map((segment) => ({
                  orderIndex: segment.order_index,
                  originalText: segment.original_text,
                  translatedText: segment.translated_text,
                })),
                vocabulary: vocabResult.data.map((item) => ({
                  term: item.term,
                  phonetic: item.phonetic,
                  partOfSpeech: item.part_of_speech,
                  chineseDefinition: item.chinese_definition,
                  exampleSentence: item.example_sentence,
                  difficulty: item.difficulty,
                })),
                expressions: expressionResult.data.map((item) => ({
                  expression: item.expression,
                  chineseMeaning: item.chinese_meaning,
                  usageNote: item.usage_note,
                  exampleSentence: item.example_sentence,
                  rewriteTemplate: item.rewrite_template,
                })),
              }),
              "utf8",
            ),
          }
        : {
            contentType: "application/pdf",
            extension: "pdf",
            buffer: await renderPdfExport({
              title: documentResult.data.title,
              sourceUrl: documentResult.data.source_url,
              segments: segmentsResult.data.map((segment) => ({
                orderIndex: segment.order_index,
                originalText: segment.original_text,
                translatedText: segment.translated_text,
              })),
              vocabulary: vocabResult.data.map((item) => ({
                term: item.term,
                phonetic: item.phonetic,
                partOfSpeech: item.part_of_speech,
                chineseDefinition: item.chinese_definition,
                exampleSentence: item.example_sentence,
                difficulty: item.difficulty,
              })),
              expressions: expressionResult.data.map((item) => ({
                expression: item.expression,
                chineseMeaning: item.chinese_meaning,
                usageNote: item.usage_note,
                exampleSentence: item.example_sentence,
                rewriteTemplate: item.rewrite_template,
              })),
              cjkFontPath: process.env.PDF_CJK_FONT_PATH,
            }),
          };

    const filePath = `${input.userId}/${input.documentId}/${input.exportId}.${output.extension}`;
    const { error: uploadError } = await admin.storage
      .from("exports")
      .upload(filePath, output.buffer, {
        contentType: output.contentType,
        upsert: true,
      });
    if (uploadError) throw uploadError;

    await admin
      .from("exports")
      .update({ status: "ready", file_path: filePath })
      .eq("id", input.exportId);
    await admin.from("usage_events").insert({
      user_id: input.userId,
      document_id: input.documentId,
      kind: "export_created",
      quantity: 1,
      metadata: { format: input.format },
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown error";
    await admin
      .from("exports")
      .update({ status: "failed", error_message: message })
      .eq("id", input.exportId)
      .eq("user_id", input.userId);
    throw error;
  }
}
