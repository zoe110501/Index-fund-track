import { inngest } from "./client";
import { analyzeSegmentsWithOpenAI } from "@/lib/ai/client";
import { createSupabaseAdminClient } from "@/lib/supabase/admin";
import { parseUploadedDocument } from "@/lib/documents/parse";
import { splitIntoSegments } from "@/lib/text/segment";
import { renderMarkdownExport } from "@/lib/exports/markdown";
import { renderPdfExport } from "@/lib/exports/pdf";
import { persistAnalyzedBatch } from "@/lib/jobs/persist-analysis";

export const processDocument = inngest.createFunction(
  {
    id: "process-document",
    retries: 2,
  },
  { event: "document/process.requested" },
  async ({ event, step }) => {
    const { documentId, userId } = event.data as {
      documentId: string;
      userId: string;
    };
    const admin = createSupabaseAdminClient();

    await step.run("mark-processing", async () => {
      await admin
        .from("documents")
        .update({ status: "processing", error_message: null })
        .eq("id", documentId)
        .eq("user_id", userId);
      await admin
        .from("processing_jobs")
        .update({ status: "running", attempts: 1 })
        .eq("document_id", documentId)
        .eq("kind", "process_document");
    });

    const document = await step.run("load-document", async () => {
      const { data, error } = await admin
        .from("documents")
        .select("id,user_id,title,source_type,raw_text,raw_file_path,mime_type")
        .eq("id", documentId)
        .eq("user_id", userId)
        .single();
      if (error) throw error;
      return data;
    });

    const profile = await step.run("load-profile", async () => {
      const { data, error } = await admin
        .from("profiles")
        .select("learner_level")
        .eq("id", userId)
        .single();
      if (error) throw error;
      return data;
    });

    const rawText = await step.run("extract-text", async () => {
      if (document.source_type === "web") {
        return document.raw_text ?? "";
      }

      if (!document.raw_file_path) {
        throw new Error("Document has no uploaded file path.");
      }

      const { data, error } = await admin.storage
        .from("raw-documents")
        .download(document.raw_file_path);
      if (error) throw error;

      const buffer = Buffer.from(await data.arrayBuffer());
      return parseUploadedDocument({
        kind: document.source_type as "pdf" | "docx",
        buffer,
      });
    });

    const segmentDrafts = await step.run("split-segments", async () => {
      const drafts = splitIntoSegments(rawText);
      if (drafts.length === 0) {
        throw new Error("No readable English text was found.");
      }
      return drafts;
    });

    await step.run("reset-analysis", async () => {
      await admin.from("segments").delete().eq("document_id", documentId);
      await admin
        .from("vocabulary_items")
        .delete()
        .eq("document_id", documentId);
      await admin
        .from("expression_items")
        .delete()
        .eq("document_id", documentId);
    });

    await step.run("analyze-and-persist-batches", async () => {
      const seenVocabulary = new Set<string>();
      const seenExpressions = new Set<string>();
      await analyzeSegmentsWithOpenAI({
        segments: segmentDrafts,
        userLevel: profile.learner_level,
        onBatchAnalyzed: async ({
          segments,
          analysis,
          batchIndex,
          batchCount,
        }) => {
          await persistAnalyzedBatch({
            admin,
            documentId,
            userId,
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
            .eq("document_id", documentId)
            .eq("kind", "process_document");
        },
      });
    });

    await step.run("mark-ready", async () => {
      await admin
        .from("documents")
        .update({
          status: "ready",
          character_count: rawText.length,
          raw_text: document.source_type === "web" ? rawText : null,
        })
        .eq("id", documentId);
      await admin
        .from("processing_jobs")
        .update({ status: "succeeded" })
        .eq("document_id", documentId)
        .eq("kind", "process_document");
      await admin.from("usage_events").insert({
        user_id: userId,
        document_id: documentId,
        kind: "document_processed",
        quantity: rawText.length,
      });
    });

    return { documentId, segmentCount: segmentDrafts.length };
  },
);

export const exportDocument = inngest.createFunction(
  {
    id: "export-document",
    retries: 2,
  },
  { event: "document/export.requested" },
  async ({ event, step }) => {
    const { exportId, documentId, userId, format } = event.data as {
      exportId: string;
      documentId: string;
      userId: string;
      format: "markdown" | "pdf";
    };
    const admin = createSupabaseAdminClient();

    await step.run("mark-export-running", async () => {
      await admin
        .from("exports")
        .update({ status: "running", error_message: null })
        .eq("id", exportId)
        .eq("user_id", userId);
    });

    const bundle = await step.run("load-export-bundle", async () => {
      const [documentResult, segmentsResult, vocabResult, expressionResult] =
        await Promise.all([
          admin
            .from("documents")
            .select("id,title,source_url")
            .eq("id", documentId)
            .eq("user_id", userId)
            .single(),
          admin
            .from("segments")
            .select("order_index,original_text,translated_text")
            .eq("document_id", documentId)
            .order("order_index", { ascending: true }),
          admin
            .from("vocabulary_items")
            .select(
              "term,phonetic,part_of_speech,chinese_definition,example_sentence,difficulty",
            )
            .eq("document_id", documentId),
          admin
            .from("expression_items")
            .select(
              "expression,chinese_meaning,usage_note,example_sentence,rewrite_template",
            )
            .eq("document_id", documentId),
        ]);

      if (documentResult.error) throw documentResult.error;
      if (segmentsResult.error) throw segmentsResult.error;
      if (vocabResult.error) throw vocabResult.error;
      if (expressionResult.error) throw expressionResult.error;

      return {
        document: documentResult.data,
        segments: segmentsResult.data,
        vocabulary: vocabResult.data,
        expressions: expressionResult.data,
      };
    });

    const output = await step.run("render-export", async () => {
      if (format === "markdown") {
        return {
          contentType: "text/markdown; charset=utf-8",
          extension: "md",
          bytes: Buffer.from(
            renderMarkdownExport({
              document: {
                title: bundle.document.title,
                sourceUrl: bundle.document.source_url,
              },
              segments: bundle.segments.map((segment) => ({
                orderIndex: segment.order_index,
                originalText: segment.original_text,
                translatedText: segment.translated_text,
              })),
              vocabulary: bundle.vocabulary.map((item) => ({
                term: item.term,
                phonetic: item.phonetic,
                partOfSpeech: item.part_of_speech,
                chineseDefinition: item.chinese_definition,
                exampleSentence: item.example_sentence,
                difficulty: item.difficulty,
              })),
              expressions: bundle.expressions.map((item) => ({
                expression: item.expression,
                chineseMeaning: item.chinese_meaning,
                usageNote: item.usage_note,
                exampleSentence: item.example_sentence,
                rewriteTemplate: item.rewrite_template,
              })),
            }),
            "utf8",
          ).toString("base64"),
        };
      }

      const pdfBuffer = await renderPdfExport({
        title: bundle.document.title,
        sourceUrl: bundle.document.source_url,
        segments: bundle.segments.map((segment) => ({
          orderIndex: segment.order_index,
          originalText: segment.original_text,
          translatedText: segment.translated_text,
        })),
        vocabulary: bundle.vocabulary.map((item) => ({
          term: item.term,
          phonetic: item.phonetic,
          partOfSpeech: item.part_of_speech,
          chineseDefinition: item.chinese_definition,
          exampleSentence: item.example_sentence,
          difficulty: item.difficulty,
        })),
        expressions: bundle.expressions.map((item) => ({
          expression: item.expression,
          chineseMeaning: item.chinese_meaning,
          usageNote: item.usage_note,
          exampleSentence: item.example_sentence,
          rewriteTemplate: item.rewrite_template,
        })),
        cjkFontPath: process.env.PDF_CJK_FONT_PATH,
      });

      return {
        contentType: "application/pdf",
        extension: "pdf",
        bytes: pdfBuffer.toString("base64"),
      };
    });

    await step.run("upload-export", async () => {
      const filePath = `${userId}/${documentId}/${exportId}.${output.extension}`;
      const { error: uploadError } = await admin.storage
        .from("exports")
        .upload(filePath, Buffer.from(output.bytes, "base64"), {
          contentType: output.contentType,
          upsert: true,
        });
      if (uploadError) throw uploadError;

      await admin
        .from("exports")
        .update({ status: "ready", file_path: filePath })
        .eq("id", exportId);
      await admin.from("usage_events").insert({
        user_id: userId,
        document_id: documentId,
        kind: "export_created",
        quantity: 1,
        metadata: { format },
      });
    });

    return { exportId, format };
  },
);

export const cleanupRawFiles = inngest.createFunction(
  {
    id: "cleanup-raw-files",
  },
  { cron: "0 * * * *" },
  async ({ step }) => {
    const admin = createSupabaseAdminClient();
    const cutoff = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();

    const documents = await step.run("find-old-raw-files", async () => {
      const { data, error } = await admin
        .from("documents")
        .select("id,raw_file_path")
        .not("raw_file_path", "is", null)
        .lt("updated_at", cutoff)
        .eq("status", "ready");
      if (error) throw error;
      return data ?? [];
    });

    await step.run("delete-old-raw-files", async () => {
      const paths = documents
        .map((document) => document.raw_file_path)
        .filter(Boolean) as string[];
      if (paths.length === 0) return;

      await admin.storage.from("raw-documents").remove(paths);
      await admin
        .from("documents")
        .update({ raw_file_path: null })
        .in(
          "id",
          documents.map((document) => document.id),
        );
    });

    return { removed: documents.length };
  },
);

export const functions = [processDocument, exportDocument, cleanupRawFiles];
