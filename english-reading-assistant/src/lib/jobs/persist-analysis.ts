import type { DocumentAnalysis } from "@/lib/ai/schemas";
import { createSupabaseAdminClient } from "@/lib/supabase/admin";
import type { SegmentDraft } from "@/lib/text/segment";

type SupabaseAdmin = ReturnType<typeof createSupabaseAdminClient>;

export async function persistAnalyzedBatch(input: {
  admin: SupabaseAdmin;
  documentId: string;
  userId: string;
  segments: SegmentDraft[];
  analysis: DocumentAnalysis;
  seenVocabulary: Set<string>;
  seenExpressions: Set<string>;
}) {
  const { error: segmentError } = await input.admin.from("segments").upsert(
    input.segments.map((draft) => {
      const analyzed = input.analysis.segments.find(
        (segment) => segment.orderIndex === draft.orderIndex,
      );
      return {
        document_id: input.documentId,
        user_id: input.userId,
        order_index: draft.orderIndex,
        kind: draft.kind,
        original_text: draft.text,
        translated_text: analyzed?.translatedText ?? "",
      };
    }),
    { onConflict: "document_id,order_index" },
  );
  if (segmentError) throw segmentError;

  const vocabularyRows = input.analysis.vocabulary
    .filter((item) => {
      const key = item.term.trim().toLowerCase();
      if (!key || input.seenVocabulary.has(key)) return false;
      input.seenVocabulary.add(key);
      return true;
    })
    .map((item) => ({
      document_id: input.documentId,
      user_id: input.userId,
      term: item.term,
      phonetic: item.phonetic ?? null,
      part_of_speech: item.partOfSpeech ?? null,
      chinese_definition: item.chineseDefinition,
      example_sentence: item.exampleSentence ?? null,
      difficulty: item.difficulty ?? null,
    }));

  if (vocabularyRows.length > 0) {
    const { error } = await input.admin
      .from("vocabulary_items")
      .insert(vocabularyRows);
    if (error) throw error;
  }

  const expressionRows = input.analysis.expressions
    .filter((item) => {
      const key = item.expression.trim().toLowerCase();
      if (!key || input.seenExpressions.has(key)) return false;
      input.seenExpressions.add(key);
      return true;
    })
    .map((item) => ({
      document_id: input.documentId,
      user_id: input.userId,
      expression: item.expression,
      chinese_meaning: item.chineseMeaning,
      usage_note: item.usageNote ?? null,
      example_sentence: item.exampleSentence ?? null,
      rewrite_template: item.rewriteTemplate ?? null,
    }));

  if (expressionRows.length > 0) {
    const { error } = await input.admin
      .from("expression_items")
      .insert(expressionRows);
    if (error) throw error;
  }
}
