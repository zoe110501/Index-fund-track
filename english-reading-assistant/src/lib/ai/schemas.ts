import { z } from "zod";

export const analyzedSegmentSchema = z.object({
  orderIndex: z.number().int().nonnegative(),
  translatedText: z.string().min(1),
});

export const vocabularyItemSchema = z.object({
  term: z.string().min(1),
  phonetic: z.string().nullable().optional(),
  partOfSpeech: z.string().nullable().optional(),
  chineseDefinition: z.string().min(1),
  exampleSentence: z.string().nullable().optional(),
  difficulty: z.string().nullable().optional(),
});

export const expressionItemSchema = z.object({
  expression: z.string().min(1),
  chineseMeaning: z.string().min(1),
  usageNote: z.string().nullable().optional(),
  exampleSentence: z.string().nullable().optional(),
  rewriteTemplate: z.string().nullable().optional(),
});

export const documentAnalysisSchema = z.object({
  segments: z.array(analyzedSegmentSchema),
  vocabulary: z.array(vocabularyItemSchema),
  expressions: z.array(expressionItemSchema),
});

export type DocumentAnalysis = z.infer<typeof documentAnalysisSchema>;

export function parseDocumentAnalysis(value: unknown): DocumentAnalysis {
  return documentAnalysisSchema.parse(value);
}
