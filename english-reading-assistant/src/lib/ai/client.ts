import OpenAI from "openai";
import { zodResponseFormat } from "openai/helpers/zod";

import {
  documentAnalysisSchema,
  parseDocumentAnalysis,
  type DocumentAnalysis,
} from "./schemas";
import { getAiProvider } from "@/lib/env";
import type { SegmentDraft } from "@/lib/text/segment";

const DEFAULT_OPENAI_MODEL = "gpt-4o-mini";
const DEFAULT_DEEPSEEK_MODEL = "deepseek-reasoner";
const DEFAULT_BATCH_CHAR_LIMIT = 12_000;

export async function analyzeSegmentsWithOpenAI(input: {
  segments: SegmentDraft[];
  userLevel?: string | null;
  onBatchAnalyzed?: (result: {
    batchIndex: number;
    batchCount: number;
    segments: SegmentDraft[];
    analysis: DocumentAnalysis;
  }) => Promise<void> | void;
}): Promise<DocumentAnalysis> {
  const provider = getAiProvider();
  const apiKey =
    provider === "deepseek"
      ? process.env.DEEPSEEK_API_KEY
      : process.env.OPENAI_API_KEY;
  if (!apiKey) {
    throw new Error(
      provider === "deepseek"
        ? "DEEPSEEK_API_KEY is required for document processing."
        : "OPENAI_API_KEY is required for document processing.",
    );
  }

  const client = new OpenAI({
    apiKey,
    baseURL:
      provider === "deepseek"
        ? (process.env.DEEPSEEK_BASE_URL ??
          process.env.AI_BASE_URL ??
          "https://api.deepseek.com")
        : undefined,
  });
  const model =
    provider === "deepseek"
      ? (process.env.DEEPSEEK_MODEL ??
        process.env.AI_MODEL ??
        DEFAULT_DEEPSEEK_MODEL)
      : (process.env.OPENAI_MODEL ??
        process.env.AI_MODEL ??
        DEFAULT_OPENAI_MODEL);
  const batches = createSegmentBatches(
    input.segments,
    getBatchCharLimit(provider),
  );

  const merged: DocumentAnalysis = {
    segments: [],
    vocabulary: [],
    expressions: [],
  };

  for (const [index, batch] of batches.entries()) {
    if (batches.length > 1) {
      console.log(
        `Analyzing document batch ${index + 1}/${batches.length} with ${provider}.`,
      );
    }

    const analysis = await analyzeSegmentBatch({
      client,
      provider,
      model,
      segments: batch,
      userLevel: input.userLevel,
    });

    merged.segments.push(...analysis.segments);
    merged.vocabulary.push(...analysis.vocabulary);
    merged.expressions.push(...analysis.expressions);

    await input.onBatchAnalyzed?.({
      batchIndex: index,
      batchCount: batches.length,
      segments: batch,
      analysis,
    });
  }

  return {
    segments: merged.segments.sort((a, b) => a.orderIndex - b.orderIndex),
    vocabulary: dedupeBy(merged.vocabulary, (item) => item.term),
    expressions: dedupeBy(merged.expressions, (item) => item.expression),
  };
}

async function analyzeSegmentBatch(input: {
  client: OpenAI;
  provider: "openai" | "deepseek";
  model: string;
  segments: SegmentDraft[];
  userLevel?: string | null;
}): Promise<DocumentAnalysis> {
  if (input.provider === "deepseek") {
    const completion = await input.client.chat.completions.create({
      model: input.model,
      response_format: { type: "json_object" },
      ...(input.model.includes("reasoner") ? {} : { temperature: 0.2 }),
      messages: [
        {
          role: "system",
          content:
            "You are an English intensive-reading assistant for Chinese learners. Return valid JSON only. Translate faithfully into Simplified Chinese, identify useful vocabulary for the learner level, and extract authentic phrases, collocations, and sentence patterns.",
        },
        {
          role: "user",
          content: JSON.stringify({
            responseShape: {
              segments: [
                {
                  orderIndex: 0,
                  translatedText: "简体中文译文",
                },
              ],
              vocabulary: [
                {
                  term: "word or phrase",
                  phonetic: "optional phonetic",
                  partOfSpeech: "optional part of speech",
                  chineseDefinition: "简体中文释义",
                  exampleSentence: "optional English example",
                  difficulty: "optional learner difficulty",
                },
              ],
              expressions: [
                {
                  expression: "authentic English expression",
                  chineseMeaning: "简体中文含义",
                  usageNote: "optional usage note in Chinese",
                  exampleSentence: "optional English example",
                  rewriteTemplate: "optional reusable pattern",
                },
              ],
            },
            learnerLevel: input.userLevel ?? "B1",
            task:
              "Return one JSON object with segments, vocabulary, and expressions. Translate every segment and preserve each segment orderIndex exactly.",
            segments: input.segments,
          }),
        },
      ],
    });

    const content = completion.choices[0]?.message.content;
    if (!content) {
      throw new Error("DeepSeek returned no structured analysis.");
    }

    return parseDocumentAnalysis(parseJsonObject(content));
  }

  const completion = await input.client.chat.completions.parse({
    model: input.model,
    temperature: 0.2,
    response_format: zodResponseFormat(
      documentAnalysisSchema,
      "english_reading_document_analysis",
    ),
    messages: [
      {
        role: "system",
        content:
          "You are an English intensive-reading assistant for Chinese learners. Return strict JSON only. Translate faithfully into Simplified Chinese, identify useful vocabulary for the learner level, and extract authentic phrases, collocations, and sentence patterns.",
      },
      {
        role: "user",
        content: JSON.stringify({
          learnerLevel: input.userLevel ?? "B1",
          task:
            "Translate each segment and extract vocabulary plus authentic expressions. Preserve each segment orderIndex exactly.",
          segments: input.segments,
        }),
      },
    ],
  });

  const parsed = completion.choices[0]?.message.parsed;
  if (!parsed) {
    throw new Error("OpenAI returned no structured analysis.");
  }

  return parseDocumentAnalysis(parsed);
}

function parseJsonObject(content: string): unknown {
  const trimmed = content.trim();
  const fenced = trimmed.match(/^```(?:json)?\s*([\s\S]*?)\s*```$/i);
  return JSON.parse(fenced?.[1] ?? trimmed);
}

function createSegmentBatches(
  segments: SegmentDraft[],
  charLimit: number,
): SegmentDraft[][] {
  const batches: SegmentDraft[][] = [];
  let current: SegmentDraft[] = [];
  let currentChars = 0;

  for (const segment of segments) {
    const length = segment.text.length;
    if (current.length > 0 && currentChars + length > charLimit) {
      batches.push(current);
      current = [];
      currentChars = 0;
    }

    current.push(segment);
    currentChars += length;
  }

  if (current.length > 0) {
    batches.push(current);
  }

  return batches;
}

function getBatchCharLimit(provider: "openai" | "deepseek"): number {
  const raw = process.env.AI_BATCH_CHAR_LIMIT;
  const parsed = raw ? Number(raw) : NaN;
  if (Number.isFinite(parsed) && parsed >= 2_000) {
    return parsed;
  }

  return provider === "deepseek" ? DEFAULT_BATCH_CHAR_LIMIT : 20_000;
}

function dedupeBy<T>(items: T[], getKey: (item: T) => string): T[] {
  const seen = new Set<string>();
  const result: T[] = [];

  for (const item of items) {
    const key = getKey(item).trim().toLowerCase();
    if (!key || seen.has(key)) continue;
    seen.add(key);
    result.push(item);
  }

  return result;
}
