export type InlineStudySegment = {
  id: string;
  order_index: number;
  kind: "heading" | "paragraph";
  original_text: string;
  translated_text: string;
};

export type InlineStudyVocabularyItem = {
  id: string;
  term: string;
  phonetic: string | null;
  part_of_speech: string | null;
  chinese_definition: string;
  example_sentence: string | null;
  difficulty: string | null;
  status: string;
};

export type InlineStudyExpressionItem = {
  id: string;
  expression: string;
  chinese_meaning: string;
  usage_note: string | null;
  example_sentence: string | null;
};

export type InlineStudyCard = {
  id: string;
  afterSegmentId: string;
  segmentIds: string[];
  wordCount: number;
  vocabulary: InlineStudyVocabularyItem[];
  expressions: InlineStudyExpressionItem[];
};

export type InlineStudyBlock =
  | { type: "segment"; segment: InlineStudySegment }
  | { type: "study-card"; card: InlineStudyCard };

export type HighlightMatch = {
  id: string;
  type: "vocabulary" | "expression";
  text: string;
  start: number;
  end: number;
};

export function buildInlineStudyBlocks({
  segments,
  vocabulary,
  expressions,
  maxParagraphs = 4,
  maxWords = 500,
  maxVocabularyPerCard = 5,
  maxExpressionsPerCard = 4,
}: {
  segments: InlineStudySegment[];
  vocabulary: InlineStudyVocabularyItem[];
  expressions: InlineStudyExpressionItem[];
  maxParagraphs?: number;
  maxWords?: number;
  maxVocabularyPerCard?: number;
  maxExpressionsPerCard?: number;
}): InlineStudyBlock[] {
  const blocks: InlineStudyBlock[] = [];
  let group: InlineStudySegment[] = [];
  let paragraphCount = 0;
  let wordCount = 0;

  function flushGroup() {
    if (group.length === 0) return;

    const groupText = group.map((segment) => segment.original_text).join("\n");
    const matchedVocabulary = vocabulary
      .filter((item) => textContainsTerm(groupText, item.term))
      .slice(0, maxVocabularyPerCard);
    const matchedExpressions = expressions
      .filter((item) => textContainsPhrase(groupText, item.expression))
      .slice(0, maxExpressionsPerCard);

    if (matchedVocabulary.length > 0 || matchedExpressions.length > 0) {
      const afterSegment = [...group]
        .reverse()
        .find((segment) => !isStandaloneSpeakerLine(segment.original_text));

      blocks.push({
        type: "study-card",
        card: {
          id: `study-${group[0].id}-${group[group.length - 1].id}`,
          afterSegmentId: afterSegment?.id ?? group[group.length - 1].id,
          segmentIds: group.map((segment) => segment.id),
          wordCount,
          vocabulary: matchedVocabulary,
          expressions: matchedExpressions,
        },
      });
    }

    group = [];
    paragraphCount = 0;
    wordCount = 0;
  }

  for (const segment of segments) {
    blocks.push({ type: "segment", segment });
    group.push(segment);

    if (isStandaloneSpeakerLine(segment.original_text)) continue;

    paragraphCount += 1;
    wordCount += countWords(segment.original_text);

    if (paragraphCount >= maxParagraphs || wordCount >= maxWords) {
      flushGroup();
    }
  }

  flushGroup();

  return blocks;
}

export function getHighlightMatches(
  text: string,
  vocabulary: InlineStudyVocabularyItem[],
  expressions: InlineStudyExpressionItem[],
  {
    maxVocabulary = 3,
    maxExpressions = 2,
  }: { maxVocabulary?: number; maxExpressions?: number } = {},
): HighlightMatch[] {
  const matches: HighlightMatch[] = [];

  for (const expression of expressions.slice(0, maxExpressions)) {
    const found = findPhrase(text, expression.expression);
    if (!found || overlaps(matches, found.start, found.end)) continue;
    matches.push({
      id: expression.id,
      type: "expression",
      text: text.slice(found.start, found.end),
      start: found.start,
      end: found.end,
    });
  }

  for (const item of vocabulary.slice(0, maxVocabulary)) {
    const found = findWholeWord(text, item.term);
    if (!found || overlaps(matches, found.start, found.end)) continue;
    matches.push({
      id: item.id,
      type: "vocabulary",
      text: text.slice(found.start, found.end),
      start: found.start,
      end: found.end,
    });
  }

  return matches.sort((a, b) => a.start - b.start);
}

function isStandaloneSpeakerLine(text: string) {
  return /^.+?\s*\(\d{2}:\d{2}:\d{2}\):$/.test(text.trim());
}

function countWords(text: string) {
  return text.split(/\s+/).filter(Boolean).length;
}

function textContainsTerm(text: string, term: string) {
  return Boolean(findWholeWord(text, term));
}

function textContainsPhrase(text: string, phrase: string) {
  return Boolean(findPhrase(text, phrase));
}

function findWholeWord(text: string, term: string) {
  const cleaned = term.trim();
  if (!cleaned) return null;
  const regex = new RegExp(`(?<![\\p{L}\\p{N}_])${escapeRegExp(cleaned)}(?![\\p{L}\\p{N}_])`, "iu");
  const match = regex.exec(text);
  if (!match || match.index < 0) return null;
  return { start: match.index, end: match.index + match[0].length };
}

function findPhrase(text: string, phrase: string) {
  const cleaned = phrase.trim().replace(/\s+/g, " ");
  if (!cleaned) return null;
  const normalizedText = text.replace(/\s+/g, " ");
  const index = normalizedText.toLocaleLowerCase().indexOf(cleaned.toLocaleLowerCase());
  if (index < 0) return null;

  // The normalized string only changes whitespace width; map back by scanning text.
  const regex = new RegExp(escapeRegExp(cleaned).replace(/\\ /g, "\\s+"), "iu");
  const match = regex.exec(text);
  if (!match || match.index < 0) return null;
  return { start: match.index, end: match.index + match[0].length };
}

function overlaps(matches: HighlightMatch[], start: number, end: number) {
  return matches.some((match) => start < match.end && end > match.start);
}

function escapeRegExp(value: string) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
