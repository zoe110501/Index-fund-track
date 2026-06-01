export type SegmentKind = "heading" | "paragraph";

export type SegmentDraft = {
  orderIndex: number;
  kind: SegmentKind;
  text: string;
};

const DEFAULT_MAX_SEGMENT_CHARS = 1_200;

export function splitIntoSegments(
  text: string,
  maxSegmentChars = DEFAULT_MAX_SEGMENT_CHARS,
): SegmentDraft[] {
  const blocks = text
    .replace(/\r\n/g, "\n")
    .split(/\n{2,}/)
    .map((block) => normalizeWhitespace(block))
    .filter(Boolean);

  const segments = blocks.flatMap((block, blockIndex) => {
    const kind = inferKind(block, blockIndex);
    if (block.length <= maxSegmentChars) {
      return [{ kind, text: block }];
    }

    return splitLongParagraph(block, maxSegmentChars).map((text) => ({
      kind: "paragraph" as const,
      text,
    }));
  });

  return segments.map((segment, orderIndex) => ({
    orderIndex,
    ...segment,
  }));
}

function inferKind(block: string, blockIndex: number): SegmentKind {
  const wordCount = block.split(/\s+/).length;
  const endsLikeSentence = /[.!?。！？]$/.test(block);

  if (blockIndex === 0 && wordCount <= 10 && !endsLikeSentence) {
    return "heading";
  }

  return "paragraph";
}

function splitLongParagraph(text: string, maxSegmentChars: number): string[] {
  const sentences = text
    .split(/(?<=[.!?。！？])\s+/)
    .map((sentence) => sentence.trim())
    .filter(Boolean);

  if (sentences.length <= 1) {
    return chunkByLength(text, maxSegmentChars);
  }

  const chunks: string[] = [];
  let current = "";

  for (const sentence of sentences) {
    const candidate = current ? `${current} ${sentence}` : sentence;
    if (candidate.length > maxSegmentChars && current) {
      chunks.push(current);
      current = sentence;
    } else if (sentence.length > maxSegmentChars) {
      chunks.push(...chunkByLength(sentence, maxSegmentChars));
      current = "";
    } else {
      current = candidate;
    }
  }

  if (current) {
    chunks.push(current);
  }

  return chunks;
}

function chunkByLength(text: string, maxSegmentChars: number): string[] {
  const chunks: string[] = [];
  for (let start = 0; start < text.length; start += maxSegmentChars) {
    chunks.push(text.slice(start, start + maxSegmentChars).trim());
  }
  return chunks.filter(Boolean);
}

function normalizeWhitespace(value: string): string {
  return value.replace(/\s+/g, " ").trim();
}
