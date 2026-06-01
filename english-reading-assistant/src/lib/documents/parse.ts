import mammoth from "mammoth";
import { PDFParse } from "pdf-parse";

import type { UploadKind } from "./limits";

export async function parseUploadedDocument(input: {
  kind: UploadKind;
  buffer: Buffer;
}): Promise<string> {
  if (input.kind === "docx") {
    const result = await mammoth.extractRawText({ buffer: input.buffer });
    return normalizeExtractedText(result.value);
  }

  const parser = new PDFParse({ data: input.buffer });
  try {
    const result = await parser.getText();
    return normalizeExtractedText(result.text);
  } finally {
    await parser.destroy();
  }
}

export function normalizeExtractedText(text: string): string {
  return text
    .replace(/\r\n/g, "\n")
    .replace(/[ \t]+\n/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .replace(/[ \t]{2,}/g, " ")
    .trim();
}

export function extractReadableTextFromHtml(html: string): string {
  return normalizeExtractedText(
    html
      .replace(/<script[\s\S]*?<\/script>/gi, " ")
      .replace(/<style[\s\S]*?<\/style>/gi, " ")
      .replace(/<\/(p|h1|h2|h3|li|blockquote|section|article)>/gi, "\n\n")
      .replace(/<[^>]+>/g, " ")
      .replace(/&nbsp;/g, " ")
      .replace(/&amp;/g, "&")
      .replace(/&lt;/g, "<")
      .replace(/&gt;/g, ">")
      .replace(/&quot;/g, '"')
      .replace(/&#39;/g, "'"),
  );
}
