import fs from "node:fs";

import PDFDocument from "pdfkit";

import type {
  BilingualSegment,
  ExpressionItem,
  VocabularyItem,
} from "@/lib/domain";

export async function renderPdfExport(input: {
  title: string;
  sourceUrl?: string | null;
  segments: BilingualSegment[];
  vocabulary: VocabularyItem[];
  expressions: ExpressionItem[];
  cjkFontPath?: string | null;
}): Promise<Buffer> {
  const chunks: Buffer[] = [];
  const doc = new PDFDocument({ margin: 48, size: "A4" });

  doc.on("data", (chunk: Buffer) => chunks.push(chunk));
  const finished = new Promise<Buffer>((resolve, reject) => {
    doc.on("end", () => resolve(Buffer.concat(chunks)));
    doc.on("error", reject);
  });

  if (input.cjkFontPath && fs.existsSync(input.cjkFontPath)) {
    doc.font(input.cjkFontPath);
  }

  doc.fontSize(22).text(input.title, { lineGap: 4 });
  if (input.sourceUrl) {
    doc.moveDown(0.5).fontSize(9).fillColor("#64748b").text(input.sourceUrl);
  }

  doc.moveDown(1).fillColor("#111827").fontSize(15).text("双语精读");
  for (const segment of input.segments.sort(
    (a, b) => a.orderIndex - b.orderIndex,
  )) {
    doc.moveDown(0.8).fontSize(10).fillColor("#0f172a").text("EN");
    doc.fontSize(11).fillColor("#111827").text(segment.originalText, {
      lineGap: 3,
    });
    doc.moveDown(0.3).fontSize(10).fillColor("#0f766e").text("中");
    doc.fontSize(11).fillColor("#111827").text(segment.translatedText, {
      lineGap: 3,
    });
  }

  doc.addPage().fontSize(15).fillColor("#111827").text("生词表");
  for (const item of input.vocabulary) {
    doc
      .moveDown(0.5)
      .fontSize(11)
      .fillColor("#111827")
      .text(`${item.term} ${item.phonetic ?? ""} ${item.partOfSpeech ?? ""}`);
    doc
      .fontSize(10)
      .fillColor("#334155")
      .text(`${item.chineseDefinition} ${item.difficulty ?? ""}`);
    if (item.exampleSentence) {
      doc.fontSize(9).fillColor("#64748b").text(item.exampleSentence);
    }
  }

  doc.addPage().fontSize(15).fillColor("#111827").text("地道表达表");
  for (const item of input.expressions) {
    doc
      .moveDown(0.5)
      .fontSize(11)
      .fillColor("#111827")
      .text(item.expression);
    doc.fontSize(10).fillColor("#334155").text(item.chineseMeaning);
    if (item.usageNote) {
      doc.fontSize(9).fillColor("#64748b").text(item.usageNote);
    }
    if (item.rewriteTemplate) {
      doc.fontSize(9).fillColor("#0f766e").text(item.rewriteTemplate);
    }
  }

  doc.end();
  return finished;
}
