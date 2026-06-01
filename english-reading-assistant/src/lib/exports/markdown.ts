type MarkdownDocument = {
  title: string;
  sourceUrl?: string | null;
};

type MarkdownSegment = {
  orderIndex: number;
  originalText: string;
  translatedText: string;
};

type MarkdownVocabularyItem = {
  term: string;
  partOfSpeech?: string | null;
  phonetic?: string | null;
  chineseDefinition: string;
  exampleSentence?: string | null;
  difficulty?: string | null;
};

type MarkdownExpressionItem = {
  expression: string;
  chineseMeaning: string;
  usageNote?: string | null;
  exampleSentence?: string | null;
  rewriteTemplate?: string | null;
};

export function renderMarkdownExport(input: {
  document: MarkdownDocument;
  segments: MarkdownSegment[];
  vocabulary: MarkdownVocabularyItem[];
  expressions: MarkdownExpressionItem[];
}): string {
  const lines = [`# ${input.document.title}`, ""];

  if (input.document.sourceUrl) {
    lines.push(`> Source: ${input.document.sourceUrl}`, "");
  }

  lines.push("## 双语精读", "");
  for (const segment of [...input.segments].sort(
    (a, b) => a.orderIndex - b.orderIndex,
  )) {
    lines.push(`**EN:** ${segment.originalText}`, "");
    lines.push(`**中:** ${segment.translatedText}`, "");
  }

  lines.push("## 生词表", "");
  lines.push("| 词/短语 | 词性 | 音标 | 中文释义 | 难度 | 原句 |");
  lines.push("| --- | --- | --- | --- | --- | --- |");
  for (const item of input.vocabulary) {
    lines.push(
      [
        item.term,
        item.partOfSpeech ?? "",
        item.phonetic ?? "",
        item.chineseDefinition,
        item.difficulty ?? "",
        item.exampleSentence ?? "",
      ]
        .map(escapeTableCell)
        .join(" | ")
        .replace(/^/, "| ")
        .concat(" |"),
    );
  }

  lines.push("", "## 地道表达表", "");
  lines.push("| 表达 | 中文含义 | 用法说明 | 原句 | 仿写模板 |");
  lines.push("| --- | --- | --- | --- | --- |");
  for (const item of input.expressions) {
    lines.push(
      [
        item.expression,
        item.chineseMeaning,
        item.usageNote ?? "",
        item.exampleSentence ?? "",
        item.rewriteTemplate ?? "",
      ]
        .map(escapeTableCell)
        .join(" | ")
        .replace(/^/, "| ")
        .concat(" |"),
    );
  }

  return `${lines.join("\n").trim()}\n`;
}

function escapeTableCell(value: string): string {
  return value.replace(/\|/g, "\\|").replace(/\n/g, " ").trim();
}
