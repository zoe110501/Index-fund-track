import { describe, expect, it } from "vitest";

import { renderMarkdownExport } from "./markdown";

describe("renderMarkdownExport", () => {
  it("renders bilingual segments, vocabulary, and authentic expressions", () => {
    const markdown = renderMarkdownExport({
      document: {
        title: "Reading Better",
        sourceUrl: "https://example.com/reading",
      },
      segments: [
        {
          orderIndex: 0,
          originalText: "Good readers notice patterns.",
          translatedText: "优秀的读者会注意语言模式。",
        },
      ],
      vocabulary: [
        {
          term: "pattern",
          partOfSpeech: "noun",
          phonetic: "/ˈpætərn/",
          chineseDefinition: "模式；规律",
          exampleSentence: "Good readers notice patterns.",
          difficulty: "B1",
        },
      ],
      expressions: [
        {
          expression: "notice patterns",
          chineseMeaning: "注意到反复出现的模式",
          usageNote: "比 see patterns 更强调主动观察。",
          exampleSentence: "Good readers notice patterns.",
          rewriteTemplate: "Strong [role] notice [useful signal].",
        },
      ],
    });

    expect(markdown).toContain("# Reading Better");
    expect(markdown).toContain("> Source: https://example.com/reading");
    expect(markdown).toContain("## 双语精读");
    expect(markdown).toContain("**EN:** Good readers notice patterns.");
    expect(markdown).toContain("**中:** 优秀的读者会注意语言模式。");
    expect(markdown).toContain("| pattern | noun | /ˈpætərn/ |");
    expect(markdown).toContain("| notice patterns | 注意到反复出现的模式 |");
  });
});
