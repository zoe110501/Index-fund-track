import { describe, expect, it } from "vitest";

import {
  buildInlineStudyBlocks,
  getHighlightMatches,
} from "./inline-study";

const segments = [
  {
    id: "s1",
    order_index: 1,
    kind: "paragraph" as const,
    original_text: "Cat Wu (00:00:01):",
    translated_text: "",
  },
  {
    id: "s2",
    order_index: 2,
    kind: "paragraph" as const,
    original_text: "How do you elicit the maximum capability from the model?",
    translated_text: "",
  },
  {
    id: "s3",
    order_index: 3,
    kind: "paragraph" as const,
    original_text: "It takes the right amount of AGI-pilled judgment.",
    translated_text: "",
  },
  {
    id: "s4",
    order_index: 4,
    kind: "paragraph" as const,
    original_text: "You can remove every barrier before shipping.",
    translated_text: "",
  },
  {
    id: "s5",
    order_index: 5,
    kind: "paragraph" as const,
    original_text: "Teams leverage taste when deciding what to launch.",
    translated_text: "",
  },
  {
    id: "s6",
    order_index: 6,
    kind: "paragraph" as const,
    original_text: "The next topic starts with a different idea.",
    translated_text: "",
  },
];

describe("inline study helpers", () => {
  it("inserts a study card after a readable group and scopes items to that group", () => {
    const blocks = buildInlineStudyBlocks({
      segments,
      vocabulary: [
        {
          id: "v1",
          term: "elicit",
          phonetic: "/i'lɪsɪt/",
          part_of_speech: "v.",
          chinese_definition: "引出；激发",
          example_sentence: "How do you elicit the maximum capability?",
          difficulty: "B2",
          status: "learning",
        },
        {
          id: "v2",
          term: "unrelated",
          phonetic: null,
          part_of_speech: null,
          chinese_definition: "无关的",
          example_sentence: null,
          difficulty: null,
          status: "new",
        },
      ],
      expressions: [
        {
          id: "e1",
          expression: "the right amount of AGI-pilled",
          chinese_meaning: "恰到好处的 AGI 狂热",
          usage_note: "描述对 AGI 前景保持平衡的态度",
          example_sentence: "the right amount of AGI-pilled judgment",
        },
      ],
      maxParagraphs: 4,
    });

    const cardBlock = blocks.find((block) => block.type === "study-card");

    expect(cardBlock).toBeTruthy();
    expect(cardBlock?.type === "study-card" && cardBlock.card.afterSegmentId).toBe("s5");
    expect(cardBlock?.type === "study-card" && cardBlock.card.vocabulary).toHaveLength(1);
    expect(cardBlock?.type === "study-card" && cardBlock.card.expressions).toHaveLength(1);
  });

  it("finds phrase highlights before vocabulary highlights", () => {
    const matches = getHighlightMatches(
      "How do you elicit the right amount of AGI-pilled behavior?",
      [
        {
          id: "v1",
          term: "elicit",
          phonetic: null,
          part_of_speech: null,
          chinese_definition: "引出",
          example_sentence: null,
          difficulty: null,
          status: "learning",
        },
      ],
      [
        {
          id: "e1",
          expression: "the right amount of AGI-pilled",
          chinese_meaning: "恰到好处的 AGI 狂热",
          usage_note: null,
          example_sentence: null,
        },
      ],
    );

    expect(matches.map((match) => match.type)).toEqual(["vocabulary", "expression"]);
    expect(matches.map((match) => match.text)).toEqual([
      "elicit",
      "the right amount of AGI-pilled",
    ]);
  });
});
