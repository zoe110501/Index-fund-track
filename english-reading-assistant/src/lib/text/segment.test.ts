import { describe, expect, it } from "vitest";

import { splitIntoSegments } from "./segment";

describe("splitIntoSegments", () => {
  it("keeps paragraph order and removes empty whitespace", () => {
    const segments = splitIntoSegments(`
      Why English feels different

      Good readers do not translate every word.

      They notice phrases, rhythm, and intent.
    `);

    expect(segments).toEqual([
      {
        orderIndex: 0,
        kind: "heading",
        text: "Why English feels different",
      },
      {
        orderIndex: 1,
        kind: "paragraph",
        text: "Good readers do not translate every word.",
      },
      {
        orderIndex: 2,
        kind: "paragraph",
        text: "They notice phrases, rhythm, and intent.",
      },
    ]);
  });

  it("splits oversized paragraphs at sentence boundaries", () => {
    const segments = splitIntoSegments(
      "First sentence is short. Second sentence carries more detail. Third sentence closes the thought.",
      54,
    );

    expect(segments.map((segment) => segment.text)).toEqual([
      "First sentence is short.",
      "Second sentence carries more detail.",
      "Third sentence closes the thought.",
    ]);
    expect(segments.every((segment) => segment.kind === "paragraph")).toBe(
      true,
    );
  });
});
