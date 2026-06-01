import { describe, expect, it } from "vitest";

import {
  DOCX_MAX_BYTES,
  PDF_MAX_BYTES,
  WEB_TEXT_MAX_CHARS,
  validateUpload,
  validateWebImport,
} from "./limits";

describe("document limits", () => {
  it("accepts PDF, DOCX, and web imports inside production limits", () => {
    expect(
      validateUpload({
        name: "essay.pdf",
        type: "application/pdf",
        size: PDF_MAX_BYTES,
      }),
    ).toEqual({ ok: true, kind: "pdf" });

    expect(
      validateUpload({
        name: "memo.docx",
        type: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        size: DOCX_MAX_BYTES,
      }),
    ).toEqual({ ok: true, kind: "docx" });

    expect(validateWebImport("a".repeat(WEB_TEXT_MAX_CHARS))).toEqual({
      ok: true,
    });
  });

  it("returns actionable errors for unsupported or oversized inputs", () => {
    expect(
      validateUpload({
        name: "scan.png",
        type: "image/png",
        size: 10,
      }),
    ).toMatchObject({ ok: false, reason: "unsupported_type" });

    expect(
      validateUpload({
        name: "huge.pdf",
        type: "application/pdf",
        size: PDF_MAX_BYTES + 1,
      }),
    ).toMatchObject({ ok: false, reason: "file_too_large" });

    expect(validateWebImport("a".repeat(WEB_TEXT_MAX_CHARS + 1))).toEqual({
      ok: false,
      reason: "web_text_too_large",
      message: "网页正文超过 100,000 字符，请拆分后再导入。",
    });
  });
});
