export const WEB_TEXT_MAX_CHARS = 100_000;
export const PDF_MAX_BYTES = 25 * 1024 * 1024;
export const DOCX_MAX_BYTES = 15 * 1024 * 1024;

export type UploadKind = "pdf" | "docx";

export type UploadValidation =
  | { ok: true; kind: UploadKind }
  | {
      ok: false;
      reason:
        | "unsupported_type"
        | "file_too_large"
        | "empty_file"
        | "missing_name";
      message: string;
    };

export type WebImportValidation =
  | { ok: true }
  | { ok: false; reason: "web_text_too_large" | "empty_web_text"; message: string };

type UploadInput = {
  name: string;
  type: string;
  size: number;
};

export function validateUpload(input: UploadInput): UploadValidation {
  if (!input.name.trim()) {
    return {
      ok: false,
      reason: "missing_name",
      message: "文件名为空，请重新选择文件。",
    };
  }

  if (input.size <= 0) {
    return {
      ok: false,
      reason: "empty_file",
      message: "文件内容为空，请重新选择文件。",
    };
  }

  const kind = detectUploadKind(input.name, input.type);
  if (!kind) {
    return {
      ok: false,
      reason: "unsupported_type",
      message: "暂时只支持 PDF 和 Word DOCX 文档。",
    };
  }

  const limit = kind === "pdf" ? PDF_MAX_BYTES : DOCX_MAX_BYTES;
  if (input.size > limit) {
    return {
      ok: false,
      reason: "file_too_large",
      message:
        kind === "pdf"
          ? "PDF 最大支持 25MB，请拆分后再上传。"
          : "DOCX 最大支持 15MB，请拆分后再上传。",
    };
  }

  return { ok: true, kind };
}

export function validateWebImport(text: string): WebImportValidation {
  const normalized = text.trim();
  if (!normalized) {
    return {
      ok: false,
      reason: "empty_web_text",
      message: "没有识别到网页正文，请手动复制正文后再试。",
    };
  }

  if (normalized.length > WEB_TEXT_MAX_CHARS) {
    return {
      ok: false,
      reason: "web_text_too_large",
      message: "网页正文超过 100,000 字符，请拆分后再导入。",
    };
  }

  return { ok: true };
}

function detectUploadKind(name: string, mimeType: string): UploadKind | null {
  const lowerName = name.toLowerCase();
  if (mimeType === "application/pdf" || lowerName.endsWith(".pdf")) {
    return "pdf";
  }

  if (
    mimeType ===
      "application/vnd.openxmlformats-officedocument.wordprocessingml.document" ||
    lowerName.endsWith(".docx")
  ) {
    return "docx";
  }

  return null;
}
