export type DocumentStatus = "queued" | "processing" | "ready" | "failed";

export type DocumentSourceType = "web" | "pdf" | "docx";

export type VocabularyStatus = "new" | "known" | "learning" | "mastered";

export type BilingualSegment = {
  id?: string;
  orderIndex: number;
  originalText: string;
  translatedText: string;
  kind?: "heading" | "paragraph";
};

export type VocabularyItem = {
  id?: string;
  term: string;
  phonetic?: string | null;
  partOfSpeech?: string | null;
  chineseDefinition: string;
  exampleSentence?: string | null;
  difficulty?: string | null;
  status?: VocabularyStatus;
};

export type ExpressionItem = {
  id?: string;
  expression: string;
  chineseMeaning: string;
  usageNote?: string | null;
  exampleSentence?: string | null;
  rewriteTemplate?: string | null;
};

export type ReaderDocument = {
  id: string;
  title: string;
  sourceType: DocumentSourceType;
  sourceUrl?: string | null;
  status: DocumentStatus;
  errorMessage?: string | null;
  createdAt?: string;
};
