"use client";

import {
  Fragment,
  useEffect,
  useMemo,
  useState,
  useTransition,
  type ReactNode,
} from "react";
import {
  BookOpenCheck,
  ChevronDown,
  Copy,
  Eye,
  EyeOff,
  FileDown,
  Languages,
  MessageSquareQuote,
  MoreHorizontal,
  PanelRightClose,
  PanelRightOpen,
  Pin,
  Sparkles,
  Star,
  Volume2,
} from "lucide-react";

import { ExportActions } from "@/components/export-actions";
import { StatusPill } from "@/components/status-pill";
import {
  buildInlineStudyBlocks,
  getHighlightMatches,
  type HighlightMatch,
  type InlineStudyCard,
} from "@/lib/reading/inline-study";

type Segment = {
  id: string;
  order_index: number;
  kind: "heading" | "paragraph";
  original_text: string;
  translated_text: string;
};

type VocabularyItem = {
  id: string;
  term: string;
  phonetic: string | null;
  part_of_speech: string | null;
  chinese_definition: string;
  example_sentence: string | null;
  difficulty: string | null;
  status: string;
};

type ExpressionItem = {
  id: string;
  expression: string;
  chinese_meaning: string;
  usage_note: string | null;
  example_sentence: string | null;
};

type DocumentInfo = {
  id: string;
  title: string;
  source_type: string;
  source_url: string | null;
  status: string;
  character_count: number;
};

type ReadingMode = "bilingual" | "original" | "translation" | "quiz";
type PanelTab = "vocabulary" | "expressions";
type SpeakerInfo = { name: string; time: string };

const modeLabels: Record<ReadingMode, string> = {
  bilingual: "双语",
  original: "原文",
  translation: "译文",
  quiz: "自测",
};

export function ReadingWorkbench({
  document,
  segments,
  vocabulary,
  expressions,
}: {
  document: DocumentInfo;
  segments: Segment[];
  vocabulary: VocabularyItem[];
  expressions: ExpressionItem[];
}) {
  const [mode, setMode] = useState<ReadingMode>("bilingual");
  const [panelTab, setPanelTab] = useState<PanelTab>("vocabulary");
  const [panelOpen, setPanelOpen] = useState(true);
  const [exportOpen, setExportOpen] = useState(false);
  const [selection, setSelection] = useState<{
    text: string;
    context: string;
    x: number;
    y: number;
    placement: "above" | "below";
    kind: "word" | "phrase";
  } | null>(null);
  const [toast, setToast] = useState<string | null>(null);
  const [isPending, startTransition] = useTransition();

  const stats = useMemo(() => {
    const characters =
      document.character_count ||
      segments.reduce((total, segment) => total + segment.original_text.length, 0);
    const words = segments.reduce(
      (total, segment) =>
        total + segment.original_text.split(/\s+/).filter(Boolean).length,
      0,
    );
    const minutes = Math.max(1, Math.ceil(words / 160));
    const difficulty = words > 7000 ? "B2 进阶" : words > 2500 ? "B1-B2" : "B1";
    return { characters, words, minutes, difficulty };
  }, [document.character_count, segments]);

  const displayTitle = useMemo(
    () => getDisplayTitle(document.title, segments),
    [document.title, segments],
  );
  const inlineBlocks = useMemo(
    () =>
      buildInlineStudyBlocks({
        segments,
        vocabulary,
        expressions,
      }),
    [expressions, segments, vocabulary],
  );

  useEffect(() => {
    function dismissSelection(event: PointerEvent) {
      const target = event.target as HTMLElement | null;
      if (target?.closest("[data-selection-toolbar]")) return;
      setSelection(null);
    }

    window.document.addEventListener("pointerdown", dismissSelection);
    return () => window.document.removeEventListener("pointerdown", dismissSelection);
  }, []);

  function captureSelection(context: string) {
    const currentSelection = window.getSelection();
    const text = currentSelection?.toString().trim();
    if (!text || text.length < 2) {
      setSelection(null);
      return;
    }
    const range =
      currentSelection && currentSelection.rangeCount > 0
        ? currentSelection.getRangeAt(0)
        : null;
    const rect = range?.getBoundingClientRect();
    if (!rect) return;

    const toolbarWidth = text.split(/\s+/).filter(Boolean).length > 1 ? 324 : 260;
    const center = rect.left + rect.width / 2;
    const x = Math.min(
      Math.max(center, toolbarWidth / 2 + 16),
      window.innerWidth - toolbarWidth / 2 - 16,
    );
    const placement = rect.top > 72 ? "above" : "below";
    const y = placement === "above" ? rect.top - 10 : rect.bottom + 10;

    setSelection({
      text: text.slice(0, 240),
      context: context.slice(0, 1200),
      x,
      y,
      placement,
      kind: text.split(/\s+/).filter(Boolean).length > 1 ? "phrase" : "word",
    });
  }

  function saveSelection(type: "vocabulary" | "expression") {
    if (!selection) return;
    setToast(null);
    startTransition(async () => {
      const response = await fetch(`/api/documents/${document.id}/selection`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          type,
          text: selection.text,
          context: selection.context,
        }),
      });
      if (!response.ok) {
        const payload = await response.json();
        setToast(payload.error?.message ?? "保存失败");
        return;
      }
      setToast(type === "vocabulary" ? "已加入生词本" : "已加入表达本");
      setSelection(null);
      window.location.reload();
    });
  }

  function speakSelection() {
    if (!selection || typeof window === "undefined" || !window.speechSynthesis) return;
    window.speechSynthesis.cancel();
    window.speechSynthesis.speak(new SpeechSynthesisUtterance(selection.text));
  }

  async function copySelection() {
    if (!selection || !navigator.clipboard) return;
    await navigator.clipboard.writeText(selection.text);
    setToast("已复制所选文本");
    setSelection(null);
  }

  function explainSelection() {
    if (!selection) return;
    const normalized = selection.text.toLocaleLowerCase();
    const matchedVocabulary = vocabulary.find(
      (item) => item.term.toLocaleLowerCase() === normalized,
    );
    if (matchedVocabulary) {
      setToast(`${matchedVocabulary.term}: ${matchedVocabulary.chinese_definition}`);
      setSelection(null);
      return;
    }

    const matchedExpression = expressions.find((item) => {
      const expression = item.expression.toLocaleLowerCase();
      return expression === normalized || expression.includes(normalized) || normalized.includes(expression);
    });
    if (matchedExpression) {
      setToast(`${matchedExpression.expression}: ${matchedExpression.chinese_meaning}`);
      setSelection(null);
      return;
    }

    setToast("这段还没有现成解释，可以先加入生词本或表达本。");
    setSelection(null);
  }

  return (
    <div className="space-y-8">
      <section className="toolbar-material sticky top-0 z-20 -mx-4 border-b border-[var(--line)] px-4 pt-5 sm:-mx-6 sm:px-6 lg:-mx-10 lg:px-10">
        <div className="mx-auto max-w-6xl">
          <div className="mx-auto max-w-[720px] text-left sm:text-center">
            <h1 className="text-[28px] font-bold leading-[1.2] text-[var(--foreground)] sm:text-[34px]">
              {displayTitle}
            </h1>
            <div className="mt-2 flex flex-wrap items-center gap-x-2 gap-y-1 text-[13px] leading-5 text-[var(--muted)] sm:justify-center">
              <span className="uppercase">{document.source_type}</span>
              <MetaDot />
              <span>{stats.words.toLocaleString()} 词</span>
              <MetaDot />
              <span>{stats.minutes} 分钟</span>
              <MetaDot />
              <span>{stats.difficulty}</span>
              <MetaDot />
              <span className="font-medium text-[#34c759]">
                {document.status === "ready" ? "✓ 已完成" : "处理中"}
              </span>
              {document.title !== displayTitle ? (
                <>
                  <MetaDot />
                  <span className="truncate">{document.title}</span>
                </>
              ) : null}
            </div>
          </div>

          <div className="mt-5 flex flex-wrap items-center justify-between gap-3 border-t border-[var(--line)] py-3">
            <div className="inline-grid grid-cols-4 rounded-full bg-[var(--surface-strong)] p-1">
              {(Object.keys(modeLabels) as ReadingMode[]).map((value) => (
                <button
                  key={value}
                  type="button"
                  onClick={() => setMode(value)}
                  className={`apple-spring h-9 rounded-full px-3 text-xs font-semibold transition duration-200 active:opacity-60 ${
                    mode === value
                      ? "bg-[var(--surface)] text-[var(--foreground)] shadow-[0_1px_2px_rgba(0,0,0,0.06)]"
                      : "text-[var(--muted)] hover:bg-[var(--paper)]"
                  }`}
                >
                  {modeLabels[value]}
                </button>
              ))}
            </div>

            <div className="flex items-center gap-2">
              <button
                type="button"
                onClick={() => setPanelOpen((value) => !value)}
                className="focus-ring apple-spring inline-flex h-9 items-center gap-2 rounded-full bg-[var(--surface-strong)] px-3 text-sm font-medium text-[var(--muted)] transition duration-200 hover:bg-[var(--paper)] hover:text-[var(--accent)] active:opacity-60"
              >
                {panelOpen ? (
                  <PanelRightClose className="h-4 w-4" />
                ) : (
                  <PanelRightOpen className="h-4 w-4" />
                )}
                学习面板
              </button>

              <div className="relative">
                <button
                  type="button"
                  onClick={() => setExportOpen((value) => !value)}
                  className="focus-ring apple-spring grid h-9 w-9 place-items-center rounded-full text-[var(--muted)] transition duration-200 hover:bg-[var(--surface-strong)] hover:text-[var(--accent)] active:opacity-60"
                  aria-label="更多操作"
                >
                  <MoreHorizontal className="h-5 w-5" />
                </button>
                {exportOpen ? (
                  <div className="thin-material absolute right-0 z-20 mt-2 w-56 rounded-[14px] border border-[var(--line)] p-3">
                    <div className="mb-2 flex items-center gap-2 text-xs font-semibold text-[var(--muted)]">
                      <FileDown className="h-3.5 w-3.5" />
                      导出阅读笔记
                    </div>
                    <ExportActions documentId={document.id} />
                  </div>
                ) : null}
              </div>
            </div>
          </div>
        </div>

        <div className="-mx-4 h-px overflow-hidden bg-transparent sm:-mx-6 lg:-mx-10">
          <div
            className="h-full rounded-full bg-[var(--accent)]"
            style={{
              width:
                document.status === "ready"
                  ? "100%"
                  : `${segments.length > 0 ? 42 : 12}%`,
            }}
          />
        </div>
      </section>

      <div
        className={`mx-auto grid max-w-6xl justify-center gap-8 ${
          panelOpen ? "xl:grid-cols-[minmax(0,720px)_340px]" : "xl:grid-cols-[minmax(0,720px)]"
        }`}
      >
        <section className="w-full max-w-[720px] space-y-8">
          {selection ? (
            <div
              data-selection-toolbar
              className={`selection-toolbar selection-toolbar-${selection.placement}`}
              style={{
                left: selection.x,
                top: selection.y,
                transform:
                  selection.placement === "above"
                    ? "translate(-50%, -100%)"
                    : "translate(-50%, 0)",
              }}
              onMouseDown={(event) => event.preventDefault()}
            >
              {selection.kind === "word" ? (
                <button
                  type="button"
                  disabled={isPending}
                  onClick={() => saveSelection("vocabulary")}
                  className="selection-toolbar-button"
                >
                  <Pin className="h-4 w-4" />
                  生词
                </button>
              ) : (
                <button
                  type="button"
                  disabled={isPending}
                  onClick={() => saveSelection("expression")}
                  className="selection-toolbar-button"
                >
                  <MessageSquareQuote className="h-4 w-4" />
                  表达
                </button>
              )}
              <span className="selection-toolbar-divider" />
              <button
                type="button"
                onClick={speakSelection}
                className="selection-toolbar-button"
              >
                <Volume2 className="h-4 w-4" />
                朗读
              </button>
              <span className="selection-toolbar-divider" />
              <button
                type="button"
                onClick={explainSelection}
                className="selection-toolbar-button"
              >
                <Sparkles className="h-4 w-4" />
                解释
              </button>
              {selection.kind === "phrase" ? (
                <>
                  <span className="selection-toolbar-divider" />
                  <button
                    type="button"
                    onClick={copySelection}
                    className="selection-toolbar-button"
                  >
                    <Copy className="h-4 w-4" />
                    复制
                  </button>
                </>
              ) : null}
            </div>
          ) : null}
          {toast ? (
            <p className="rounded-[14px] bg-[rgba(52,199,89,0.14)] px-3 py-2 text-sm text-[#248a3d]">
              {toast}
            </p>
          ) : null}

          {inlineBlocks.map((block) => {
            if (block.type === "study-card") {
              return (
                <InlineStudyCardView
                  key={block.card.id}
                  card={block.card}
                  onNotice={setToast}
                />
              );
            }

            const speaker = parseSpeakerLine(block.segment.original_text);
            if (speaker) {
              return <SpeakerHeading key={block.segment.id} speaker={speaker} />;
            }

            return (
              <ReadingParagraph
                key={block.segment.id}
                segment={block.segment}
                mode={mode}
                vocabulary={vocabulary}
                expressions={expressions}
                onSelect={captureSelection}
              />
            );
          })}
        </section>

        {panelOpen ? (
          <aside className="xl:sticky xl:top-24 xl:h-[calc(100vh-112px)]">
            <div className="thin-material flex h-full flex-col overflow-hidden rounded-[22px] border border-[var(--line)]">
              <div className="p-3">
                <div className="grid grid-cols-2 rounded-full bg-[var(--surface-strong)] p-1">
                  <button
                    type="button"
                    onClick={() => setPanelTab("vocabulary")}
                    className={`apple-spring flex h-10 items-center justify-center gap-2 rounded-full text-sm font-semibold transition duration-200 active:opacity-60 ${
                      panelTab === "vocabulary"
                        ? "bg-[var(--surface)] text-[var(--foreground)] shadow-[0_1px_2px_rgba(0,0,0,0.06)]"
                        : "text-[var(--muted)] hover:bg-[var(--paper)]"
                    }`}
                  >
                    <Languages className="h-4 w-4" />
                    生词
                  </button>
                  <button
                    type="button"
                    onClick={() => setPanelTab("expressions")}
                    className={`apple-spring flex h-10 items-center justify-center gap-2 rounded-full text-sm font-semibold transition duration-200 active:opacity-60 ${
                      panelTab === "expressions"
                        ? "bg-[var(--surface)] text-[var(--foreground)] shadow-[0_1px_2px_rgba(0,0,0,0.06)]"
                        : "text-[var(--muted)] hover:bg-[var(--paper)]"
                    }`}
                  >
                    <MessageSquareQuote className="h-4 w-4" />
                    表达
                  </button>
                </div>
              </div>

              <div className="min-h-0 flex-1 overflow-auto border-t border-[var(--line)] p-4">
                {panelTab === "vocabulary" ? (
                  <VocabularyPanel items={vocabulary} />
                ) : (
                  <ExpressionPanel items={expressions} />
                )}
              </div>
            </div>
          </aside>
        ) : null}
      </div>
    </div>
  );
}

function VocabularyPanel({ items }: { items: VocabularyItem[] }) {
  if (items.length === 0) {
    return <EmptyPanel text="处理中会逐步出现生词，划选单词也可以手动加入。" />;
  }

  return (
    <div className="space-y-3">
      {items.map((item) => (
        <article key={item.id} className="rounded-[14px] bg-[var(--surface)] p-3">
          <div className="flex items-start justify-between gap-3">
            <div>
              <h3 className="text-[17px] font-semibold text-[var(--foreground)]">{item.term}</h3>
              <p className="mt-1 text-xs text-[var(--muted)]">
                {[item.part_of_speech, item.phonetic].filter(Boolean).join(" · ")}
              </p>
            </div>
            {item.difficulty ? (
              <span className="rounded-full bg-[var(--accent-soft)] px-2 py-1 text-[11px] font-semibold text-[var(--accent)]">
                {item.difficulty}
              </span>
            ) : null}
          </div>
          <p className="mt-2 text-sm leading-6 text-[var(--foreground)]">
            {item.chinese_definition}
          </p>
          {item.example_sentence ? (
            <p className="mt-2 rounded-[10px] bg-[var(--paper)] p-2 text-xs leading-5 text-[var(--muted)]">
              {item.example_sentence}
            </p>
          ) : null}
          <StatusPill status={item.status} />
        </article>
      ))}
    </div>
  );
}

function ExpressionPanel({ items }: { items: ExpressionItem[] }) {
  if (items.length === 0) {
    return <EmptyPanel text="处理中会逐步出现地道表达，划选短语也可以手动加入。" />;
  }

  return (
    <div className="space-y-3">
      {items.map((item) => (
        <article key={item.id} className="rounded-[14px] bg-[var(--surface)] p-3">
          <h3 className="text-[17px] font-semibold text-[var(--foreground)]">
            {item.expression}
          </h3>
          <p className="mt-2 text-sm leading-6 text-[var(--foreground)]">
            {item.chinese_meaning}
          </p>
          {item.usage_note ? (
            <p className="mt-2 rounded-[10px] bg-[var(--paper)] p-2 text-xs leading-5 text-[var(--muted)]">
              {item.usage_note}
            </p>
          ) : null}
        </article>
      ))}
    </div>
  );
}

function EmptyPanel({ text }: { text: string }) {
  return (
    <div className="thin-material grid min-h-48 place-items-center rounded-[14px] p-5 text-center text-sm leading-6 text-[var(--muted)]">
      <div>
        <ChevronDown className="mx-auto mb-2 h-5 w-5 opacity-60" />
        {text}
      </div>
    </div>
  );
}

function MetaDot() {
  return <span className="text-[var(--tertiary)]">·</span>;
}

function parseSpeakerLine(text: string): SpeakerInfo | null {
  const match = text.trim().match(/^(.+?)\s*\((\d{2}:\d{2}:\d{2})\):$/);
  if (!match) return null;
  return { name: match[1], time: match[2] };
}

function getDisplayTitle(title: string, segments: Segment[]) {
  const cleanedTitle = title.replace(/\.(docx|pdf|md|txt)$/i, "").trim();
  const firstSpeaker = segments
    .map((segment) => parseSpeakerLine(segment.original_text))
    .find((speaker): speaker is SpeakerInfo => Boolean(speaker));

  if (firstSpeaker && /\.(docx|pdf|md|txt)$/i.test(title)) {
    return `${firstSpeaker.name} 访谈精读`;
  }

  return cleanedTitle || title;
}

function getSpeakerAccent(name: string) {
  const normalized = name.toLowerCase();
  if (normalized.includes("lenny")) return "var(--purple)";
  if (normalized.includes("cat")) return "var(--accent)";
  return "var(--accent)";
}

function SpeakerHeading({ speaker }: { speaker: SpeakerInfo }) {
  const accent = getSpeakerAccent(speaker.name);

  return (
    <div
      className="flex items-center justify-between gap-4 border-l-4 pl-3"
      style={{ borderColor: accent }}
    >
      <div className="flex min-w-0 items-center gap-2">
        <span
          className="h-1.5 w-1.5 shrink-0 rounded-full"
          style={{ backgroundColor: accent }}
        />
        <span className="truncate text-[17px] font-semibold leading-7 text-[var(--foreground)]">
          {speaker.name}
        </span>
      </div>
      <button
        type="button"
        className="apple-spring shrink-0 font-mono text-[13px] leading-5 text-[var(--tertiary)] transition duration-200 hover:text-[var(--accent)] active:opacity-60"
      >
        {speaker.time}
      </button>
    </div>
  );
}

function ReadingParagraph({
  segment,
  mode,
  vocabulary,
  expressions,
  onSelect,
}: {
  segment: Segment;
  mode: ReadingMode;
  vocabulary: VocabularyItem[];
  expressions: ExpressionItem[];
  onSelect: (context: string) => void;
}) {
  const sourceClassName =
    segment.kind === "heading"
      ? "text-[22px] font-semibold leading-[1.35] text-[var(--foreground)]"
      : "text-[17px] font-normal leading-[1.6] text-[var(--foreground)]";

  return (
    <article className="group relative py-1" onMouseUp={() => onSelect(segment.original_text)}>
      <div className="pointer-events-none absolute -right-14 top-0 hidden flex-col items-center gap-2 opacity-0 transition duration-200 group-hover:pointer-events-auto group-hover:opacity-100 xl:flex">
        <button
          type="button"
          className="thin-material apple-spring grid h-9 w-9 place-items-center rounded-full text-[var(--muted)] transition duration-200 hover:scale-[1.05] hover:text-[var(--accent)] active:opacity-60"
          title="朗读"
          aria-label="朗读这一段"
        >
          <Volume2 className="h-4 w-4" />
        </button>
        <button
          type="button"
          className="thin-material apple-spring grid h-9 w-9 place-items-center rounded-full text-[var(--muted)] transition duration-200 hover:scale-[1.05] hover:text-[var(--accent)] active:opacity-60"
          title="难点解析"
          aria-label="查看这一段的难点解析"
        >
          <BookOpenCheck className="h-4 w-4" />
        </button>
      </div>

      {mode !== "translation" ? (
        segment.kind === "heading" ? (
          <h2 className={sourceClassName}>
            <HighlightedText
              text={segment.original_text}
              vocabulary={vocabulary}
              expressions={expressions}
            />
          </h2>
        ) : (
          <p className={sourceClassName}>
            <HighlightedText
              text={segment.original_text}
              vocabulary={vocabulary}
              expressions={expressions}
            />
          </p>
        )
      ) : null}

      {mode === "quiz" ? (
        <details className="mt-3 rounded-[14px] bg-[var(--paper)] px-3 py-2 text-sm text-[var(--muted)]">
          <summary className="flex cursor-pointer items-center gap-2 font-semibold">
            <Eye className="h-4 w-4" />
            查看译文
          </summary>
          <p className="mt-2 border-l-[3px] border-[var(--accent)] pl-4 text-[15px] leading-[1.55] text-[var(--translation)]">
            {segment.translated_text}
          </p>
        </details>
      ) : mode !== "original" ? (
        <p
          className={`border-l-[3px] border-[var(--accent)] pl-4 text-[15px] font-normal leading-[1.55] text-[var(--translation)] transition duration-200 group-hover:text-[var(--muted)] ${
            mode === "translation" ? "mt-0" : "mt-3"
          }`}
        >
          {segment.translated_text}
        </p>
      ) : (
        <div className="mt-3 flex items-center gap-2 text-xs text-[var(--muted)]">
          <EyeOff className="h-3.5 w-3.5" />
          译文已隐藏
        </div>
      )}
    </article>
  );
}

function HighlightedText({
  text,
  vocabulary,
  expressions,
}: {
  text: string;
  vocabulary: VocabularyItem[];
  expressions: ExpressionItem[];
}) {
  const matches = getHighlightMatches(
    text,
    vocabulary.filter((item) => item.status !== "mastered"),
    expressions,
  );

  if (matches.length === 0) return text;

  const nodes: React.ReactNode[] = [];
  let cursor = 0;

  for (const match of matches) {
    if (match.start > cursor) {
      nodes.push(
        <Fragment key={`${match.id}-text-${cursor}`}>
          {text.slice(cursor, match.start)}
        </Fragment>,
      );
    }
    nodes.push(
      <span
        key={`${match.type}-${match.id}-${match.start}`}
        className={
          match.type === "vocabulary"
            ? "inline-vocab-highlight"
            : "inline-phrase-highlight"
        }
        title={getHighlightTitle(match, vocabulary, expressions)}
      >
        {text.slice(match.start, match.end)}
      </span>,
    );
    cursor = match.end;
  }

  if (cursor < text.length) {
    nodes.push(<Fragment key="tail">{text.slice(cursor)}</Fragment>);
  }

  return <>{nodes}</>;
}

function getHighlightTitle(
  match: HighlightMatch,
  vocabulary: VocabularyItem[],
  expressions: ExpressionItem[],
) {
  if (match.type === "vocabulary") {
    const item = vocabulary.find((entry) => entry.id === match.id);
    return item ? `${item.term}: ${item.chinese_definition}` : match.text;
  }

  const item = expressions.find((entry) => entry.id === match.id);
  return item ? `${item.expression}: ${item.chinese_meaning}` : match.text;
}

function InlineStudyCardView({
  card,
  onNotice,
}: {
  card: InlineStudyCard;
  onNotice: (message: string) => void;
}) {
  const storageKey = `era-inline-study-card:${card.id}`;
  const [collapsed, setCollapsed] = useState(
    () =>
      typeof window !== "undefined" &&
      window.localStorage.getItem(storageKey) === "collapsed",
  );

  function toggleCollapsed() {
    setCollapsed((value) => {
      const next = !value;
      window.localStorage.setItem(storageKey, next ? "collapsed" : "expanded");
      return next;
    });
  }

  const total = card.vocabulary.length + card.expressions.length;

  return (
    <section className="inline-study-card">
      <button
        type="button"
        onClick={toggleCollapsed}
        className="inline-study-card-header apple-spring w-full transition duration-200 active:opacity-60"
      >
        <span className="inline-flex items-center gap-2">
          <BookOpenCheck className="h-4 w-4 text-[var(--accent)]" />
          <span>
            这一段学到 {card.vocabulary.length} 个生词、{card.expressions.length} 个表达
          </span>
        </span>
        <span className="inline-flex items-center gap-1">
          {collapsed ? "展开" : "收起"}
          <ChevronDown
            className={`h-4 w-4 transition ${collapsed ? "-rotate-90" : ""}`}
          />
        </span>
      </button>

      {collapsed ? null : (
        <div className="mt-4 space-y-5">
          {card.vocabulary.length > 0 ? (
            <div>
              <StudySectionTitle>生词</StudySectionTitle>
              <div className="space-y-3">
                {card.vocabulary.map((item) => (
                  <div key={item.id} className="inline-study-entry">
                    <div className="flex items-start justify-between gap-3">
                      <div className="min-w-0">
                        <p className="flex flex-wrap items-baseline gap-x-2 gap-y-1">
                          <span className="text-[17px] font-semibold text-[var(--accent)]">
                            {item.term}
                          </span>
                          {item.phonetic ? (
                            <span className="font-mono text-[13px] text-[var(--muted)]">
                              {item.phonetic}
                            </span>
                          ) : null}
                          {item.part_of_speech ? (
                            <span className="text-[13px] text-[var(--muted)]">
                              {item.part_of_speech}
                            </span>
                          ) : null}
                          <span className="text-[17px] text-[var(--foreground)]">
                            {item.chinese_definition}
                          </span>
                        </p>
                        {item.example_sentence ? (
                          <p className="mt-1 text-[13px] leading-5 text-[var(--tertiary)]">
                            <HighlightedExample text={item.example_sentence} target={item.term} />
                          </p>
                        ) : null}
                      </div>
                      <StarButton active={item.status !== "new"} />
                    </div>
                  </div>
                ))}
              </div>
            </div>
          ) : null}

          {card.expressions.length > 0 ? (
            <div>
              <StudySectionTitle>地道表达</StudySectionTitle>
              <div className="space-y-3">
                {card.expressions.map((item) => (
                  <div key={item.id} className="inline-study-entry">
                    <div className="flex items-start justify-between gap-3">
                      <div className="min-w-0">
                        <p className="text-[17px] font-semibold leading-6 text-[var(--foreground)]">
                          {item.expression}
                        </p>
                        <p className="mt-1 text-[15px] leading-6 text-[var(--translation)]">
                          {item.chinese_meaning}
                        </p>
                        {item.usage_note ? (
                          <p className="mt-1 text-[13px] italic leading-5 text-[var(--tertiary)]">
                            {item.usage_note}
                          </p>
                        ) : null}
                      </div>
                      <StarButton active />
                    </div>
                  </div>
                ))}
              </div>
            </div>
          ) : null}

          <div className="flex flex-wrap gap-2 border-t border-[var(--line)] pt-4">
            <button
              type="button"
              onClick={() => onNotice(`${total} 个项目已在本文学习流中标记。`)}
              className="apple-spring inline-flex h-9 items-center gap-2 rounded-full bg-[var(--accent)] px-3 text-sm font-semibold text-white transition duration-200 active:opacity-60"
            >
              <Star className="h-4 w-4" />
              全部加入复习
            </button>
            <button
              type="button"
              onClick={() => onNotice("右侧学习面板保留全文词汇汇总，适合通览和筛选。")}
              className="apple-spring inline-flex h-9 items-center gap-2 rounded-full bg-[var(--surface)] px-3 text-sm font-semibold text-[var(--accent)] transition duration-200 hover:bg-[var(--paper)] active:opacity-60"
            >
              <Languages className="h-4 w-4" />
              在面板查看
            </button>
          </div>
        </div>
      )}
    </section>
  );
}

function StudySectionTitle({ children }: { children: ReactNode }) {
  return (
    <div className="mb-2 flex items-center gap-3">
      <p className="text-[13px] font-normal uppercase tracking-[0.04em] text-[var(--muted)]">
        {children}
      </p>
      <span className="h-px flex-1 bg-[var(--line)]" />
    </div>
  );
}

function HighlightedExample({ text, target }: { text: string; target: string }) {
  const found = text.toLocaleLowerCase().indexOf(target.toLocaleLowerCase());
  if (found < 0) return text;

  return (
    <>
      {text.slice(0, found)}
      <span className="decoration-[var(--orange)] decoration-1 underline underline-offset-4">
        {text.slice(found, found + target.length)}
      </span>
      {text.slice(found + target.length)}
    </>
  );
}

function StarButton({ active }: { active: boolean }) {
  return (
    <button
      type="button"
      className={`apple-spring grid h-6 w-6 shrink-0 place-items-center rounded-full transition duration-200 hover:scale-110 active:opacity-60 ${
        active ? "text-[#ffcc00]" : "text-[var(--tertiary)] hover:text-[#ffcc00]"
      }`}
      aria-label={active ? "已收藏" : "收藏"}
    >
      <Star className={`h-4 w-4 ${active ? "fill-current" : ""}`} />
    </button>
  );
}
