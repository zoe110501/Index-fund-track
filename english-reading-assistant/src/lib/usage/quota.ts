export const DEFAULT_MONTHLY_CHARACTER_QUOTA = 250_000;

export type UsageEventKind =
  | "document_processed"
  | "document_imported"
  | "export_created"
  | "ai_retry";

export type UsageEvent = {
  kind: UsageEventKind;
  quantity: number;
};

export type QuotaSnapshot = {
  limit: number;
  used: number;
  remaining: number;
  canProcess: boolean;
};

export function getQuotaSnapshot(input: {
  monthlyCharacterQuota?: number | null;
  events: UsageEvent[];
}): QuotaSnapshot {
  const limit = input.monthlyCharacterQuota ?? DEFAULT_MONTHLY_CHARACTER_QUOTA;
  const used = input.events
    .filter((event) => event.kind === "document_processed")
    .reduce((total, event) => total + Math.max(0, event.quantity), 0);
  const remaining = Math.max(0, limit - used);

  return {
    limit,
    used,
    remaining,
    canProcess: remaining > 0,
  };
}
