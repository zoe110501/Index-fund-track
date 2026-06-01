import { describe, expect, it } from "vitest";

import { DEFAULT_MONTHLY_CHARACTER_QUOTA, getQuotaSnapshot } from "./quota";

describe("getQuotaSnapshot", () => {
  it("subtracts processing usage from the monthly character quota", () => {
    const snapshot = getQuotaSnapshot({
      monthlyCharacterQuota: DEFAULT_MONTHLY_CHARACTER_QUOTA,
      events: [
        { kind: "document_processed", quantity: 1200 },
        { kind: "document_processed", quantity: 3800 },
        { kind: "export_created", quantity: 1 },
      ],
    });

    expect(snapshot).toEqual({
      limit: DEFAULT_MONTHLY_CHARACTER_QUOTA,
      used: 5000,
      remaining: DEFAULT_MONTHLY_CHARACTER_QUOTA - 5000,
      canProcess: true,
    });
  });

  it("blocks processing when no quota remains", () => {
    const snapshot = getQuotaSnapshot({
      monthlyCharacterQuota: 1000,
      events: [{ kind: "document_processed", quantity: 1200 }],
    });

    expect(snapshot).toMatchObject({
      used: 1200,
      remaining: 0,
      canProcess: false,
    });
  });
});
