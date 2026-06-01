import { describe, expect, it } from "vitest";

import { deleteExpressionItem } from "./remove";

class FakeDeleteQuery {
  filters: Array<{ column: string; value: string }> = [];
  selected: string | null = null;

  delete() {
    return this;
  }

  eq(column: string, value: string) {
    this.filters.push({ column, value });
    return this;
  }

  select(columns: string) {
    this.selected = columns;
    return this;
  }

  async maybeSingle() {
    return { data: { id: "expr-1" }, error: null };
  }
}

class FakeSupabase {
  query = new FakeDeleteQuery();
  table: string | null = null;

  from(table: string) {
    this.table = table;
    return this.query;
  }
}

describe("deleteExpressionItem", () => {
  it("deletes an expression only inside the current user's ownership scope", async () => {
    const supabase = new FakeSupabase();

    const deleted = await deleteExpressionItem(supabase, {
      expressionId: "expr-1",
      userId: "user-1",
    });

    expect(deleted).toBe(true);
    expect(supabase.table).toBe("expression_items");
    expect(supabase.query.selected).toBe("id");
    expect(supabase.query.filters).toEqual([
      { column: "id", value: "expr-1" },
      { column: "user_id", value: "user-1" },
    ]);
  });
});
