type DeleteResult = {
  data: { id: string } | null;
  error: unknown | null;
};

type DeleteQuery = {
  eq(column: string, value: string): DeleteQuery;
  select(columns: string): {
    maybeSingle(): PromiseLike<DeleteResult>;
  };
};

type ExpressionDeleteClient = {
  from(table: "expression_items"): {
    delete(): DeleteQuery;
  };
};

export async function deleteExpressionItem(
  client: ExpressionDeleteClient,
  {
    expressionId,
    userId,
  }: {
    expressionId: string;
    userId: string;
  },
) {
  const { data, error } = await client
    .from("expression_items")
    .delete()
    .eq("id", expressionId)
    .eq("user_id", userId)
    .select("id")
    .maybeSingle();

  if (error) throw error;
  return Boolean(data);
}
