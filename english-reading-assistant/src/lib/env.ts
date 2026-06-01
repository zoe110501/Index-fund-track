export function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`${name} is required.`);
  }
  return value;
}

export const SUPABASE_ENV_NAMES = [
  "NEXT_PUBLIC_SUPABASE_URL",
  "NEXT_PUBLIC_SUPABASE_ANON_KEY",
  "SUPABASE_SERVICE_ROLE_KEY",
] as const;

export function getMissingSupabaseEnv(): string[] {
  return SUPABASE_ENV_NAMES.filter((name) => !process.env[name]);
}

export function hasSupabaseEnv(): boolean {
  return getMissingSupabaseEnv().length === 0;
}

export function getAppUrl(): string {
  return process.env.NEXT_PUBLIC_APP_URL ?? "http://localhost:3000";
}

export function getAdminEmails(): string[] {
  return (process.env.ADMIN_EMAILS ?? "")
    .split(",")
    .map((email) => email.trim().toLowerCase())
    .filter(Boolean);
}

export function isLocalAdminLoginEnabled(): boolean {
  if (process.env.ENABLE_LOCAL_ADMIN_LOGIN === "true") {
    return true;
  }

  return getAppUrl().startsWith("http://localhost");
}

export type AiProvider = "openai" | "deepseek";

export function getAiProvider(): AiProvider {
  const provider = process.env.AI_PROVIDER?.trim().toLowerCase();
  if (provider === "deepseek" || provider === "openai") {
    return provider;
  }

  return process.env.DEEPSEEK_API_KEY ? "deepseek" : "openai";
}

export function hasAiEnv(): boolean {
  const provider = getAiProvider();
  return provider === "deepseek"
    ? Boolean(process.env.DEEPSEEK_API_KEY)
    : Boolean(process.env.OPENAI_API_KEY);
}

export function getAiProviderLabel(): string {
  return getAiProvider() === "deepseek" ? "DeepSeek" : "OpenAI";
}
