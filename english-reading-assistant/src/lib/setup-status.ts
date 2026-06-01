import { getMissingSupabaseEnv } from "@/lib/env";
import { createSupabaseAdminClient } from "@/lib/supabase/admin";

export type SetupStatus = {
  missingEnv: string[];
  databaseReady: boolean;
  databaseError?: string;
};

export async function getSetupStatus(): Promise<SetupStatus> {
  const missingEnv = getMissingSupabaseEnv();
  if (missingEnv.length > 0) {
    return { missingEnv, databaseReady: false };
  }

  try {
    const admin = createSupabaseAdminClient();
    const { error } = await admin.from("profiles").select("id").limit(1);
    if (error) {
      return {
        missingEnv,
        databaseReady: false,
        databaseError: error.message,
      };
    }
  } catch (error) {
    return {
      missingEnv,
      databaseReady: false,
      databaseError: error instanceof Error ? error.message : "Unknown error",
    };
  }

  return { missingEnv, databaseReady: true };
}
