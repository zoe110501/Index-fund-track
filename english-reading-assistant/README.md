# 英读助手

邀请制英语精读双语助手 MVP。用户可以上传 PDF/DOCX，或通过 Chrome/Edge 插件保存网页，后台生成逐段双语精读、生词表、地道表达表，并导出 Markdown/PDF。

## Stack

- Next.js App Router + TypeScript + Tailwind CSS
- Supabase Auth / Postgres / Storage / RLS
- Inngest background jobs
- OpenAI Structured Outputs or DeepSeek JSON Output
- Chrome/Edge Manifest V3 extension
- Vitest core logic tests

## Local Setup

```bash
npm install
cp .env.example .env.local
npm run dev
```

On Windows, you can also use the startup batch file:

```bat
auto-start.bat
auto-start.bat dev
auto-start.bat install
auto-start.bat uninstall
auto-start.bat status
```

`auto-start.bat` starts the server in the background and opens `http://localhost:3000/login`. `auto-start.bat install` registers a current-user Startup shortcut. Startup logs are written to `logs/auto-start.log`, and Next.js server logs are written to `logs/server.log`. Set `ERA_PORT` before running the script if you want another port.

Required environment variables:

```bash
NEXT_PUBLIC_APP_URL=http://localhost:3000
NEXT_PUBLIC_SUPABASE_URL=...
NEXT_PUBLIC_SUPABASE_ANON_KEY=...
SUPABASE_SERVICE_ROLE_KEY=...
OPENAI_API_KEY=...
OPENAI_MODEL=gpt-4o-mini
ADMIN_EMAILS=you@example.com
```

Optional:

```bash
AI_PROVIDER=deepseek
AI_BATCH_CHAR_LIMIT=12000
DEEPSEEK_API_KEY=...
DEEPSEEK_MODEL=deepseek-reasoner
DEEPSEEK_BASE_URL=https://api.deepseek.com
INNGEST_EVENT_KEY=...
INNGEST_SIGNING_KEY=...
PDF_CJK_FONT_PATH=/path/to/NotoSansCJK-Regular.otf
```

If you use DeepSeek, set `AI_PROVIDER=deepseek` and `DEEPSEEK_API_KEY`.
`deepseek-reasoner` is supported; the app uses DeepSeek JSON Output and skips
OpenAI-only structured-output helpers for that provider.

`PDF_CJK_FONT_PATH` is strongly recommended in production so Simplified Chinese text renders correctly in exported PDF files.

## Supabase

Apply the migration in `supabase/migrations/0001_initial.sql`.

It creates:

- `profiles`, `invites`, `documents`, `segments`
- `vocabulary_items`, `expression_items`
- `processing_jobs`, `exports`, `usage_events`, `extension_tokens`
- private Storage buckets: `raw-documents`, `exports`
- Row Level Security policies for owner access and admin access

Bootstrap the first admin by setting `ADMIN_EMAILS` before logging in. The first login with an admin email automatically creates an admin profile.

## Background Jobs

Inngest endpoint:

```text
/api/inngest
```

Events:

- `document/process.requested`
- `document/export.requested`
- hourly cron cleanup for raw uploaded files older than 24 hours after successful processing

## Browser Extension

The extension lives in `extension/`.

1. Open `/documents/import`.
2. Generate a browser extension Token.
3. Open Chrome/Edge extension management.
4. Load the unpacked `extension` folder.
5. Fill in the deployed app URL and Token.
6. Click “导入当前页” on an English article page.

## Production Deploy

1. Create Supabase project and apply the SQL migration.
2. Create Vercel project from this folder.
3. Add all environment variables from `.env.example`.
4. Configure Inngest with the deployed `/api/inngest` endpoint.
5. Set Supabase Auth redirect URL to `${NEXT_PUBLIC_APP_URL}/auth/callback`.
6. Log in with an email from `ADMIN_EMAILS`.
7. Use the admin panel to invite users.

## Verification

```bash
npm test
npm run lint
npm run build
```
