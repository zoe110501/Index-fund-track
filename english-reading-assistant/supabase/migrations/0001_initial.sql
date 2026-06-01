create extension if not exists pgcrypto;

create or replace function public.set_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

create table public.profiles (
  id uuid primary key references auth.users(id) on delete cascade,
  email text not null,
  display_name text,
  role text not null default 'learner' check (role in ('learner', 'admin')),
  status text not null default 'active' check (status in ('active', 'paused')),
  learner_level text not null default 'B1',
  monthly_character_quota integer not null default 250000,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index profiles_email_unique on public.profiles (lower(email));

create table public.invites (
  id uuid primary key default gen_random_uuid(),
  email text not null,
  code_hash text not null unique,
  status text not null default 'pending' check (status in ('pending', 'accepted', 'revoked', 'expired')),
  expires_at timestamptz not null default now() + interval '14 days',
  accepted_by uuid references public.profiles(id) on delete set null,
  created_by uuid references public.profiles(id) on delete set null,
  created_at timestamptz not null default now(),
  accepted_at timestamptz
);

create unique index invites_pending_email_unique
on public.invites (lower(email))
where status = 'pending';

create table public.documents (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references public.profiles(id) on delete cascade,
  title text not null,
  source_type text not null check (source_type in ('web', 'pdf', 'docx')),
  source_url text,
  raw_text text,
  raw_file_path text,
  mime_type text,
  status text not null default 'queued' check (status in ('queued', 'processing', 'ready', 'failed')),
  character_count integer not null default 0,
  error_message text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table public.segments (
  id uuid primary key default gen_random_uuid(),
  document_id uuid not null references public.documents(id) on delete cascade,
  user_id uuid not null references public.profiles(id) on delete cascade,
  order_index integer not null,
  kind text not null default 'paragraph' check (kind in ('heading', 'paragraph')),
  original_text text not null,
  translated_text text not null,
  created_at timestamptz not null default now(),
  unique (document_id, order_index)
);

create table public.vocabulary_items (
  id uuid primary key default gen_random_uuid(),
  document_id uuid not null references public.documents(id) on delete cascade,
  user_id uuid not null references public.profiles(id) on delete cascade,
  term text not null,
  phonetic text,
  part_of_speech text,
  chinese_definition text not null,
  example_sentence text,
  difficulty text,
  status text not null default 'new' check (status in ('new', 'known', 'learning', 'mastered')),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table public.expression_items (
  id uuid primary key default gen_random_uuid(),
  document_id uuid not null references public.documents(id) on delete cascade,
  user_id uuid not null references public.profiles(id) on delete cascade,
  expression text not null,
  chinese_meaning text not null,
  usage_note text,
  example_sentence text,
  rewrite_template text,
  created_at timestamptz not null default now()
);

create table public.processing_jobs (
  id uuid primary key default gen_random_uuid(),
  document_id uuid references public.documents(id) on delete cascade,
  user_id uuid not null references public.profiles(id) on delete cascade,
  kind text not null check (kind in ('process_document', 'export_document', 'cleanup_raw_file')),
  status text not null default 'queued' check (status in ('queued', 'running', 'succeeded', 'failed')),
  inngest_event_id text,
  attempts integer not null default 0,
  error_message text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table public.exports (
  id uuid primary key default gen_random_uuid(),
  document_id uuid not null references public.documents(id) on delete cascade,
  user_id uuid not null references public.profiles(id) on delete cascade,
  format text not null check (format in ('markdown', 'pdf')),
  status text not null default 'queued' check (status in ('queued', 'running', 'ready', 'failed')),
  file_path text,
  error_message text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table public.usage_events (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references public.profiles(id) on delete cascade,
  document_id uuid references public.documents(id) on delete set null,
  kind text not null check (kind in ('document_imported', 'document_processed', 'export_created', 'ai_retry')),
  quantity integer not null default 0,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create table public.extension_tokens (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references public.profiles(id) on delete cascade,
  name text not null default 'Browser extension',
  token_hash text not null unique,
  last_used_at timestamptz,
  revoked_at timestamptz,
  created_at timestamptz not null default now()
);

create index documents_user_created_idx on public.documents (user_id, created_at desc);
create index segments_document_order_idx on public.segments (document_id, order_index);
create index vocabulary_user_status_idx on public.vocabulary_items (user_id, status, created_at desc);
create index expressions_document_idx on public.expression_items (document_id);
create index usage_user_created_idx on public.usage_events (user_id, created_at desc);

create trigger profiles_set_updated_at
before update on public.profiles
for each row execute function public.set_updated_at();

create trigger documents_set_updated_at
before update on public.documents
for each row execute function public.set_updated_at();

create trigger vocabulary_set_updated_at
before update on public.vocabulary_items
for each row execute function public.set_updated_at();

create trigger processing_jobs_set_updated_at
before update on public.processing_jobs
for each row execute function public.set_updated_at();

create trigger exports_set_updated_at
before update on public.exports
for each row execute function public.set_updated_at();

create or replace function public.is_admin()
returns boolean
language sql
security definer
set search_path = public
as $$
  select exists (
    select 1
    from public.profiles
    where id = auth.uid()
      and role = 'admin'
      and status = 'active'
  );
$$;

alter table public.profiles enable row level security;
alter table public.invites enable row level security;
alter table public.documents enable row level security;
alter table public.segments enable row level security;
alter table public.vocabulary_items enable row level security;
alter table public.expression_items enable row level security;
alter table public.processing_jobs enable row level security;
alter table public.exports enable row level security;
alter table public.usage_events enable row level security;
alter table public.extension_tokens enable row level security;

create policy "profiles_read_own_or_admin"
on public.profiles for select
using (auth.uid() = id or public.is_admin());

create policy "profiles_update_own_or_admin"
on public.profiles for update
using (auth.uid() = id or public.is_admin())
with check (auth.uid() = id or public.is_admin());

create policy "invites_admin_all"
on public.invites for all
using (public.is_admin())
with check (public.is_admin());

create policy "documents_owner_all"
on public.documents for all
using (auth.uid() = user_id or public.is_admin())
with check (auth.uid() = user_id or public.is_admin());

create policy "segments_owner_all"
on public.segments for all
using (auth.uid() = user_id or public.is_admin())
with check (auth.uid() = user_id or public.is_admin());

create policy "vocabulary_owner_all"
on public.vocabulary_items for all
using (auth.uid() = user_id or public.is_admin())
with check (auth.uid() = user_id or public.is_admin());

create policy "expressions_owner_all"
on public.expression_items for all
using (auth.uid() = user_id or public.is_admin())
with check (auth.uid() = user_id or public.is_admin());

create policy "jobs_owner_read"
on public.processing_jobs for select
using (auth.uid() = user_id or public.is_admin());

create policy "exports_owner_all"
on public.exports for all
using (auth.uid() = user_id or public.is_admin())
with check (auth.uid() = user_id or public.is_admin());

create policy "usage_owner_read"
on public.usage_events for select
using (auth.uid() = user_id or public.is_admin());

create policy "extension_tokens_owner_all"
on public.extension_tokens for all
using (auth.uid() = user_id or public.is_admin())
with check (auth.uid() = user_id or public.is_admin());

insert into storage.buckets (id, name, public, file_size_limit)
values
  ('raw-documents', 'raw-documents', false, 26214400),
  ('exports', 'exports', false, 26214400)
on conflict (id) do nothing;

create policy "raw_documents_owner_objects"
on storage.objects for all
using (
  bucket_id = 'raw-documents'
  and (storage.foldername(name))[1] = auth.uid()::text
)
with check (
  bucket_id = 'raw-documents'
  and (storage.foldername(name))[1] = auth.uid()::text
);

create policy "exports_owner_objects"
on storage.objects for all
using (
  bucket_id = 'exports'
  and (storage.foldername(name))[1] = auth.uid()::text
)
with check (
  bucket_id = 'exports'
  and (storage.foldername(name))[1] = auth.uid()::text
);
