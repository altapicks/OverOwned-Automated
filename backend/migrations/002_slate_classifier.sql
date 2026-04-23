-- ═══════════════════════════════════════════════════════════════════════
-- Migration 002 — Slate classifier support
--
-- Adds is_fallback flag to slates (true = Showdown ingested because no
-- Classic was available) and ensures skipped slates can be logged without
-- polluting the slates table.
--
-- Run in Supabase SQL editor. Idempotent — safe to re-run.
-- ═══════════════════════════════════════════════════════════════════════

-- Is-fallback flag on slates.
alter table slates add column if not exists is_fallback boolean not null default false;

-- Track which DK draft groups we deliberately skipped (and why). Useful for
-- debugging "why isn't my slate showing up" questions.
create table if not exists skipped_draft_groups (
    id              bigserial primary key,
    dk_draft_group_id bigint not null,
    sport           text not null,
    contest_type    text,
    slate_label     text,
    reason          text not null,     -- 'classified_other' | 'showdown_without_fallback' | 'disallowed_type'
    classification  text,              -- 'classic' | 'showdown' | 'other'
    context         jsonb,
    first_seen_at   timestamptz not null default now(),
    unique (dk_draft_group_id)
);

create index if not exists idx_skipped_dgid on skipped_draft_groups(dk_draft_group_id);
create index if not exists idx_skipped_sport_time on skipped_draft_groups(sport, first_seen_at desc);

-- Ensure RLS on new table
alter table skipped_draft_groups enable row level security;
