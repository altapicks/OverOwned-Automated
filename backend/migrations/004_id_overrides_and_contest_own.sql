-- ═══════════════════════════════════════════════════════════════════════
-- Migration 004 — Manual overrides for DK IDs + contest ownership tracking
--
-- Flat, idempotent. Safe to re-run. Paste whole file into Supabase SQL
-- editor and click Run.
--
-- Adds:
--   * slate_players.dk_player_id_override          (user-settable, not touched by the ingestion worker)
--   * contest_ownership                            (actual field ownership % per player, uploaded once per slate)
--   * contest_ownership_history                    (append-only snapshots of every upload, for audit)
--
-- Problem this solves:
--   The slate_watcher worker runs every 15 min and upserts slate_players
--   with the dk_player_id it scraped from DK. This overwrites any manual
--   SQL edits to dk_player_id — making operator corrections impossible
--   to keep in place across ingestion cycles.
--
--   Mirrors the pattern of ss_pool_own: the worker writes to the raw column,
--   the admin/operator writes to the override column, and the frontend reads
--   the override-or-fallback value. Worker touches dk_player_id, never touches
--   dk_player_id_override.
-- ═══════════════════════════════════════════════════════════════════════


-- ── 1. slate_players.dk_player_id_override ──────────────────────────────
alter table slate_players
  add column if not exists dk_player_id_override bigint;

comment on column slate_players.dk_player_id_override is
'Manual override for dk_player_id. Populated by operator when the scraped
dk_player_id is wrong/stale and breaks DK upload CSVs. slate_reader.py
prefers this value when present. The slate_watcher worker does NOT touch
this column — it persists across ingestion cycles. Reset to NULL to
fall back to the scraped dk_player_id.';


-- ── 2. contest_ownership ────────────────────────────────────────────────
-- One row per (slate, player). Upserting replaces the stored ownership
-- for that player on that slate. Used by the Live Leverage Tracker tab
-- to show real field ownership vs our sim.
create table if not exists contest_ownership (
    slate_id        uuid not null references slates(id) on delete cascade,
    player_name     text not null,
    actual_own_pct  numeric(5,2) not null check (actual_own_pct >= 0 and actual_own_pct <= 200),
    uploaded_at     timestamptz not null default now(),
    uploaded_by     text,
    contest_name    text,
    total_entries   int,
    primary key (slate_id, player_name)
);

comment on table contest_ownership is
'Actual field ownership from DK contest entrants export. Shared across all
users of the app — one operator uploads the CSV, everyone sees the same
leverage data. Upserting replaces any prior value for that (slate, player).
Cap is 200% to allow Showdown sums (CPT + UTIL) to exceed 100.';

create index if not exists idx_contest_ownership_slate
  on contest_ownership(slate_id);


-- ── 3. contest_ownership_history ────────────────────────────────────────
-- Append-only snapshot table. One row per CSV upload, captures the full
-- (slate, player, actual_own_pct) snapshot. Useful for audit and for
-- computing ownership drift across multiple uploads of the same contest.
create table if not exists contest_ownership_history (
    id              uuid primary key default gen_random_uuid(),
    slate_id        uuid not null references slates(id) on delete cascade,
    player_name     text not null,
    actual_own_pct  numeric(5,2) not null,
    uploaded_at     timestamptz not null default now(),
    uploaded_by     text,
    contest_name    text,
    total_entries   int
);

comment on table contest_ownership_history is
'Append-only log of every contest CSV upload. Unlike contest_ownership,
rows are never overwritten. Each upload inserts a full snapshot (one row
per player). Used for drift analysis and post-mortem audits.';

create index if not exists idx_contest_ownership_history_slate
  on contest_ownership_history(slate_id, uploaded_at desc);


-- ── 4. matches.opening_odds ─────────────────────────────────────────────
-- Captures the first-ever odds observed for a match, frozen for life.
-- Used by the frontend to show canonical "opened at" vs "current" movement
-- on archived slates (live slates use per-user localStorage baselines as
-- before). Worker writes this exactly ONCE per match — first time odds
-- appear for that match — and never updates it thereafter.
alter table matches
  add column if not exists opening_odds jsonb not null default '{}'::jsonb;

comment on column matches.opening_odds is
'First-observed odds for this match. Frozen on first ingest, never updated.
Shape mirrors matches.odds: {"kalshi": {"implied_prob_a": ..., "implied_prob_b": ..., "fetched_at": ...}, "the_odds_api": {...}}. Used to compute canonical
closing-line movement on archived slates. Empty object means odds were
never ingested for this match (old/historical data).';


-- ── 5. RLS ──────────────────────────────────────────────────────────────
-- Reads public. Writes require admin (enforced at the API layer via
-- the service_role key + is_admin_user check; RLS here provides a
-- second layer for any direct anon-key writes).
alter table contest_ownership enable row level security;
alter table contest_ownership_history enable row level security;

drop policy if exists "public read contest_ownership" on contest_ownership;
create policy "public read contest_ownership"
  on contest_ownership for select
  using (true);

drop policy if exists "public read contest_ownership_history" on contest_ownership_history;
create policy "public read contest_ownership_history"
  on contest_ownership_history for select
  using (true);

-- No INSERT/UPDATE/DELETE policies for anon — only service_role (which
-- bypasses RLS entirely) can write.


-- ═══════════════════════════════════════════════════════════════════════
-- DONE. Expected output:
--   ALTER TABLE
--   COMMENT
--   CREATE TABLE
--   COMMENT
--   CREATE INDEX
--   CREATE TABLE
--   COMMENT
--   CREATE INDEX
--   ALTER TABLE
--   ALTER TABLE
--   DROP POLICY (if re-running) / CREATE POLICY
--   DROP POLICY / CREATE POLICY
-- ═══════════════════════════════════════════════════════════════════════
