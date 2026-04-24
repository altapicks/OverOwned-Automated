-- ═══════════════════════════════════════════════════════════════════════
-- Migration 003 — Odds Ingestion + Manual PrizePicks Entry
--
-- Flat, idempotent. Safe to re-run. Paste whole file into Supabase SQL
-- editor and click Run.
--
-- Adds:
--   * matches.odds jsonb column                (populated by odds_api + kalshi services)
--   * odds_history                             (append-only snapshots of every odds fetch)
--   * prizepicks_lines                         (current state of every live PP projection)
--   * line_movements                           (append-only diff log, written by trigger)
--   * admin_users                              (who is allowed to write PP lines)
--   * is_admin() helper                        (used by RLS + API route guards)
--   * prizepicks_line_movement_trigger         (fires on INSERT/UPDATE of prizepicks_lines)
-- ═══════════════════════════════════════════════════════════════════════

-- ── 1. Extend matches table ────────────────────────────────────
alter table matches add column if not exists odds jsonb not null default '{}'::jsonb;

comment on column matches.odds is
'Market odds keyed by source, e.g. { "the_odds_api": { ml_a: -175, ml_b: 137, gw_a_line: 12.5, gw_a_over: -138, ..., raw: {...}, fetched_at: "..." }, "kalshi": { implied_prob_a: 0.62, market_ticker: "...", fetched_at: "..." } }. Flat per-engine keys (ml_a, ml_b, gw_a_line, etc.) at source root so engine.js merging is trivial.';

create index if not exists idx_matches_odds_not_empty on matches((odds::text)) where odds::text <> '{}';

-- ── 2. odds_history ─────────────────────────────────────────────
create table if not exists odds_history (
    id uuid primary key default gen_random_uuid(),
    match_id uuid references matches(id) on delete cascade,
    slate_id uuid references slates(id) on delete cascade,
    source text not null,
    market text not null,
    payload jsonb not null,
    fetched_at timestamptz not null default now()
);

create index if not exists idx_odds_history_match_source_time on odds_history(match_id, source, fetched_at desc);
create index if not exists idx_odds_history_slate_time on odds_history(slate_id, fetched_at desc);

-- ── 3. admin_users ──────────────────────────────────────────────
create table if not exists admin_users (
    user_id uuid primary key references auth.users(id) on delete cascade,
    granted_at timestamptz not null default now(),
    granted_by uuid references auth.users(id),
    notes text
);

-- Used by RLS policies and API route guards.
create or replace function is_admin() returns boolean
language sql stable security definer
as $$
  select exists (select 1 from admin_users where user_id = auth.uid());
$$;

-- ── 4. prizepicks_lines ─────────────────────────────────────────
create table if not exists prizepicks_lines (
    id uuid primary key default gen_random_uuid(),
    slate_id uuid not null references slates(id) on delete cascade,
    match_id uuid references matches(id),
    player_id text references players(canonical_id),
    raw_player_name text not null,
    stat_type text not null,
    current_line numeric not null,
    league text not null default 'tennis',
    notes text,
    entered_by uuid references auth.users(id),
    is_active boolean not null default true,
    first_seen_at timestamptz not null default now(),
    last_updated_at timestamptz not null default now()
);

create unique index if not exists uq_pp_lines_slate_player_stat
    on prizepicks_lines(slate_id, raw_player_name, stat_type)
    where is_active = true;

create index if not exists idx_pp_lines_slate_active on prizepicks_lines(slate_id, is_active);
create index if not exists idx_pp_lines_player on prizepicks_lines(player_id, is_active);

-- ── 5. line_movements ───────────────────────────────────────────
create table if not exists line_movements (
    id uuid primary key default gen_random_uuid(),
    pp_line_id uuid references prizepicks_lines(id) on delete cascade,
    slate_id uuid not null references slates(id) on delete cascade,
    player_id text references players(canonical_id),
    raw_player_name text not null,
    stat_type text not null,
    old_line numeric,
    new_line numeric not null,
    delta numeric generated always as (new_line - coalesce(old_line, new_line)) stored,
    direction text not null,
    changed_by uuid references auth.users(id),
    detected_at timestamptz not null default now()
);

create index if not exists idx_line_movements_slate_time on line_movements(slate_id, detected_at desc);
create index if not exists idx_line_movements_time on line_movements(detected_at desc);
create index if not exists idx_line_movements_player_time on line_movements(player_id, detected_at desc);

-- ── 6. Trigger: auto-populate line_movements on prizepicks_lines changes ─
create or replace function record_line_movement() returns trigger
language plpgsql as $$
declare
    v_direction text;
    v_old_line numeric;
    v_changed_by uuid;
begin
    v_changed_by := coalesce(new.entered_by, old.entered_by);

    if tg_op = 'INSERT' then
        v_direction := 'new';
        v_old_line := null;
    elsif tg_op = 'UPDATE' then
        -- Case: soft-delete (is_active flipped true → false)
        if old.is_active = true and new.is_active = false then
            v_direction := 'removed';
            v_old_line := old.current_line;
        -- Case: re-activation (false → true) — treat as new
        elsif old.is_active = false and new.is_active = true then
            v_direction := 'new';
            v_old_line := null;
        -- Case: line changed while active
        elsif new.is_active = true and new.current_line <> old.current_line then
            v_old_line := old.current_line;
            if new.current_line > old.current_line then
                v_direction := 'up';
            else
                v_direction := 'down';
            end if;
        else
            -- No meaningful change, skip movement row
            return new;
        end if;
    else
        return new;
    end if;

    insert into line_movements (
        pp_line_id, slate_id, player_id, raw_player_name, stat_type,
        old_line, new_line, direction, changed_by
    ) values (
        new.id, new.slate_id, new.player_id, new.raw_player_name, new.stat_type,
        v_old_line, new.current_line, v_direction, v_changed_by
    );

    return new;
end;
$$;

drop trigger if exists trg_prizepicks_line_movement on prizepicks_lines;
create trigger trg_prizepicks_line_movement
    after insert or update on prizepicks_lines
    for each row execute function record_line_movement();

-- ── 7. updated_at trigger for prizepicks_lines ──────────────────
drop trigger if exists trg_pp_lines_updated_at on prizepicks_lines;
create trigger trg_pp_lines_updated_at
    before update on prizepicks_lines
    for each row execute function update_updated_at_column();
-- (update_updated_at_column was defined in migration 001)

-- Bridge: our function uses 'updated_at' naming, but prizepicks_lines uses
-- 'last_updated_at'. Define a compatible trigger function.
create or replace function update_last_updated_at_column() returns trigger
language plpgsql as $$
begin
    new.last_updated_at = now();
    return new;
end;
$$;

drop trigger if exists trg_pp_lines_updated_at on prizepicks_lines;
create trigger trg_pp_lines_updated_at
    before update on prizepicks_lines
    for each row execute function update_last_updated_at_column();

-- ── 8. Row-level security ──────────────────────────────────────
alter table odds_history enable row level security;
alter table prizepicks_lines enable row level security;
alter table line_movements enable row level security;
alter table admin_users enable row level security;

-- odds_history: anon can read rows tied to active slates
drop policy if exists "odds_history_read_active" on odds_history;
create policy "odds_history_read_active" on odds_history for select to anon
using (exists (select 1 from slates s where s.id = slate_id and s.status = 'active'));

-- prizepicks_lines: anon reads active lines on active slates; admins can write
drop policy if exists "pp_lines_read" on prizepicks_lines;
create policy "pp_lines_read" on prizepicks_lines for select to anon, authenticated
using (is_active = true and exists (select 1 from slates s where s.id = slate_id and s.status = 'active'));

drop policy if exists "pp_lines_admin_insert" on prizepicks_lines;
create policy "pp_lines_admin_insert" on prizepicks_lines for insert to authenticated
with check (is_admin());

drop policy if exists "pp_lines_admin_update" on prizepicks_lines;
create policy "pp_lines_admin_update" on prizepicks_lines for update to authenticated
using (is_admin()) with check (is_admin());

drop policy if exists "pp_lines_admin_delete" on prizepicks_lines;
create policy "pp_lines_admin_delete" on prizepicks_lines for delete to authenticated
using (is_admin());

-- line_movements: anon reads rows tied to active slates (realtime needs select)
drop policy if exists "line_movements_read" on line_movements;
create policy "line_movements_read" on line_movements for select to anon, authenticated
using (exists (select 1 from slates s where s.id = slate_id and s.status = 'active'));

-- admin_users: only admins can see the list; no public writes (seeded via service_role)
drop policy if exists "admin_users_read" on admin_users;
create policy "admin_users_read" on admin_users for select to authenticated
using (is_admin());

-- ── 9. Enable Realtime on the relevant tables ──────────────────
-- Supabase Realtime works off the supabase_realtime publication. Adding the
-- tables lets the frontend subscribe via supabase-js channels.
-- Idempotent adds — IF NOT EXISTS isn't supported on ALTER PUBLICATION, so
-- we do it defensively via a DO block.
do $$
begin
    begin
        alter publication supabase_realtime add table prizepicks_lines;
    exception when duplicate_object then null;
    end;
    begin
        alter publication supabase_realtime add table line_movements;
    exception when duplicate_object then null;
    end;
end $$;

-- ═══════════════════════════════════════════════════════════════════════
-- Post-migration manual step (run once by user in SQL editor):
--
--   insert into admin_users (user_id, notes)
--   select id, 'founder' from auth.users where email = '<YOUR EMAIL>';
--
-- Replace <YOUR EMAIL> with the email you used to sign in to the live site.
-- ═══════════════════════════════════════════════════════════════════════
