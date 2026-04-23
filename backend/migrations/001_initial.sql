-- ═══════════════════════════════════════════════════════════════════════
-- OverOwned Backend — Initial Schema
-- Run in Supabase SQL editor once. All tables use text primary keys where
-- possible so they're easy to reason about; UUIDs elsewhere.
-- ═══════════════════════════════════════════════════════════════════════

-- ── Player master table ──────────────────────────────────────────────
-- Canonical player identity. Every player on every slate, every PP line,
-- every odds row joins through this. The canonical_id is human-readable
-- and stable (slug form of the player's real name).
--
-- Aliases column holds name variants from each source (DK, PP, Kalshi, TA, etc).
-- Fuzzy-matching is always against aliases, never canonical.
create table if not exists players (
    canonical_id    text primary key,           -- 'jannik_sinner'
    display_name    text not null,              -- 'Jannik Sinner'
    sport           text not null,              -- 'tennis' | 'nba' | 'mma'
    aliases         jsonb not null default '{}', -- { "dk": "Sinner J.", "pp": "Jannik Sinner", ... }
    country         text,
    hand            text,                        -- 'R' | 'L' | null
    birth_date      date,
    created_at      timestamptz not null default now(),
    updated_at      timestamptz not null default now()
);

create index if not exists idx_players_sport on players(sport);
create index if not exists idx_players_display on players using gin (to_tsvector('simple', display_name));

-- ── Slates ──────────────────────────────────────────────────────────
-- One row per DK draft group we've ingested. One slate per sport per day
-- is typical but DK can have multiple (early/main/late).
create table if not exists slates (
    id              uuid primary key default gen_random_uuid(),
    sport           text not null,
    dk_draft_group_id bigint not null unique,   -- DK's identifier
    slate_date      date not null,
    slate_label     text,                       -- 'Main', 'Early', 'Showdown', etc.
    contest_type    text,                       -- 'Classic' | 'Showdown'
    salary_cap      integer not null default 50000,
    roster_size     integer,
    lock_time       timestamptz,                -- earliest match start / slate lock
    status          text not null default 'active',  -- active | locked | completed | cancelled
    first_seen_at   timestamptz not null default now(),
    last_synced_at  timestamptz not null default now()
);

create index if not exists idx_slates_date on slates(slate_date desc);
create index if not exists idx_slates_sport_date on slates(sport, slate_date desc);
create index if not exists idx_slates_status on slates(status);

-- ── Slate players ────────────────────────────────────────────────────
-- One row per (slate, player). Salary is fixed for the slate (DK doesn't
-- adjust salaries mid-slate), so no history needed here.
create table if not exists slate_players (
    slate_id        uuid not null references slates(id) on delete cascade,
    player_id       text not null references players(canonical_id),
    dk_player_id    bigint not null,             -- DK's per-slate playerId
    dk_display_name text not null,               -- the exact string DK used
    salary          integer not null,
    avg_ppg         numeric(6, 2),               -- DK's average points-per-game field
    roster_position text,                         -- 'P' for tennis classic, 'CPT'/'FLEX' for showdown
    match_id        uuid,                         -- fk → matches.id (nullable until match detected)
    ingested_at     timestamptz not null default now(),
    primary key (slate_id, player_id, roster_position)
);

create index if not exists idx_slate_players_slate on slate_players(slate_id);
create index if not exists idx_slate_players_player on slate_players(player_id);

-- ── Matches ──────────────────────────────────────────────────────────
-- One row per tennis match on a slate. Both players always present;
-- odds, adjustments, and results are populated over time by other services.
create table if not exists matches (
    id              uuid primary key default gen_random_uuid(),
    slate_id        uuid not null references slates(id) on delete cascade,
    player_a_id     text not null references players(canonical_id),
    player_b_id     text not null references players(canonical_id),
    tournament      text,
    surface         text,                        -- 'hard' | 'clay' | 'grass' | 'carpet'
    best_of         integer not null default 3,
    start_time      timestamptz,
    dk_competition_id bigint,                    -- DK's competition identifier
    status          text not null default 'scheduled', -- scheduled | live | completed | cancelled
    winner_id       text references players(canonical_id), -- null until completed
    final_score     text,                         -- e.g. '6-4 7-5'
    created_at      timestamptz not null default now(),
    updated_at      timestamptz not null default now()
);

create unique index if not exists idx_matches_slate_pair on matches(slate_id, player_a_id, player_b_id);
create index if not exists idx_matches_status on matches(status);
create index if not exists idx_matches_start_time on matches(start_time);

-- ── Ingestion log ────────────────────────────────────────────────────
-- Every pull from DK (or any other source) logs here. Lets us debug
-- latency, detect outages, and backfill retrospectively.
create table if not exists ingestion_log (
    id              bigserial primary key,
    source          text not null,               -- 'dk_lobby' | 'dk_draftables' | 'kalshi_ws' etc.
    sport           text,
    status          text not null,               -- 'ok' | 'error' | 'partial'
    items_processed integer,
    duration_ms     integer,
    error_message   text,
    context         jsonb,                        -- arbitrary metadata
    started_at      timestamptz not null default now()
);

create index if not exists idx_ingestion_log_source_time on ingestion_log(source, started_at desc);
create index if not exists idx_ingestion_log_status on ingestion_log(status) where status != 'ok';

-- ── Unmatched names queue ────────────────────────────────────────────
-- When we can't confidently match a new DK name against our master table,
-- we log it here for manual review. The Discord notifier pings you about it.
create table if not exists unmatched_names (
    id              bigserial primary key,
    source          text not null,               -- 'dk' | 'pp' | 'kalshi' etc.
    sport           text not null,
    raw_name        text not null,
    context         jsonb,                        -- { "dk_player_id": 123, "salary": 8200, ... }
    best_guess_id   text,                         -- top fuzzy candidate, if any
    best_guess_score real,                        -- fuzzy match score 0-100
    resolved        boolean not null default false,
    resolved_to     text references players(canonical_id),
    first_seen_at   timestamptz not null default now(),
    unique (source, sport, raw_name)
);

create index if not exists idx_unmatched_unresolved on unmatched_names(resolved) where resolved = false;

-- ── Updated-at trigger ───────────────────────────────────────────────
create or replace function update_updated_at_column()
returns trigger as $$
begin
    new.updated_at = now();
    return new;
end;
$$ language plpgsql;

drop trigger if exists update_players_updated_at on players;
create trigger update_players_updated_at before update on players
    for each row execute function update_updated_at_column();

drop trigger if exists update_matches_updated_at on matches;
create trigger update_matches_updated_at before update on matches
    for each row execute function update_updated_at_column();

-- ── Row-level security ──────────────────────────────────────────────
-- Backend uses service_role (bypasses RLS). Frontend uses anon; we enable
-- RLS with a read-only policy so the React app can fetch but not mutate.
alter table players          enable row level security;
alter table slates           enable row level security;
alter table slate_players    enable row level security;
alter table matches          enable row level security;
alter table ingestion_log    enable row level security;
alter table unmatched_names  enable row level security;

-- Public read for the data the frontend needs
drop policy if exists "anon read players" on players;
create policy "anon read players" on players for select to anon using (true);

drop policy if exists "anon read slates" on slates;
create policy "anon read slates" on slates for select to anon using (true);

drop policy if exists "anon read slate_players" on slate_players;
create policy "anon read slate_players" on slate_players for select to anon using (true);

drop policy if exists "anon read matches" on matches;
create policy "anon read matches" on matches for select to anon using (true);

-- Internal tables: no anon access. Backend-only via service_role.
