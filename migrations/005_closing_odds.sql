-- ═══════════════════════════════════════════════════════════════════════
-- Migration 005 — Lock-time odds snapshot (closing_odds)
--
-- Problem: once a slate locks, the DK, Projections, OverOwned Mode, and
-- Biggest Traps/Hidden Gems tabs all kept reading LIVE Kalshi odds — which
-- drift throughout the matches. A player flagged as a trap at 4:55 ET
-- (pre-lock) might show very different odds at 7 ET because the market
-- moved after lock. This corrupts the historical record of WHY a player
-- was flagged.
--
-- Fix: snapshot matches.odds into matches.closing_odds when the slate
-- passes its lock_time. The slate_watcher worker handles this on each
-- 15-min cycle — idempotent, only writes when closing_odds is still empty.
-- Frontend prefers closing_odds everywhere EXCEPT the Live Leverage
-- Tracker, which always shows live odds (that's its whole purpose).
--
-- Flat, idempotent. Safe to re-run.
-- ═══════════════════════════════════════════════════════════════════════

alter table matches
  add column if not exists closing_odds jsonb not null default '{}'::jsonb;

comment on column matches.closing_odds is
'Snapshot of matches.odds taken when the slate crossed its lock_time.
Frozen forever after — slate_watcher writes this exactly once per match
(when closing_odds is empty and now >= lock_time). Frontend prefers this
over live odds everywhere except the Live Leverage Tracker (which needs
the live-updating value). Empty object = lock has not occurred yet or
this is legacy data that predates the migration.';

-- No index needed — this is always accessed via the match row's PK.
