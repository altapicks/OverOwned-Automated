-- ═══════════════════════════════════════════════════════════════════════
-- Migration 007 — v6.0 schema prep
--
-- Two changes:
--   1. Allow slates.dk_draft_group_id to be NULL (was NOT NULL UNIQUE).
--      Manual slates use synthetic negative IDs so the UNIQUE constraint
--      still prevents collisions, but the NOT NULL was DK-specific. Future
--      manual uploads might also leave it null entirely.
--
--   2. Mark all currently-active legacy slates that came from the watcher's
--      DK auto-ingest as 'archived' so v6.0 starts with a clean slate
--      selection. The 50f7dab2 Madrid slate stays active explicitly because
--      we ran SQL backfills against it — we want it to survive as today's
--      working slate until the operator does a manual upload to replace it.
--
-- Idempotent. Safe to re-run.
-- ═══════════════════════════════════════════════════════════════════════

alter table slates alter column dk_draft_group_id drop not null;

-- Sanity: archive every currently-active slate EXCEPT the Madrid one we
-- want to preserve as the live working slate until the next manual upload.
update slates
set status = 'archived'
where status = 'active'
  and id != '50f7dab2-7da1-4565-a29d-40b1119e52aa';

-- Confirm: only one active slate remains
select id, sport, slate_date, slate_label, status, dk_draft_group_id
from slates
where status = 'active'
order by slate_date desc;
