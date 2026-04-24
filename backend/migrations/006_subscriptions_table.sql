-- ═══════════════════════════════════════════════════════════════════════
-- Migration 006 — subscriptions table stub
--
-- Creates the public.subscriptions table the frontend's auth-context
-- expects. Until Stripe billing is actually wired up, this table just
-- sits empty — auth-context's .maybeSingle() call returns null, which
-- is handled gracefully (user shows as "Free" tier, which admins override).
--
-- Eliminates the 404 warnings in the browser console.
--
-- Full schema mirrors the Stripe subscription object's useful fields.
-- When billing is wired up, the webhook handler writes/updates rows here
-- and no frontend code changes are needed.
--
-- Flat, idempotent. Safe to re-run.
-- ═══════════════════════════════════════════════════════════════════════

create table if not exists public.subscriptions (
    id                       uuid primary key default gen_random_uuid(),
    user_id                  uuid not null references auth.users(id) on delete cascade,
    stripe_customer_id       text,
    stripe_subscription_id   text unique,
    tier                     text,                  -- 'monthly' | 'season' | 'weekly'
    status                   text not null default 'inactive',
                                                   -- 'active' | 'trialing' | 'past_due' | 'canceled' | 'inactive'
    current_period_start     timestamptz,
    current_period_end       timestamptz,
    cancel_at_period_end     boolean not null default false,
    canceled_at              timestamptz,
    trial_end                timestamptz,
    created_at               timestamptz not null default now(),
    updated_at               timestamptz not null default now()
);

comment on table public.subscriptions is
'Stripe subscription state, one row per Stripe subscription. Keyed by
stripe_subscription_id for idempotent webhook upserts. user_id refers to
the auth.users record. Empty until billing is wired up — frontend handles
the empty state as "Free tier".';

create index if not exists idx_subscriptions_user_id
  on public.subscriptions(user_id);
create index if not exists idx_subscriptions_status
  on public.subscriptions(status);
create index if not exists idx_subscriptions_period_end
  on public.subscriptions(current_period_end);

-- ── RLS ────────────────────────────────────────────────────────────────
-- Users can read their own subscription row(s). Writes are service-role
-- only (Stripe webhook handler). No anon access.
alter table public.subscriptions enable row level security;

drop policy if exists "subscriptions_read_own" on public.subscriptions;
create policy "subscriptions_read_own" on public.subscriptions
  for select to authenticated
  using (user_id = auth.uid());

-- No INSERT/UPDATE/DELETE policies — only service_role (bypasses RLS)
-- can write. Keeps anon + authenticated from forging subscription state.
