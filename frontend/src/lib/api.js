// ═══════════════════════════════════════════════════════════════════════
// OverOwned API client
//
// Tries the new backend (VITE_API_URL) first. On failure, falls back to
// the existing static /slate.json / /slates/{sport}/{date}.json files so
// the site never breaks even if the backend is down or mid-deploy.
//
// The response shape is identical in both cases — matches the existing
// slate.json schema exactly — so no UI changes are required.
// ═══════════════════════════════════════════════════════════════════════

import { supabase } from './supabase';

const API_BASE = import.meta.env.VITE_API_URL || '';

// Toggle to force static-only mode (useful for debugging). Set
// VITE_USE_API=false in .env to disable the API entirely.
const USE_API = import.meta.env.VITE_USE_API !== 'false';

async function fetchJson(url, { timeoutMs = 8000 } = {}) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const r = await fetch(url, { signal: ctrl.signal });
    if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
    return await r.json();
  } finally {
    clearTimeout(t);
  }
}

// ── Static fallback URLs (existing behavior) ─────────────────────────
function staticLiveUrl(sport) {
  if (sport === 'mma') return './slate-mma.json';
  if (sport === 'nba') return './slate-nba.json';
  return './slate.json';
}

function staticArchiveUrl(sport, date) {
  return `/slates/${sport}/${date}.json`;
}

function staticManifestUrl(sport) {
  return `/slates/${sport}/manifest.json`;
}

// ── Public loaders ──────────────────────────────────────────────────
export async function loadSlate(sport, slateDate = 'live') {
  const isArchive = slateDate !== 'live';

  // Try the API first
  if (USE_API && API_BASE && !isArchive) {
    try {
      const data = await fetchJson(`${API_BASE}/api/slates/today?sport=${sport}`);
      return { data, source: 'api' };
    } catch (err) {
      console.warn('[overowned] API slate fetch failed, falling back to static', err.message);
    }
  }

  // Fall back to static files
  const url = isArchive ? staticArchiveUrl(sport, slateDate) : staticLiveUrl(sport);
  const data = await fetchJson(url);
  return { data, source: 'static' };
}

export async function loadManifest(sport) {
  if (USE_API && API_BASE) {
    try {
      const data = await fetchJson(`${API_BASE}/api/slates/manifest/${sport}`);
      return data.slates || [];
    } catch (err) {
      console.warn('[overowned] API manifest fetch failed, falling back to static', err.message);
    }
  }
  try {
    const data = await fetchJson(staticManifestUrl(sport));
    return data.slates || [];
  } catch {
    return [];
  }
}

export async function checkHealth() {
  if (!API_BASE) return { status: 'no_api_configured' };
  try {
    return await fetchJson(`${API_BASE}/health`, { timeoutMs: 3000 });
  } catch (err) {
    return { status: 'down', error: err.message };
  }
}

// ── Feature flags ───────────────────────────────────────────────────
// Read from env so we can toggle without code changes.
export const FEATURES = {
  tennis: true,
  mma: import.meta.env.VITE_SHOW_MMA === 'true',
  nba: import.meta.env.VITE_SHOW_NBA === 'true',
};

export function isSportEnabled(sport) {
  return FEATURES[sport] === true;
}

// ── Live Leverage Tracker: contest ownership API ─────────────────────
// Read is public (no auth). Write is admin-only; uses supabase.auth.getSession()
// to obtain the access token so the backend can verify admin_users membership.

export async function fetchContestOwnership(slateId) {
  if (!slateId) return { ownership: {}, uploaded_at: null, contest_name: null, total_entries: null };
  if (!API_BASE) return { ownership: {}, uploaded_at: null, contest_name: null, total_entries: null };
  try {
    return await fetchJson(`${API_BASE}/api/tracker/${slateId}/ownership`, { timeoutMs: 10000 });
  } catch (err) {
    console.warn('[overowned] contest ownership fetch failed', err.message);
    return { ownership: {}, uploaded_at: null, contest_name: null, total_entries: null };
  }
}

async function getAccessToken() {
  // Supabase-JS uses navigator.locks internally for session reads. When
  // multiple tabs/components hit auth simultaneously, the later call "steals"
  // the lock and the earlier one throws a NavigatorLockAcquireTimeoutError /
  // "lock was released because another request stole it". Harmless, but
  // transient — on retry the stealing call has completed and we succeed.
  const tryOnce = async () => {
    const { data: { session } } = await supabase.auth.getSession();
    return session?.access_token;
  };
  let token;
  try {
    token = await tryOnce();
  } catch (err) {
    const msg = String(err?.message || err || '');
    if (msg.toLowerCase().includes('lock') && msg.toLowerCase().includes('released')) {
      // One retry after a brief yield
      await new Promise((r) => setTimeout(r, 120));
      token = await tryOnce();
    } else {
      throw err;
    }
  }
  if (!token) throw new Error('Not signed in — sign in as an admin to upload contest CSVs.');
  return token;
}

export async function uploadContestOwnership(slateId, file) {
  if (!API_BASE) throw new Error('Backend API not configured');
  const token = await getAccessToken();
  const form = new FormData();
  form.append('file', file);
  const r = await fetch(`${API_BASE}/api/tracker/${slateId}/ownership`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}` },
    body: form,
  });
  if (!r.ok) {
    const text = await r.text().catch(() => '');
    if (r.status === 403) throw new Error("You're signed in but not in the admin list. Ask an admin to grant access.");
    if (r.status === 401) throw new Error('Session expired. Sign in again and retry.');
    throw new Error(`${r.status}: ${text || r.statusText}`);
  }
  return await r.json();
}

export async function clearContestOwnership(slateId) {
  if (!API_BASE) throw new Error('Backend API not configured');
  const token = await getAccessToken();
  const r = await fetch(`${API_BASE}/api/tracker/${slateId}/ownership`, {
    method: 'DELETE',
    headers: { Authorization: `Bearer ${token}` },
  });
  if (!r.ok) {
    const text = await r.text().catch(() => '');
    throw new Error(`${r.status}: ${text || r.statusText}`);
  }
  return await r.json();
}
