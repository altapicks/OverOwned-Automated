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

  if (USE_API && API_BASE && !isArchive) {
    try {
      const data = await fetchJson(`${API_BASE}/api/slates/today?sport=${sport}`);
      return { data, source: 'api' };
    } catch (err) {
      console.warn('[overowned] API slate fetch failed, falling back to static', err.message);
    }
  }

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
export const FEATURES = {
  tennis: true,
  mma: import.meta.env.VITE_SHOW_MMA === 'true',
  nba: import.meta.env.VITE_SHOW_NBA === 'true',
};

export function isSportEnabled(sport) {
  return FEATURES[sport] === true;
}

// ── Live Leverage Tracker: contest ownership API ─────────────────────

export async function fetchContestOwnership(slateId) {
  const empty = { ownership: {}, uploaded_at: null, contest_name: null, total_entries: null, _error: null };
  if (!slateId) return { ...empty, _error: 'No slate selected' };
  if (!API_BASE) return { ...empty, _error: 'Backend API not configured (VITE_API_URL missing)' };
  try {
    return await fetchJson(`${API_BASE}/api/tracker/${slateId}/ownership`, { timeoutMs: 10000 });
  } catch (err) {
    console.warn('[overowned] contest ownership fetch failed', err.message);
    return { ...empty, _error: err.message };
  }
}

async function getAccessToken() {
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

// ── Manual slate upload (v6.0b admin) ────────────────────────────────
// Operator-supplied CSV that creates / re-syncs a slate. Backend is at
// POST /api/admin/slates/upload (multipart). Auth via Supabase JWT.
//
// `meta` shape: { sport, slate_date, tournament, surface?, lockTimeETLocal? }
// where lockTimeETLocal is the value of an <input type="datetime-local">
// e.g. "2026-04-25T11:20" — interpreted as America/New_York wall clock and
// converted to UTC ISO before being sent to the backend.

/**
 * Convert a datetime-local input value (interpreted as America/New_York
 * wall clock) into a UTC ISO string. Handles DST transitions correctly
 * by querying Intl for the actual offset on that date.
 *
 * @param {string} dateTimeLocal e.g. "2026-04-25T11:20"
 * @returns {string|null} ISO 8601 UTC, e.g. "2026-04-25T15:20:00.000Z"
 */
export function etLocalToUTCISO(dateTimeLocal) {
  if (!dateTimeLocal) return null;
  const m = String(dateTimeLocal).match(
    /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d{2}))?$/
  );
  if (!m) return null;
  const [, y, mo, d, hh, mm, ss] = m;
  const year = +y, month = +mo, day = +d, hour = +hh, minute = +mm;
  const second = ss ? +ss : 0;

  // Build a Date with these calendar values *as if they were UTC*.
  const naiveUTC = new Date(Date.UTC(year, month - 1, day, hour, minute, second));

  // Determine the New York offset on that calendar date. longOffset gives
  // "GMT-04:00" (EDT) or "GMT-05:00" (EST) handling DST automatically.
  let offsetMs = -5 * 60 * 60 * 1000; // fallback: EST
  try {
    const fmt = new Intl.DateTimeFormat('en-US', {
      timeZone: 'America/New_York',
      timeZoneName: 'longOffset',
    });
    const parts = fmt.formatToParts(naiveUTC);
    const tz = parts.find((p) => p.type === 'timeZoneName')?.value || '';
    const om = tz.match(/GMT([+-])(\d{2}):(\d{2})/);
    if (om) {
      const sign = om[1] === '+' ? 1 : -1;
      const oh = parseInt(om[2], 10);
      const omin = parseInt(om[3], 10);
      offsetMs = sign * (oh * 60 + omin) * 60 * 1000;
    }
  } catch {
    /* fallback above */
  }

  // ET wall = UTC + offsetMs (offset is negative for west of UTC).
  // So UTC = ET wall - offsetMs.
  return new Date(naiveUTC.getTime() - offsetMs).toISOString();
}

/**
 * Upload a manual slate CSV.
 *
 * @param {File} file        The CSV File object
 * @param {Object} meta      { sport, slate_date, tournament, surface, lockTimeETLocal }
 * @param {boolean} dryRun   If true, backend validates without writing
 * @returns {Promise<Object>} Backend result: { ok, summary, warnings, errors, unmatched_names, slate_id? }
 */
export async function uploadSlateManual(file, meta, dryRun = false) {
  if (!API_BASE) throw new Error('Backend API not configured');
  if (!file) throw new Error('No CSV file provided');
  if (!meta?.sport) throw new Error('Sport is required');
  if (!meta?.slate_date) throw new Error('Slate date is required');
  if (!meta?.tournament) throw new Error('Tournament is required');

  const lockTimeUTC = meta.lockTimeETLocal
    ? etLocalToUTCISO(meta.lockTimeETLocal)
    : null;

  const token = await getAccessToken();

  const form = new FormData();
  form.append('csv', file);
  form.append('sport', meta.sport);
  form.append('slate_date', meta.slate_date);
  form.append('tournament', meta.tournament);
  if (meta.surface) form.append('surface', meta.surface);
  if (lockTimeUTC) form.append('lock_time', lockTimeUTC);
  form.append('dry_run', dryRun ? 'true' : 'false');

  const r = await fetch(`${API_BASE}/api/admin/slates/upload`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}` },
    body: form,
  });

  if (!r.ok) {
    const text = await r.text().catch(() => '');
    if (r.status === 403) {
      throw new Error("You're signed in but not in the admin list. Ask an admin to grant access.");
    }
    if (r.status === 401) {
      throw new Error('Session expired. Sign in again and retry.');
    }
    throw new Error(`${r.status}: ${text || r.statusText}`);
  }
  return await r.json();
}
