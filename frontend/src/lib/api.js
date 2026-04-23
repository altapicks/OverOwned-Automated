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
