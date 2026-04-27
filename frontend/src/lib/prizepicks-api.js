// ═══════════════════════════════════════════════════════════════════════
// PrizePicks admin + realtime helpers
//
// Admin writes: hit the backend /api/prizepicks/* endpoints with the
// user's Supabase JWT as a Bearer token. The backend verifies the token
// with Supabase and checks the admin_users table.
//
// Public reads: query Supabase directly (fast, no backend hop). RLS
// enforces that only active lines on active slates are visible.
//
// Realtime: subscribe to prizepicks_lines + line_movements via supabase-js.
// ═══════════════════════════════════════════════════════════════════════
import { supabase } from './supabase';

const API_BASE = import.meta.env.VITE_API_URL || '';

async function authedFetch(path, opts = {}) {
  const { data: { session } } = await supabase.auth.getSession();
  const token = session?.access_token;
  if (!token) throw new Error('Not signed in');

  const r = await fetch(`${API_BASE}${path}`, {
    ...opts,
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${token}`,
      ...(opts.headers || {}),
    },
  });
  if (!r.ok) {
    const text = await r.text().catch(() => '');
    throw new Error(`${r.status}: ${text || r.statusText}`);
  }
  return r.json();
}

// ── Reads (public via Supabase, RLS-enforced) ─────────────────────
export async function fetchLines(slateId, statType = 'Fantasy Score') {
  // PP tab board displays Fantasy Score only by default. Other stat types
  // (Aces, Break Points Won, Double Faults, etc.) are still ingested into
  // prizepicks_lines and consumed by the DK engine via slate.pp_lines for
  // True Fantasy Score projection — but they don't belong in the PP table.
  // Pass statType='all' to get every stat type, or a specific stat name.
  let q = supabase
    .from('prizepicks_lines')
    .select('*')
    .eq('slate_id', slateId)
    .eq('is_active', true);
  if (statType && statType !== 'all') {
    q = q.eq('stat_type', statType);
  }
  const { data, error } = await q.order('last_updated_at', { ascending: false });
  if (error) throw error;
  return data || [];
}

export async function fetchRecentMovements(slateId, limit = 50) {
  const { data, error } = await supabase
    .from('line_movements')
    .select('*')
    .eq('slate_id', slateId)
    .order('detected_at', { ascending: false })
    .limit(limit);
  if (error) throw error;
  return data || [];
}

// ── Writes (admin-gated via backend API) ──────────────────────────
export async function createLine({ slateId, rawPlayerName, statType, currentLine, matchId, notes }) {
  return authedFetch('/api/prizepicks/lines', {
    method: 'POST',
    body: JSON.stringify({
      slate_id: slateId,
      raw_player_name: rawPlayerName,
      stat_type: statType,
      current_line: currentLine,
      match_id: matchId || null,
      notes: notes || null,
    }),
  });
}

export async function updateLine(lineId, { currentLine, notes }) {
  return authedFetch(`/api/prizepicks/lines/${lineId}`, {
    method: 'PATCH',
    body: JSON.stringify({
      current_line: currentLine,
      notes: notes || null,
    }),
  });
}

export async function deleteLine(lineId) {
  return authedFetch(`/api/prizepicks/lines/${lineId}`, { method: 'DELETE' });
}

export async function bulkUpsertLines(slateId, rows) {
  return authedFetch('/api/prizepicks/lines/bulk', {
    method: 'POST',
    body: JSON.stringify({ slate_id: slateId, rows }),
  });
}

// ── Admin check (read-only; safe to call from unauth too) ─────────
export async function checkIsAdmin() {
  const { data: { user } } = await supabase.auth.getUser();
  if (!user) return false;
  const { data, error } = await supabase
    .from('admin_users')
    .select('user_id')
    .eq('user_id', user.id)
    .maybeSingle();
  // RLS: if user IS admin, they can read their own row. If not, select
  // returns null. Either way, truthy data means admin.
  return !!(data && !error);
}

// ── Realtime subscriptions ────────────────────────────────────────
export function subscribeToLines(slateId, onChange) {
  const channel = supabase
    .channel(`pp_lines:${slateId}`)
    .on('postgres_changes',
      { event: '*', schema: 'public', table: 'prizepicks_lines', filter: `slate_id=eq.${slateId}` },
      (payload) => onChange(payload))
    .subscribe();
  return () => supabase.removeChannel(channel);
}

export function subscribeToMovements(slateId, onNew) {
  const channel = supabase
    .channel(`line_movements:${slateId}`)
    .on('postgres_changes',
      { event: 'INSERT', schema: 'public', table: 'line_movements', filter: `slate_id=eq.${slateId}` },
      (payload) => onNew(payload.new))
    .subscribe();
  return () => supabase.removeChannel(channel);
}

// ── CSV paste parser ──────────────────────────────────────────────
// Accepts "player,stat,line" per line. Extra columns are ignored.
// Blank lines and leading "#" comment lines are skipped.
export function parseCsvLines(text) {
  const rows = [];
  for (const line of (text || '').split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;
    const parts = trimmed.split(',').map(s => s.trim());
    if (parts.length < 3) continue;
    const [player, stat, lineVal, notes] = parts;
    const num = parseFloat(lineVal);
    if (!player || !stat || isNaN(num)) continue;
    rows.push({
      raw_player_name: player,
      stat_type: stat,
      current_line: num,
      notes: notes || null,
    });
  }
  return rows;
}
// ═══════════════════════════════════════════════════════════════════════
// v6.13 — ADD THESE TWO FUNCTIONS to your existing prizepicks-api.js
//
// File location: frontend/src/lib/prizepicks-api.js
//
// Just paste these at the bottom of the file (after the existing exports).
//
// Auth: matches the existing createLine / patchLine / deleteLine pattern
// — Authorization: Bearer <supabase-jwt> header. The backend verifies the
// JWT via supabase auth and gates against the admin_users table.
//
// IMPORTANT: this file imports `supabase` from './supabase' — make sure
// that import is already present at the top of your prizepicks-api.js
// (it almost certainly is, since the existing functions do the same thing).
// If your existing file uses a different helper to grab the JWT (e.g.
// a `getAuthHeaders()` helper), look at how createLine builds its
// fetch headers and copy that pattern instead — see "Adapting to your
// existing pattern" note below.
// ═══════════════════════════════════════════════════════════════════════

/**
 * Fetch the admin's gem + trap picks for a slate. Public — no auth needed.
 *
 * Returns:
 *   {
 *     slate_id, gem_player_name, trap_player_name,
 *     gem_set_at, trap_set_at, gem_set_by, trap_set_by
 *   }
 *   Fields are null when nothing has been picked yet. The shape is
 *   stable so callers don't need to handle 404s.
 */
export async function fetchAdminPicks(slateId) {
  if (!slateId) return null;
  // API_BASE — same constant your existing fetchLines / fetchRecentMovements
  // functions use. If your file uses a different constant name, swap it in.
  const r = await fetch(
    `${API_BASE}/api/prizepicks/admin-picks?slate_id=${encodeURIComponent(slateId)}`,
    { method: 'GET' }
  );
  if (!r.ok) {
    throw new Error(`fetchAdminPicks failed: ${r.status} ${await r.text().catch(() => '')}`);
  }
  return r.json();
}

/**
 * Set or clear the admin's gem or trap pick for a slate.
 * Admin-gated server-side via Bearer JWT.
 *
 * @param {Object} args
 * @param {string} args.slateId - the active slate UUID
 * @param {'gem'|'trap'} args.kind - which pick to set
 * @param {string|null} args.rawPlayerName - player name to mark, or null/'' to clear
 * @returns {Promise<Object>} the updated picks row
 */
export async function setAdminPick({ slateId, kind, rawPlayerName }) {
  if (!slateId) throw new Error('slateId required');
  if (kind !== 'gem' && kind !== 'trap') {
    throw new Error("kind must be 'gem' or 'trap'");
  }

  // Get current Supabase session for the JWT. Same approach as createLine.
  const { data: { session } } = await supabase.auth.getSession();
  if (!session?.access_token) {
    throw new Error('Not signed in');
  }
  const userEmail = session?.user?.email || null;

  const r = await fetch(`${API_BASE}/api/prizepicks/admin-picks`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${session.access_token}`,
    },
    body: JSON.stringify({
      slate_id: slateId,
      kind,
      raw_player_name: rawPlayerName,
      set_by_label: userEmail,
    }),
  });
  if (!r.ok) {
    const text = await r.text().catch(() => '');
    throw new Error(`setAdminPick failed: ${r.status} ${text}`);
  }
  return r.json();
}

// ═══════════════════════════════════════════════════════════════════════
// Adapting to your existing pattern
// ═══════════════════════════════════════════════════════════════════════
//
// If your existing prizepicks-api.js has a helper like:
//
//   async function getAuthHeaders() {
//     const { data: { session } } = await supabase.auth.getSession();
//     return { 'Authorization': `Bearer ${session.access_token}` };
//   }
//
// then replace the inline session/header logic in setAdminPick with:
//
//   const headers = {
//     'Content-Type': 'application/json',
//     ...(await getAuthHeaders()),
//   };
//
// The semantics are identical; this just keeps style consistent with the
// rest of your file.
// ═══════════════════════════════════════════════════════════════════════
