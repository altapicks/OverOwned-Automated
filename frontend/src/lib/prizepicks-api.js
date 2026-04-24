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
export async function fetchLines(slateId) {
  const { data, error } = await supabase
    .from('prizepicks_lines')
    .select('*')
    .eq('slate_id', slateId)
    .eq('is_active', true)
    .order('last_updated_at', { ascending: false });
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
