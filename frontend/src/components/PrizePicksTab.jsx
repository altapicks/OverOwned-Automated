// ═══════════════════════════════════════════════════════════════════════
// PrizePicks tab — admin entry + realtime display
//
// Wire into App.jsx by:
//   1. import { PrizePicksTab } from './components/PrizePicksTab';
//   2. Add a tab button next to existing ones: { key: 'pp', label: 'PrizePicks' }
//   3. Render <PrizePicksTab slateId={data?.meta?.slate_id || ...} /> when tab === 'pp'
//
// Public reads go directly to Supabase (fast, RLS-enforced).
// Writes go through the backend API with admin JWT verification.
// ═══════════════════════════════════════════════════════════════════════
import React, { useState, useEffect, useMemo, useCallback } from 'react';
import {
  fetchLines, fetchRecentMovements,
  createLine, updateLine, deleteLine, bulkUpsertLines,
  checkIsAdmin, subscribeToLines, subscribeToMovements, parseCsvLines,
} from '../lib/prizepicks-api';

export function PrizePicksTab({ slateId, players = [] }) {
  const [lines, setLines] = useState([]);
  const [movements, setMovements] = useState([]);
  const [loading, setLoading] = useState(true);
  const [isAdmin, setIsAdmin] = useState(false);
  const [flashId, setFlashId] = useState(null);
  const [showAdd, setShowAdd] = useState(false);
  const [showBulk, setShowBulk] = useState(false);
  const [error, setError] = useState(null);
  const [sortKey, setSortKey] = useState('edge');
  const [sortDir, setSortDir] = useState('desc');

  // Initial load + admin check
  useEffect(() => {
    if (!slateId) return;
    let cancelled = false;
    setLoading(true);
    Promise.all([
      fetchLines(slateId),
      fetchRecentMovements(slateId, 50),
      checkIsAdmin(),
    ]).then(([ls, ms, admin]) => {
      if (cancelled) return;
      setLines(ls);
      setMovements(ms);
      setIsAdmin(admin);
    }).catch(e => !cancelled && setError(e.message)).finally(() => !cancelled && setLoading(false));
    return () => { cancelled = true; };
  }, [slateId]);

  // Realtime subscriptions
  useEffect(() => {
    if (!slateId) return;
    const unsubLines = subscribeToLines(slateId, (payload) => {
      const { eventType, new: row, old: oldRow } = payload;
      setLines(prev => {
        if (eventType === 'INSERT') return [row, ...prev];
        if (eventType === 'UPDATE') {
          if (row && row.is_active === false) return prev.filter(l => l.id !== row.id);
          setFlashId(row.id);
          setTimeout(() => setFlashId(null), 2000);
          return prev.map(l => l.id === row.id ? row : l);
        }
        if (eventType === 'DELETE') return prev.filter(l => l.id !== (oldRow?.id));
        return prev;
      });
    });
    const unsubMoves = subscribeToMovements(slateId, (move) => {
      setMovements(prev => [move, ...prev].slice(0, 50));
    });
    return () => { unsubLines(); unsubMoves(); };
  }, [slateId]);

  const onAdd = useCallback(async (form) => {
    try {
      await createLine({ slateId, ...form });
      setShowAdd(false);
    } catch (e) { setError(e.message); }
  }, [slateId]);

  const onInlineUpdate = useCallback(async (line, newValue) => {
    if (newValue === line.current_line) return;
    try { await updateLine(line.id, { currentLine: newValue }); }
    catch (e) { setError(e.message); }
  }, []);

  const onDelete = useCallback(async (lineId) => {
    try { await deleteLine(lineId); }
    catch (e) { setError(e.message); }
  }, []);

  const onBulk = useCallback(async (csvText) => {
    const rows = parseCsvLines(csvText);
    if (rows.length === 0) { setError('No valid rows parsed'); return; }
    try {
      await bulkUpsertLines(slateId, rows);
      setShowBulk(false);
    } catch (e) { setError(e.message); }
  }, [slateId]);

  // Build a lookup of projected values per (player, stat) for edge computation.
  // Lines are compared against our model's projection to produce edge signals.
  // Color coding: green = MORE (project over line), red = LESS (project under).
  // Edge drives Hidden Gem + PP Fade signals in the DK tab.
  //
  // RULES-OF-HOOKS: all useMemo/useCallback MUST be above the early-return
  // guards below. React tracks hooks by call order — returning early before
  // calling these hooks on the first render and then calling them on the
  // second render produces error #310.
  // Player lookup with multiple fallback strategies. PP/UD feeds often drop
  // middle particles ("Alex Minaur" vs DK's "Alex de Minaur") or use
  // different case/punctuation. We try strategies in descending specificity:
  //   1. Exact case-sensitive match
  //   2. Normalized exact (case/punct insensitive)
  //   3. First+last words (drops middle particles, unique-enough for tennis)
  //   4. Surname only (last-ditch for single-name rows)
  //
  // If lookup fails, we console.warn once per unique name so failures are
  // visible in DevTools but don't spam the log.
  const playersByName = useMemo(() => {
    const normalize = s => String(s || '')
      .toLowerCase()
      .replace(/[.,'’`]/g, '')   // drop common punctuation
      .replace(/\s+/g, ' ')
      .trim();
    const firstLast = s => {
      const parts = normalize(s).split(' ').filter(Boolean);
      if (parts.length === 0) return '';
      if (parts.length === 1) return parts[0];
      return parts[0] + ' ' + parts[parts.length - 1];
    };
    const surname = s => {
      const parts = normalize(s).split(' ').filter(Boolean);
      return parts[parts.length - 1] || '';
    };

    const exact = {};           // raw name → player
    const normExact = {};       // normalized full → player
    const byFirstLast = {};     // "first last" → player (or null if collision)
    const bySurname = {};       // "last" → player (or null if collision)

    (players || []).forEach(p => {
      exact[String(p.name || '').trim()] = p;
      const ne = normalize(p.name);
      if (ne) normExact[ne] = p;
      const fl = firstLast(p.name);
      if (fl) byFirstLast[fl] = (byFirstLast[fl] === undefined) ? p : null;
      const sn = surname(p.name);
      if (sn) bySurname[sn] = (bySurname[sn] === undefined) ? p : null;
    });
    return { exact, normExact, byFirstLast, bySurname, normalize, firstLast, surname };
  }, [players]);

  // Diagnostic: remember which raw names we've already warned about so the
  // console doesn't get spammed. Cleared when `players` changes (new slate).
  const warnedMissingRef = React.useRef(new Set());
  React.useEffect(() => { warnedMissingRef.current = new Set(); }, [players]);

  const lookupPlayer = useCallback((rawName) => {
    if (!rawName) return null;
    const { exact, normExact, byFirstLast, bySurname, normalize, firstLast, surname } = playersByName;
    const trimmed = String(rawName).trim();
    // 1. Exact
    if (exact[trimmed]) return exact[trimmed];
    // 2. Normalized exact
    const ne = normalize(trimmed);
    if (normExact[ne]) return normExact[ne];
    // 3. First+last (handles "Alex Minaur" → "Alex de Minaur")
    const fl = firstLast(trimmed);
    if (fl && byFirstLast[fl]) return byFirstLast[fl];
    // 4. Surname only (handles single-name rows, last-ditch)
    const sn = surname(trimmed);
    if (sn && bySurname[sn]) return bySurname[sn];
    // Nothing matched — log diagnostic once per unique name
    if (!warnedMissingRef.current.has(trimmed)) {
      warnedMissingRef.current.add(trimmed);
      const availableNames = Object.keys(exact).slice(0, 5).join(', ');
      console.warn(`[PrizePicksTab] No player match for "${trimmed}" (normalized: "${ne}"). First 5 DK names: ${availableNames}`);
    }
    return null;
  }, [playersByName]);

  const getProjectedForStat = useCallback((playerName, stat) => {
    const p = lookupPlayer(playerName);
    if (!p) return null;
    // Stat label → projected field on player object
    switch (stat) {
      case 'Fantasy Score': return p.ppProj;
      case 'Games Won': return p.gw;
      case 'Games Played': return (p.gw || 0) + (p.gl || 0);
      case 'Aces': return p.aces;
      case 'Double Faults': return p.dfs;
      case 'Breakpoints Won': return p.breaks;
      case 'Sets Won': return p.sw;
      case 'Sets Played': return (p.sw || 0) + (p.sl || 0);
      case '1st Set Games Won': return p.stats?.firstSetGamesWon;  // may be undefined
      case '1st Set Games Played': return p.stats?.firstSetGamesPlayed;
      case 'Tiebreakers Played': return p.stats?.tiebreakersPlayed;
      default: return null;
    }
  }, [lookupPlayer]);

  // Enrich each line with projected + edge + direction, ready for display/sort.
  const enrichedLines = useMemo(() => {
    return (lines || []).map(line => {
      const projected = getProjectedForStat(line.raw_player_name, line.stat_type);
      const edge = (projected != null) ? Math.round((projected - line.current_line) * 100) / 100 : null;
      const direction = edge == null ? '-' : (edge > 0.2 ? 'MORE' : edge < -0.2 ? 'LESS' : '-');
      return { ...line, projected, edge, direction };
    });
  }, [lines, getProjectedForStat]);

  // Sort — edge desc by default, but clicking a header cycles the sort.
  const sortedLines = useMemo(() => {
    const arr = [...enrichedLines];
    arr.sort((a, b) => {
      let va, vb;
      switch (sortKey) {
        case 'player': va = a.raw_player_name; vb = b.raw_player_name; break;
        case 'stat': va = a.stat_type; vb = b.stat_type; break;
        case 'line': va = a.current_line; vb = b.current_line; break;
        case 'projected': va = a.projected ?? -Infinity; vb = b.projected ?? -Infinity; break;
        case 'edge':
        default:
          va = a.edge ?? -Infinity; vb = b.edge ?? -Infinity;
      }
      if (va === vb) return 0;
      const cmp = va > vb ? 1 : -1;
      return sortDir === 'asc' ? cmp : -cmp;
    });
    return arr;
  }, [enrichedLines, sortKey, sortDir]);

  const toggleSort = (key) => {
    if (sortKey === key) setSortDir(d => d === 'asc' ? 'desc' : 'asc');
    else { setSortKey(key); setSortDir(key === 'player' || key === 'stat' ? 'asc' : 'desc'); }
  };

  const SortHeader = ({ label, col, num }) => {
    const active = sortKey === col;
    return (
      <th className={num ? 'num' : ''} style={{ cursor: 'pointer' }} onClick={() => toggleSort(col)}>
        {label}
        {active && <span className="sort-arrow" style={{ marginLeft: 4 }}>{sortDir === 'asc' ? '▲' : '▼'}</span>}
      </th>
    );
  };

  // Early returns must come AFTER all hooks — see rules-of-hooks note above.
  if (!slateId) return <div className="empty"><p>No active slate.</p></div>;
  if (loading) return <div className="empty"><p>Loading PrizePicks lines…</p></div>;

  return (
    <div style={{ display: 'grid', gridTemplateColumns: '1fr 320px', gap: 16 }}>
      <div>
        {/* Header: icon card on the left, title + subtitle in the middle,
            admin buttons on the right. Matches the mockup — larger card-style
            bullseye treatment, subtitle explains what "edge" means so the
            hint strip below can focus on actionable guidance. */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 14, marginBottom: 14 }}>
          <div style={{
            width: 56, height: 56, borderRadius: 10,
            background: 'rgba(245,197,24,0.08)',
            border: '1px solid rgba(245,197,24,0.25)',
            display: 'flex', alignItems: 'center', justifyContent: 'center',
            flexShrink: 0,
          }}>
            <svg viewBox="0 0 24 24" width="28" height="28" fill="none"
                 stroke="#F5C518" strokeWidth="1.75"
                 strokeLinecap="round" strokeLinejoin="round">
              <circle cx="12" cy="12" r="9"/>
              <circle cx="12" cy="12" r="5"/>
              <circle cx="12" cy="12" r="1.5" fill="#F5C518" stroke="none"/>
            </svg>
          </div>
          <div style={{ flex: 1, minWidth: 0 }}>
            <h2 className="section-head" style={{ margin: 0, lineHeight: 1.1 }}>PrizePicks Projections</h2>
            <p className="section-sub" style={{ margin: '4px 0 0 0' }}>
              All plays sorted by edge · Edge = Projected − PP Line
            </p>
          </div>
          {isAdmin && (
            <div style={{ display: 'flex', gap: 8, flexShrink: 0 }}>
              <button className="btn btn-outline" style={{ width: 'auto' }} onClick={() => setShowAdd(true)}>Add Line</button>
              <button className="btn btn-primary" style={{ width: 'auto' }} onClick={() => setShowBulk(true)}>Paste CSV</button>
            </div>
          )}
        </div>

        {/* Hint strip: amber-accented contextual guidance. Style matches the
            error banner below (same padding/radius/font) but uses amber
            instead of red so users distinguish "tip" from "problem". */}
        <div style={{
          background: 'rgba(245,197,24,0.08)',
          border: '1px solid rgba(245,197,24,0.25)',
          color: 'var(--text-muted)',
          padding: '10px 14px',
          borderRadius: 6,
          marginBottom: 12,
          fontSize: 12,
          display: 'flex', alignItems: 'center', gap: 10,
        }}>
          <span style={{ color: '#F5C518', fontSize: 14, lineHeight: 1 }}>⤵</span>
          <span><strong style={{ color: '#F5C518', fontWeight: 600 }}>Hint:</strong> PrizePicks bad value will typically reverse</span>
        </div>

        {error && <div style={{ background: 'rgba(239,68,68,0.1)', border: '1px solid rgba(239,68,68,0.3)', color: '#EF4444', padding: 10, borderRadius: 6, marginBottom: 12, fontSize: 12 }}>{error} <span style={{ cursor: 'pointer', marginLeft: 10 }} onClick={() => setError(null)}>✕</span></div>}

        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <SortHeader label="Player" col="player" />
                <SortHeader label="Stat" col="stat" />
                <SortHeader label="Line" col="line" num />
                <SortHeader label="Proj" col="projected" num />
                <SortHeader label="Edge" col="edge" num />
                <th className="muted">Updated</th>
                {isAdmin && <th></th>}
              </tr>
            </thead>
            <tbody>
              {sortedLines.length === 0 && (
                <tr><td colSpan={isAdmin ? 7 : 6} style={{ textAlign: 'center', padding: 30, color: 'var(--text-dim)' }}>No lines yet{isAdmin ? '. Click "Add Line" or "Paste CSV" to get started.' : '.'}</td></tr>
              )}
              {sortedLines.map(line => (
                <tr key={line.id} style={flashId === line.id ? { background: 'rgba(245,197,24,0.15)', transition: 'background 0.3s' } : {}}>
                  <td className="name">{line.raw_player_name}</td>
                  <td className="muted">{line.stat_type}</td>
                  <td className="num">
                    {isAdmin ? (
                      <InlineLineEdit value={line.current_line} onSave={(v) => onInlineUpdate(line, v)} />
                    ) : (
                      <span className="cell-proj">{line.current_line}</span>
                    )}
                  </td>
                  <td className="num muted">{line.projected != null ? (Math.round(line.projected * 100) / 100).toFixed(2) : '—'}</td>
                  <td className="num"><EdgeCell edge={line.edge} direction={line.direction} /></td>
                  <td className="muted" style={{ fontSize: 11 }}>{timeAgo(line.last_updated_at)}</td>
                  {isAdmin && (
                    <td><button onClick={() => onDelete(line.id)} style={{ background: 'none', border: 'none', color: 'var(--text-dim)', cursor: 'pointer', fontSize: 18 }}>×</button></td>
                  )}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      <div>
        <h3 className="section-head" style={{ fontSize: 14 }}>Live Line Movements</h3>
        <p className="section-sub">Last {movements.length}</p>
        <div style={{ maxHeight: 600, overflow: 'auto', background: 'var(--card)', border: '1px solid var(--border)', borderRadius: 8, padding: 6 }}>
          {movements.length === 0 && <div style={{ padding: 18, textAlign: 'center', color: 'var(--text-dim)', fontSize: 12 }}>No movements yet.</div>}
          {movements.map(m => <MovementRow key={m.id} move={m} />)}
        </div>
      </div>

      {showAdd && <AddLineModal onSave={onAdd} onClose={() => setShowAdd(false)} />}
      {showBulk && <BulkModal onSave={onBulk} onClose={() => setShowBulk(false)} />}
    </div>
  );
}

// Edge cell: projected − line. Green = MORE (we project over), red = LESS
// (we project under), muted em-dash when we don't have a projection for
// that stat (e.g., engine doesn't model 1st-set breakdowns yet). Magnitude
// threshold of ±0.2 avoids flagging noise as directional signal.
function EdgeCell({ edge, direction }) {
  if (edge == null) return <span style={{ color: 'var(--text-dim)' }}>—</span>;
  const color = direction === 'MORE' ? '#4ADE80'
              : direction === 'LESS' ? '#EF4444'
              : 'var(--text-muted)';
  const sign = edge > 0 ? '+' : '';
  return <span style={{ color, fontWeight: 600 }}>{sign}{edge.toFixed(2)}</span>;
}

function InlineLineEdit({ value, onSave }) {
  const [editing, setEditing] = useState(false);
  const [val, setVal] = useState(String(value));
  useEffect(() => { setVal(String(value)); }, [value]);
  if (!editing) {
    return <span className="cell-proj" style={{ cursor: 'pointer' }} onClick={() => setEditing(true)}>{value}</span>;
  }
  return (
    <input type="number" step="0.5" value={val} autoFocus onChange={e => setVal(e.target.value)}
      onBlur={() => { setEditing(false); const n = parseFloat(val); if (!isNaN(n)) onSave(n); }}
      onKeyDown={e => { if (e.key === 'Enter') e.currentTarget.blur(); if (e.key === 'Escape') { setVal(String(value)); setEditing(false); } }}
      style={{ width: 64, padding: '4px 6px', background: 'var(--bg)', border: '1px solid var(--primary)', borderRadius: 4, color: 'var(--text)', fontSize: 13, textAlign: 'center' }} />
  );
}

function MovementRow({ move }) {
  const colorMap = { up: '#4ADE80', down: '#EF4444', new: '#F5C518', removed: 'var(--text-dim)' };
  const arrowMap = { up: '↑', down: '↓', new: '+', removed: '✕' };
  const c = colorMap[move.direction] || 'var(--text)';
  return (
    <div style={{ padding: '8px 10px', borderBottom: '1px solid var(--border)', fontSize: 12 }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline' }}>
        <span style={{ fontWeight: 600 }}>{move.raw_player_name}</span>
        <span style={{ color: 'var(--text-dim)', fontSize: 10 }}>{timeAgo(move.detected_at)}</span>
      </div>
      <div style={{ color: 'var(--text-muted)', marginTop: 2 }}>
        {move.stat_type} · <span style={{ color: c, fontWeight: 600 }}>{arrowMap[move.direction]} {move.old_line ?? '—'} → {move.new_line}</span>
      </div>
    </div>
  );
}

function AddLineModal({ onSave, onClose }) {
  const [rawPlayerName, setName] = useState('');
  const [statType, setStat] = useState('Fantasy Score');
  const [currentLine, setLine] = useState('');
  const submit = (e) => {
    e.preventDefault();
    const n = parseFloat(currentLine);
    if (!rawPlayerName || !statType || isNaN(n)) return;
    onSave({ rawPlayerName, statType, currentLine: n });
  };
  return (
    <ModalShell onClose={onClose} title="Add PrizePicks Line">
      <form onSubmit={submit}>
        <Field label="Player name"><input autoFocus value={rawPlayerName} onChange={e => setName(e.target.value)} placeholder="e.g. Jannik Sinner" style={INPUT} /></Field>
        <Field label="Stat type"><select value={statType} onChange={e => setStat(e.target.value)} style={INPUT}>
          {['Fantasy Score', 'Total Games Won', 'Total Games', 'Aces', 'Double Faults', 'Total Sets', 'Breaks', 'Sets Won'].map(s => <option key={s}>{s}</option>)}
        </select></Field>
        <Field label="Line"><input type="number" step="0.5" value={currentLine} onChange={e => setLine(e.target.value)} placeholder="e.g. 22.5" style={INPUT} /></Field>
        <button className="btn btn-primary" type="submit" style={{ marginTop: 16 }}>Save</button>
      </form>
    </ModalShell>
  );
}

function BulkModal({ onSave, onClose }) {
  const [text, setText] = useState('');
  return (
    <ModalShell onClose={onClose} title="Paste CSV">
      <p style={{ fontSize: 12, color: 'var(--text-muted)', marginBottom: 10 }}>
        Format: <code>player,stat,line</code> — one row per line. Lines starting with # are skipped.
      </p>
      <textarea value={text} onChange={e => setText(e.target.value)} rows={10}
        placeholder={"Jannik Sinner,Fantasy Score,22.5\nCarlos Alcaraz,Aces,6.5\n# comment lines ok"}
        style={{ ...INPUT, fontFamily: 'monospace', fontSize: 12, height: 200 }} />
      <button className="btn btn-primary" onClick={() => onSave(text)} style={{ marginTop: 12 }}>Upload</button>
    </ModalShell>
  );
}

function ModalShell({ children, title, onClose }) {
  return (
    <div onClick={onClose} style={{ position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.5)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 }}>
      <div onClick={e => e.stopPropagation()} style={{ background: 'var(--card)', border: '1px solid var(--border-light)', borderRadius: 10, padding: 24, width: 420, maxWidth: '90vw' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
          <h3 style={{ fontSize: 16, fontWeight: 700 }}>{title}</h3>
          <button onClick={onClose} style={{ background: 'none', border: 'none', color: 'var(--text-dim)', fontSize: 20, cursor: 'pointer' }}>×</button>
        </div>
        {children}
      </div>
    </div>
  );
}

function Field({ label, children }) {
  return <div style={{ marginBottom: 12 }}>
    <label style={{ display: 'block', fontSize: 11, fontWeight: 600, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: 4 }}>{label}</label>
    {children}
  </div>;
}

const INPUT = { width: '100%', padding: '8px 10px', background: 'var(--bg)', border: '1px solid var(--border-light)', borderRadius: 6, color: 'var(--text)', fontSize: 13, fontFamily: 'inherit' };

function timeAgo(iso) {
  if (!iso) return '—';
  const s = Math.floor((Date.now() - new Date(iso).getTime()) / 1000);
  if (s < 60) return `${s}s ago`;
  if (s < 3600) return `${Math.floor(s / 60)}m ago`;
  if (s < 86400) return `${Math.floor(s / 3600)}h ago`;
  return `${Math.floor(s / 86400)}d ago`;
}
