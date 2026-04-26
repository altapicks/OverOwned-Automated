// ═══════════════════════════════════════════════════════════════════════
// PrizePicks tab — admin entry + realtime display
//
// v6.5.1 — two targeted changes on top of v6.5:
//   1. Player search bar above the table — filters rows in real time by
//      raw_player_name (case/punctuation-insensitive). Plays well with
//      the stat-category tabs: search narrows the active tab.
//   2. Projections + Edge are now ONLY computed for Fantasy Score per
//      the user's spec. Other tabs (Aces, DFs, Breakpoints, Games Won,
//      Total Sets, etc.) display the line + multiplier + odds_type as
//      browse-only — Proj and Edge columns show "—". This stops misleading
//      stat-by-stat edge signals from competing with the sharp FS edge.
//
// v6.5 — stat-category tab bar at top. Default tab is Fantasy Score.
// Backend default also changed: /api/prizepicks/lines now returns every
// stat type by default; client filters via the active tab.
// ═══════════════════════════════════════════════════════════════════════
import React, { useState, useEffect, useMemo, useCallback } from 'react';
import {
  fetchLines, fetchRecentMovements,
  createLine, updateLine, deleteLine, bulkUpsertLines,
  checkIsAdmin, subscribeToLines, subscribeToMovements, parseCsvLines,
} from '../lib/prizepicks-api';

const STAT_TABS = [
  { key: 'Fantasy Score',     label: 'Fantasy Score' },
  { key: 'Aces',              label: 'Aces' },
  { key: 'Double Faults',     label: 'Double Faults' },
  { key: 'Break Points Won',  label: 'Breakpoints' },
  { key: 'Total Games Won',   label: 'Games Won' },
  { key: 'Total Games',       label: 'Total Games' },
  { key: 'Total Sets',        label: 'Total Sets' },
  { key: 'Total Tie Breaks',  label: 'Tiebreakers' },
];
const DEFAULT_TAB = 'Fantasy Score';

// Per spec: only Fantasy Score gets a model projection + edge in this tab.
const PROJECTED_STATS = new Set(['Fantasy Score']);

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
  const [activeStat, setActiveStat] = useState(DEFAULT_TAB);
  const [searchQ, setSearchQ] = useState('');

  useEffect(() => {
    if (!slateId) return;
    let cancelled = false;
    setLoading(true);
    Promise.all([
      fetchLines(slateId, 'all'),
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

  const tabCounts = useMemo(() => {
    const counts = {};
    (lines || []).forEach(l => {
      const k = l.stat_type;
      if (k) counts[k] = (counts[k] || 0) + 1;
    });
    return counts;
  }, [lines]);

  const playersByName = useMemo(() => {
    const normalize = s => String(s || '')
      .toLowerCase()
      .replace(/[.,'’`]/g, '')
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

    const exact = {};
    const normExact = {};
    const byFirstLast = {};
    const bySurname = {};

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

  const warnedMissingRef = React.useRef(new Set());
  React.useEffect(() => { warnedMissingRef.current = new Set(); }, [players]);

  const lookupPlayer = useCallback((rawName) => {
    if (!rawName) return null;
    const { exact, normExact, byFirstLast, bySurname, normalize, firstLast, surname } = playersByName;
    const trimmed = String(rawName).trim();
    if (exact[trimmed]) return exact[trimmed];
    const ne = normalize(trimmed);
    if (normExact[ne]) return normExact[ne];
    const fl = firstLast(trimmed);
    if (fl && byFirstLast[fl]) return byFirstLast[fl];
    const sn = surname(trimmed);
    if (sn && bySurname[sn]) return bySurname[sn];
    if (!warnedMissingRef.current.has(trimmed)) {
      warnedMissingRef.current.add(trimmed);
      const availableNames = Object.keys(exact).slice(0, 5).join(', ');
      console.warn(`[PrizePicksTab] No player match for "${trimmed}" (normalized: "${ne}"). First 5 DK names: ${availableNames}`);
    }
    return null;
  }, [playersByName]);

  // v6.5.1: only Fantasy Score gets a model projection. Per spec — leave
  // non-FS lines as browse-only so noisy stat-by-stat edges don't compete
  // with the sharp FS edge.
  const getProjectedForStat = useCallback((playerName, stat) => {
    if (!PROJECTED_STATS.has(stat)) return null;
    const p = lookupPlayer(playerName);
    if (!p) return null;
    if (stat === 'Fantasy Score') return p.ppProj;
    return null;
  }, [lookupPlayer]);

  const enrichedLines = useMemo(() => {
    return (lines || []).map(line => {
      const projected = getProjectedForStat(line.raw_player_name, line.stat_type);
      const edge = (projected != null) ? Math.round((projected - line.current_line) * 100) / 100 : null;
      const direction = edge == null ? '-' : (edge > 0.2 ? 'MORE' : edge < -0.2 ? 'LESS' : '-');
      return { ...line, projected, edge, direction };
    });
  }, [lines, getProjectedForStat]);

  // Filter by active stat tab AND search query.
  const filteredLines = useMemo(() => {
    const q = (searchQ || '').toLowerCase().trim()
      .replace(/[.,'’`]/g, '').replace(/\s+/g, ' ');
    return enrichedLines.filter(l => {
      if (l.stat_type !== activeStat) return false;
      if (!q) return true;
      const hay = String(l.raw_player_name || '').toLowerCase()
        .replace(/[.,'’`]/g, '').replace(/\s+/g, ' ');
      return hay.includes(q);
    });
  }, [enrichedLines, activeStat, searchQ]);

  // Top 3 PP fades — only meaningful on Fantasy Score now (other tabs have
  // null edge so this Set will always be empty for them).
  const topFadeIds = useMemo(() => {
    const withEdge = filteredLines.filter(l => l.edge != null && l.edge < -0.5);
    withEdge.sort((a, b) => a.edge - b.edge);
    return new Set(withEdge.slice(0, 3).map(l => l.id));
  }, [filteredLines]);

  const sortedLines = useMemo(() => {
    const arr = [...filteredLines];
    arr.sort((a, b) => {
      let va, vb;
      switch (sortKey) {
        case 'player': va = a.raw_player_name; vb = b.raw_player_name; break;
        case 'stat': va = a.stat_type; vb = b.stat_type; break;
        case 'line': va = a.current_line; vb = b.current_line; break;
        case 'mult':
          va = a.multiplier != null ? Number(a.multiplier) : -Infinity;
          vb = b.multiplier != null ? Number(b.multiplier) : -Infinity;
          break;
        case 'odds_type':
          va = a.odds_type || ''; vb = b.odds_type || ''; break;
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
  }, [filteredLines, sortKey, sortDir]);

  const toggleSort = (key) => {
    if (sortKey === key) setSortDir(d => d === 'asc' ? 'desc' : 'asc');
    else { setSortKey(key); setSortDir(key === 'player' || key === 'stat' || key === 'odds_type' ? 'asc' : 'desc'); }
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

  if (!slateId) return <div className="empty"><p>No active slate.</p></div>;
  if (loading) return <div className="empty"><p>Loading PrizePicks lines…</p></div>;

  const isFsTab = activeStat === 'Fantasy Score';
  const totalRowsForTab = (lines || []).filter(l => l.stat_type === activeStat).length;

  return (
    <div className="pp-grid">
      <div>
        <div className="section-hero">
          <div className="section-hero-icon-wrap">
            <svg className="section-hero-icon" viewBox="0 0 24 24" fill="none" stroke="#F5C518">
              <circle cx="12" cy="12" r="9"/>
              <circle cx="12" cy="12" r="5"/>
              <circle cx="12" cy="12" r="1.5" fill="#F5C518" stroke="none"/>
            </svg>
          </div>
          <div className="section-hero-text">
            <h2 className="section-hero-title">PrizePicks Projections</h2>
            <div className="section-hero-sub">
              {isFsTab
                ? 'Sorted by edge · Edge = Projected − PP Line'
                : `Browse ${activeStat} lines · projections are FS-only`}
            </div>
          </div>
          {isAdmin && (
            <div style={{ display: 'flex', gap: 8, flexShrink: 0 }}>
              <button className="btn btn-outline" style={{ width: 'auto', padding: '7px 14px', fontSize: 13 }} onClick={() => setShowAdd(true)}>Add Line</button>
              <button className="btn btn-primary" style={{ width: 'auto', padding: '7px 14px', fontSize: 13 }} onClick={() => setShowBulk(true)}>Paste CSV</button>
            </div>
          )}
        </div>

        <StatTabs
          tabs={STAT_TABS}
          counts={tabCounts}
          active={activeStat}
          onChange={setActiveStat}
        />

        <SearchBar
          value={searchQ}
          onChange={setSearchQ}
          placeholder={`Search ${activeStat} players…`}
          totalCount={totalRowsForTab}
          shownCount={filteredLines.length}
        />

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
          <svg viewBox="0 0 24 24" width="16" height="16" fill="none"
               stroke="#F5C518" strokeWidth="2"
               strokeLinecap="round" strokeLinejoin="round"
               style={{ flexShrink: 0 }}>
            <polyline points="3 17 9 11 13 14 21 6"/>
            <polyline points="15 6 21 6 21 12"/>
          </svg>
          <span>
            <strong style={{ color: '#F5C518', fontWeight: 600 }}>Hint:</strong>{' '}
            {isFsTab
              ? 'PrizePicks bad value will typically reverse'
              : 'Browse-only — only Fantasy Score has a model projection to compare against'}
          </span>
        </div>

        {error && <div style={{ background: 'rgba(239,68,68,0.1)', border: '1px solid rgba(239,68,68,0.3)', color: '#EF4444', padding: 10, borderRadius: 6, marginBottom: 12, fontSize: 12 }}>{error} <span style={{ cursor: 'pointer', marginLeft: 10 }} onClick={() => setError(null)}>✕</span></div>}

        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th style={{ width: 24 }}></th>
                <SortHeader label="Player" col="player" />
                <SortHeader label="Stat" col="stat" />
                <SortHeader label="Line" col="line" num />
                <SortHeader label="Mult" col="mult" num />
                <SortHeader label="Type" col="odds_type" />
                <SortHeader label="Proj" col="projected" num />
                <SortHeader label="Edge" col="edge" num />
                <th className="muted">Updated</th>
                {isAdmin && <th></th>}
              </tr>
            </thead>
            <tbody>
              {sortedLines.length === 0 && (
                <tr><td colSpan={isAdmin ? 10 : 9} style={{ textAlign: 'center', padding: 30, color: 'var(--text-dim)' }}>
                  {searchQ
                    ? <>No {activeStat} lines match "<strong>{searchQ}</strong>".</>
                    : <>No PrizePicks lines for {activeStat} on this slate.</>}
                  {isAdmin && !searchQ && <> Click "Add Line" or "Paste CSV" to add some.</>}
                </td></tr>
              )}
              {sortedLines.map(line => {
                const isFade = topFadeIds.has(line.id);
                const isFlash = flashId === line.id;
                const cellBg = isFlash
                  ? 'rgba(245,197,24,0.15)'
                  : isFade
                    ? 'rgba(74,222,128,0.10)'
                    : 'transparent';
                const cellStyle = { background: cellBg, transition: 'background 0.3s' };
                const multStr = line.multiplier != null
                  ? Number(line.multiplier).toFixed(2)
                  : '—';
                return (
                <tr key={line.id}>
                  <td style={{ ...cellStyle, textAlign: 'center', padding: '6px 4px' }}>
                    {isFade && (
                      <span title="Top PP Fade — model projects significantly under the posted line">
                        <svg viewBox="0 0 24 24" width="14" height="14" fill="none"
                             stroke="#4ADE80" strokeWidth="1.75"
                             strokeLinecap="round" strokeLinejoin="round"
                             style={{ display: 'block', margin: '0 auto' }}>
                          <path d="M6 3h12l3 6-9 12L3 9z"/>
                          <path d="M3 9h18"/>
                          <path d="M9 3l3 6 3-6"/>
                        </svg>
                      </span>
                    )}
                  </td>
                  <td className="name" style={cellStyle}>{line.raw_player_name}</td>
                  <td className="muted" style={cellStyle}>{line.stat_type}</td>
                  <td className="num" style={cellStyle}>
                    {isAdmin ? (
                      <InlineLineEdit value={line.current_line} onSave={(v) => onInlineUpdate(line, v)} />
                    ) : (
                      <span className="cell-proj">{line.current_line}</span>
                    )}
                  </td>
                  <td className="num muted" style={cellStyle}>{multStr}</td>
                  <td style={cellStyle}><OddsTypePill type={line.odds_type} /></td>
                  <td className="num muted" style={cellStyle}>{line.projected != null ? (Math.round(line.projected * 100) / 100).toFixed(2) : '—'}</td>
                  <td className="num" style={cellStyle}><EdgeCell edge={line.edge} direction={line.direction} /></td>
                  <td className="muted" style={{ ...cellStyle, fontSize: 11 }}>{timeAgo(line.last_updated_at)}</td>
                  {isAdmin && (
                    <td style={cellStyle}><button onClick={() => onDelete(line.id)} style={{ background: 'none', border: 'none', color: 'var(--text-dim)', cursor: 'pointer', fontSize: 18 }}>×</button></td>
                  )}
                </tr>
                );
              })}
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

// ────────────────────────────────────────────────────────────────────
// Search bar (v6.5.1)
// Magnifying-glass icon on the left, clear-✕ on the right when there's
// a query, count chip on the far right showing "shown / total" for the
// active tab.
// ────────────────────────────────────────────────────────────────────
function SearchBar({ value, onChange, placeholder, totalCount, shownCount }) {
  const hasQuery = value && value.length > 0;
  return (
    <div style={{
      position: 'relative',
      marginBottom: 12,
      display: 'flex',
      alignItems: 'center',
    }}>
      <svg viewBox="0 0 24 24" width="14" height="14" fill="none"
           stroke="var(--text-dim)" strokeWidth="2"
           strokeLinecap="round" strokeLinejoin="round"
           style={{ position: 'absolute', left: 12, pointerEvents: 'none' }}>
        <circle cx="11" cy="11" r="7"/>
        <line x1="21" y1="21" x2="16.5" y2="16.5"/>
      </svg>
      <input
        type="text"
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
        style={{
          width: '100%',
          padding: '9px 12px 9px 34px',
          background: 'var(--bg)',
          border: '1px solid var(--border-light)',
          borderRadius: 7,
          color: 'var(--text)',
          fontSize: 13,
          fontFamily: 'inherit',
          outline: 'none',
          transition: 'border-color 0.15s',
        }}
        onFocus={(e) => { e.currentTarget.style.borderColor = 'rgba(245,197,24,0.4)'; }}
        onBlur={(e) => { e.currentTarget.style.borderColor = 'var(--border-light)'; }}
      />
      {hasQuery && (
        <button
          onClick={() => onChange('')}
          aria-label="Clear search"
          style={{
            position: 'absolute',
            right: totalCount != null ? 84 : 10,
            background: 'none',
            border: 'none',
            color: 'var(--text-dim)',
            cursor: 'pointer',
            fontSize: 16,
            padding: '0 4px',
            lineHeight: 1,
          }}
        >×</button>
      )}
      {totalCount != null && (
        <span style={{
          position: 'absolute',
          right: 12,
          fontSize: 11,
          color: 'var(--text-muted)',
          fontVariantNumeric: 'tabular-nums',
          pointerEvents: 'none',
        }}>
          {hasQuery ? `${shownCount} / ${totalCount}` : `${totalCount}`}
        </span>
      )}
    </div>
  );
}

function StatTabs({ tabs, counts, active, onChange }) {
  return (
    <div style={{
      display: 'flex',
      gap: 6,
      marginBottom: 10,
      overflowX: 'auto',
      paddingBottom: 4,
      borderBottom: '1px solid var(--border)',
    }}>
      {tabs.map(t => {
        const isActive = t.key === active;
        const count = counts[t.key] || 0;
        return (
          <button
            key={t.key}
            onClick={() => onChange(t.key)}
            style={{
              flexShrink: 0,
              padding: '7px 12px',
              border: `1px solid ${isActive ? '#F5C518' : 'var(--border-light)'}`,
              borderRadius: 6,
              background: isActive ? 'rgba(245,197,24,0.08)' : 'transparent',
              color: isActive ? '#F5C518' : 'var(--text-muted)',
              fontSize: 12,
              fontWeight: isActive ? 600 : 500,
              cursor: 'pointer',
              transition: 'border-color 0.15s, color 0.15s, background 0.15s',
              whiteSpace: 'nowrap',
              fontFamily: 'inherit',
            }}
            onMouseEnter={(e) => {
              if (!isActive) e.currentTarget.style.color = 'var(--text)';
            }}
            onMouseLeave={(e) => {
              if (!isActive) e.currentTarget.style.color = 'var(--text-muted)';
            }}
          >
            {t.label}
            {count > 0 && (
              <span style={{
                marginLeft: 6,
                fontSize: 10,
                opacity: 0.7,
                fontWeight: 500,
              }}>
                {count}
              </span>
            )}
          </button>
        );
      })}
    </div>
  );
}

function EdgeCell({ edge, direction }) {
  if (edge == null) return <span style={{ color: 'var(--text-dim)' }}>—</span>;
  const color = direction === 'MORE' ? '#4ADE80'
              : direction === 'LESS' ? '#EF4444'
              : 'var(--text-muted)';
  const sign = edge > 0 ? '+' : '';
  return <span style={{ color, fontWeight: 600 }}>{sign}{edge.toFixed(2)}</span>;
}

function OddsTypePill({ type }) {
  const t = (type || 'standard').toLowerCase();
  const styles = {
    goblin:   { bg: 'rgba(74,222,128,0.12)',  border: 'rgba(74,222,128,0.4)',  color: '#4ADE80', label: 'Gob' },
    standard: { bg: 'rgba(139,154,186,0.10)', border: 'rgba(139,154,186,0.3)', color: '#8B9ABA', label: 'Std' },
    demon:    { bg: 'rgba(239,68,68,0.10)',   border: 'rgba(239,68,68,0.35)',  color: '#EF4444', label: 'Dem' },
  };
  const s = styles[t] || styles.standard;
  return (
    <span style={{
      display: 'inline-block',
      padding: '2px 6px',
      fontSize: 10,
      fontWeight: 600,
      letterSpacing: '0.04em',
      textTransform: 'uppercase',
      background: s.bg,
      border: `1px solid ${s.border}`,
      borderRadius: 4,
      color: s.color,
    }}>{s.label}</span>
  );
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
          {STAT_TABS.map(t => <option key={t.key} value={t.key}>{t.key}</option>)}
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
