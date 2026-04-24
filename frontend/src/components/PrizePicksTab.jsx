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

export function PrizePicksTab({ slateId }) {
  const [lines, setLines] = useState([]);
  const [movements, setMovements] = useState([]);
  const [loading, setLoading] = useState(true);
  const [isAdmin, setIsAdmin] = useState(false);
  const [flashId, setFlashId] = useState(null);
  const [showAdd, setShowAdd] = useState(false);
  const [showBulk, setShowBulk] = useState(false);
  const [error, setError] = useState(null);

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

  if (!slateId) return <div className="empty"><p>No active slate.</p></div>;
  if (loading) return <div className="empty"><p>Loading PrizePicks lines…</p></div>;

  return (
    <div style={{ display: 'grid', gridTemplateColumns: '1fr 320px', gap: 16 }}>
      <div>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 14 }}>
          <div>
            <h2 className="section-head">PrizePicks Lines</h2>
            <p className="section-sub">{lines.length} active line{lines.length === 1 ? '' : 's'}</p>
          </div>
          {isAdmin && (
            <div style={{ display: 'flex', gap: 8 }}>
              <button className="btn btn-outline" style={{ width: 'auto' }} onClick={() => setShowAdd(true)}>Add Line</button>
              <button className="btn btn-primary" style={{ width: 'auto' }} onClick={() => setShowBulk(true)}>Paste CSV</button>
            </div>
          )}
        </div>

        {error && <div style={{ background: 'rgba(239,68,68,0.1)', border: '1px solid rgba(239,68,68,0.3)', color: '#EF4444', padding: 10, borderRadius: 6, marginBottom: 12, fontSize: 12 }}>{error} <span style={{ cursor: 'pointer', marginLeft: 10 }} onClick={() => setError(null)}>✕</span></div>}

        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Player</th>
                <th>Stat</th>
                <th className="num">Line</th>
                <th className="muted">Updated</th>
                {isAdmin && <th></th>}
              </tr>
            </thead>
            <tbody>
              {lines.length === 0 && (
                <tr><td colSpan={isAdmin ? 5 : 4} style={{ textAlign: 'center', padding: 30, color: 'var(--text-dim)' }}>No lines yet{isAdmin ? '. Click "Add Line" or "Paste CSV" to get started.' : '.'}</td></tr>
              )}
              {lines.map(line => (
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
  const colorMap = { up: '#EF4444', down: '#4ADE80', new: '#F5C518', removed: 'var(--text-dim)' };
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
