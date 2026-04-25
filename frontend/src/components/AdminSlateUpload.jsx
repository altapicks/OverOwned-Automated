// ═══════════════════════════════════════════════════════════════════════
// AdminSlateUpload — operator-facing modal for v6.0 manual slate uploads.
//
// Two-step flow:
//   1. FORM      — operator fills in slate metadata + drops a CSV
//   2. PREVIEW   — backend dry_run result; warnings/orphans surfaced;
//                  "Confirm & Publish" sends the real upload
//   3. SUCCESS   — slate_id + write summary; close to dismiss
//
// Auth + endpoint: api.uploadSlateManual handles Supabase JWT, ET→UTC
// conversion of lock_time, and FormData assembly.
//
// Styling uses the app's CSS vars (--card, --border-light, --text,
// --text-muted, --text-dim, --primary, --green, --red, --amber) and the
// existing .btn / .btn-primary classes — no new global styles introduced.
// ═══════════════════════════════════════════════════════════════════════

import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { uploadSlateManual } from '../lib/api';

const SURFACES = ['clay', 'hard', 'grass', 'indoor hard'];

function todayISODate() {
  const d = new Date();
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, '0');
  const day = String(d.getDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
}

export function AdminSlateUpload({ onClose }) {
  const [step, setStep] = useState('form'); // 'form' | 'preview' | 'success'
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState(null);

  // Form state
  const [sport, setSport] = useState('tennis');
  const [slateDate, setSlateDate] = useState(todayISODate());
  const [tournament, setTournament] = useState('');
  const [surface, setSurface] = useState('clay');
  const [lockTimeETLocal, setLockTimeETLocal] = useState('');
  const [file, setFile] = useState(null);
  const [dragOver, setDragOver] = useState(false);

  // Result state
  const [previewResult, setPreviewResult] = useState(null);
  const [publishResult, setPublishResult] = useState(null);

  const fileInputRef = useRef(null);

  // ── ESC closes modal ───────────────────────────────────────────────
  useEffect(() => {
    const onKey = (e) => {
      if (e.key === 'Escape' && !busy) onClose?.();
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onClose, busy]);

  // ── File drop handlers ────────────────────────────────────────────
  const onDragOver = useCallback((e) => {
    e.preventDefault();
    setDragOver(true);
  }, []);
  const onDragLeave = useCallback(() => setDragOver(false), []);
  const onDrop = useCallback((e) => {
    e.preventDefault();
    setDragOver(false);
    const f = e.dataTransfer?.files?.[0];
    if (f) setFile(f);
  }, []);
  const onFilePick = useCallback((e) => {
    const f = e.target.files?.[0];
    if (f) setFile(f);
  }, []);

  // ── Validation ─────────────────────────────────────────────────────
  const formValid = useMemo(
    () => Boolean(file && sport && slateDate && tournament),
    [file, sport, slateDate, tournament]
  );

  // ── Run dry-run ────────────────────────────────────────────────────
  const runDryRun = useCallback(async () => {
    if (!formValid || busy) return;
    setBusy(true);
    setErr(null);
    try {
      const meta = { sport, slate_date: slateDate, tournament, surface, lockTimeETLocal };
      const res = await uploadSlateManual(file, meta, true);
      setPreviewResult(res);
      setStep('preview');
    } catch (e) {
      setErr(e?.message || 'Dry-run failed');
    } finally {
      setBusy(false);
    }
  }, [file, sport, slateDate, tournament, surface, lockTimeETLocal, formValid, busy]);

  // ── Run real publish ───────────────────────────────────────────────
  const runPublish = useCallback(async () => {
    if (busy) return;
    setBusy(true);
    setErr(null);
    try {
      const meta = { sport, slate_date: slateDate, tournament, surface, lockTimeETLocal };
      const res = await uploadSlateManual(file, meta, false);
      setPublishResult(res);
      setStep('success');
    } catch (e) {
      setErr(e?.message || 'Publish failed');
    } finally {
      setBusy(false);
    }
  }, [file, sport, slateDate, tournament, surface, lockTimeETLocal, busy]);

  // ── Render helpers ─────────────────────────────────────────────────
  const okToPublish =
    previewResult?.ok &&
    (previewResult?.errors || []).length === 0 &&
    (previewResult?.unmatched_names || []).length === 0;

  return (
    <div
      className="modal-overlay"
      onClick={() => !busy && onClose?.()}
      style={{
        position: 'fixed',
        inset: 0,
        background: 'rgba(0,0,0,0.7)',
        display: 'flex',
        alignItems: 'flex-start',
        justifyContent: 'center',
        zIndex: 1000,
        padding: '60px 20px 40px',
        overflowY: 'auto',
      }}
    >
      <div
        className="modal-card"
        onClick={(e) => e.stopPropagation()}
        style={{
          background: 'var(--card)',
          border: '1px solid var(--border-light)',
          borderRadius: 12,
          maxWidth: 720,
          width: '100%',
          padding: 24,
          color: 'var(--text)',
        }}
      >
        {/* Header */}
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 16 }}>
          <h2 style={{ margin: 0, fontSize: 20, fontWeight: 700 }}>
            Slate Upload <span style={{ color: 'var(--text-dim)', fontWeight: 400, fontSize: 13 }}>v6.0</span>
          </h2>
          <button
            onClick={() => !busy && onClose?.()}
            disabled={busy}
            style={{
              background: 'transparent',
              border: 'none',
              color: 'var(--text-muted)',
              fontSize: 22,
              cursor: busy ? 'not-allowed' : 'pointer',
              padding: '0 6px',
            }}
            title="Close (Esc)"
          >
            ×
          </button>
        </div>

        {err && (
          <div
            style={{
              background: 'rgba(239,68,68,0.1)',
              border: '1px solid rgba(239,68,68,0.3)',
              color: 'var(--red)',
              padding: 10,
              borderRadius: 6,
              marginBottom: 12,
              fontSize: 13,
            }}
          >
            {err}
          </div>
        )}

        {step === 'form' && (
          <FormStep
            sport={sport}
            setSport={setSport}
            slateDate={slateDate}
            setSlateDate={setSlateDate}
            tournament={tournament}
            setTournament={setTournament}
            surface={surface}
            setSurface={setSurface}
            lockTimeETLocal={lockTimeETLocal}
            setLockTimeETLocal={setLockTimeETLocal}
            file={file}
            setFile={setFile}
            dragOver={dragOver}
            onDragOver={onDragOver}
            onDragLeave={onDragLeave}
            onDrop={onDrop}
            onFilePick={onFilePick}
            fileInputRef={fileInputRef}
            formValid={formValid}
            busy={busy}
            onCancel={onClose}
            onDryRun={runDryRun}
          />
        )}

        {step === 'preview' && previewResult && (
          <PreviewStep
            result={previewResult}
            okToPublish={okToPublish}
            busy={busy}
            onBack={() => setStep('form')}
            onPublish={runPublish}
          />
        )}

        {step === 'success' && publishResult && (
          <SuccessStep result={publishResult} onClose={onClose} />
        )}
      </div>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────
// Step components
// ─────────────────────────────────────────────────────────────────────

function FormStep(props) {
  const {
    sport, setSport,
    slateDate, setSlateDate,
    tournament, setTournament,
    surface, setSurface,
    lockTimeETLocal, setLockTimeETLocal,
    file, setFile,
    dragOver, onDragOver, onDragLeave, onDrop, onFilePick,
    fileInputRef,
    formValid, busy,
    onCancel, onDryRun,
  } = props;

  return (
    <>
      <p style={{ color: 'var(--text-muted)', fontSize: 13, marginTop: 0, marginBottom: 16 }}>
        Upload a CSV to create or update today's slate. Re-uploads to the same date
        update in place — match.odds, closing_odds, and Kalshi data are preserved.
      </p>

      {/* Metadata grid */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12, marginBottom: 12 }}>
        <Field label="Sport">
          <select
            value={sport}
            onChange={(e) => setSport(e.target.value)}
            disabled={busy}
            style={inputStyle}
          >
            <option value="tennis">tennis</option>
            <option value="mma">mma</option>
            <option value="nba">nba</option>
          </select>
        </Field>

        <Field label="Slate date">
          <input
            type="date"
            value={slateDate}
            onChange={(e) => setSlateDate(e.target.value)}
            disabled={busy}
            style={inputStyle}
          />
        </Field>

        <Field label="Tournament" hint="e.g. Madrid">
          <input
            type="text"
            value={tournament}
            onChange={(e) => setTournament(e.target.value)}
            placeholder="Tournament name"
            disabled={busy}
            style={inputStyle}
          />
        </Field>

        <Field label="Surface">
          <select
            value={surface}
            onChange={(e) => setSurface(e.target.value)}
            disabled={busy}
            style={inputStyle}
          >
            {SURFACES.map((s) => (
              <option key={s} value={s}>{s}</option>
            ))}
          </select>
        </Field>

        <Field label="Lock time (ET)" hint="Earliest match start in ET">
          <input
            type="datetime-local"
            value={lockTimeETLocal}
            onChange={(e) => setLockTimeETLocal(e.target.value)}
            disabled={busy}
            style={inputStyle}
          />
        </Field>
      </div>

      {/* File drop zone */}
      <div
        onDragOver={onDragOver}
        onDragLeave={onDragLeave}
        onDrop={onDrop}
        onClick={() => fileInputRef.current?.click()}
        style={{
          border: dragOver
            ? '2px dashed var(--primary)'
            : '2px dashed var(--border-light)',
          borderRadius: 8,
          padding: '28px 16px',
          textAlign: 'center',
          cursor: busy ? 'not-allowed' : 'pointer',
          background: dragOver ? 'rgba(245,197,24,0.06)' : 'transparent',
          transition: 'border-color 120ms, background 120ms',
          marginBottom: 12,
        }}
      >
        <div style={{ fontSize: 32, marginBottom: 6, color: 'var(--text-dim)' }}>📄</div>
        {file ? (
          <>
            <div style={{ fontSize: 14, fontWeight: 600, color: 'var(--text)' }}>{file.name}</div>
            <div style={{ fontSize: 12, color: 'var(--text-muted)', marginTop: 4 }}>
              {(file.size / 1024).toFixed(1)} KB · click to replace
            </div>
          </>
        ) : (
          <>
            <div style={{ fontSize: 14, color: 'var(--text)' }}>Drop CSV here, or click to choose</div>
            <div style={{ fontSize: 12, color: 'var(--text-muted)', marginTop: 4 }}>
              Filename convention: <code>{`{sport}-{YYYY-MM-DD}.csv`}</code>
            </div>
          </>
        )}
        <input
          ref={fileInputRef}
          type="file"
          accept=".csv,text/csv"
          onChange={onFilePick}
          disabled={busy}
          style={{ display: 'none' }}
        />
      </div>

      {/* Actions */}
      <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 8, marginTop: 8 }}>
        <button
          className="btn"
          onClick={onCancel}
          disabled={busy}
          style={btnStyle}
        >
          Cancel
        </button>
        <button
          className="btn btn-primary"
          onClick={onDryRun}
          disabled={!formValid || busy}
          style={{ ...btnStyle, ...btnPrimaryStyle, opacity: !formValid || busy ? 0.5 : 1 }}
        >
          {busy ? 'Validating…' : 'Run Dry-Run Preview'}
        </button>
      </div>
    </>
  );
}

function PreviewStep({ result, okToPublish, busy, onBack, onPublish }) {
  const s = result.summary || {};
  const warnings = result.warnings || [];
  const errors = result.errors || [];
  const unmatched = result.unmatched_names || [];

  const orphanMatches = s.orphan_preview_matches || 0;
  const orphanPlayers = s.orphan_preview_slate_players || 0;
  const hasOrphans = orphanMatches > 0 || orphanPlayers > 0;

  return (
    <>
      <div style={{ marginBottom: 16 }}>
        <Stat label="Rows parsed" value={s.rows_parsed} />
        <Stat label="Matches paired" value={s.matches_paired} />
        <Stat label="Players paired" value={s.players_paired} />
        <Stat label="FS lines" value={s.fs_lines} />
        <Stat
          label="Sim ownership total"
          value={s.sim_own_total !== undefined ? `${s.sim_own_total}%` : '—'}
        />
      </div>

      {/* Errors block */}
      {errors.length > 0 && (
        <Block tone="error" title={`${errors.length} error${errors.length === 1 ? '' : 's'}`}>
          {errors.map((e, i) => (
            <div key={i}>{e}</div>
          ))}
        </Block>
      )}

      {/* Unmatched names block */}
      {unmatched.length > 0 && (
        <Block tone="error" title={`Unmatched names (${unmatched.length})`}>
          <div style={{ fontSize: 12, marginBottom: 6 }}>
            These names don't exist in the players table. Add aliases or fix spelling, then re-upload.
          </div>
          <div style={{ fontFamily: 'monospace', fontSize: 12 }}>
            {unmatched.join(', ')}
          </div>
        </Block>
      )}

      {/* Orphan preview block */}
      {hasOrphans && (
        <Block
          tone="warn"
          title="Heads-up: this upload would orphan existing rows"
        >
          <div style={{ fontSize: 13 }}>
            {orphanMatches > 0 && (
              <div>{orphanMatches} match(es) will be deleted (present in DB, missing from new CSV)</div>
            )}
            {orphanPlayers > 0 && (
              <div>{orphanPlayers} slate_player row(s) will be deleted</div>
            )}
            <div style={{ marginTop: 6, color: 'var(--text-muted)', fontSize: 12 }}>
              If this is unexpected, you may have uploaded the wrong CSV. Cancel and verify.
            </div>
          </div>
        </Block>
      )}

      {/* Warnings block */}
      {warnings.length > 0 && (
        <Block tone="warn" title={`${warnings.length} warning${warnings.length === 1 ? '' : 's'}`}>
          {warnings.map((w, i) => (
            <div key={i} style={{ fontSize: 12 }}>{w}</div>
          ))}
        </Block>
      )}

      {okToPublish && errors.length === 0 && unmatched.length === 0 && (
        <Block tone="ok" title="Ready to publish">
          <div style={{ fontSize: 13 }}>
            Validation passed. Confirm to write to the database.
          </div>
        </Block>
      )}

      {/* Actions */}
      <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 8, marginTop: 16 }}>
        <button
          className="btn"
          onClick={onBack}
          disabled={busy}
          style={btnStyle}
        >
          Back
        </button>
        <button
          className="btn btn-primary"
          onClick={onPublish}
          disabled={!okToPublish || busy}
          style={{ ...btnStyle, ...btnPrimaryStyle, opacity: !okToPublish || busy ? 0.5 : 1 }}
        >
          {busy ? 'Publishing…' : 'Confirm & Publish'}
        </button>
      </div>
    </>
  );
}

function SuccessStep({ result, onClose }) {
  const s = result.summary || {};
  return (
    <>
      <div
        style={{
          background: 'rgba(34,197,94,0.1)',
          border: '1px solid rgba(34,197,94,0.3)',
          borderRadius: 8,
          padding: 14,
          marginBottom: 16,
        }}
      >
        <div style={{ fontSize: 16, fontWeight: 600, color: 'var(--green)', marginBottom: 4 }}>
          Slate published ✓
        </div>
        <div style={{ fontSize: 12, color: 'var(--text-muted)', fontFamily: 'monospace' }}>
          slate_id: {result.slate_id || '—'}
        </div>
      </div>

      <div style={{ marginBottom: 16 }}>
        <Stat label="Matches inserted" value={s.matches_inserted} />
        <Stat label="Matches updated" value={s.matches_updated} />
        {(s.matches_orphaned_and_deleted ?? 0) > 0 && (
          <Stat label="Matches deleted (orphans)" value={s.matches_orphaned_and_deleted} tone="warn" />
        )}
        <Stat label="Slate players inserted" value={s.slate_players_inserted} />
        <Stat label="Slate players updated" value={s.slate_players_updated} />
        {(s.slate_players_orphaned_and_deleted ?? 0) > 0 && (
          <Stat label="Slate players deleted (orphans)" value={s.slate_players_orphaned_and_deleted} tone="warn" />
        )}
        <Stat label="PrizePicks FS lines written" value={s.pp_fs_lines_written} />
      </div>

      <div style={{ fontSize: 12, color: 'var(--text-muted)', marginBottom: 16 }}>
        Kalshi will attach live odds within ~15 minutes on the next worker cycle.
        Hard refresh the app to see the new slate.
      </div>

      <div style={{ display: 'flex', justifyContent: 'flex-end' }}>
        <button
          className="btn btn-primary"
          onClick={onClose}
          style={{ ...btnStyle, ...btnPrimaryStyle }}
        >
          Done
        </button>
      </div>
    </>
  );
}

// ─────────────────────────────────────────────────────────────────────
// Small UI helpers
// ─────────────────────────────────────────────────────────────────────

function Field({ label, hint, children }) {
  return (
    <label style={{ display: 'block' }}>
      <div style={{ fontSize: 12, color: 'var(--text-muted)', marginBottom: 4, display: 'flex', justifyContent: 'space-between' }}>
        <span>{label}</span>
        {hint && <span style={{ color: 'var(--text-dim)', fontSize: 11 }}>{hint}</span>}
      </div>
      {children}
    </label>
  );
}

function Stat({ label, value, tone }) {
  const valColor = tone === 'warn' ? 'var(--amber)' : 'var(--text)';
  return (
    <div style={{ display: 'flex', justifyContent: 'space-between', padding: '4px 0', fontSize: 13 }}>
      <span style={{ color: 'var(--text-muted)' }}>{label}</span>
      <span style={{ color: valColor, fontWeight: 600 }}>{value !== undefined && value !== null ? value : '—'}</span>
    </div>
  );
}

function Block({ tone, title, children }) {
  const palette = {
    error: { bg: 'rgba(239,68,68,0.08)', border: 'rgba(239,68,68,0.3)', color: 'var(--red)' },
    warn:  { bg: 'rgba(251,191,36,0.08)', border: 'rgba(251,191,36,0.3)', color: 'var(--amber)' },
    ok:    { bg: 'rgba(34,197,94,0.08)', border: 'rgba(34,197,94,0.3)', color: 'var(--green)' },
  }[tone] || { bg: 'transparent', border: 'var(--border-light)', color: 'var(--text)' };
  return (
    <div
      style={{
        background: palette.bg,
        border: `1px solid ${palette.border}`,
        borderRadius: 6,
        padding: 10,
        marginBottom: 10,
      }}
    >
      <div style={{ color: palette.color, fontSize: 13, fontWeight: 600, marginBottom: 4 }}>
        {title}
      </div>
      <div style={{ color: 'var(--text)' }}>{children}</div>
    </div>
  );
}

const inputStyle = {
  width: '100%',
  background: 'var(--card)',
  border: '1px solid var(--border-light)',
  borderRadius: 6,
  padding: '7px 10px',
  color: 'var(--text)',
  fontSize: 13,
  fontFamily: 'inherit',
};

const btnStyle = {
  padding: '8px 16px',
  fontSize: 13,
  fontWeight: 600,
  borderRadius: 6,
  border: '1px solid var(--border-light)',
  background: 'transparent',
  color: 'var(--text)',
  cursor: 'pointer',
};

const btnPrimaryStyle = {
  background: 'var(--primary)',
  color: '#000',
  borderColor: 'var(--primary)',
};

export default AdminSlateUpload;
