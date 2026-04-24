import { Component, useState, useEffect, useMemo, useCallback, useRef } from 'react';
import { processMatch, dkProjection, ppProjection, ppEV, optimize, optimizeShowdown } from './engine.js';
import { processFight, dkMMAProjection, dkMMACeiling, ppMMAProjection, ppMMACeiling, optimizeMMA } from './engine-mma.js';
import {
  buildPlayerStats as nbaBuildPlayerStats,
  applyInjuryAdjustments as nbaApplyInjuryAdjustments,
  dkProjection as nbaDkProjection,
  dkCeiling as nbaDkCeiling,
  ppProjection as nbaPpProjection,
  projectPPStat as nbaProjectPPStat,
  ppEV as nbaPpEV,
  optimizeShowdown as nbaOptimizeShowdown,
  optimizeClassic as nbaOptimizeClassic,
  devig as nbaDevig,
  lineToProjection as nbaLineToProjection,
} from './engine-nba.js';
import { useAuth } from './lib/auth-context';
import { UserMenu } from './components/UserMenu';
import { SignInPrompt } from './components/SignInPrompt';
import { PrizePicksTab } from './components/PrizePicksTab';
import { loadSlate, loadManifest, isSportEnabled } from './lib/api';

// ═══════════════════════════════════════════════════════════════════════
// ERROR BOUNDARY — catches any runtime crash and shows it on screen
// ═══════════════════════════════════════════════════════════════════════
class ErrorBoundary extends Component {
  constructor(props) {
    super(props);
    this.state = { hasError: false, error: null, errorInfo: null };
  }
  static getDerivedStateFromError(error) { return { hasError: true, error }; }
  componentDidCatch(error, errorInfo) {
    console.error('ErrorBoundary caught:', error, errorInfo);
    this.setState({ errorInfo });
  }
  render() {
    if (this.state.hasError) {
      const errMsg = this.state.error?.message || String(this.state.error) || 'Unknown error';
      const stack = this.state.error?.stack || '';
      const compStack = this.state.errorInfo?.componentStack || '';
      return (
        <div style={{ padding: '40px 20px' }}>
          <div style={{ textAlign: 'center', marginBottom: 16 }}>
            <h2 style={{ color: '#EF4444', margin: 0, display: 'flex', alignItems: 'center', gap: 8 }}><Icon name="warning" size={18} color="#EF4444"/> Runtime error</h2>
            <p style={{ color: 'var(--text-muted)', fontSize: 13, marginTop: 8 }}>The app crashed rendering this view. Details below — please share this with support.</p>
          </div>
          <div style={{ background: 'rgba(239,68,68,0.08)', border: '1px solid rgba(239,68,68,0.4)', borderRadius: 8, padding: 16, maxWidth: 900, margin: '0 auto', fontSize: 12, fontFamily: 'monospace', color: '#EF4444', whiteSpace: 'pre-wrap', overflow: 'auto', wordBreak: 'break-word' }}>
            <div style={{ fontWeight: 700, fontSize: 14, marginBottom: 12 }}>{errMsg}</div>
            {compStack && <div style={{ color: 'var(--text-muted)', fontSize: 11, marginBottom: 12 }}><strong style={{ color: '#F5C518' }}>Component stack:</strong>{compStack}</div>}
            {stack && <div style={{ color: 'var(--text-dim)', fontSize: 11 }}><strong style={{ color: '#F5C518' }}>Stack trace:</strong>{'\n' + stack}</div>}
          </div>
          <div style={{ textAlign: 'center', marginTop: 16 }}>
            <button onClick={() => this.setState({ hasError: false, error: null, errorInfo: null })}
              style={{ background: '#F5C518', color: '#0A1628', border: 'none', borderRadius: 6, padding: '8px 20px', fontSize: 13, fontWeight: 700, cursor: 'pointer' }}>
              Reset & Try Again
            </button>
          </div>
        </div>
      );
    }
    return this.props.children;
  }
}

// ═══════════════════════════════════════════════════════════════════════
// GLOSSARIES — one per sport
// ═══════════════════════════════════════════════════════════════════════
const GLOSSARY_TENNIS = [
  { icon: 'trophy',        label: 'Top 3 Value', desc: 'Highest pts/$1K salary' },
  { icon: 'target',        label: 'Top 3 Straight Sets', desc: 'Most likely straight-set win (+6 bonus)' },
  { icon: 'gem',           label: 'Hidden Gem', desc: 'Low ownership + high upside' },
  { icon: 'bomb',          label: 'Trap', desc: 'Who the field needs most — prime fade' },
  { icon: 'flame',         label: 'Top PP EV', desc: 'Best expected value vs PP line' },
  { icon: 'trending-down', label: 'Worst PP EV', desc: 'Strong LESS play' },
];
const GLOSSARY_MMA = [
  { icon: 'trophy',        label: 'Top 3 Value', desc: 'Highest pts/$1K salary' },
  { icon: 'fist',          label: 'Top 3 Finish Path', desc: 'Highest R1/R2 finish upside (+90/+70 bonus)' },
  { icon: 'gem',           label: 'Hidden Gem', desc: 'Low ownership + high ceiling' },
  { icon: 'bomb',          label: 'Trap', desc: 'Who the field needs most — prime fade' },
  { icon: 'flame',         label: 'Top PP EV', desc: 'Best expected value vs PP line' },
  { icon: 'trending-down', label: 'Worst PP EV', desc: 'Strong LESS play' },
];
const GLOSSARY_NBA = [
  { icon: 'trophy',        label: 'Top 3 Value', desc: 'Highest pts/$1K salary' },
  { icon: 'rocket',        label: 'Top 3 Ceiling', desc: 'Highest 85th-percentile projection' },
  { icon: 'gem',           label: 'Hidden Gem', desc: 'Best value in salary band below biggest trap' },
  { icon: 'bomb',          label: 'Biggest Trap', desc: 'Who the field needs most — prime fade' },
  { icon: 'flame',         label: 'Top PP EV', desc: 'Best expected value vs PP line' },
  { icon: 'trending-down', label: 'Worst PP EV', desc: 'Strong LESS play' },
];

// ═══════════════════════════════════════════════════════════════════════
// CONTRARIAN LOGIC (shared across sports)
// ═══════════════════════════════════════════════════════════════════════
// Value-aware fair ownership: players with elite value deserve higher "fair" ownership.
// Elite value (ratio 1.5x) → fair_own = 20 x 1.25 = 25%
// Poor value (ratio 0.5x) → fair_own = 20 x 0.75 = 15%
// So chalky STUDS get minor fades (lobby needs them), chalky TRAPS get heavy fades.
function computeFairOwn(value, avgValue, baseline = 20) {
  if (!avgValue || avgValue <= 0) return baseline;
  const ratio = value / avgValue;
  const clamped = Math.max(-0.5, Math.min(0.5, ratio - 1));
  return baseline * (1 + 0.5 * clamped);
}

// Projection multiplier driven by ownership delta × value context.
// BIGGEST TRAP (high own + low value) → maximum fade.
// Chalky stud (high own + high value) → mild fade.
// Underowned value → moderate boost (not as aggressive as fade side — avoids flier-stacking).
// At strength=0.6, 12pt ownership overweight, val=0.8×avg: ~17% projection fade
// At strength=0.6, 12pt ownership overweight, val=1.4×avg: ~7% projection fade
// At strength=1.0 max: chalky trap gets up to ~30% fade
function applyContrarian(proj, ownership, strength, fairOwn = 20, value = null, avgValue = null) {
  if (!strength || ownership == null) return proj;
  const delta = fairOwn - ownership;                        // +ve = under-owned, -ve = over-owned
  const clampedDelta = Math.max(-40, Math.min(40, delta));

  // Asymmetric scaling: fade side is more aggressive than boost side
  const isOverowned = clampedDelta < 0;
  const baseScale = isOverowned ? 0.022 : 0.012;            // fade 2.2%/pt, boost 1.2%/pt

  // Value weighting on fade side only: amplify fade for poor-value chalk (true traps)
  let valueWeight = 1;
  if (isOverowned && value != null && avgValue > 0) {
    const valRatio = value / avgValue;
    valueWeight = Math.max(0.5, Math.min(1.5, 2 - valRatio));
    // valRatio=0.7 → valueWeight=1.3 (amp fade by 30%) — true trap
    // valRatio=1.0 → valueWeight=1.0 (neutral)
    // valRatio=1.4 → valueWeight=0.6 (dampen fade by 40%) — chalky stud
  }

  return proj * (1 + strength * clampedDelta * baseScale * valueWeight);
}

// v3.24.14: one-line definitions for OverOwned Mode signal badges. Used
// as `title` attributes on badge spans so hovering reveals what the
// signal means without opening the full explainer modal.
const SIGNAL_TOOLTIPS = {
  'Trap': 'TRAP — Highly owned but win-probability does not justify the price. Field piles in; OverOwned fades.',
  'Or Trap': 'OR TRAP — Alternative trap candidate when the primary trap is locked or excluded.',
  'Gem': 'HIDDEN GEM — Underowned relative to win equity, often the trap\'s opponent. OverOwned floors exposure above field.',
  'Pivot': 'PIVOT — Secondary gem, typically emerging from a PrizePicks line the DFS field has not pricing in yet.',
  'Or Pivot': 'OR PIVOT — Salary-equivalent alternative to the primary gem. Diversifies the contrarian stack.',
  'PP Fade': 'PP FADE — PrizePicks market is flagging this player to miss their line by a clear margin. Cross-market signal.',
  'PP Fade Opp': 'PP FADE OPPONENT — Opponent of a PP fade. When the fade misses, their opponent is more likely to win.',
  'Gem Opp': 'GEM OPPONENT — Opponent of a Hidden Gem. Field over-rosters this player to block the gem; OverOwned fades that block.',
  'Straight Sets': 'STRAIGHT-SETS CAP — Top favorite most likely to win without dropping a set. Capped to avoid cash-lineup overlap.',
  'Top Val': 'TOP VALUE CAP — One of the slate\'s top 10 by value. Capped hard on 16+ match slates to prevent same-core overlap.',
  'Top Val Opp': 'TOP VALUE OPPONENT — Opponent of a high-value chalk play. Boosted when the chalk has equity-undermining win rate.',
  'Mid Fade': 'MID-TIER FADE — High-ownership salary-band winner. Capped below field to force diversity on deep slates.',
  'Mid Pivot': 'MID-TIER PIVOT — Second-highest mid-tier. Floored above its own simulated ownership as a mid-salary leverage play.',
};
const signalTooltip = (label) => {
  if (!label) return '';
  // Pivot #1, Pivot #2 etc. — strip trailing rank for lookup
  const key = label.replace(/\s*#\d+$/, '').trim();
  return SIGNAL_TOOLTIPS[key] || SIGNAL_TOOLTIPS[label] || '';
};

function ContrarianPanel({ enabled, onToggle, strength, onStrengthChange }) {
  const [showHelp, setShowHelp] = useState(false);          // v3.24.14: explainer modal
  return (
    <div style={{
      background: 'linear-gradient(135deg, rgba(245,197,24,0.08), rgba(245,197,24,0.02))',
      border: '1px solid rgba(245,197,24,0.3)', borderRadius: 10,
      padding: '14px 18px', marginBottom: 16,
    }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: enabled ? 12 : 0 }}>
        <div style={{ fontSize: 14, fontWeight: 700, color: 'var(--primary)', display: 'flex', alignItems: 'center', gap: 8 }}>
          <Icon name="swords" size={15} color="#F5C518"/>
          <span>OverOwned Mode</span>
          <button
            onClick={() => setShowHelp(true)}
            aria-label="What does OverOwned Mode do?"
            title="What does OverOwned Mode do?"
            className="oo-help-toggle"
            style={{
              width: 18, height: 18, padding: 0, borderRadius: '50%',
              background: 'rgba(245,197,24,0.12)',
              border: '1px solid rgba(245,197,24,0.35)',
              color: 'var(--primary)', fontSize: 11, fontWeight: 700,
              cursor: 'pointer', lineHeight: '15px', textAlign: 'center',
              fontFamily: 'inherit', display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
            }}
          >?</button>
          <span style={{ color: 'var(--text-muted)', fontSize: 11, fontWeight: 500, marginLeft: 4 }}>
            (fade chalk, force leverage)
          </span>
        </div>
        <button onClick={() => onToggle(!enabled)} aria-label="Toggle OverOwned Mode" style={{
          width: 40, minWidth: 40, height: 22, minHeight: 22, maxHeight: 22,
          padding: 0, flexShrink: 0,
          background: enabled ? 'var(--primary)' : 'var(--border)',
          borderRadius: 11, position: 'relative', cursor: 'pointer', border: 'none',
          transition: 'background 0.15s',
        }}>
          <span style={{
            position: 'absolute', width: 18, height: 18,
            background: enabled ? '#0A1628' : 'var(--text-dim)',
            borderRadius: '50%', top: 2, left: enabled ? 20 : 2,
            transition: 'left 0.15s',
          }} />
        </button>
      </div>
      {enabled && (<>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <label style={{ fontSize: 12, color: 'var(--text-muted)', whiteSpace: 'nowrap' }}>Strength</label>
          <input type="range" min="0" max="100" value={Math.round(strength * 100)}
            onChange={e => onStrengthChange(+e.target.value / 100)}
            style={{ flex: 1, accentColor: 'var(--primary)' }} />
          <span style={{ fontSize: 13, fontWeight: 700, color: 'var(--primary)', minWidth: 90, textAlign: 'right' }}>
            {Math.round(strength * 100)}% · −{Math.round(strength * 50)}pp
          </span>
        </div>
        <div style={{ fontSize: 11, color: 'var(--text-dim)', marginTop: 8, fontStyle: 'italic' }}>
          OverOwned Mode is a formula derived from thousands of studied samples — it fades the chalk and forces the lower-owned ceiling into lineups.
        </div>
      </>)}
      {showHelp && <OverOwnedHelpModal onClose={() => setShowHelp(false)} />}
    </div>
  );
}

// v3.24.14: OverOwned Mode explainer modal. Opened from the "?" icon next
// to the toggle. Covers: what the mode does mechanically, what each signal
// badge means, and when to dial strength up vs. down. Tennis-centric
// because the rule pipeline is tennis-specific (16+ match slates).
function OverOwnedHelpModal({ onClose }) {
  useEffect(() => {
    const onEsc = (e) => { if (e.key === 'Escape') onClose(); };
    window.addEventListener('keydown', onEsc);
    document.body.style.overflow = 'hidden';
    return () => { window.removeEventListener('keydown', onEsc); document.body.style.overflow = ''; };
  }, [onClose]);
  const Row = ({ badge, color, children }) => (
    <div className="oo-help-glossary-row" style={{ display: 'flex', gap: 12, alignItems: 'flex-start', padding: '8px 0', borderBottom: '1px solid var(--border)' }}>
      <span className="oo-help-badge" style={{
        flex: '0 0 auto', padding: '2px 8px', borderRadius: 4,
        background: `${color}22`, border: `1px solid ${color}66`,
        color, fontSize: 10, fontWeight: 700, letterSpacing: 0.4,
        textTransform: 'uppercase', minWidth: 62, textAlign: 'center',
        fontVariantCaps: 'all-small-caps'
      }}>{badge}</span>
      <div style={{ fontSize: 12, color: 'var(--text-muted)', lineHeight: 1.55, flex: 1 }}>{children}</div>
    </div>
  );
  return (
    <div
      onClick={onClose}
      style={{
        position: 'fixed', inset: 0, background: 'rgba(10,22,40,0.7)',
        backdropFilter: 'blur(4px)', zIndex: 200,
        display: 'flex', alignItems: 'center', justifyContent: 'center',
        padding: 'clamp(8px, 3vw, 20px)', animation: 'oo-modal-in 0.18s ease-out',
      }}
    >
      <style>{`
        @keyframes oo-modal-in { from { opacity: 0; transform: scale(0.97); } to { opacity: 1; transform: scale(1); } }
        @media (max-width: 640px) {
          .oo-help-modal { padding: 16px 16px !important; max-height: 92vh !important; }
          .oo-help-modal h2 { font-size: 16px !important; }
          .oo-help-modal .oo-help-lede { font-size: 12px !important; }
          .oo-help-modal .oo-help-mechanics { padding: 10px 12px !important; }
          .oo-help-modal .oo-help-mechanics li { font-size: 11.5px !important; }
          .oo-help-modal .oo-help-glossary-row { padding: 7px 0 !important; gap: 10px !important; }
          .oo-help-modal .oo-help-badge { min-width: 54px !important; font-size: 9px !important; padding: 2px 6px !important; }
          .oo-help-modal .oo-help-footer { font-size: 10.5px !important; }
        }
      `}</style>
      <div
        className="oo-help-modal"
        onClick={e => e.stopPropagation()}
        style={{
          background: 'var(--card)', borderRadius: 12, padding: '22px 24px',
          maxWidth: 560, width: '100%', maxHeight: '85vh', overflow: 'auto',
          border: '1px solid var(--border-light)',
          boxShadow: '0 20px 60px rgba(0,0,0,0.4)',
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 14 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <Icon name="swords" size={18} color="#F5C518"/>
            <h2 style={{ fontSize: 18, fontWeight: 700, color: 'var(--text)', margin: 0 }}>OverOwned Mode — what it does</h2>
          </div>
          <button onClick={onClose} aria-label="Close" style={{
            background: 'transparent', border: 'none', color: 'var(--text-muted)',
            fontSize: 26, cursor: 'pointer', lineHeight: 1, padding: '0 8px',
            minWidth: 36, minHeight: 36,  // iOS tap target
          }}>×</button>
        </div>
        <p className="oo-help-lede" style={{ fontSize: 13, color: 'var(--text-muted)', lineHeight: 1.6, marginBottom: 14 }}>
          OverOwned Mode rewrites per-player exposure caps to fade the chalk the field is about to over-roster and boost the hidden leverage plays the field will under-roster. It's the difference between an optimizer that builds the field's lineups and one that builds <em>around</em> the field's lineups.
        </p>
        <div className="oo-help-mechanics" style={{ background: 'var(--bg)', borderRadius: 8, padding: '12px 14px', marginBottom: 14, border: '1px solid var(--border)' }}>
          <div style={{ fontSize: 11, color: 'var(--text-dim)', fontWeight: 600, letterSpacing: 0.8, textTransform: 'uppercase', marginBottom: 6 }}>When OverOwned is ON</div>
          <ul style={{ margin: 0, paddingLeft: 18, fontSize: 12, color: 'var(--text-muted)', lineHeight: 1.6 }}>
            <li>Trap player's exposure is <strong style={{ color: 'var(--red)' }}>capped</strong> well below their projected field ownership</li>
            <li>Hidden Gem's exposure is <strong style={{ color: 'var(--green)' }}>floored</strong> well above their field ownership</li>
            <li>PP Fade opponents and gem opponents get secondary caps so you don't leak back into the chalk through the side door</li>
            <li>On 10+ match slates, mid-tier and top-value plays also get tiered caps to prevent structural salary-filler overlap</li>
          </ul>
        </div>
        <div style={{ fontSize: 10, color: 'var(--text-dim)', fontWeight: 600, letterSpacing: 0.8, textTransform: 'uppercase', marginBottom: 2 }}>Signal glossary</div>
        <Row badge="Trap" color="#EF4444">Highly projected, highly owned, but win-probability doesn't justify the price. Field piles in; you fade.</Row>
        <Row badge="Gem" color="#4ADE80">Opponent of the Trap (or the best salary-band value if opponent doesn't qualify). Underowned relative to their win equity — boost hard.</Row>
        <Row badge="Pivot" color="#4ADE80">A secondary gem the field also misses. Typically emerges from PP line comparisons when the PP market is flagging an edge the DFS field hasn't caught up to.</Row>
        <Row badge="Or Pivot" color="#4ADE80">A "pivot off the gem" — same slot, lower ownership, similar projection ceiling. Diversifies your contrarian stack.</Row>
        <Row badge="PP Fade" color="#EF4444">Player the PrizePicks market has flagged as a fade (projected to miss their line by a clear margin). Cross-market signal the DFS field is ignoring.</Row>
        <Row badge="PP Fade Opp" color="#F59E0B">Opponent of a PP fade. When the fade is likely to miss, their opponent is more likely to win — secondary leverage.</Row>
        <Row badge="Gem Opp" color="#F59E0B">Opponent of a Hidden Gem. Field over-rosters the gem's opponent to block them; you fade those blockers.</Row>
        <p className="oo-help-footer" style={{ fontSize: 11, color: 'var(--text-dim)', marginTop: 14, marginBottom: 0, lineHeight: 1.5, fontStyle: 'italic' }}>
          Strength controls how aggressive the caps are: 60% (the default) is calibrated to the long-run hit rate on a 16+ match slate. Push higher on large-field GPPs where leverage pays more, lower on cash/small-field where variance hurts more than it helps.
        </p>
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════════════
// SLATE DATA HOOK — sport-aware
// ═══════════════════════════════════════════════════════════════════════
function useSlateData(sport, slateDate) {
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  const hasLoadedRef = useRef(false);
  useEffect(() => {
    // Cancel token — if sport/slateDate changes mid-fetch, the cleanup
    // below flips `cancelled` to true and the in-flight callback no-ops.
    // Guards against stale fetches clobbering current state when the user
    // switches sports faster than a fetch can resolve.
    let cancelled = false;
    setData(null); setError(null);
    const startTime = Date.now();
    // First load: full splash (5000ms). Tennis/NBA switch: 2000ms so the
    // ball-bounce animation plays. UFC switch: 900ms (dots only).
    // Archive picks: 600ms (quick).
    const isArchive = slateDate && slateDate !== 'live';
    const MIN_LOAD_MS = !hasLoadedRef.current
      ? 5000
      : isArchive ? 600
      : (sport === 'tennis' || sport === 'nba') ? 2000
      : 900;
    const finalize = (cb) => {
      const elapsed = Date.now() - startTime;
      const delay = Math.max(0, MIN_LOAD_MS - elapsed);
      setTimeout(() => { if (!cancelled) cb(); }, delay);
    };
    // API first, static fallback (handled inside loadSlate).
    loadSlate(sport, slateDate)
      .then(({ data: d, source }) => finalize(() => {
        if (cancelled) return;
        hasLoadedRef.current = true;
        setData(d);
        if (source === 'static') {
          console.info('[overowned] Loaded slate from static fallback');
        }
      }))
      .catch(e => finalize(() => { if (!cancelled) setError(e.message); }));
    return () => { cancelled = true; };
  }, [sport, slateDate]);
  return { data, error };
}

// Loads the list of archived slates from /slates/{sport}/manifest.json.
// Manifest shape: { slates: [{ date: "YYYY-MM-DD", label: "optional label" }, ...] }
function useSlateManifest(sport) {
  const [slates, setSlates] = useState([]);
  useEffect(() => {
    loadManifest(sport).then(slates => setSlates(slates)).catch(() => setSlates([]));
  }, [sport]);
  return slates;
}

// ═══════════════════════════════════════════════════════════════════════
// TENNIS PROJECTION BUILDER — UNCHANGED from v5
// ═══════════════════════════════════════════════════════════════════════

// Build the "Odds" column display value per match.
// Priority: Kalshi implied prob (market, no vig) → Odds API vig-removed prob
// Returns { a: {prob, source}, b: {prob, source} } where source is:
//   'kalshi'  → market-sourced probability from Kalshi
//   'market'  → vig-removed probability from Odds API consensus
//   null      → no odds data for this match
function computeOddsDisplay(odds) {
  const empty = { a: { prob: null, source: null }, b: { prob: null, source: null } };
  if (!odds || typeof odds !== 'object') return empty;

  // Kalshi first — most trustworthy (no vig, market-resolved)
  const kA = odds.kalshi_prob_a;
  const kB = odds.kalshi_prob_b;
  if (kA != null && kB != null && kA >= 0 && kA <= 1) {
    return {
      a: { prob: kA, source: 'kalshi' },
      b: { prob: kB, source: 'kalshi' },
    };
  }

  // Fallback: Odds API moneyline, vig-removed
  const mlA = odds.ml_a;
  const mlB = odds.ml_b;
  if (mlA != null && mlB != null) {
    const pA = americanToImpliedProb(mlA);
    const pB = americanToImpliedProb(mlB);
    if (pA > 0 && pB > 0) {
      const total = pA + pB;  // total > 1 due to vig; normalize
      return {
        a: { prob: pA / total, source: 'market' },
        b: { prob: pB / total, source: 'market' },
      };
    }
  }

  return empty;
}

function americanToImpliedProb(ml) {
  if (ml == null) return 0;
  const n = Number(ml);
  if (!isFinite(n) || n === 0) return 0;
  if (n > 0) return 100 / (n + 100);
  return (-n) / ((-n) + 100);
}

// Render the Odds column cell. When Kalshi data is present, shows the
// probability in gold with a small "K" badge. When falling back to market
// consensus, shows it in the normal text color with no badge. When neither
// source has data, shows a dash.
function OddsCell({ prob, source, fallbackWp }) {
  // If we have a real source, use it. Otherwise fall back to the engine's
  // wp (which is itself computed from whatever odds fields are available —
  // may be 0.5 if no odds landed).
  const displayProb = prob != null ? prob : fallbackWp;
  const displaySource = source || (fallbackWp > 0 && fallbackWp !== 0.5 ? 'market' : null);
  if (displayProb == null || displayProb === 0.5 && !displaySource) {
    return <span style={{ color: 'var(--text-dim)' }}>—</span>;
  }
  const pct = Math.round(displayProb * 100);
  if (displaySource === 'kalshi') {
    return (
      <span style={{ display: 'inline-flex', alignItems: 'center', gap: 4, justifyContent: 'flex-end' }}>
        <span style={{ color: 'var(--primary, #F5C518)', fontWeight: 600 }}>{pct}%</span>
        <span
          title="Sourced from Kalshi prediction market"
          style={{
            fontSize: 9,
            fontWeight: 700,
            padding: '1px 4px',
            borderRadius: 3,
            background: 'rgba(245, 197, 24, 0.15)',
            color: 'var(--primary, #F5C518)',
            letterSpacing: '0.05em',
          }}
        >
          K
        </span>
      </span>
    );
  }
  return <span>{pct}%</span>;
}

function buildProjections(data) {
  if (!data || !data.matches || !data.dk_players) return { dkPlayers: [], ppRows: [] };
  const dkMap = {}; data.dk_players.forEach(p => { dkMap[p.name] = p; });
  const oppMap = {}; data.matches.forEach(m => { oppMap[m.player_a] = m.player_b; oppMap[m.player_b] = m.player_a; });
  const mtMap = {}; data.matches.forEach(m => { mtMap[m.player_a] = { time: m.start_time, t: m.tournament }; mtMap[m.player_b] = { time: m.start_time, t: m.tournament }; });
  const dkPlayers = [];
  data.matches.forEach(match => {
    const stats = processMatch(match);
    // Odds-for-display: prefer Kalshi implied prob (market-sourced, no vig),
    // fall back to vig-removed Odds API implied prob. Stored alongside engine
    // wp so the UI can show source attribution without re-doing the math.
    const o = match.odds || {};
    const oddsForPlayer = computeOddsDisplay(o);
    [['player_a', stats.player_a, 'a'], ['player_b', stats.player_b, 'b']].forEach(([side, s, abKey]) => {
      const name = match[side]; const dk = dkMap[name]; if (!dk) return;
      const proj = dkProjection(s);
      const val = dk.salary > 0 ? Math.round(proj / (dk.salary / 1000) * 100) / 100 : 0;
      dkPlayers.push({ name, salary: dk.salary, id: dk.id, avgPPG: dk.avg_ppg, opponent: oppMap[name] || '', tournament: mtMap[name]?.t || '', startTime: mtMap[name]?.time || '', wp: s.wp, proj, val, pStraight: s.pStraightWin, p3set: s.p3set, gw: s.gw, gl: s.gl, sw: s.setsWon, sl: s.setsLost, aces: s.aces, dfs: s.dfs, breaks: s.breaks, p10ace: s.p10ace, pNoDF: s.pNoDF, ppProj: ppProjection(s), stats: s,
        // Odds display: probability + source (kalshi | market | null)
        oddsProb: oddsForPlayer[abKey].prob,
        oddsSource: oddsForPlayer[abKey].source,
        // Showdown tiers — undefined for classic slates (harmless)
        cpt_id: dk.cpt_id, cpt_salary: dk.cpt_salary,
        acpt_id: dk.acpt_id, acpt_salary: dk.acpt_salary,
        flex_id: dk.flex_id, flex_salary: dk.flex_salary,
        // v3.25: SaberSims Pool Own replaces Monte Carlo simOwn when shipped
        // in the slate JSON. Must be manually pasted in during slate build.
        ss_pool_own: dk.ss_pool_own,
      });
    });
  });
  const ppRows = [];
  if (data.pp_lines) {
    data.pp_lines.forEach(line => {
      const player = dkPlayers.find(p => p.name === line.player);
      let projected = 0;
      if (!player) { ppRows.push({ player: line.player, stat: line.stat, line: line.line, projected: 0, ev: 0, opponent: '?', wp: 0, direction: '-', mult: line.mult || '' }); return; }
      if (line.stat === 'Fantasy Score') projected = player.ppProj;
      else if (line.stat === 'Breaks') projected = player.breaks;
      else if (line.stat === 'Games Won') projected = player.gw;
      else if (line.stat === 'Total Games') projected = player.gw + player.gl;
      else if (line.stat === 'Aces') projected = player.aces;
      else if (line.stat === 'Double Faults') projected = player.dfs;
      else if (line.stat === 'Sets Won') projected = player.sw;
      const ev = ppEV(projected, line.line);
      // Attach PP Fantasy Score line + edge for PP Boost consumption in Builder.
      // Edge = player.ppProj − PP line (matches what the PP tab displays in
      // the EDGE column — uses tennis PP scoring, not DK scoring, since tennis
      // PP uses different stat weights than DK).
      if (line.stat === 'Fantasy Score') {
        player.ppLine = line.line;
        player.ppEdge = Math.round((player.ppProj - line.line) * 100) / 100;
      }
      ppRows.push({ player: line.player, stat: line.stat, line: line.line, projected: Math.round(projected * 100) / 100, ev, opponent: player.opponent, wp: player.wp, direction: ev > 0 ? 'MORE' : ev < 0 ? 'LESS' : '-', mult: line.mult || '' });
    });
  }
  return { dkPlayers, ppRows };
}

// ═══════════════════════════════════════════════════════════════════════
// MMA PROJECTION BUILDER — NEW
// ═══════════════════════════════════════════════════════════════════════
function buildMMAProjections(data) {
  if (!data || !data.fights || !data.dk_players) return { dkPlayers: [], ppRows: [] };
  const dkMap = {}; data.dk_players.forEach(p => { dkMap[p.name] = p; });
  const dkPlayers = [];
  data.fights.forEach(fight => {
    const { fighter_a, fighter_b } = processFight(fight);
    const totalRoundsMax = (fight.rounds || 3) * 5;
    // Expected fight time from round distribution (midpoint of each round)
    const fightTime = Math.round((
      (fighter_a.pR1 + fighter_b.pR1) * 2.5 +
      (fighter_a.pR2 + fighter_b.pR2) * 7.5 +
      (fighter_a.pR3 + fighter_b.pR3) * 12.5 +
      (fighter_a.pR4 + fighter_b.pR4) * 17.5 +
      (fighter_a.pR5 + fighter_b.pR5) * 22.5 +
      (fighter_a.pDec + fighter_b.pDec) * totalRoundsMax
    ) * 100) / 100;

    [['fighter_a', fighter_a], ['fighter_b', fighter_b]].forEach(([side, s]) => {
      const name = fight[side];
      const opp = side === 'fighter_a' ? fight.fighter_b : fight.fighter_a;
      const dk = dkMap[name];
      if (!dk) return;
      const proj = dkMMAProjection(s);
      const ceil = dkMMACeiling(s);
      const val = dk.salary > 0 ? Math.round(proj / (dk.salary / 1000) * 100) / 100 : 0;
      const cval = dk.salary > 0 ? Math.round(ceil / (dk.salary / 1000) * 100) / 100 : 0;
      const finishProb = s.pKO + s.pSub;
      const finishUpside = s.pR1 * 90 + s.pR2 * 70;  // expected finish bonus
      const ppProj = ppMMAProjection(s);
      const ppCeil = ppMMACeiling(s);
      dkPlayers.push({
        name, salary: dk.salary, id: dk.id, avgPPG: dk.avg_ppg, opponent: opp,
        startTime: fight.start_time || '', rounds: fight.rounds || 3,
        wp: s.wp, proj, ceil, val, cval,
        pR1: s.pR1, pR2: s.pR2, pR3: s.pR3, pR4: s.pR4, pR5: s.pR5, pDec: s.pDec,
        pKO: s.pKO, pSub: s.pSub, finishProb, finishUpside,
        sigStr: s.sigStr, takedowns: s.takedowns, ctMin: s.ctSec / 60,
        knockdowns: s.knockdowns, subAttempts: s.subAttempts,
        ppProj, ppCeil, fightTime, stats: s,
        // Source tag — determines PP SS tab inclusion (only fighters with Bet365 O/U SS line)
        ssSource: side === 'fighter_a' ? fight.ss_source_a : fight.ss_source_b,
        // Combined fight-level round probabilities (needed for bimodal Fight Time edge)
        fightPR1: fighter_a.pR1 + fighter_b.pR1,
        fightPR2: fighter_a.pR2 + fighter_b.pR2,
        fightPR3: fighter_a.pR3 + fighter_b.pR3,
        fightPR4: fighter_a.pR4 + fighter_b.pR4,
        fightPR5: fighter_a.pR5 + fighter_b.pR5,
        fightPDec: fighter_a.pDec + fighter_b.pDec,
        fightMaxMin: (fight.rounds || 3) * 5,
        // v3.25: SaberSims Pool Own replaces Monte Carlo simOwn when shipped
        // in the slate JSON. Must be manually pasted in during slate build.
        ss_pool_own: dk.ss_pool_own,
      });
    });
  });

  // Helper: median fight time — where 50% of outcomes fall below
  // This is what PP uses to set lines (not mean, which is pulled up by decisions)
  function medianFightTime(p) {
    const rounds = [p.fightPR1, p.fightPR2, p.fightPR3, p.fightPR4, p.fightPR5];
    const maxRounds = p.fightMaxMin / 5;
    let cum = 0;
    for (let i = 0; i < maxRounds; i++) {
      const newCum = cum + rounds[i];
      if (newCum >= 0.5) {
        const frac = (0.5 - cum) / rounds[i];
        return Math.round((i * 5 + frac * 5) * 100) / 100;
      }
      cum = newCum;
    }
    // Decision dominates (finishes < 50%) → median is at decision time
    return p.fightMaxMin;
  }

  // Helper: P(fight time > line) computed from combined round distribution
  function pFightTimeOver(p, line) {
    const rounds = [p.fightPR1, p.fightPR2, p.fightPR3, p.fightPR4, p.fightPR5];
    let pOver = 0;
    for (let i = 0; i < 5; i++) {
      const rStart = i * 5, rEnd = (i + 1) * 5;
      if (rEnd > p.fightMaxMin) break;
      if (line >= rEnd) continue;
      if (line < rStart) pOver += rounds[i];
      else pOver += rounds[i] * (rEnd - line) / 5;
    }
    if (line < p.fightMaxMin) pOver += p.fightPDec;
    return Math.max(0, Math.min(1, pOver));
  }

  // Build PP rows
  const ppRows = [];
  if (data.pp_lines) {
    data.pp_lines.forEach(line => {
      const player = dkPlayers.find(p => p.name === line.player);
      if (!player) { ppRows.push({ player: line.player, stat: line.stat, line: line.line, projected: 0, ev: 0, opponent: '?', wp: 0, direction: '-', mult: line.mult || '' }); return; }
      let projected = 0, ev = 0, direction = '-';
      if (line.stat === 'Significant Strikes') {
        // NEW RULE: only include PP SS rows for fighters with Bet365 direct O/U line.
        // Without Bet365 data, projection would equal PP line → no edge → drop.
        if (player.ssSource !== 'bet365') return;
        projected = player.sigStr;
        ev = Math.round((projected - line.line) * 100) / 100;
        direction = ev > 0 ? 'MORE' : ev < 0 ? 'LESS' : '-';
      } else if (line.stat === 'Fantasy Score') {
        projected = player.ppProj;
        ev = Math.round((projected - line.line) * 100) / 100;
        direction = ev > 0 ? 'MORE' : ev < 0 ? 'LESS' : '-';
        // Attach FS line + edge for PP Boost consumption in Builder.
        // Uses ppProj (MMA PP scoring) to match the PP tab's EDGE column.
        player.ppLine = line.line;
        player.ppEdge = Math.round((player.ppProj - line.line) * 100) / 100;
      } else if (line.stat === 'Fight Time') {
        // Projected = MEDIAN fight time (where 50% of outcomes land below)
        // PP sets their lines at median, not mean — mean gets pulled up by decisions
        projected = medianFightTime(player);
        ev = Math.round((projected - line.line) * 100) / 100;
        direction = ev > 0 ? 'MORE' : ev < 0 ? 'LESS' : '-';
      } else if (line.stat === 'Takedowns') {
        projected = player.takedowns;
        ev = Math.round((projected - line.line) * 100) / 100;
        direction = ev > 0 ? 'MORE' : ev < 0 ? 'LESS' : '-';
      } else if (line.stat === 'Control Time') {
        projected = player.ctMin;
        ev = Math.round((projected - line.line) * 100) / 100;
        direction = ev > 0 ? 'MORE' : ev < 0 ? 'LESS' : '-';
      }
      ppRows.push({ player: line.player, stat: line.stat, line: line.line, projected: Math.round(projected * 100) / 100, ev, opponent: player.opponent, wp: player.wp, direction, mult: line.mult || '' });
    });
  }
  return { dkPlayers, ppRows };
}

// ═══════════════════════════════════════════════════════════════════════
// OWNERSHIP SIMULATORS
// ═══════════════════════════════════════════════════════════════════════
// Tennis ownership sim — exposure from top 1500 highest-scoring lineups.
// For showdown, also tracks captain-specific ownership (some players chalky
// as CPT but rare as FLEX, e.g. low-salary dogs optimizers love to captain).
// Returns { overall, cpt }.
function simulateOwnership(players, n = 1500) {
  const isShowdown = players.some(p => p.salary > 0 && p.cpt_salary != null);
  if (isShowdown) {
    const pData = players.filter(p => p.salary > 0).map(p => ({
      name: p.name, projection: p.proj, opponent: p.opponent,
      cpt_salary: p.cpt_salary, acpt_salary: p.acpt_salary, flex_salary: p.flex_salary,
      cpt_id: p.cpt_id, acpt_id: p.acpt_id, flex_id: p.flex_id,
      maxExp: 100, minExp: 0,
    }));
    try {
      const res = optimizeShowdown(pData, n, 50000, 48000);
      const overall = {}, cpt = {};
      pData.forEach((p, i) => {
        overall[p.name] = res.lineups.length ? res.counts[i] / res.lineups.length * 100 : 0;
        cpt[p.name] = res.lineups.length ? (res.cptCounts?.[i] || 0) / res.lineups.length * 100 : 0;
      });
      return { overall, cpt };
    } catch { return { overall: {}, cpt: {} }; }
  }
  const pData = players.filter(p => p.salary > 0).map(p => ({
    name: p.name, salary: p.salary, id: p.id, projection: p.proj,
    opponent: p.opponent, maxExp: 100, minExp: 0,
  }));
  try {
    // v3.23: 18+ match tennis slates use a $49,200 minSalary floor (up from
    // $48,000). Forces meaningful cap utilization when the slate is deep
    // enough that cheap-filler lineups become a structural vulnerability,
    // and pays a modest speed dividend via smaller lineup pool. Ownership
    // simulation uses the same floor as the builder so field expectations
    // stay calibrated. Derived from pData.length/2 since tennis classic
    // pairs each match into exactly 2 DK players.
    const matchCount = Math.floor(pData.length / 2);
    const minSal = matchCount >= 18 ? 49200 : 48000;
    const res = optimize(pData, n, 50000, 6, minSal);
    const overall = {};
    pData.forEach((p, i) => { overall[p.name] = res.counts[i] / res.lineups.length * 100; });
    return { overall, cpt: {} };
  } catch { return { overall: {}, cpt: {} }; }
}

function simulateMMAOwnership(fighters, n = 1500) {
  const pData = fighters.filter(f => f.salary > 0).map(f => ({
    name: f.name, salary: f.salary, id: f.id,
    projection: f.proj, ceiling: f.ceil,
    opponent: f.opponent, maxExp: 100, minExp: 0
  }));
  try {
    const res = optimizeMMA(pData, n, 50000, 6, "median", 48000);
    const overall = {};
    pData.forEach((p, i) => { overall[p.name] = res.counts[i] / res.lineups.length * 100; });
    return { overall, cpt: {} };
  } catch { return { overall: {}, cpt: {} }; }
}

// ═══════════════════════════════════════════════════════════════════════
// NBA PROJECTION BUILDER
// Pure DraftKings prop devig — no minute/pace/blowout/cascade scaling.
// Players without a DK Points line are marked unprojectable and excluded
// from the builder pool.
// ═══════════════════════════════════════════════════════════════════════
function buildNBAProjections(data) {
  if (!data || !data.dk_players) return { dkPlayers: [], ppRows: [] };
  const game = data.game || {};

  const dkPlayers = data.dk_players.map(p => {
    const stats = nbaBuildPlayerStats(p);   // context-free
    const proj = stats.projectable ? nbaDkProjection(stats) : 0;
    const ceil = stats.projectable ? nbaDkCeiling(stats) : 0;
    const ppProj = stats.projectable ? nbaPpProjection(stats) : 0;
    const sal = p.util_salary || p.salary || 0;
    const val  = stats.projectable && sal > 0 ? Math.round(proj / (sal / 1000) * 100) / 100 : 0;
    const cval = stats.projectable && sal > 0 ? Math.round(ceil / (sal / 1000) * 100) / 100 : 0;
    // Count how many of the 5 core stats we actually have DK lines for.
    const hasData = stats.hasStatData || {};
    const statCoverage = ['points', 'rebounds', 'assists', 'threes', 'stls_blks']
      .filter(k => hasData[k]).length;
    return {
      name: p.name,
      team: p.team,
      opponent: p.opponent || (p.team === game.home ? game.away : game.home),
      positions: p.positions || [],
      positions_str: p.positions_str || (p.positions || []).join('/'),
      salary: sal,
      id: p.util_id || p.id,
      util_id: p.util_id, util_salary: p.util_salary,
      cpt_id: p.cpt_id,   cpt_salary: p.cpt_salary,
      avgPPG: p.avg_ppg || 0,
      proj, ceil, val, cval, ppProj,
      projMins: stats.projMins,      // informational only
      projectable: stats.projectable,
      statCoverage,                   // 0–5
      hasStatData: hasData,
      status: stats.status,
      stats,
      startTime: game.tip || '',
      // v3.25: manual Pool Own replaces Monte Carlo simOwn when shipped in
      // the slate JSON. Must be manually pasted in during slate build.
      ss_pool_own: p.ss_pool_own,
    };
  });

  // PP rows — ONLY PP Fantasy Score. PrizePicks' individual Points / Rebounds /
  // Assists / 3PM / Stls+Blks lines are intentionally NOT shown here: that
  // would be comparing the devigged DK line for stat X against the PP line
  // for stat X, which is a comparison of two bookmakers' lines. The PP tab's
  // purpose is to surface PP Fantasy Score vs our devigged DK-derived PP
  // Fantasy Score projection, and report edge.
  const ppRows = [];
  if (data.pp_lines) {
    const byName = {}; dkPlayers.forEach(p => { byName[p.name] = p; });
    data.pp_lines.forEach(line => {
      if (line.stat !== 'Fantasy Score') return;
      const player = byName[line.player];
      if (!player || !player.projectable) return;
      const projected = Math.round(nbaPpProjection(player.stats) * 100) / 100;
      const ev = nbaPpEV(projected, line.line);
      // Attach PP line + edge to the player so Builder can read them without
      // re-joining through ppRows. Edge = ppProj − PP line (uses NBA PP
      // scoring to match the PP tab's EDGE column; slightly different from
      // DK proj for NBA since PP doesn't always apply double-double bonuses).
      player.ppLine = line.line;
      player.ppEdge = Math.round((player.ppProj - line.line) * 100) / 100;
      ppRows.push({
        player: line.player, stat: 'Fantasy Score', line: line.line,
        projected, ev,
        opponent: player.opponent, team: player.team,
        direction: ev > 0.8 ? 'MORE' : ev < -0.8 ? 'LESS' : '-',
        mult: line.mult || 'normal',
      });
    });
  }
  return { dkPlayers, ppRows };
}

// ═══════════════════════════════════════════════════════════════════════
// NBA OWNERSHIP SIM — exposure from top 1500 highest-scoring lineups
// (top-N enumeration by DK Fantasy Score, NOT weighted random field sim).
// Returns { overall, cpt } where:
//   overall[name] = % of top-1500 lineups containing the player in ANY slot
//   cpt[name]     = % of top-1500 lineups where the player is captain
// ═══════════════════════════════════════════════════════════════════════
function simulateNBAOwnership(players, slateType = 'showdown') {
  const active = players.filter(p =>
    p.salary > 0 &&
    p.projectable !== false &&
    (p.status || 'ACTIVE').toUpperCase() !== 'OUT'
  );
  if (active.length < 6) return { overall: {}, cpt: {} };

  const pData = active.map(p => ({
    name: p.name, team: p.team, projection: p.proj,
    util_salary: p.util_salary || p.salary, cpt_salary: p.cpt_salary,
    util_id: p.util_id || p.id, cpt_id: p.cpt_id,
    positions: p.positions || [], salary: p.util_salary || p.salary,
    status: p.status,
  }));

  try {
    if (slateType === 'classic') {
      const res = nbaOptimizeClassic(pData, 1500, 50000, 48000);
      const overall = {};
      if (!res.lineups.length) return { overall, cpt: {} };
      pData.forEach((p, i) => { overall[p.name] = res.counts[i] / res.lineups.length * 100; });
      return { overall, cpt: {} };
    }
    // Showdown — top 1500 highest-scoring lineups by DK Fantasy projection
    const res = nbaOptimizeShowdown(pData, 1500, 50000, 48000);
    const overall = {}, cpt = {};
    if (!res.lineups.length) return { overall, cpt };
    pData.forEach((p, i) => {
      overall[p.name] = res.counts[i] / res.lineups.length * 100;
      cpt[p.name] = (res.cptCounts?.[i] || 0) / res.lineups.length * 100;
    });
    return { overall, cpt };
  } catch (e) {
    console.error('simulateNBAOwnership error:', e);
    return { overall: {}, cpt: {} };
  }
}

// ═══════════════════════════════════════════════════════════════════════
// SHARED HELPERS — UNCHANGED
// ═══════════════════════════════════════════════════════════════════════
function useSort(data, dk = 'val', dd = 'desc') {
  const [sk, setSk] = useState(dk); const [sd, setSd] = useState(dd);
  const sorted = useMemo(() => { const a = [...data]; a.sort((x, y) => { let va = x[sk], vb = y[sk]; if (typeof va === 'string') { va = va.toLowerCase(); vb = (vb||'').toLowerCase(); } if (va < vb) return sd === 'asc' ? -1 : 1; if (va > vb) return sd === 'asc' ? 1 : -1; return 0; }); return a; }, [data, sk, sd]);
  const toggle = useCallback(k => { if (k === sk) setSd(d => d === 'asc' ? 'desc' : 'asc'); else { setSk(k); setSd('desc'); } }, [sk]);
  return { sorted, sortKey: sk, sortDir: sd, toggleSort: toggle };
}
function SH({ label, colKey, sortKey, sortDir, onSort, num, tip }) {
  const a = colKey === sortKey;
  const cls = [a ? 'sorted' : '', num ? 'num' : ''].filter(Boolean).join(' ');
  return <th className={cls} onClick={() => onSort(colKey)} title={tip}>{label}{a && <span className="sort-arrow">{sortDir === 'asc' ? '▲' : '▼'}</span>}</th>;
}

// Small inline 🔒 / 🚫 button pair rendered in its own column on DK tabs.
// Click lock → player must appear in every generated lineup.
// Click exclude → player is removed from the lineup pool entirely.
// Locking and excluding are mutually exclusive (enforced at App-level state).
//
// Design (Option A): buttons live in a dedicated action column, right-aligned.
// At rest: gentle 8% tint (gold for lock, red for exclude) so they're visible
// but don't shout. Hover brightens to 15%. Active (pressed) state fills to 30%
// with bolder border. 26×24 hit target — comfortable on desktop and mobile.
function LockExcludeButtons({ name, isLocked, isExcluded, onToggleLock, onToggleExclude }) {
  const btn = (active, color, onClick, title, icon) => {
    const restBg = active ? `${color}4D` : `${color}14`;
    const restBorder = active ? color : `${color}38`;
    const restColor = color;
    return (
      <button onClick={e => { e.stopPropagation(); onClick(name); }}
        title={title}
        style={{
          background: restBg,
          border: `1px solid ${restBorder}`,
          color: restColor,
          width: 26, height: 24, borderRadius: 5, padding: 0,
          display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
          cursor: 'pointer', flexShrink: 0,
          transition: 'background 0.12s, border-color 0.12s',
        }}
        onMouseEnter={e => {
          e.currentTarget.style.background = active ? `${color}66` : `${color}26`;
          e.currentTarget.style.borderColor = active ? color : `${color}66`;
        }}
        onMouseLeave={e => {
          e.currentTarget.style.background = restBg;
          e.currentTarget.style.borderColor = restBorder;
        }}>
        {icon}
      </button>
    );
  };
  const lockIcon = <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round"><rect x="4" y="11" width="16" height="10" rx="2"/><path d="M8 11V7a4 4 0 0 1 8 0v4"/></svg>;
  const excludeIcon = <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round"><circle cx="12" cy="12" r="9"/><line x1="5.6" y1="5.6" x2="18.4" y2="18.4"/></svg>;
  return (
    <span style={{ display: 'inline-flex', gap: 6, verticalAlign: 'middle' }}>
      {btn(isLocked, '#F5C518', onToggleLock, isLocked ? 'Locked — click to unlock' : 'Lock into every lineup', lockIcon)}
      {btn(isExcluded, '#EF4444', onToggleExclude, isExcluded ? 'Excluded — click to allow' : 'Exclude from all lineups', excludeIcon)}
    </span>
  );
}

// LockBar — strip rendered above a DK tab's table showing active locks and excludes.
// Each name is a pill (gold for locked, red for excluded) with an × to remove it.
// Also provides "Clear all" links for each group. Renders nothing when both lists are empty.
function LockBar({ lockedPlayers = [], excludedPlayers = [], onToggleLock, onToggleExclude, onClearLocks, onClearExcludes }) {
  if (lockedPlayers.length === 0 && excludedPlayers.length === 0) return null;
  const pill = (name, color, onRemove) => (
    <span key={name} style={{
      display: 'inline-flex', alignItems: 'center', gap: 5,
      padding: '3px 4px 3px 9px', borderRadius: 12,
      background: `${color}1F`, border: `1px solid ${color}55`,
      fontSize: 11, fontWeight: 500, color,
    }}>
      {name}
      <button onClick={() => onRemove(name)} title="Remove"
        style={{ background: 'transparent', border: 'none', color,
                 width: 16, height: 16, borderRadius: '50%', cursor: 'pointer',
                 padding: 0, display: 'inline-flex', alignItems: 'center',
                 justifyContent: 'center', fontSize: 12, lineHeight: 1 }}>×</button>
    </span>
  );
  return (
    <div style={{
      display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: 10,
      padding: '10px 14px', marginBottom: 10,
      background: 'var(--card)', border: '1px solid var(--border)', borderRadius: 8,
    }}>
      {lockedPlayers.length > 0 && (
        <div style={{ display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: 6 }}>
          <span style={{ fontSize: 10, color: 'var(--text-muted)', fontWeight: 600,
                          textTransform: 'uppercase', letterSpacing: '0.05em', marginRight: 2 }}>Locked</span>
          {lockedPlayers.map(n => pill(n, 'var(--primary)', onToggleLock))}
          {lockedPlayers.length > 1 && (
            <button onClick={onClearLocks}
              style={{ background: 'transparent', border: 'none', color: 'var(--text-dim)',
                       fontSize: 11, cursor: 'pointer', textDecoration: 'underline',
                       padding: '0 4px' }}>clear</button>
          )}
        </div>
      )}
      {lockedPlayers.length > 0 && excludedPlayers.length > 0 && (
        <span style={{ width: 1, height: 16, background: 'var(--border-light)' }} />
      )}
      {excludedPlayers.length > 0 && (
        <div style={{ display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: 6 }}>
          <span style={{ fontSize: 10, color: 'var(--text-muted)', fontWeight: 600,
                          textTransform: 'uppercase', letterSpacing: '0.05em', marginRight: 2 }}>Excluded</span>
          {excludedPlayers.map(n => pill(n, 'var(--red)', onToggleExclude))}
          {excludedPlayers.length > 1 && (
            <button onClick={onClearExcludes}
              style={{ background: 'transparent', border: 'none', color: 'var(--text-dim)',
                       fontSize: 11, cursor: 'pointer', textDecoration: 'underline',
                       padding: '0 4px' }}>clear</button>
          )}
        </div>
      )}
    </div>
  );
}

// Shared player-table search bar. Dark card surface + magnifying glass, focus
// ring in primary gold. Right side shows total count when empty, "N of M"
// (in gold) plus an X clear button while filtering. Esc clears. Matches the
// .metric / .table-wrap / input focus styling used elsewhere.
function SearchBar({ value, onChange, placeholder = 'Search players', total, filtered }) {
  const hasQuery = (value || '').trim().length > 0;
  const onKey = e => { if (e.key === 'Escape') onChange(''); };
  return (
    <div className={`oo-search${hasQuery ? ' oo-search-active' : ''}`}>
      <style>{`
        .oo-search {
          display: flex; align-items: center; gap: 0;
          background: var(--card); border: 1px solid var(--border); border-radius: 8px;
          padding: 0 14px; height: 38px; margin-bottom: 14px;
          transition: border-color 0.12s, box-shadow 0.12s;
        }
        .oo-search:focus-within {
          border-color: var(--primary);
          box-shadow: 0 0 0 3px rgba(245,197,24,0.08);
        }
        .oo-search-icon { flex-shrink: 0; margin-right: 10px; color: var(--text-dim); }
        .oo-search:focus-within .oo-search-icon { color: var(--primary); }
        .oo-search input {
          flex: 1; background: transparent; border: none; outline: none;
          color: var(--text); font-size: 13px; font-family: inherit; height: 100%;
          min-width: 0;
        }
        .oo-search input::placeholder { color: var(--text-dim); }
        .oo-search-count {
          font-size: 11px; color: var(--text-dim); font-variant-numeric: tabular-nums;
          flex-shrink: 0; margin-left: 10px; white-space: nowrap;
        }
        .oo-search-active .oo-search-count { color: var(--primary); font-weight: 600; }
        .oo-search-clear {
          flex-shrink: 0; background: transparent; border: none;
          color: var(--text-dim); cursor: pointer; padding: 2px; margin-left: 8px;
          display: flex; align-items: center; border-radius: 4px;
          transition: color 0.12s, background 0.12s;
        }
        .oo-search-clear:hover { color: var(--primary); background: var(--border); }
        @media (max-width: 600px) {
          .oo-search { height: 40px; padding: 0 12px; }
          .oo-search input { font-size: 14px; }
          .oo-search-count { font-size: 10px; }
        }
      `}</style>
      <svg className="oo-search-icon" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
        <circle cx="11" cy="11" r="7"/>
        <line x1="21" y1="21" x2="16.65" y2="16.65"/>
      </svg>
      <input
        type="text"
        value={value}
        onChange={e => onChange(e.target.value)}
        onKeyDown={onKey}
        placeholder={placeholder}
        autoComplete="off"
        spellCheck="false"
      />
      {hasQuery ? (
        <>
          <span className="oo-search-count">{filtered} of {total}</span>
          <button className="oo-search-clear" onClick={() => onChange('')} title="Clear (Esc)" aria-label="Clear search">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/>
            </svg>
          </button>
        </>
      ) : (
        <span className="oo-search-count">{total} {total === 1 ? 'player' : 'players'}</span>
      )}
    </div>
  );
}

// Normalize + case-insensitive match on any of the supplied string fields.
function matchesSearch(item, query, fields = ['name', 'player', 'team', 'opponent']) {
  const q = (query || '').trim().toLowerCase();
  if (!q) return true;
  for (const f of fields) {
    const v = item[f];
    if (typeof v === 'string' && v.toLowerCase().includes(q)) return true;
  }
  return false;
}
const fmt = (n, d = 1) => typeof n === 'number' ? n.toFixed(d) : '-';
const fmtPct = n => typeof n === 'number' ? (n * 100).toFixed(0) + '%' : '-';
const fmtSal = n => '$' + n.toLocaleString();
const fmtTime = s => { if (!s) return '-'; const m = s.match(/(\d{1,2})\/(\d{1,2})\/(\d{4})\s+(\d{1,2}):(\d{2})(AM|PM)/i); if (m) { let h = parseInt(m[4]); const ap = m[6].toUpperCase(); if (ap === 'PM' && h !== 12) h += 12; if (ap === 'AM' && h === 12) h = 0; return (h > 12 ? h - 12 : h || 12) + ':' + m[5] + ' ' + ap; } try { const d = new Date(s); if (!isNaN(d)) return d.toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' }); } catch {} return s; };

// ═══════════════════════════════════════════════════════════════════════
// BUILD ANIMATION (v3.24.13) — "Lineup Ticker" loading state shown in
// place of ExposureResults while the optimizer is running. CSS-only
// keyframe animation on opacity + transform so the compositor keeps it
// smooth even while the main thread is blocked by the optimize call.
// Usage (per builder tab):
//   const [isBuilding, setIsBuilding] = useState(false);
//   // wrap run() body so isBuilding flips true → work → min-duration → false
//   {isBuilding ? <BuildAnimation count={nL} /> : res && <ExposureResults …/>}
// ═══════════════════════════════════════════════════════════════════════
// v3.24.14: keyboard shortcuts for builder tabs. B and R both trigger
// build/rebuild (single action since the builder has no separate "rebuild"
// entry point). Ignored while user is typing in any input/textarea/select
// and while a modifier is held (so Cmd+R / Ctrl+R browser reload still
// works). Call from inside a builder tab.
function useBuilderShortcuts({ run, canBuild, isBuilding }) {
  useEffect(() => {
    const onKey = (e) => {
      const tag = document.activeElement?.tagName?.toLowerCase();
      if (tag === 'input' || tag === 'textarea' || tag === 'select') return;
      if (e.metaKey || e.ctrlKey || e.altKey) return;
      const k = e.key?.toLowerCase();
      if ((k === 'b' || k === 'r') && canBuild && !isBuilding) {
        e.preventDefault();
        run();
      }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [run, canBuild, isBuilding]);
}

function BuildAnimation({ count, label = 'Building', unit = 'lineups' }) {
  // Six rows of varying slot widths so each row reads differently —
  // evokes lineup structure without committing to a specific roster shape.
  const rows = [
    [22, 18, 12, 16, 10, 14],
    [16, 14, 22, 12, 18],
    [14, 10, 18, 22, 12, 14],
    [18, 22, 14, 10, 16],
    [12, 18, 14, 22, 10, 16],
    [16, 12, 22, 14, 18],
  ];
  return (
    <>
      <style>{`
        @keyframes build-anim-row-in {
          0%   { opacity: 0; transform: translateX(-8px); }
          10%  { opacity: 1; transform: translateX(0); }
          70%  { opacity: 1; }
          85%  { opacity: 0.32; }
          100% { opacity: 0.32; }
        }
        @keyframes build-anim-dot {
          0%, 100% { opacity: 0.25; }
          50%      { opacity: 1; }
        }
        .build-anim-wrap {
          background: var(--card, #0F1D33);
          border: 1px solid var(--border, #1E2D4A);
          border-radius: 10px;
          padding: 0;
          min-height: 260px;
          position: relative;
          overflow: hidden;
        }
        .build-anim-caption {
          position: absolute;
          left: 20px;
          top: 18px;
          font-size: 10px;
          color: var(--text-muted, #8B9ABA);
          letter-spacing: 1.5px;
          text-transform: uppercase;
          font-weight: 600;
          display: flex;
          align-items: center;
          gap: 6px;
        }
        .build-anim-caption .dot {
          display: inline-block;
          width: 4px;
          height: 4px;
          border-radius: 50%;
          background: var(--primary, #F5C518);
          animation: build-anim-dot 1.2s ease-in-out infinite;
        }
        .build-anim-count {
          position: absolute;
          right: 20px;
          top: 18px;
          font-size: 11px;
          color: var(--primary, #F5C518);
          font-weight: 600;
          font-variant-numeric: tabular-nums;
          letter-spacing: 0.4px;
        }
        .build-anim-row {
          position: absolute;
          left: 20px;
          right: 20px;
          height: 18px;
          display: flex;
          gap: 8px;
          align-items: center;
          opacity: 0;
          animation: build-anim-row-in 2.8s ease-in-out infinite;
          transform-origin: left center;
        }
        .build-anim-row::before {
          content: '';
          width: 3px;
          height: 100%;
          background: var(--primary, #F5C518);
          flex: 0 0 auto;
          border-radius: 1px;
        }
        .build-anim-slot {
          height: 8px;
          background: rgba(245, 197, 24, 0.14);
          border-radius: 2px;
        }
        @media (max-width: 768px) {
          .build-anim-wrap { min-height: 240px; }
          .build-anim-caption { left: 14px; top: 14px; font-size: 9px; letter-spacing: 1.2px; }
          .build-anim-count { right: 14px; top: 14px; font-size: 10px; }
          .build-anim-row { left: 14px; right: 14px; gap: 6px; height: 16px; }
          .build-anim-slot { height: 7px; }
        }
      `}</style>
      <div className="build-anim-wrap">
        <div className="build-anim-caption"><span className="dot"></span>{label}</div>
        <div className="build-anim-count">{count ? `${count} ${unit}` : ''}</div>
        {rows.map((widths, i) => (
          <div
            key={i}
            className="build-anim-row"
            style={{ top: `${58 + i * 28}px`, animationDelay: `${i * 0.22}s` }}
          >
            {widths.map((w, j) => <span key={j} className="build-anim-slot" style={{ width: `${w}%` }} />)}
          </div>
        ))}
      </div>
    </>
  );
}

function Icon({ name, size = 14, color, style, className }) {
  const commonStyle = { width: size, height: size, flexShrink: 0, verticalAlign: '-0.15em', display: 'inline-block', ...style };
  const p = { viewBox: '0 0 24 24', fill: 'none', stroke: color || 'currentColor', strokeWidth: 1.75, strokeLinecap: 'round', strokeLinejoin: 'round', style: commonStyle, className };
  switch (name) {
    case 'warning':        return <svg {...p}><path d="M12 3l10 18H2L12 3z"/><path d="M12 10v5"/><circle cx="12" cy="18.5" r="0.7" fill={color || 'currentColor'} stroke="none"/></svg>;
    case 'trophy':         return <svg {...p}><path d="M7 4h10v4a5 5 0 0 1-10 0V4z"/><path d="M7 6H4a2 2 0 0 0 0 4h3"/><path d="M17 6h3a2 2 0 0 1 0 4h-3"/><path d="M12 13v4"/><path d="M8 21h8"/></svg>;
    case 'target':         return <svg {...p}><circle cx="12" cy="12" r="9"/><path d="M8 12l3 3 5-6"/></svg>;
    case 'gem':            return <svg {...p}><path d="M6 3h12l3 6-9 12L3 9z"/><path d="M3 9h18"/><path d="M9 3l3 6 3-6"/></svg>;
    case 'sleeper':        return <svg {...p}><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/><path d="M18 3v2M17 4h2"/></svg>;
    case 'bomb':           return <svg {...p}><circle cx="10" cy="14" r="7"/><path d="M14 8l3-3"/><path d="M18 3h3M19.5 1.5v3"/></svg>;
    case 'flame':          return <svg {...p}><path d="M13 2C13 5 14 7 15 8C15 6 16 5 16 5C18 7 19 11 19 14A7 7 0 0 1 5 14C5 11 7 10 8 9C9 10 10 11 10 10C10 7 11 5 13 2Z"/><path d="M12 13C11 14 10 15 10 17A2.5 2.5 0 0 0 15 17C15 15 14 14 12 13Z"/></svg>;
    case 'trending-down':  return <svg {...p}><path d="M2 7l6.5 6.5 5-5 8.5 8.5"/><path d="M16 17h6v-6"/></svg>;
    case 'fist':           return <svg {...p}><path d="M9 4h5a3 3 0 0 1 3 3v5a3 3 0 0 1-3 3H8a3 3 0 0 1-3-3V7a3 3 0 0 1 3-3z"/><path d="M5 10a1.5 1.5 0 0 0 0 3"/><path d="M8 17v3h6v-3"/></svg>;
    case 'swords':         return <svg {...p}><path d="M7 6l6 6-6 6"/><path d="M17 6l-6 6 6 6"/></svg>;
    case 'bolt':           return <svg {...p} fill={color || 'currentColor'} stroke="none"><path d="M13 2L4 14h7l-1 8 10-12h-7l1-8z"/></svg>;
    case 'link':           return <svg {...p}><path d="M10 14a5 5 0 0 0 7 0l3-3a5 5 0 0 0-7-7l-1 1"/><path d="M14 10a5 5 0 0 0-7 0l-3 3a5 5 0 0 0 7 7l1-1"/></svg>;
    case 'download':       return <svg {...p}><path d="M12 4v12"/><path d="M7 11l5 5 5-5"/><path d="M4 20h16"/></svg>;
    case 'check':          return <svg {...p}><path d="M5 12l5 5 9-10"/></svg>;
    case 'chart':          return <svg {...p}><path d="M3 3v18h18"/><rect x="7" y="10" width="3" height="8"/><rect x="13" y="6" width="3" height="12"/><rect x="19" y="13" width="2" height="5"/></svg>;
    case 'refresh':        return <svg {...p}><path d="M3 12a9 9 0 0 1 15-6.7L21 8"/><path d="M21 3v5h-5"/><path d="M21 12a9 9 0 0 1-15 6.7L3 16"/><path d="M3 21v-5h5"/></svg>;
    case 'dollar':         return <svg {...p}><path d="M12 3v18"/><path d="M17 7h-6a3 3 0 0 0 0 6h2a3 3 0 0 1 0 6H6"/></svg>;
    case 'rocket':         return <svg {...p}><path d="M12 3c3 2 5 5 5 9v6l-5-3-5 3v-6c0-4 2-7 5-9z"/><circle cx="12" cy="10" r="1.5"/><path d="M7 17l-3 4M17 17l3 4"/></svg>;
    case 'tennis':         return <svg {...p}><circle cx="12" cy="12" r="9"/><path d="M5 6c3 3 3 9 0 12"/><path d="M19 6c-3 3-3 9 0 12"/></svg>;
    case 'basketball':     return <svg {...p}><circle cx="12" cy="12" r="9"/><path d="M3 12h18"/><path d="M12 3a9 9 0 0 1 0 18"/><path d="M5.5 5.5c3 2 3 11 0 13"/><path d="M18.5 5.5c-3 2-3 11 0 13"/></svg>;
    case 'chart-line':     return <svg {...p}><path d="M3 3v18h18"/><path d="M7 15l4-5 4 3 6-8"/></svg>;
    default: return null;
  }
}
function Tip({ icon, emoji, label, size = 14, desc }) {
  const [s, setS] = useState(false);
  // v3.24.14: auto-lookup a one-line definition for known OverOwned signal
  // labels so hovering Trap/Gem/Pivot/etc. reveals what they mean without
  // needing to open the full explainer modal.
  const autoDesc = typeof signalTooltip === 'function' ? signalTooltip(label) : '';
  const description = desc || autoDesc;
  // v3.24.15: mobile support. onMouseEnter/Leave don't fire on touch
  // devices, so taps did nothing. Add onClick to toggle; outside tap
  // closes via window listener while open. stopPropagation so tapping
  // the badge doesn't fire parent row handlers.
  useEffect(() => {
    if (!s) return;
    const onAway = () => setS(false);
    // Defer one frame so the same tap that opened doesn't immediately close
    const id = setTimeout(() => {
      window.addEventListener('click', onAway);
      window.addEventListener('touchstart', onAway);
      window.addEventListener('scroll', onAway, true);
    }, 0);
    return () => {
      clearTimeout(id);
      window.removeEventListener('click', onAway);
      window.removeEventListener('touchstart', onAway);
      window.removeEventListener('scroll', onAway, true);
    };
  }, [s]);
  return <span
    style={{ position: 'relative', cursor: 'help', display: 'inline-flex', alignItems: 'center' }}
    onMouseEnter={() => setS(true)}
    onMouseLeave={() => setS(false)}
    onClick={(e) => { e.stopPropagation(); setS(v => !v); }}
  >{icon ? <Icon name={icon} size={size}/> : emoji}{s && <span style={{ position: 'absolute', bottom: '120%', left: '50%', transform: 'translateX(-50%)', background: '#1E2433', border: '1px solid #2A3040', borderRadius: 6, padding: description ? '8px 12px' : '6px 10px', fontSize: 11, color: '#E2E8F0', zIndex: 999, fontWeight: 500, whiteSpace: description ? 'normal' : 'nowrap', width: description ? 260 : 'auto', maxWidth: description ? 'min(260px, calc(100vw - 24px))' : 'none', textAlign: description ? 'left' : 'center', lineHeight: description ? 1.4 : 1, pointerEvents: 'none' }}><span style={{ fontWeight: 700, display: 'block' }}>{label}</span>{description && <span style={{ display: 'block', marginTop: 4, color: 'var(--text-muted)', fontWeight: 400, fontSize: 10.5 }}>{description}</span>}</span>}</span>;
}

// ═══════════════════════════════════════════════════════════════════════
// SPLASH SCREEN — cinematic first-load brand intro
// ═══════════════════════════════════════════════════════════════════════
function SplashScreen({ sport }) {
  const isTennis = sport === 'tennis';
  const isNba = sport === 'nba';
  return (
    <div style={{ position: 'fixed', inset: 0, background: 'radial-gradient(ellipse at 50% 40%, #0F1D35 0%, #0A1628 40%, #060F1F 100%)', zIndex: 999, display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', padding: '40px 20px', overflow: 'hidden' }}>
      <style>{`
        @keyframes oo-logo-enter  { 0% { opacity: 0; transform: scale(0.55); filter: blur(8px); } 60% { opacity: 1; filter: blur(0); } 100% { opacity: 1; transform: scale(1); filter: blur(0); } }
        @keyframes oo-halo-burst  { 0% { opacity: 0; transform: scale(0.6); } 50% { opacity: 1; transform: scale(1.15); } 100% { opacity: 0.65; transform: scale(1); } }
        @keyframes oo-halo-pulse  { 0%, 100% { opacity: 0.5; transform: scale(1); } 50% { opacity: 0.8; transform: scale(1.08); } }
        @keyframes oo-ring-fade   { to { opacity: 1; } }
        @keyframes oo-ring-spin   { to { transform: rotate(360deg); } }
        @keyframes oo-breathe     { 0%, 100% { transform: scale(1); } 50% { transform: scale(1.04); } }
        @keyframes oo-fade-up     { to { opacity: 1; transform: translateY(0); } }
        @keyframes oo-fade-in     { to { opacity: 1; } }
        @keyframes oo-preserve    { 0%, 100% { transform: translateY(0) scaleX(1.18) scaleY(0.82); } 10% { transform: translateY(-8px) scaleX(1.04) scaleY(0.96); } 50% { transform: translateY(-58px) scaleX(0.95) scaleY(1.05); } 90% { transform: translateY(-8px) scaleX(1.04) scaleY(0.96); } }
        @keyframes oo-shadow-beat { 0%, 100% { opacity: 0.65; transform: translateX(-50%) scaleX(1); } 50% { opacity: 0.18; transform: translateX(-50%) scaleX(0.5); } }
        @keyframes oo-dot-blink   { 0%, 100% { opacity: 0.2; } 50% { opacity: 1; } }
        @keyframes oo-drift       { 0%, 100% { opacity: 0; transform: translateY(0); } 25% { opacity: 0.8; } 50% { transform: translateY(-20px); opacity: 0.4; } 75% { opacity: 0.7; } }
      `}</style>

      {/* Drifting particles */}
      {[{t:'20%',l:'15%',d:'0.5s',dur:'8s'},{t:'30%',l:'85%',d:'1.2s',dur:'7s'},{t:'65%',l:'10%',d:'0.8s',dur:'9s'},{t:'75%',l:'78%',d:'2s',dur:'6s'},{t:'45%',l:'92%',d:'1.5s',dur:'7.5s'},{t:'55%',l:'5%',d:'0.3s',dur:'8.5s'}].map((p, i) => (
        <div key={i} style={{ position: 'absolute', top: p.t, left: p.l, width: 2, height: 2, borderRadius: '50%', background: '#F5C518', opacity: 0, boxShadow: '0 0 4px #F5C518', animation: `oo-drift ${p.dur} ease-in-out ${p.d} infinite` }} />
      ))}

      {/* Logo with halo + rotating ring */}
      <div style={{ position: 'relative', width: 140, height: 140, marginBottom: 32, opacity: 0, transform: 'scale(0.55)', animation: 'oo-logo-enter 1.1s cubic-bezier(0.34, 1.35, 0.64, 1) 0.25s forwards' }}>
        <div style={{ position: 'absolute', inset: -40, borderRadius: '50%', background: 'radial-gradient(circle, rgba(245,197,24,0.5) 0%, rgba(245,197,24,0.15) 40%, transparent 70%)', opacity: 0, animation: 'oo-halo-burst 1.4s ease-out 0.35s forwards, oo-halo-pulse 3.2s ease-in-out 1.7s infinite' }} />
        <div style={{ position: 'absolute', inset: -8, border: '1px solid rgba(245,197,24,0.25)', borderTopColor: '#F5C518', borderRadius: '50%', opacity: 0, animation: 'oo-ring-fade 0.6s ease-out 0.9s forwards, oo-ring-spin 4s linear 0.9s infinite' }} />
        <svg viewBox="0 0 100 100" fill="none" xmlns="http://www.w3.org/2000/svg" style={{ position: 'relative', width: '100%', height: '100%', filter: 'drop-shadow(0 8px 32px rgba(245,197,24,0.35))', animation: 'oo-breathe 3.2s ease-in-out 1.7s infinite' }}>
          <circle cx="50" cy="50" r="38" fill="none" stroke="#F5C518" strokeWidth="14"/>
          <path d="M 30 64 L 45 40 L 54 52 L 63 40 L 70 64 Z" fill="#F5C518"/>
          <circle cx="45" cy="40" r="1.8" fill="#FFFFFF"/>
        </svg>
      </div>

      {/* Wordmark */}
      <div style={{ fontFamily: "'Inter', sans-serif", fontWeight: 800, fontSize: 48, letterSpacing: '-0.03em', color: '#F8FAFC', marginBottom: 20, opacity: 0, transform: 'translateY(12px)', animation: 'oo-fade-up 0.9s cubic-bezier(0.2, 0.8, 0.2, 1) 0.95s forwards' }}>
        Over<span style={{ color: '#F5C518', display: 'inline-block', textShadow: '0 0 20px rgba(245,197,24,0.4)' }}>O</span>wned
      </div>

      {/* Tennis ball pre-serve bounce — scaled up to sit with the wordmark */}
      {isTennis && (
        <div style={{ position: 'relative', width: 140, height: 100, marginBottom: 24, opacity: 0, animation: 'oo-fade-in 0.6s ease-out 1.4s forwards' }}>
          <div style={{ position: 'absolute', bottom: 0, left: '50%', transform: 'translateX(-50%)', width: 100, height: 2, background: 'linear-gradient(90deg, transparent 0%, rgba(245,197,24,0.55) 50%, transparent 100%)' }} />
          <div style={{ position: 'absolute', bottom: 2, left: '50%', marginLeft: -20, width: 40, height: 40 }}>
            <div style={{ position: 'absolute', bottom: -4, left: '50%', width: 34, height: 6, background: 'rgba(0,0,0,0.6)', borderRadius: '50%', filter: 'blur(2.5px)', transform: 'translateX(-50%)', animation: 'oo-shadow-beat 1.4s cubic-bezier(0.5, 0, 0.5, 1) 1.6s infinite' }} />
            <div style={{ transformOrigin: '50% 100%', animation: 'oo-preserve 1.4s cubic-bezier(0.5, 0, 0.5, 1) 1.6s infinite' }}>
              <svg width="40" height="40" viewBox="0 0 40 40" xmlns="http://www.w3.org/2000/svg">
                <defs>
                  <radialGradient id="ballGrad" cx="0.35" cy="0.3" r="0.75">
                    <stop offset="0%" stopColor="#F0FF7A"/>
                    <stop offset="60%" stopColor="#DDFF4F"/>
                    <stop offset="100%" stopColor="#B8D438"/>
                  </radialGradient>
                </defs>
                <circle cx="20" cy="20" r="18" fill="url(#ballGrad)" stroke="#8BA132" strokeWidth="0.6"/>
                <path d="M 3 20 C 9 13, 15 13, 20 20 C 25 27, 31 27, 37 20" fill="none" stroke="white" strokeWidth="2" strokeLinecap="round"/>
              </svg>
            </div>
          </div>
        </div>
      )}
      {isNba && (
        <div style={{ position: 'relative', width: 140, height: 100, marginBottom: 24, opacity: 0, animation: 'oo-fade-in 0.6s ease-out 1.4s forwards' }}>
          <div style={{ position: 'absolute', bottom: 0, left: '50%', transform: 'translateX(-50%)', width: 100, height: 2, background: 'linear-gradient(90deg, transparent 0%, rgba(245,197,24,0.55) 50%, transparent 100%)' }} />
          <div style={{ position: 'absolute', bottom: 2, left: '50%', marginLeft: -20, width: 40, height: 40 }}>
            <div style={{ position: 'absolute', bottom: -4, left: '50%', width: 34, height: 6, background: 'rgba(0,0,0,0.6)', borderRadius: '50%', filter: 'blur(2.5px)', transform: 'translateX(-50%)', animation: 'oo-shadow-beat 1.4s cubic-bezier(0.5, 0, 0.5, 1) 1.6s infinite' }} />
            <div style={{ transformOrigin: '50% 100%', animation: 'oo-preserve 1.4s cubic-bezier(0.5, 0, 0.5, 1) 1.6s infinite' }}>
              <svg width="40" height="40" viewBox="0 0 40 40" xmlns="http://www.w3.org/2000/svg">
                <defs>
                  <radialGradient id="bballSplash" cx="0.35" cy="0.3" r="0.8">
                    <stop offset="0%" stopColor="#FFB672"/>
                    <stop offset="55%" stopColor="#E8722C"/>
                    <stop offset="100%" stopColor="#A74712"/>
                  </radialGradient>
                </defs>
                <circle cx="20" cy="20" r="18" fill="url(#bballSplash)" stroke="#5C2A0A" strokeWidth="0.8"/>
                <path d="M 2 20 H 38" fill="none" stroke="rgba(0,0,0,0.6)" strokeWidth="1.4" strokeLinecap="round"/>
                <path d="M 20 2 V 38" fill="none" stroke="rgba(0,0,0,0.6)" strokeWidth="1.4" strokeLinecap="round"/>
                <path d="M 6.5 6.5 Q 20 20 6.5 33.5" fill="none" stroke="rgba(0,0,0,0.5)" strokeWidth="1.3" strokeLinecap="round"/>
                <path d="M 33.5 6.5 Q 20 20 33.5 33.5" fill="none" stroke="rgba(0,0,0,0.5)" strokeWidth="1.3" strokeLinecap="round"/>
              </svg>
            </div>
          </div>
        </div>
      )}
      {!isTennis && !isNba && (
        <div style={{ display: 'flex', gap: 8, justifyContent: 'center', marginBottom: 20, opacity: 0, animation: 'oo-fade-in 0.6s ease-out 1.4s forwards' }}>
          {[0, 1, 2].map(i => <div key={i} style={{ width: 10, height: 10, borderRadius: '50%', background: '#F5C518', animation: `oo-dot-blink 1.2s ease-in-out ${i * 0.18}s infinite` }} />)}
        </div>
      )}

      {/* Vignette */}
      <div style={{ position: 'absolute', inset: 0, background: 'radial-gradient(ellipse at center, transparent 40%, rgba(0,0,0,0.6) 100%)', pointerEvents: 'none' }} />
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════════════
// SPORT SWITCH LOADER — simple, clean, quick (used after first load)
// ═══════════════════════════════════════════════════════════════════════
function SportSwitchLoader({ sport }) {
  const isTennis = sport === 'tennis';
  const isNba = sport === 'nba';
  const label = isTennis ? 'Tennis' : isNba ? 'NBA' : 'UFC';
  const iconName = isTennis ? 'tennis' : isNba ? 'basketball' : 'fist';
  return (
    <div style={{ padding: '80px 20px 60px', textAlign: 'center' }}>
      <style>{`
        @keyframes sw-fade      { 0% { opacity: 0; transform: translateY(6px); } 100% { opacity: 1; transform: translateY(0); } }
        @keyframes sw-preserve  { 0%, 100% { transform: translateY(0) scaleX(1.18) scaleY(0.82); } 10% { transform: translateY(-8px) scaleX(1.04) scaleY(0.96); } 50% { transform: translateY(-44px) scaleX(0.95) scaleY(1.05); } 90% { transform: translateY(-8px) scaleX(1.04) scaleY(0.96); } }
        @keyframes sw-shadow    { 0%, 100% { opacity: 0.5; transform: translateX(-50%) scaleX(1); } 50% { opacity: 0.15; transform: translateX(-50%) scaleX(0.4); } }
        @keyframes sw-dot       { 0%, 100% { opacity: 0.2; } 50% { opacity: 1; } }
        @keyframes sw-spin      { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }
      `}</style>
      <div style={{ fontFamily: "'Inter', sans-serif", fontSize: 15, fontWeight: 600, color: 'var(--text-muted)', letterSpacing: '0.06em', textTransform: 'uppercase', marginBottom: 24, opacity: 0, animation: 'sw-fade 0.4s ease-out 0.1s forwards' }}>
        Switching to {label} <Icon name={iconName} size={16} color="#F5C518"/>
      </div>
      {isTennis && (
        <div style={{ position: 'relative', width: 100, height: 70, margin: '0 auto 20px', opacity: 0, animation: 'sw-fade 0.4s ease-out 0.2s forwards' }}>
          <div style={{ position: 'absolute', bottom: 0, left: '50%', transform: 'translateX(-50%)', width: 64, height: 2, background: 'linear-gradient(90deg, transparent, rgba(245,197,24,0.5), transparent)' }} />
          <div style={{ position: 'absolute', bottom: 2, left: '50%', marginLeft: -14, width: 28, height: 28 }}>
            <div style={{ position: 'absolute', bottom: -3, left: '50%', width: 22, height: 4, background: 'rgba(0,0,0,0.55)', borderRadius: '50%', filter: 'blur(2px)', transform: 'translateX(-50%)', animation: 'sw-shadow 1.3s cubic-bezier(0.5, 0, 0.5, 1) infinite' }} />
            <div style={{ transformOrigin: '50% 100%', animation: 'sw-preserve 1.3s cubic-bezier(0.5, 0, 0.5, 1) infinite' }}>
              <svg width="28" height="28" viewBox="0 0 40 40">
                <circle cx="20" cy="20" r="18" fill="#DDFF4F" stroke="#95A835" strokeWidth="0.8" />
                <path d="M 3 20 C 9 13, 15 13, 20 20 C 25 27, 31 27, 37 20" fill="none" stroke="white" strokeWidth="2" strokeLinecap="round" />
              </svg>
            </div>
          </div>
        </div>
      )}
      {isNba && (
        <div style={{ position: 'relative', width: 100, height: 70, margin: '0 auto 20px', opacity: 0, animation: 'sw-fade 0.4s ease-out 0.2s forwards' }}>
          <div style={{ position: 'absolute', bottom: 0, left: '50%', transform: 'translateX(-50%)', width: 64, height: 2, background: 'linear-gradient(90deg, transparent, rgba(245,197,24,0.5), transparent)' }} />
          <div style={{ position: 'absolute', bottom: 2, left: '50%', marginLeft: -14, width: 28, height: 28 }}>
            <div style={{ position: 'absolute', bottom: -3, left: '50%', width: 22, height: 4, background: 'rgba(0,0,0,0.55)', borderRadius: '50%', filter: 'blur(2px)', transform: 'translateX(-50%)', animation: 'sw-shadow 1.3s cubic-bezier(0.5, 0, 0.5, 1) infinite' }} />
            <div style={{ transformOrigin: '50% 100%', animation: 'sw-preserve 1.3s cubic-bezier(0.5, 0, 0.5, 1) infinite' }}>
              <svg width="28" height="28" viewBox="0 0 40 40">
                <defs>
                  <radialGradient id="bballSw" cx="0.35" cy="0.3" r="0.8">
                    <stop offset="0%" stopColor="#FFA05E"/>
                    <stop offset="55%" stopColor="#E8722C"/>
                    <stop offset="100%" stopColor="#A74712"/>
                  </radialGradient>
                </defs>
                <circle cx="20" cy="20" r="18" fill="url(#bballSw)" stroke="#5C2A0A" strokeWidth="0.8" />
                <path d="M 2 20 H 38" fill="none" stroke="rgba(0,0,0,0.55)" strokeWidth="1.3" strokeLinecap="round" />
                <path d="M 20 2 V 38" fill="none" stroke="rgba(0,0,0,0.55)" strokeWidth="1.3" strokeLinecap="round" />
                <path d="M 6.5 6.5 Q 20 20 6.5 33.5" fill="none" stroke="rgba(0,0,0,0.45)" strokeWidth="1.2" strokeLinecap="round" />
                <path d="M 33.5 6.5 Q 20 20 33.5 33.5" fill="none" stroke="rgba(0,0,0,0.45)" strokeWidth="1.2" strokeLinecap="round" />
              </svg>
            </div>
          </div>
        </div>
      )}
      {!isTennis && !isNba && (
        <div style={{ display: 'flex', gap: 8, justifyContent: 'center', marginBottom: 20, opacity: 0, animation: 'sw-fade 0.4s ease-out 0.2s forwards' }}>
          {[0, 1, 2].map(i => <div key={i} style={{ width: 10, height: 10, borderRadius: '50%', background: 'var(--primary)', animation: `sw-dot 1.2s ease-in-out ${i * 0.15}s infinite` }} />)}
        </div>
      )}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════════════
// MAIN APP — adds sport toggle on top of existing structure
// ═══════════════════════════════════════════════════════════════════════
export default function App() {
  // Phase 1 auth — #signin hash reveals the magic-link sign-in card.
  // Doesn't gate anything yet (Phase 3 adds paywall overlays on paid tabs).
  const [showSignIn, setShowSignIn] = useState(() => typeof window !== 'undefined' && window.location.hash === '#signin');
  useEffect(() => {
    const sync = () => setShowSignIn(window.location.hash === '#signin');
    window.addEventListener('hashchange', sync);
    return () => window.removeEventListener('hashchange', sync);
  }, []);
  // v3.24.14: clean up unknown hashes on mount so bookmarked/shared deep
  // links (like /#lineup) don't leave the user on a confusing blank state.
  // Only #signin is a real route; anything else gets stripped so the app
  // renders its default home instead.
  useEffect(() => {
    if (typeof window === 'undefined') return;
    const h = window.location.hash;
    if (h && h !== '#signin' && h !== '') {
      history.replaceState(null, '', window.location.pathname + window.location.search);
    }
  }, []);
  if (showSignIn) return <SignInPrompt />;

  // v3.24.14: persist last sport to localStorage. DFS users typically
  // stick with one sport — forcing Tennis on every reload is annoying
  // for MMA/NBA regulars.
  const [sport, setSportRaw] = useState(() => {
    try {
      const saved = typeof window !== 'undefined' && localStorage.getItem('oo_last_sport');
      if (saved === 'tennis' || saved === 'mma' || saved === 'nba') return saved;
    } catch {}
    return 'tennis';
  });
  const [slateDate, setSlateDate] = useState('live'); // 'live' or YYYY-MM-DD
  // Atomic sport switch — resets slateDate to 'live' in the SAME render as
  // the sport change. Without this, useSlateData would fire one extra fetch
  // with the old archive date against the new sport (e.g., NBA 2025-11-15 →
  // /slates/tennis/2025-11-15.json) which fails and leaves the error state
  // stuck even after the live fetch resolves. React 18 batches both setters
  // so the first render already has slateDate='live'.
  const setSport = useCallback((nextSport) => {
    setSportRaw(nextSport);
    setSlateDate('live');
    try { if (typeof window !== 'undefined') localStorage.setItem('oo_last_sport', nextSport); } catch {}
  }, []);
  // If the saved sport is feature-flagged off (e.g. MMA/NBA hidden),
  // automatically redirect back to tennis.
  useEffect(() => {
    if (!isSportEnabled(sport)) setSportRaw('tennis');
  }, [sport]);
  const { data, error } = useSlateData(sport, slateDate);
  const manifestSlates = useSlateManifest(sport);
  const [tab, setTab] = useState('dk');
  const [hasLoadedOnce, setHasLoadedOnce] = useState(false);
  // Reset to DK tab when sport changes — non-DK tabs can show confusing
  // error/empty states when the new sport's slate isn't loaded yet (or
  // doesn't exist for today). DK tab handles the empty state cleanly.
  useEffect(() => { setTab('dk'); }, [sport]);
  // Projection overrides: user-entered projection values keyed by player name. Reset when slate/sport swaps.
  const [projOverrides, setProjOverrides] = useState({});
  useEffect(() => { setProjOverrides({}); }, [sport, data]);
  const onOverrideProj = useCallback((name, value) => {
    setProjOverrides(prev => {
      if (value === '' || value == null || isNaN(+value)) {
        const next = { ...prev }; delete next[name]; return next;
      }
      return { ...prev, [name]: +value };
    });
  }, []);

  // Lock / Exclude player sets. Lock = MUST be in every lineup. Exclude = NEVER in any lineup.
  // Stored as arrays in state (React needs fresh references to re-render); converted to Set when
  // passed to optimizers. Reset on slate/sport change — new slate means different active players.
  const [lockedPlayers, setLockedPlayers] = useState([]);
  const [excludedPlayers, setExcludedPlayers] = useState([]);
  // NBA-only per-slot lock/exclude. Populated when the user clicks lock/exclude
  // while the NBA DK tab is in CPT or FLEX scope. Separate from the "any-slot"
  // sets above so users can e.g. lock SGA as FLEX-only (not CPT) without
  // affecting his any-slot status. Tennis/MMA never touch these.
  const [cptLockedPlayers, setCptLockedPlayers]       = useState([]);
  const [flexLockedPlayers, setFlexLockedPlayers]     = useState([]);
  const [cptExcludedPlayers, setCptExcludedPlayers]   = useState([]);
  const [flexExcludedPlayers, setFlexExcludedPlayers] = useState([]);
  useEffect(() => {
    setLockedPlayers([]); setExcludedPlayers([]);
    setCptLockedPlayers([]); setFlexLockedPlayers([]);
    setCptExcludedPlayers([]); setFlexExcludedPlayers([]);
  }, [sport, data]);

  // Toggle handlers: lock and exclude are mutually exclusive — toggling one clears the other
  // for that player (preventing an impossible "must-be-in AND must-be-out" contradiction).
  const onToggleLock = useCallback((name) => {
    setLockedPlayers(prev => prev.includes(name) ? prev.filter(n => n !== name) : [...prev, name]);
    setExcludedPlayers(prev => prev.filter(n => n !== name));
  }, []);
  const onToggleExclude = useCallback((name) => {
    setExcludedPlayers(prev => prev.includes(name) ? prev.filter(n => n !== name) : [...prev, name]);
    setLockedPlayers(prev => prev.filter(n => n !== name));
  }, []);
  const onClearLocks = useCallback(() => setLockedPlayers([]), []);
  const onClearExcludes = useCallback(() => setExcludedPlayers([]), []);
  // Per-slot NBA toggles. Same mutual-exclusion within the slot's lock/exclude pair.
  const onToggleCptLock = useCallback((name) => {
    setCptLockedPlayers(prev => prev.includes(name) ? prev.filter(n => n !== name) : [...prev, name]);
    setCptExcludedPlayers(prev => prev.filter(n => n !== name));
  }, []);
  const onToggleCptExclude = useCallback((name) => {
    setCptExcludedPlayers(prev => prev.includes(name) ? prev.filter(n => n !== name) : [...prev, name]);
    setCptLockedPlayers(prev => prev.filter(n => n !== name));
  }, []);
  const onToggleFlexLock = useCallback((name) => {
    setFlexLockedPlayers(prev => prev.includes(name) ? prev.filter(n => n !== name) : [...prev, name]);
    setFlexExcludedPlayers(prev => prev.filter(n => n !== name));
  }, []);
  const onToggleFlexExclude = useCallback((name) => {
    setFlexExcludedPlayers(prev => prev.includes(name) ? prev.filter(n => n !== name) : [...prev, name]);
    setFlexLockedPlayers(prev => prev.filter(n => n !== name));
  }, []);
  const onClearCptLocks      = useCallback(() => setCptLockedPlayers([]), []);
  const onClearCptExcludes   = useCallback(() => setCptExcludedPlayers([]), []);
  const onClearFlexLocks     = useCallback(() => setFlexLockedPlayers([]), []);
  const onClearFlexExcludes  = useCallback(() => setFlexExcludedPlayers([]), []);
  // Cursor-tracking gold glow: follows mouse, rendered via CSS custom properties on <body>.
  // Throttled via rAF so mousemove doesn't thrash layout.
  useEffect(() => {
    let raf = 0;
    const onMove = (e) => {
      if (raf) return;
      raf = requestAnimationFrame(() => {
        document.body.style.setProperty('--mx', `${e.clientX}px`);
        document.body.style.setProperty('--my', `${e.clientY}px`);
        raf = 0;
      });
    };
    window.addEventListener('mousemove', onMove);
    return () => { window.removeEventListener('mousemove', onMove); if (raf) cancelAnimationFrame(raf); };
  }, []);
  useEffect(() => { if (data) setHasLoadedOnce(true); }, [data]);
  // Preload Instrument Serif (for the splash tagline) — runs once
  useEffect(() => {
    if (document.querySelector('link[href*="Instrument+Serif"]')) return;
    const link = document.createElement('link');
    link.rel = 'stylesheet';
    link.href = 'https://fonts.googleapis.com/css2?family=Instrument+Serif:ital@0;1&display=swap';
    document.head.appendChild(link);
  }, []);

  const tennisProjections = useMemo(() => {
    if (sport !== 'tennis' || !data || !data.matches) return { dkPlayers: [], ppRows: [] };
    try { return buildProjections(data); }
    catch (e) { console.error('buildProjections error:', e); return { dkPlayers: [], ppRows: [], buildError: e.message }; }
  }, [sport, data]);
  const mmaProjections = useMemo(() => {
    if (sport !== 'mma' || !data || !data.fights) return { dkPlayers: [], ppRows: [] };
    try { return buildMMAProjections(data); }
    catch (e) { console.error('buildMMAProjections error:', e); return { dkPlayers: [], ppRows: [], buildError: e.message }; }
  }, [sport, data]);
  const nbaProjections = useMemo(() => {
    if (sport !== 'nba' || !data || !data.dk_players) return { dkPlayers: [], ppRows: [] };
    try { return buildNBAProjections(data); }
    catch (e) { console.error('buildNBAProjections error:', e); return { dkPlayers: [], ppRows: [], buildError: e.message }; }
  }, [sport, data]);
  const { dkPlayers: rawDkPlayers, ppRows, buildError } =
    sport === 'tennis' ? tennisProjections
    : sport === 'mma' ? mmaProjections
    : nbaProjections;
  // Apply user projection overrides. For MMA, scale ceiling proportionally so proj:ceil ratio stays sane.
  // Value/cval are recomputed against salary so they reflect the new proj.
  const dkPlayers = useMemo(() => {
    if (!rawDkPlayers.length || Object.keys(projOverrides).length === 0) return rawDkPlayers;
    return rawDkPlayers.map(p => {
      const ov = projOverrides[p.name];
      if (ov == null) return p;
      const mult = p.proj > 0 ? ov / p.proj : 1;
      const newVal = p.salary > 0 ? Math.round(ov / (p.salary / 1000) * 100) / 100 : 0;
      const out = { ...p, proj: ov, val: newVal, _overridden: true };
      if (p.ceil != null) {
        out.ceil = Math.round(p.ceil * mult * 100) / 100;
        if (p.salary > 0) out.cval = Math.round(out.ceil / (p.salary / 1000) * 100) / 100;
      }
      return out;
    });
  }, [rawDkPlayers, projOverrides]);
  // Ownership now returns { overall, cpt } — captain-specific tracking for
  // showdown slates is critical since a player at 60% total ownership but
  // only 8% CPT is a different fade target than one at 60% / 40% CPT.
  //
  // CRITICAL: uses rawDkPlayers (pre-override), not dkPlayers (override-applied).
  // simulateOwnership runs a 1500-lineup Monte Carlo optimize internally — if this
  // memo depended on dkPlayers, every keystroke in a projection edit input would
  // invalidate it and re-run the full simulation, freezing the UI and eating
  // keystrokes. Ownership represents what the FIELD does based on default
  // projections, which doesn't change when the user privately edits their own
  // projections, so rawDkPlayers is also the semantically correct input.
  const ownershipData = useMemo(() => {
    if (rawDkPlayers.length === 0) return { overall: {}, cpt: {}, missingPoolOwn: [] };
    // v3.25: manual Pool Own replaces the Monte Carlo simOwn. Users paste
    // per-player pool ownership values into the slate JSON at build time.
    // If ANY player on the slate has ss_pool_own, we treat the slate as
    // "manually owned" and:
    //   - use ss_pool_own directly for players that have it
    //   - run Monte Carlo ONLY for players missing the field, as a safety
    //     net so the app still functions
    //   - surface missing players via a top banner so the user knows to fix
    //     the slate before going live
    const anyHasPoolOwn = rawDkPlayers.some(p => typeof p.ss_pool_own === 'number');
    if (anyHasPoolOwn) {
      const missing = rawDkPlayers.filter(p => typeof p.ss_pool_own !== 'number').map(p => p.name);
      let simmed = { overall: {}, cpt: {} };
      if (missing.length > 0) {
        // Fallback sim for the players without provided values
        if (sport === 'tennis') simmed = simulateOwnership(rawDkPlayers);
        else if (sport === 'mma') simmed = simulateMMAOwnership(rawDkPlayers);
        else if (sport === 'nba') simmed = simulateNBAOwnership(rawDkPlayers, data?.slate_type || 'showdown');
      }
      const overall = {};
      for (const p of rawDkPlayers) {
        overall[p.name] = typeof p.ss_pool_own === 'number'
          ? p.ss_pool_own
          : (simmed.overall[p.name] || 0);
      }
      return { overall, cpt: simmed.cpt || {}, missingPoolOwn: missing };
    }
    // No manual values at all — behave like the original simulation.
    if (sport === 'tennis') return { ...simulateOwnership(rawDkPlayers), missingPoolOwn: [] };
    if (sport === 'mma')    return { ...simulateMMAOwnership(rawDkPlayers), missingPoolOwn: [] };
    if (sport === 'nba')    return { ...simulateNBAOwnership(rawDkPlayers, data?.slate_type || 'showdown'), missingPoolOwn: [] };
    return { overall: {}, cpt: {}, missingPoolOwn: [] };
  }, [rawDkPlayers, sport, data]);
  const ownership = ownershipData.overall;
  const cptOwnership = ownershipData.cpt;
  const missingPoolOwn = ownershipData.missingPoolOwn || [];

  if (error) {
    const expectedUrl = sport === 'mma' ? './slate-mma.json'
                      : sport === 'nba' ? './slate-nba.json'
                      : './slate.json';
    const expectedPath = sport === 'mma' ? 'public/slate-mma.json'
                       : sport === 'nba' ? 'public/slate-nba.json'
                       : 'public/slate.json';
    return <div className="app">
      <Topbar sport={sport} onSportChange={setSport} data={null} />
      <div className="empty" style={{ padding: '40px 20px' }}>
        <h2 style={{ display: "flex", alignItems: "center", gap: 8 }}><Icon name="warning" size={18} color="#EF4444"/> Slate not loaded</h2>
        <p style={{ marginTop: 12 }}>Fetch failed for <code style={{ background: 'var(--card)', padding: '2px 8px', borderRadius: 4, color: 'var(--primary)' }}>{expectedUrl}</code></p>
        <p style={{ marginTop: 8, fontSize: 13 }}>Error: <span style={{ color: 'var(--red)' }}>{error}</span></p>
        <div style={{ marginTop: 20, padding: '16px 20px', background: 'var(--card)', border: '1px solid var(--border)', borderRadius: 8, maxWidth: 600, margin: '20px auto', textAlign: 'left', fontSize: 13 }}>
          <div style={{ color: 'var(--primary)', fontWeight: 700, marginBottom: 8 }}>Troubleshooting checklist:</div>
          <div style={{ color: 'var(--text-muted)', lineHeight: 1.7 }}>
            1. File must be at <code style={{ color: 'var(--primary-light)' }}>{expectedPath}</code> in your repo<br/>
            2. Confirm the file is committed + pushed to GitHub<br/>
            3. Wait 30–60s for Vercel to redeploy, then hard-refresh (Cmd+Shift+R / Ctrl+F5)<br/>
            4. Open browser DevTools → Network tab → click this sport again → check slate-mma.json status code
          </div>
        </div>
      </div>
    </div>;
  }
  if (!data) return <div className="app">
    {hasLoadedOnce && <Topbar sport={sport} onSportChange={setSport} data={null} />}
    {hasLoadedOnce ? <SportSwitchLoader sport={sport} /> : <SplashScreen sport={sport} />}
  </div>;

  // Shared icon set — tennis and MMA use the same tabs.
  // Label `l` becomes a hover tooltip via the title attribute; the tab bar shows icons only for a cleaner look.
  const buildTabs = () => [
    { id: 'dk', l: 'DraftKings Projections', icon: <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round"><path d="M3 17V9.5l4 3 2-6.5 3 6 3-6 2 6.5 4-3V17z"/><path d="M3 19h18"/></svg> },
    { id: 'pp', l: 'PrizePicks Projections', icon: <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round"><circle cx="12" cy="12" r="9"/><circle cx="12" cy="12" r="5"/><circle cx="12" cy="12" r="1.5" fill="currentColor" stroke="none"/></svg> },
    { id: 'pplines', l: 'PP Live Lines', icon: <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round"><path d="M12 20h9"/><path d="M16.5 3.5a2.121 2.121 0 1 1 3 3L7 19l-4 1 1-4 12.5-12.5z"/></svg> },
    { id: 'build', l: 'Lineup Builder', icon: <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round"><path d="M13 2L4 14h7l-1 8 10-12h-7l1-8z"/></svg> },
    { id: 'leverage', l: 'Live Leverage', icon: <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round"><path d="M7 3v18M4 7l3-4 3 4"/><path d="M17 21V3M14 17l3 4 3-4"/></svg> },
    { id: 'record', l: 'Track Record', icon: <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round"><path d="M3 3v18h18"/><path d="M7 15l4-5 4 3 6-8"/></svg> }
  ];
  const tabs = buildTabs();

  return (<div className="app">
    <style>{`
      /* Hide number input spinners in builder rows (desktop only — mobile already clean) */
      .ctrl-row input[type="number"]::-webkit-outer-spin-button,
      .ctrl-row input[type="number"]::-webkit-inner-spin-button {
        -webkit-appearance: none;
        margin: 0;
      }
      .ctrl-row input[type="number"] {
        -moz-appearance: textfield;
        text-align: center;
      }
      /* Cursor-tracking gold glow — fixed, pointer-events:none, subtle */
      .cursor-glow {
        position: fixed;
        top: 0; left: 0;
        width: 100vw; height: 100vh;
        pointer-events: none;
        z-index: 0;
        background: radial-gradient(
          600px circle at var(--mx, 50%) var(--my, 50%),
          rgba(245, 197, 24, 0.13),
          rgba(245, 197, 24, 0.08) 40%,
          transparent 70%
        );
        transition: background 0.05s ease-out;
        mix-blend-mode: screen;
      }
      @media (hover: none) { .cursor-glow { display: none; } }
      /* Projection table override input — transparent so parent cell-top3/cell-proj
         classes handle the colors (top-value gold highlight, etc). Overridden state
         wins over both via !important. */
      .proj-edit {
        background: transparent;
        border: 1px solid transparent;
        color: inherit;
        font: inherit;
        font-variant-numeric: tabular-nums;
        text-align: right;
        width: 58px;
        padding: 2px 4px;
        border-radius: 3px;
        transition: border-color 0.12s, background 0.12s;
      }
      .proj-edit:hover { border-color: var(--border); background: rgba(245, 197, 24, 0.04); }
      .proj-edit:focus { outline: none; border-color: var(--primary); background: var(--bg); }
      .proj-edit.overridden { color: var(--primary) !important; font-weight: 700; }
      .proj-edit::-webkit-outer-spin-button, .proj-edit::-webkit-inner-spin-button { -webkit-appearance: none; margin: 0; }
      .proj-edit { -moz-appearance: textfield; }

      /* v3.24.15: mobile tweaks for the builder summary bar, topbar stale-slate
         pill, and post-build action row. Paired classes are set inline on those
         elements. Desktop layout is unchanged. */
      @media (max-width: 640px) {
        .oo-summary-bar { font-size: 11.5px !important; padding: 10px 12px !important; gap: 4px 8px !important; }
        .oo-summary-bar .oo-sum-sep { display: none !important; }
        .oo-summary-bar > span { flex: 0 1 auto; }
        .oo-action-row { gap: 8px !important; }
        .oo-action-row > .btn { flex: 1 1 100% !important; }
        .oo-restore-btn { flex: 1 1 100% !important; justify-content: center !important; padding: 12px 14px !important; font-size: 12px !important; }
        .oo-stale-pill { font-size: 10px !important; padding: 2px 6px !important; margin-left: 6px !important; letter-spacing: 0.1px !important; }
        .oo-jump-btn { padding: 10px 14px !important; font-size: 12px !important; width: 100% !important; justify-content: center !important; }
        .oo-help-toggle { width: 22px !important; height: 22px !important; }
      }
      /* Icon-only tabs — compact square buttons with hover tooltip (native title attr handles the label).
         CRITICAL: active/inactive states MUST have identical box dimensions.
         We achieve this by (a) pinning width/padding/margin on all states and
         (b) using inset box-shadow (not border) for the active highlight,
         so the highlight is purely decorative and never affects layout width. */
      /* Icon-only tabs.
         Layout (width/height/padding/border-width) goes on the BASE .tab.tab-icon
         selector only — specificity (0,2,0) — so the mobile media query can
         cleanly override it. State-specific visuals (:hover, .active) only change
         colors/shadow, never dimensions — so they never fight mobile layout. */
      .tab.tab-icon {
        width: 56px !important;
        min-width: 56px !important;
        max-width: 56px !important;
        height: 40px !important;
        padding: 0 !important;
        margin: 0 !important;
        box-sizing: border-box !important;
        display: inline-flex !important;
        align-items: center !important;
        justify-content: center !important;
        border: 1px solid rgba(245, 197, 24, 0.22) !important;
        border-radius: 6px !important;
        outline: none !important;
        background: transparent !important;
        transition: border-color 140ms ease, box-shadow 140ms ease, background-color 140ms ease;
      }
      .tab.tab-icon:hover {
        border-color: rgba(245, 197, 24, 0.5) !important;
        background: rgba(245, 197, 24, 0.04) !important;
      }
      .tab.tab-icon.active {
        border-color: #F5C518 !important;
        background: rgba(245, 197, 24, 0.12) !important;
        box-shadow: 0 0 8px rgba(245, 197, 24, 0.38) !important;
      }
      .tab.tab-icon svg {
        width: 22px;
        height: 22px;
        display: block;
      }

      /* Slate picker dropdown — force native dark theme on the options list.
         color-scheme: dark tells the browser to render the popup options,
         scrollbar, and selection highlight in dark mode (Chrome/Edge/Safari).
         Explicit option bg/color handles older browsers that ignore color-scheme. */
      .slate-picker {
        color-scheme: dark !important;
        accent-color: #F5C518 !important;
      }
      .slate-picker option {
        background-color: #0A1628 !important;
        color: #E5E7EB !important;
        padding: 6px 10px !important;
      }
      .slate-picker option:checked,
      .slate-picker option:hover {
        background: linear-gradient(rgba(245, 197, 24, 0.25), rgba(245, 197, 24, 0.25)) !important;
        background-color: rgba(245, 197, 24, 0.25) !important;
        color: #F5C518 !important;
        box-shadow: inset 0 0 0 100vw rgba(245, 197, 24, 0.25) !important;
      }

      /* Row-highlight classes — OPAQUE versions of the old rgba tints.
         color-mix() renders exactly the same visual as "card + 6% green/red"
         but as a solid color, so sticky cells stay opaque and no scroll-through
         bleed-through is possible. Applies to ALL tables universally. */
      .row-hl-green { background: color-mix(in srgb, #22C55E 6%, var(--card)) !important; }
      .row-hl-red   { background: color-mix(in srgb, #EF4444 6%, var(--card)) !important; }

      /* ═══════════════════════════════════════════════════════════════════
         SECTION HERO HEADER — editorial display treatment
         Icon badge + stroked-gradient title.
         Used on PrizePicks, Lineup Builder, Live Leverage (tennis + MMA).
         ═══════════════════════════════════════════════════════════════════ */
      .section-hero {
        display: flex;
        align-items: center;
        gap: 16px;
        padding: 4px 4px 14px;
        margin-bottom: 14px;
      }
      .section-hero-icon-wrap {
        width: 54px;
        height: 54px;
        display: flex;
        align-items: center;
        justify-content: center;
        background: radial-gradient(circle at 30% 30%, rgba(245,197,24,0.28), rgba(245,197,24,0.04));
        border: 1px solid rgba(245,197,24,0.35);
        border-radius: 14px;
        flex-shrink: 0;
        box-shadow: 0 0 0 1px rgba(245,197,24,0.08) inset, 0 8px 22px -10px rgba(245,197,24,0.3);
      }
      .section-hero-icon {
        width: 26px;
        height: 26px;
        stroke-width: 1.75;
        stroke-linecap: round;
        stroke-linejoin: round;
      }
      .section-hero-text { flex: 1; min-width: 0; }
      .section-hero-title {
        font-size: 28px;
        font-weight: 800;
        letter-spacing: -0.025em;
        line-height: 1.05;
        margin: 0 0 5px;
        background: linear-gradient(175deg, #FFFFFF 0%, #F5C518 100%);
        -webkit-background-clip: text;
        background-clip: text;
        color: transparent;
        -webkit-text-stroke: 0.5px rgba(245,197,24,0.35);
        filter: drop-shadow(0 2px 14px rgba(245,197,24,0.15));
      }
      .section-hero-sub {
        font-size: 12px;
        color: var(--text-muted);
        font-weight: 500;
        letter-spacing: 0.02em;
      }
      @media (max-width: 768px) {
        .section-hero { gap: 12px; padding: 2px 4px 10px; margin-bottom: 10px; }
        .section-hero-icon-wrap { width: 44px; height: 44px; border-radius: 11px; }
        .section-hero-icon { width: 22px; height: 22px; }
        .section-hero-title { font-size: 22px; letter-spacing: -0.02em; }
        .section-hero-sub { font-size: 11px; }
      }

      /* Softer text colors — for inline (non-boxed) colored text.
         Full-saturation --green/--red/--amber are harsh against dark bg in
         body-copy contexts; these variants keep semantic meaning without
         the glare. Boxed values (cell-ev-pos, etc.) keep the saturated
         colors since the box provides contrast separation. */
      :root {
        --green-text: #4ADE80;
        --red-text:   #F87171;
        --amber-text: #FBBF24;
      }

      /* ═══════════════════════════════════════════════════════════════════
         MOBILE RESPONSIVE — phones and small tablets
         Touch targets ≥44px (iOS HIG), readable 12-13px body text,
         tables get horizontal scroll, controls stack vertically.
         ═══════════════════════════════════════════════════════════════════ */
      @media (max-width: 768px) {
        /* Content padding — tighter on mobile */
        .content { padding: 14px 10px !important; }

        /* Topbar — ONE row: brand + toggle + X button. Hide the "Updated" time info on mobile. */
        .topbar {
          flex-wrap: nowrap !important;
          gap: 8px !important;
          padding: 10px 12px !important;
          align-items: center !important;
        }
        .topbar-brand {
          font-size: 16px !important;
          flex-shrink: 1;
          min-width: 0;
          gap: 6px !important;
        }
        .topbar-brand svg { width: 24px !important; height: 24px !important; flex-shrink: 0; }
        .topbar-brand span { white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
        .topbar-right {
          flex-shrink: 0;
          gap: 6px !important;
          flex-wrap: nowrap !important;
          align-items: center;
        }
        /* Hide full date line on mobile — removes the awkward "Updated..." third row */
        .topbar-date { display: none !important; }
        /* Slate picker — compact on mobile */
        .slate-picker { font-size: 11px !important; padding: 5px 22px 5px 8px !important; max-width: 130px; }
        .mountain-watermark { display: none; }

        /* Tab bar — stretch to full width, 4 icons fill evenly, bigger touch targets */
        .tab-bar {
          display: flex !important;
          width: 100%;
          padding: 4px !important;
          gap: 4px !important;
          justify-content: space-between !important;
          margin: 0 0 12px 0 !important;
        }
        .tab.tab-icon {
          flex: 1 1 0 !important;
          width: auto !important;
          min-width: 0 !important;
          max-width: none !important;
          height: 48px !important;
        }
        .tab.tab-icon svg {
          width: 24px !important;
          height: 24px !important;
        }

        /* Tables — horizontal scroll with touch-inertia, tighter cells, smaller text */
        .table-wrap {
          overflow-x: auto !important;
          -webkit-overflow-scrolling: touch;
          margin-left: -10px;
          margin-right: -10px;
          padding: 0 10px;
        }
        .table-wrap table { font-size: 12px !important; }
        .table-wrap th, .table-wrap td { padding: 7px 8px !important; }
        /* Ensure every row has a solid default bg so sticky cells can inherit seamlessly;
           inline row highlights (green/red tint) override this and the sticky cell
           inherits the highlight too — consistent coloring across all columns. */
        .table-wrap tbody tr { background: var(--card); }
        .table-wrap td.name, .table-wrap th:first-child { position: sticky; left: 0; background: inherit; z-index: 1; }

        /* Projection edit input — bigger for thumb entry */
        .proj-edit {
          width: 56px !important;
          padding: 6px 4px !important;
          font-size: 14px !important;
        }

        /* Section headers — slightly smaller */
        .section-head { font-size: 15px !important; }
        .section-sub { font-size: 12px !important; }

        /* Builder controls — stack rows vertically, full-width inputs */
        .builder-controls { flex-direction: column !important; align-items: stretch !important; gap: 12px !important; }
        .ctrl-row { flex-wrap: wrap !important; gap: 8px !important; }
        .ctrl-row > * { flex: 1 1 auto; }
        .ctrl-name { min-width: 0 !important; flex: 2 1 120px !important; }
        .ctrl-proj { flex: 0 0 80px !important; }

        /* Buttons — ≥40px tap targets for action buttons only (NOT toggles/icon buttons) */
        .btn { min-height: 40px; }
        .btn.btn-primary, .btn.btn-outline { font-size: 13px; padding: 10px 14px; }

        /* Lineup cards — single column on phones, stacked layout */
        .lineup-grid { grid-template-columns: 1fr !important; gap: 10px !important; }
        .lu-card { padding: 12px !important; }
        .lu-row { gap: 8px !important; font-size: 12px !important; }
        .lu-name { font-size: 13px !important; }

        /* Metrics grid — 2 cols on tablet, single col on small phones handled below */
        .metrics { grid-template-columns: repeat(2, 1fr) !important; gap: 8px !important; }
        .metric { padding: 10px !important; }
        .metric-value { font-size: 18px !important; }
        .metric-label { font-size: 10px !important; }

        /* Compliance warning banner — full width, comfortable padding */
        .compliance-warning { padding: 12px !important; font-size: 12px !important; }

        /* Cursor glow — already disabled by hover:none media query above, just belt-and-suspenders */
        .cursor-glow { display: none !important; }
      }

      /* Narrow phones (iPhone SE, etc.) — even more compression */
      @media (max-width: 420px) {
        .content { padding: 10px 6px !important; }
        .topbar { padding: 8px 10px !important; }
        .topbar-brand { font-size: 16px !important; gap: 6px !important; }
        .topbar-brand svg { width: 22px !important; height: 22px !important; }
        .tab.tab-icon { height: 44px !important; }
        .tab.tab-icon svg { width: 22px !important; height: 22px !important; }
        .table-wrap table { font-size: 11px !important; }
        .table-wrap th, .table-wrap td { padding: 6px 6px !important; }
        .metrics { grid-template-columns: 1fr 1fr !important; }
        .proj-edit { width: 52px !important; font-size: 13px !important; }
        .section-head { font-size: 14px !important; }
      }
    `}</style>
    <div className="cursor-glow" aria-hidden="true" />
    <Topbar sport={sport} onSportChange={setSport} data={data} slateDate={slateDate} onSlateDateChange={setSlateDate} manifestSlates={manifestSlates} onLogoClick={() => setTab('dk')} />
    {missingPoolOwn.length > 0 && (
      <div style={{
        padding: '10px 24px', background: 'rgba(245, 158, 11, 0.09)',
        borderBottom: '1px solid rgba(245, 158, 11, 0.35)',
        fontSize: 12.5, color: 'var(--text-muted)', lineHeight: 1.45,
      }}>
        <div style={{ display: 'flex', alignItems: 'flex-start', gap: 10, maxWidth: 1400, margin: '0 auto' }}>
          <Icon name="warning" size={14} color="var(--amber)"/>
          <div>
            <strong style={{ color: 'var(--amber)' }}>Missing Pool Own for {missingPoolOwn.length} player{missingPoolOwn.length === 1 ? '' : 's'}.</strong>
            {' '}Falling back to the simulated ownership model for{' '}
            <span style={{ color: 'var(--text)' }}>{missingPoolOwn.slice(0, 6).join(', ')}{missingPoolOwn.length > 6 ? `, +${missingPoolOwn.length - 6} more` : ''}</span>.
            {' '}Add their Pool Own values to the slate JSON before going live.
          </div>
        </div>
      </div>
    )}
    <div className="tab-bar">{tabs.map(t => (
      <button key={t.id} className={`tab tab-icon ${tab === t.id ? 'active' : ''}`} onClick={() => setTab(t.id)} title={t.l} aria-label={t.l}>
        {t.icon}
      </button>
    ))}</div>
    <div className="content">
      {buildError && <div className="empty" style={{ padding: '40px 20px' }}>
        <h2 style={{ color: '#EF4444', display: 'flex', alignItems: 'center', gap: 8 }}><Icon name="warning" size={18} color="#EF4444"/> Projection build failed</h2>
        <p style={{ marginTop: 12, fontFamily: 'monospace', fontSize: 13, color: 'var(--red)' }}>{buildError}</p>
        <p style={{ marginTop: 8, fontSize: 12, color: 'var(--text-muted)' }}>Check that slate{sport === 'mma' ? '-mma' : sport === 'nba' ? '-nba' : ''}.json has valid odds fields for all {sport === 'mma' ? 'fights' : sport === 'nba' ? 'games' : 'matches'}.</p>
      </div>}
      {!buildError && <ErrorBoundary>
      {sport === 'tennis' && (<>
        {tab === 'dk' && <DKTab players={dkPlayers} mc={data.matches?.length || 0} own={ownership} onOverride={onOverrideProj} overrides={projOverrides} lockedPlayers={lockedPlayers} excludedPlayers={excludedPlayers} onToggleLock={onToggleLock} onToggleExclude={onToggleExclude} onClearLocks={onClearLocks} onClearExcludes={onClearExcludes} />}
        {tab === 'pp' && <PPTab rows={ppRows} />}
        {tab === 'pplines' && <PrizePicksTab slateId={data?.meta?.id || data?.id || data?.meta?.slate_id} />}
        {tab === 'build' && <BuilderTab players={dkPlayers} ownership={ownership} lockedPlayers={lockedPlayers} excludedPlayers={excludedPlayers} mc={data.matches?.length || 0} onGoToProjections={() => setTab('dk')} />}
        {tab === 'leverage' && <LeverageTab players={dkPlayers} />}
        {tab === 'record' && <TrackRecordTab sport={sport} />}
      </>)}
      {sport === 'mma' && (<>
        {tab === 'dk' && <MMADKTab fighters={dkPlayers} fc={data.fights?.length || 0} own={ownership} onOverride={onOverrideProj} overrides={projOverrides} lockedPlayers={lockedPlayers} excludedPlayers={excludedPlayers} onToggleLock={onToggleLock} onToggleExclude={onToggleExclude} onClearLocks={onClearLocks} onClearExcludes={onClearExcludes} />}
        {tab === 'pp' && <MMAPPTab rows={ppRows} />}
        {tab === 'build' && <MMABuilderTab fighters={dkPlayers} ownership={ownership} lockedPlayers={lockedPlayers} excludedPlayers={excludedPlayers} onGoToProjections={() => setTab('dk')} />}
        {tab === 'leverage' && <LeverageTab players={dkPlayers} />}
        {tab === 'record' && <TrackRecordTab sport={sport} />}
      </>)}
      {sport === 'nba' && (<>
        {tab === 'dk' && <NBADKTab players={dkPlayers} gameInfo={data.game} own={ownership} cptOwn={cptOwnership} onOverride={onOverrideProj} overrides={projOverrides} lockedPlayers={lockedPlayers} excludedPlayers={excludedPlayers} onToggleLock={onToggleLock} onToggleExclude={onToggleExclude} onClearLocks={onClearLocks} onClearExcludes={onClearExcludes} cptLockedPlayers={cptLockedPlayers} flexLockedPlayers={flexLockedPlayers} cptExcludedPlayers={cptExcludedPlayers} flexExcludedPlayers={flexExcludedPlayers} onToggleCptLock={onToggleCptLock} onToggleCptExclude={onToggleCptExclude} onToggleFlexLock={onToggleFlexLock} onToggleFlexExclude={onToggleFlexExclude} />}
        {tab === 'pp' && <NBAPPTab rows={ppRows} />}
        {tab === 'build' && <NBABuilderTab players={dkPlayers} ownership={ownership} cptOwnership={cptOwnership} slateType={data.slate_type || 'showdown'} gameInfo={data.game} lockedPlayers={lockedPlayers} excludedPlayers={excludedPlayers} cptLockedPlayers={cptLockedPlayers} flexLockedPlayers={flexLockedPlayers} cptExcludedPlayers={cptExcludedPlayers} flexExcludedPlayers={flexExcludedPlayers} onGoToProjections={() => setTab('dk')} />}
        {tab === 'leverage' && <LeverageTab players={dkPlayers} />}
        {tab === 'record' && <TrackRecordTab sport={sport} />}
      </>)}
      </ErrorBoundary>}
    </div>
  </div>);
}

function Topbar({ sport, onSportChange, data, slateDate = 'live', onSlateDateChange, manifestSlates = [], onLogoClick }) {
  const hasArchive = manifestSlates && manifestSlates.length > 0;
  return (<div className="topbar">
    <div
      className="topbar-brand"
      onClick={onLogoClick}
      style={onLogoClick ? { cursor: 'pointer' } : undefined}
      role={onLogoClick ? 'button' : undefined}
      tabIndex={onLogoClick ? 0 : undefined}
      onKeyDown={onLogoClick ? (e) => { if (e.key === 'Enter' || e.key === ' ') onLogoClick(); } : undefined}
      title={onLogoClick ? 'Home' : undefined}
    >
      <svg viewBox="0 0 100 100" fill="none" xmlns="http://www.w3.org/2000/svg" width="32" height="32" aria-label="OverOwned">
        <circle cx="50" cy="50" r="38" fill="none" stroke="#F5C518" strokeWidth="14"/>
        <path d="M 30 64 L 45 40 L 54 52 L 63 40 L 70 64 Z" fill="#F5C518"/>
        <circle cx="45" cy="40" r="1.8" fill="#FFFFFF"/>
      </svg>
      <span>Over<span className="brand-o">O</span>wned</span>
    </div>
    <div className="topbar-right">
      <div style={{ display: 'flex', background: 'var(--bg)', border: '1px solid var(--border-light)', borderRadius: 6, overflow: 'hidden' }}>
        <button onClick={() => onSportChange('tennis')} title="Tennis" aria-label="Tennis" style={{
          background: sport === 'tennis' ? 'var(--primary)' : 'transparent',
          color: sport === 'tennis' ? '#0A1628' : 'var(--text-muted)',
          border: 'none', padding: '7px 12px', cursor: 'pointer',
          display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
        }}>
          {/* Tennis ball — circle with two inward-bowing vertical seams (classic tennis ball silhouette) */}
          <svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round">
            <circle cx="12" cy="12" r="9"/>
            <path d="M5 6c3 3 3 9 0 12"/>
            <path d="M19 6c-3 3-3 9 0 12"/>
          </svg>
        </button>
        {isSportEnabled('mma') && <button onClick={() => onSportChange('mma')} title="MMA" aria-label="MMA" style={{
          background: sport === 'mma' ? 'var(--primary)' : 'transparent',
          color: sport === 'mma' ? '#0A1628' : 'var(--text-muted)',
          border: 'none', padding: '7px 12px', cursor: 'pointer',
          display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
        }}>
          {/* MMA glove — padded knuckles at top, thumb protrusion left, wrist cuff bottom */}
          <svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round">
            <path d="M9 4h5a3 3 0 0 1 3 3v5a3 3 0 0 1-3 3H8a3 3 0 0 1-3-3V7a3 3 0 0 1 3-3z"/>
            <path d="M5 10a1.5 1.5 0 0 0 0 3"/>
            <path d="M8 17v3h6v-3"/>
          </svg>
        </button>}
        {isSportEnabled('nba') && <button onClick={() => onSportChange('nba')} title="NBA" aria-label="NBA" style={{
          background: sport === 'nba' ? 'var(--primary)' : 'transparent',
          color: sport === 'nba' ? '#0A1628' : 'var(--text-muted)',
          border: 'none', padding: '7px 12px', cursor: 'pointer',
          display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
        }}>
          {/* Basketball — circle with horizontal equator and two curved seams */}
          <svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round">
            <circle cx="12" cy="12" r="9"/>
            <path d="M3 12h18"/>
            <path d="M12 3a9 9 0 0 1 0 18"/>
            <path d="M5.5 5.5c3 2 3 11 0 13"/>
            <path d="M18.5 5.5c-3 2-3 11 0 13"/>
          </svg>
        </button>}
      </div>
      {hasArchive && onSlateDateChange && (() => {
        // UI shows most recent 6; backend (manifest.json) keeps the full list.
        // Older slates are still accessible by direct URL or by extending the manifest
        // if a "See all" option is added later.
        const recentSlates = manifestSlates.slice(0, 6);
        const isOlderSelected = slateDate !== 'live' && !recentSlates.some(s => s.date === slateDate);
        return (
          <select
            value={slateDate}
            onChange={e => onSlateDateChange(e.target.value)}
            title="Select slate date"
            className="slate-picker"
            style={{
              background: slateDate !== 'live' ? 'rgba(245,197,24,0.12)' : 'var(--bg)',
              border: `1px solid ${slateDate !== 'live' ? 'rgba(245,197,24,0.4)' : 'var(--border-light)'}`,
              borderRadius: 6,
              color: slateDate !== 'live' ? 'var(--primary)' : 'var(--text-muted)',
              padding: '5px 22px 5px 9px',
              fontSize: 11,
              fontWeight: 600,
              cursor: 'pointer',
              appearance: 'none',
              height: 30,
              lineHeight: '18px',
              maxWidth: 140,
              backgroundImage: `url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='10' height='10' viewBox='0 0 24 24' fill='none' stroke='%23F5C518' stroke-width='2.5' stroke-linecap='round' stroke-linejoin='round'><polyline points='6 9 12 15 18 9'/></svg>")`,
              backgroundRepeat: 'no-repeat',
              backgroundPosition: 'right 7px center',
            }}>
            <option value="live">Live slate</option>
            {recentSlates.map(s => (
              <option key={s.date} value={s.date}>{s.label || s.date}</option>
            ))}
            {isOlderSelected && <option value={slateDate}>{slateDate}</option>}
          </select>
        );
      })()}
      {data && <div className="topbar-date">
        <span className="topbar-date-main">{data.date} · {
          sport === 'nba' ? `${data.game ? `${data.game.away}@${data.game.home}` : ''} · ${(data.dk_players || []).length} players`
          : sport === 'mma' ? `${data.fights?.length || 0} fights`
          : `${data.matches?.length || 0} matches`
        }</span>
        {/* v3.24.14: stale-slate indicator. When the user selected "Live" but
            the data.date is 2+ days old, warn that there's no current slate
            and they're viewing the most recent one. Prevents the "looks
            broken" misread. */}
        {(() => {
          if (slateDate !== 'live' || !data.date) return null;
          const parsed = new Date(data.date + 'T00:00:00');
          if (isNaN(parsed)) return null;
          const today = new Date(); today.setHours(0, 0, 0, 0);
          const daysOld = Math.round((today - parsed) / 86400000);
          if (daysOld < 2) return null;
          return <span className="oo-stale-pill" style={{
            marginLeft: 8, padding: '2px 8px', borderRadius: 4,
            background: 'rgba(245, 158, 11, 0.14)',
            border: '1px solid rgba(245, 158, 11, 0.4)',
            color: 'var(--amber)', fontSize: 11, fontWeight: 600,
            letterSpacing: 0.3, whiteSpace: 'nowrap'
          }} title={`No live slate — showing the most recent available slate (${daysOld} days old)`}>
            No live slate · showing most recent
          </span>;
        })()}
        {data.last_updated && <span className="topbar-date-updated"> · <span style={{color:'var(--green)',fontSize:12}}>Updated {data.last_updated}</span></span>}
      </div>}
      <a href="https://x.com/OverOwnedDFS" target="_blank" rel="noopener noreferrer" className="twitter-btn" title="@OverOwnedDFS">
        <svg viewBox="0 0 24 24" width="14" height="14"><path d="M18.244 2.25h3.308l-7.227 8.26 8.502 11.24H16.17l-5.214-6.817L4.99 21.75H1.68l7.73-8.835L1.254 2.25H8.08l4.713 6.231zm-1.161 17.52h1.833L7.084 4.126H5.117z"/></svg>
      </a>
      <UserMenu />
    </div>
    <svg className="mountain-watermark" viewBox="0 0 340 68" xmlns="http://www.w3.org/2000/svg" preserveAspectRatio="xMaxYMid slice" aria-hidden="true">
      <path d="M0,68 L40,30 L90,50 L150,20 L210,42 L260,28 L310,38 L340,32 L340,68 Z" fill="#F5C518" opacity="0.55"/>
      <path d="M0,68 L30,50 L85,58 L140,40 L200,52 L250,46 L305,54 L340,50 L340,68 Z" fill="#F5C518" opacity="0.35"/>
      <path d="M146,22 L150,20 L154,24 L150,25 Z" fill="#FFFFFF" opacity="0.9"/>
      <path d="M206,44 L210,42 L214,46 L210,47 Z" fill="#FFFFFF" opacity="0.75"/>
    </svg>
  </div>);
}

// ═══════════════════════════════════════════════════════════════════════
// TENNIS COMPONENTS — UNCHANGED from v5 except BuilderTab gets contrarian
// ═══════════════════════════════════════════════════════════════════════
function DKTab({ players, mc, own, onOverride, overrides, lockedPlayers = [], excludedPlayers = [], onToggleLock, onToggleExclude, onClearLocks, onClearExcludes }) {
  const [q, setQ] = useState('');
  const pw = useMemo(() => players.filter(p => p.salary > 0).map(p => ({ ...p, simOwn: own[p.name] || 0 })), [players, own]);
  const pwFiltered = useMemo(() => pw.filter(p => matchesSearch(p, q)), [pw, q]);
  const { sorted, sortKey, sortDir, toggleSort } = useSort(pwFiltered, 'val', 'desc');
  const t3v = useMemo(() => [...players].sort((a, b) => b.val - a.val).slice(0, 3).map(p => p.name), [players]);
  const t3s = useMemo(() => [...players].sort((a, b) => b.pStraight - a.pStraight).slice(0, 3).map(p => p.name), [players]);
  // Biggest Trap — pp difference between ownership and win probability:
  //     trapScore = simOwn − wp × 100
  // The player most over-owned relative to their actual win chances wins.
  // HARD GATE: candidates must have wp >= 30%. A player who has no
  // realistic path to win can't be a "trap" no matter how over-owned they
  // are — they're just a bad play. The 30% floor restricts the label to
  // players with genuine match-win equity, which is where the GPP bust
  // risk actually lives.
  const trap = useMemo(() => {
    const active = pw.filter(p => p.salary > 0 && (p.wp || 0) >= 0.30);
    if (active.length === 0) return '';
    const hasOwn = active.some(p => (p.simOwn || 0) > 0);
    if (!hasOwn) return [...active].sort((a, b) => b.proj - a.proj)[0]?.name || '';
    const scored = active.map(p => {
      const own = p.simOwn || 0;
      const wp  = (p.wp || 0) * 100;
      return { name: p.name, score: own - wp };
    }).sort((a, b) => b.score - a.score);
    return scored[0]?.name || '';
  }, [pw]);

  // v3.24: "Or Trap" — #2 in the same (simOwn − wp×100) ranking.
  // Only displayed on 16+ match tennis slates where the ruleset fades both.
  const orTrap = useMemo(() => {
    if ((mc || 0) < 16) return '';
    const active = pw.filter(p => p.salary > 0 && (p.wp || 0) >= 0.30 && p.name !== trap);
    if (active.length === 0) return '';
    const scored = active.map(p => {
      const own = p.simOwn || 0;
      const wp  = (p.wp || 0) * 100;
      return { name: p.name, score: own - wp };
    }).sort((a, b) => b.score - a.score);
    return scored[0]?.name || '';
  }, [pw, trap, mc]);

  // v3.24: Top 3 PP fades (ppEdge ≤ -2), excluding trap. Displayed as
  // "or pivot: A, B, C" on 16+ match slates. Just the names — we don't
  // expose the formula to users.
  const topPpFades = useMemo(() => {
    if ((mc || 0) < 16) return [];
    return pw
      .filter(p => p.salary > 0 && typeof p.ppEdge === 'number' && p.ppEdge <= -2)
      .filter(p => p.name !== trap)
      .sort((a, b) => (a.ppEdge || 0) - (b.ppEdge || 0))
      .slice(0, 3)
      .map(p => p.name);
  }, [pw, trap, mc]);

  // Hidden Gem — dual-track with primary + optional PP-based pivot:
  //   PRIMARY 1 (preferred): trap's opponent, IF they're +199 or better
  //     (wp >= 33.4%). Close matchup = salary-swap + real upset upside.
  //   PRIMARY 2 (fallback): best player in trap's salary band
  //     (-$1000 to +$300, widened to -$2500/+$1000 if empty, then
  //     highest (wp − simOwn) gap).
  //   PIVOT (v3.19): best PrizePicks LESS signal with ppEdge ≤ -2,
  //     excluding primary and trap. If primary gem = top PP LESS,
  //     the pivot falls to #2 PP LESS. If no PP data available on
  //     the slate, pivot falls back to the salary-band winner
  //     (the old pivot logic).
  const gem = useMemo(() => {
    const trapPlayer = pw.find(p => p.name === trap);
    if (!trapPlayer) return { primary: null, pivot: null };
    const trapOwn = trapPlayer.simOwn || 0;

    // Path 1: opponent (close-matchup dog)
    const opponent = pw.find(p => p.name === trapPlayer.opponent);
    const opponentQualifies = opponent && (opponent.wp || 0) >= 0.334;

    // Path 2: salary-band — try tight band first, then wider, then fallback
    const trapSal = trapPlayer.salary;
    const scoreBand = (lo, hi) => pw.filter(p => {
      if (p.name === trap) return false;
      if (opponentQualifies && opponent && p.name === opponent.name) return false;
      const diff = p.salary - trapSal;
      return diff >= lo && diff <= hi;
    }).map(p => {
      const leverage = Math.max(0, trapOwn - (p.simOwn || 0));
      const levBoost = 1 + leverage * 0.012;
      const upsideBoost = 1 + (p.pStraight || 0.3) * 0.3;
      const score = (p.val || 0) * (p.proj || 0) * levBoost * upsideBoost;
      return { name: p.name, score };
    }).sort((a, b) => b.score - a.score);

    // Compute salary-band rankings once — used for primary (if opponent
    // doesn't qualify) AND as pivot fallback when no PP data available.
    let bandRanked = scoreBand(-1000, 300);
    if (bandRanked.length === 0) bandRanked = scoreBand(-2500, 1000);
    // Leverage fallback if salary-band empty (very short slates)
    if (bandRanked.length === 0) {
      bandRanked = pw.filter(p => {
        if (p.name === trap) return false;
        if (opponentQualifies && opponent && p.name === opponent.name) return false;
        return (p.salary || 0) > 0;
      }).map(p => ({ name: p.name, score: (p.wp || 0) * 100 - (p.simOwn || 0) }))
        .sort((a, b) => b.score - a.score)
        .filter(x => x.score > 0);
    }

    // Determine primary
    // v3.23: added ≥3% sim own gate — prevents noise-level picks. Applied to
    // both the opponent path and the bandRanked path. If the opponent fails
    // the gate, we fall through to bandRanked (graceful degradation) rather
    // than null-out the primary entirely. Behavior-preserving for any slate
    // where the current primary already has ≥3% sim own.
    // v3.24.3: 16+ match slates use completely different logic. Run band
    // search for EACH trap separately (Biggest Trap AND Or Trap). Sort
    // opponents by VALUE descending, take top 2. Gem Primaries are the
    // band players paired with top 2. Or Pivot list uses same logic on
    // Or Trap band, displays opponent names.
    let primary = null;
    let primaries = [];        // v3.24.3: Hidden Gem primaries (band players from Biggest Trap band)
    let orPivotOpps = [];      // v3.24.3: Or Pivot opponents (from Or Trap band)

    // v3.24.9: Pre-compute top-3 PP fade opponent names so the gem
    // candidate pool can exclude them (tennis 16+ only). Mirrors the
    // same filter applied in computeContrarianCaps16Plus — keeps the
    // DK-tab display aligned with the OverOwned caps logic.
    const ppFadesForFilter = pw
      .filter(p => typeof p.ppLine === 'number' && typeof p.ppEdge === 'number' && p.ppEdge <= -2)
      .filter(p => p.name !== trap)
      .sort((a, b) => (a.ppEdge || 0) - (b.ppEdge || 0))
      .slice(0, 3);
    const ppFadeOppNames = new Set(ppFadesForFilter.map(f => f.opponent).filter(Boolean));

    // Helper: run band search for a trap, return top 2 {bp, opp} by opp.val
    const findGemPairs = (trapName) => {
      const trapP = pw.find(p => p.name === trapName);
      if (!trapP) return [];
      const trapSal = trapP.salary || 0;
      const inBand = (p, lo, hi) => {
        if (p.name === trapName) return false;
        const diff = (p.salary || 0) - trapSal;
        return diff >= lo && diff <= hi;
      };
      let band = pw.filter(p => p.salary > 0 && inBand(p, -1000, 300));
      if (band.length === 0) band = pw.filter(p => p.salary > 0 && inBand(p, -2500, 1000));
      // Exclude opponents that are themselves the other trap
      const otherTraps = new Set();
      if (trap) otherTraps.add(trap);
      if (orTrap) otherTraps.add(orTrap);
      otherTraps.delete(trapName);  // own trap is already excluded via inBand
      const pairs = band.map(bp => {
        // v3.24.9: exclude band player whose name is a top-3 PP fade's opp
        if ((mc || 0) >= 16 && ppFadeOppNames.has(bp.name)) return null;
        const opp = pw.find(p => p.name === bp.opponent);
        if (!opp) return null;
        if (otherTraps.has(opp.name)) return null;
        return { bp, opp, oppVal: opp.val || 0 };
      }).filter(Boolean).sort((a, b) => b.oppVal - a.oppVal);
      return pairs.slice(0, 2);
    };

    if ((mc || 0) >= 16) {
      // Biggest Trap band → Hidden Gem Primaries (display band-player names)
      const gemPairs = findGemPairs(trap);
      primaries = gemPairs.map((pair, i) => ({
        name: pair.bp.name,
        kind: 'opp-fade',
        fadedOpp: pair.opp.name,
        tier: (pair.bp.wp || 0) >= 0.30 ? 'standard' : 'hedged',
        wp: pair.bp.wp || 0,
        rank: i + 1,
      }));
      // Or Trap band → or Pivot (display opponent names)
      const orPivotPairs = findGemPairs(orTrap);
      orPivotOpps = orPivotPairs.map((pair, i) => ({
        name: pair.opp.name,
        bandPlayer: pair.bp.name,
        rank: i + 1,
      }));
      if (primaries.length > 0) primary = primaries[0];
    }
    // Fall through to default logic if no 16+ primary found (or slate < 16 matches)
    if (!primary && opponentQualifies && (opponent.simOwn || 0) >= 3) {
      primary = { name: opponent.name, kind: 'opponent', wp: opponent.wp };
    }
    if (!primary) {
      const winner = bandRanked.find(x => {
        const cand = pw.find(p => p.name === x.name);
        return (cand?.simOwn || 0) >= 3;
      });
      if (winner) primary = { name: winner.name, kind: 'value' };
    }

    // PIVOT — top PP LESS with ppEdge ≤ -2, excluding primary + trap
    const ppFades = pw
      .filter(p => typeof p.ppLine === 'number' && typeof p.ppEdge === 'number' && p.ppEdge <= -2)
      .filter(p => p.name !== trap)
      .filter(p => !primary || p.name !== primary.name)
      .sort((a, b) => (a.ppEdge || 0) - (b.ppEdge || 0))
      .slice(0, 2);

    let pivot = null;
    if (ppFades.length > 0) {
      const top = ppFades[0];
      pivot = { name: top.name, kind: 'pp-fade', ppEdge: top.ppEdge };
    } else {
      // Fallback: salary-band winner (excluding primary)
      const fallback = bandRanked.find(x => !primary || x.name !== primary.name);
      if (fallback) pivot = { name: fallback.name, kind: 'value' };
    }

    return { primary, primaries, orPivotOpps, pivot };
  }, [pw, trap, orTrap, mc]);
  const gemName = gem.primary?.name || '';
  const gemNames = (gem.primaries && gem.primaries.length > 0) ? gem.primaries.map(p => p.name) : (gemName ? [gemName] : []);
  const pivotName = gem.pivot?.name || '';
  const S = p => <SH {...p} sortKey={sortKey} sortDir={sortDir} onSort={toggleSort} />;
  return (<>
    <div className="metrics">
      <div className="metric"><div className="metric-label"><Icon name="trophy" size={13}/> Top Value</div><div className="metric-value">{t3v.map((n, i) => { const p = players.find(x => x.name === n); return <div key={i} style={{ fontSize: i === 0 ? '16px' : '13px', color: i === 0 ? undefined : 'var(--text-muted)' }}>{i + 1}. {n} <span style={{ fontSize: 11, color: 'var(--text-dim)' }}>{fmt(p?.val, 2)}</span></div>; })}</div></div>
      <div className="metric"><div className="metric-label"><Icon name="target" size={13}/> Top Straight Sets</div><div className="metric-value">{t3s.map((n, i) => { const p = players.find(x => x.name === n); return <div key={i} style={{ fontSize: i === 0 ? '16px' : '13px', color: i === 0 ? undefined : 'var(--text-muted)' }}>{n} <span style={{ fontSize: 11, color: 'var(--text-dim)' }}>{fmtPct(p?.pStraight)}</span></div>; })}</div></div>
      <div className="metric">
        <div className="metric-label"><Icon name="gem" size={13}/> Hidden Gem</div>
        {/* v3.24.1: 16+ slates show top 2 gem primaries stacked.
            ≤15 slates show single gem as before. */}
        {(mc || 0) >= 16 && gem.primaries && gem.primaries.length > 0 ? (
          <>
            <div className="metric-value" style={{ color: 'var(--green-text)' }}>
              {gem.primaries.map((p, i) => (
                <div key={i} style={{ fontSize: i === 0 ? '16px' : '13px', color: i === 0 ? 'var(--green-text)' : 'var(--text-muted)' }}>
                  {i + 1}. {p.name}
                </div>
              ))}
            </div>
          </>
        ) : (
          <>
            <div className="metric-value" style={{ color: 'var(--green-text)' }}>{gem.primary?.name || '-'}</div>
            <div className="metric-sub">
              {gem.primary?.kind === 'opp-fade'
                ? `Fade ${gem.primary.fadedOpp} chalk`
                : gem.primary?.kind === 'opponent'
                ? `Close matchup · ${fmtPct(gem.primary.wp)} win prob`
                : gem.primary?.kind === 'value'
                ? "Overlooked value in trap's price range"
                : 'Low ownership, high upside'}
            </div>
          </>
        )}
        {/* v3.24.3: 16+ slates display "or Pivot" (opponents from Or Trap band)
            and "PP Fade" (top 3 PP fades, previously labeled as "or pivot").
            On ≤15 slates, show single gem pivot as before. */}
        {(mc || 0) >= 16 && gem.orPivotOpps && gem.orPivotOpps.length > 0 && (
          <div style={{ marginTop: 6, paddingTop: 6, borderTop: '1px dashed var(--border)', fontSize: 11, color: 'var(--text-dim)' }}>
            or Pivot: <span style={{ color: 'var(--text-muted)' }}>{gem.orPivotOpps.map(x => x.bandPlayer).join(', ')}</span>
          </div>
        )}
        {(mc || 0) >= 16 && topPpFades.length > 0 ? (
          <div style={{ marginTop: 4, fontSize: 11, color: 'var(--text-dim)' }}>
            PP Fade: <span style={{ color: 'var(--text-muted)' }}>{topPpFades.join(', ')}</span>
          </div>
        ) : (mc || 0) < 16 && gem.pivot && (
          <div style={{ marginTop: 6, paddingTop: 6, borderTop: '1px dashed var(--border)', fontSize: 11, color: 'var(--text-dim)' }}>
            or pivot: <span style={{ color: 'var(--text-muted)' }}>{gem.pivot.name}</span> <span style={{ fontSize: 10 }}>({gem.pivot.kind})</span>
          </div>
        )}
      </div>
      <div className="metric">
        <div className="metric-label"><Icon name="bomb" size={13}/> Biggest Trap</div>
        <div className="metric-value" style={{ color: 'var(--red-text)' }}>{trap || '-'}</div>
        <div className="metric-sub">Who the field needs most</div>
        {/* v3.24: 16+ match slates show "or: X" for the second-worst trap. */}
        {(mc || 0) >= 16 && orTrap && (
          <div style={{ marginTop: 6, paddingTop: 6, borderTop: '1px dashed var(--border)', fontSize: 11, color: 'var(--text-dim)' }}>
            or: <span style={{ color: 'var(--red-text)' }}>{orTrap}</span>
          </div>
        )}
      </div>
    </div>
    <SearchBar value={q} onChange={setQ} placeholder="Search players, opponents" total={pw.length} filtered={pwFiltered.length} />
    <LockBar lockedPlayers={lockedPlayers} excludedPlayers={excludedPlayers} onToggleLock={onToggleLock} onToggleExclude={onToggleExclude} onClearLocks={onClearLocks} onClearExcludes={onClearExcludes} />
    <div className="table-wrap"><table><thead><tr>
      <th>#</th><th></th><S label="Player" colKey="name" /><th>Opp</th><S label="Sal" colKey="salary" num /><S label="Sim Own" colKey="simOwn" num tip="Projected field ownership (imported from Pool Own, or simulated if unavailable)" /><S label={<span style={{ display: 'inline-flex', flexDirection: 'column', alignItems: 'flex-end', lineHeight: 1.1 }}><span>Odds</span><span style={{ fontSize: 9, fontWeight: 500, opacity: 0.55, letterSpacing: '0.04em' }}>Kalshi · Market</span></span>} colKey="oddsProb" num tip="Match-winner implied probability. Primary source: Kalshi prediction market (no vig). Fallback: consensus sportsbook odds with vig removed." /><S label="Proj" colKey="proj" num /><S label="Val" colKey="val" num /><S label="P(2-0)" colKey="pStraight" num /><S label="GW" colKey="gw" num /><S label="GL" colKey="gl" num /><S label="SW" colKey="sw" num /><S label="Aces" colKey="aces" num /><S label="DFs" colKey="dfs" num /><S label="Breaks" colKey="breaks" num /><th>Time</th><th></th>
    </tr></thead>
    <tbody>{sorted.map((p, i) => {
      const iv = t3v.includes(p.name), is = t3s.includes(p.name);
      // v3.24.1: on 16+ slates gemNames is the top-2 array; ≤15 is single-name array.
      const ig = gemNames.includes(p.name);
      // v3.24: on 16+ slates, any of the top 3 PP fades gets the pivot badge
      // (not just #1), and orTrap shows as a trap.
      const isBig = (mc || 0) >= 16;
      const ip = isBig ? topPpFades.includes(p.name) : (p.name === pivotName && pivotName !== '');
      const it = p.name === trap || (isBig && orTrap && p.name === orTrap);
      // v3.24.3: or-pivot band players (boosted same as gem primaries)
      const orPivotBandNames = (isBig && gem.orPivotOpps) ? gem.orPivotOpps.map(x => x.bandPlayer) : [];
      const iop = orPivotBandNames.includes(p.name);
      const badges = [];
      if (iv) badges.push({ icon: 'trophy', label: 'Top 3 Value' });
      if (is) badges.push({ icon: 'target',  label: 'Top 3 Straight Sets' });
      if (ig) {
        // Determine specific gem label
        let gemLabel = 'Hidden Gem (value)';
        if (isBig && gem.primaries) {
          const primaryEntry = gem.primaries.find(x => x.name === p.name);
          if (primaryEntry) {
            gemLabel = `Hidden Gem #${primaryEntry.rank}`;
          }
        } else if (gem.primary?.kind === 'opp-fade') {
          gemLabel = 'Hidden Gem (chalk fade)';
        } else if (gem.primary?.kind === 'opponent') {
          gemLabel = 'Hidden Gem (opp)';
        }
        badges.push({ icon: 'gem', label: gemLabel });
      }
      if (iop) {
        const orPivotEntry = gem.orPivotOpps.find(x => x.bandPlayer === p.name);
        badges.push({ icon: 'gem', label: `Or Pivot #${orPivotEntry ? orPivotEntry.rank : ''}` });
      }
      if (ip) badges.push({ icon: 'gem',     label: isBig ? `Gem pivot #${topPpFades.indexOf(p.name) + 1}` : 'Gem pivot (value)' });
      if (it) badges.push({ icon: 'bomb',    label: isBig && p.name === orTrap ? 'Or Trap' : 'Trap' });
      const isOver = overrides && overrides[p.name] != null;
      return <tr key={p.name} className={ig ? 'row-hl-green' : iop ? 'row-hl-green' : ip ? 'row-hl-green' : it ? 'row-hl-red' : ''}>
        <td className="muted">{i + 1}</td>
        <td><span style={{ display: 'inline-flex', gap: 4, alignItems: 'center' }}>{badges.map((bd, j) => <Tip key={j} icon={bd.icon} label={bd.label} size={14} />)}</span></td>
        <td className="name">{p.name}</td><td className="muted">{p.opponent}</td>
        <td className="num">{fmtSal(p.salary)}</td>
        <td className="num" style={{ color: p.simOwn > 30 ? 'var(--amber)' : 'var(--text-muted)' }}>{fmt(p.simOwn, 1)}%</td>
        <td className="num"><OddsCell prob={p.oddsProb} source={p.oddsSource} fallbackWp={p.wp} /></td>
        <td className="num">
          <span className={iv ? 'cell-top3' : 'cell-proj'}>
            <input type="number" step="0.01" className={`proj-edit ${isOver ? 'overridden' : ''}`}
              value={isOver ? overrides[p.name] : (p.proj != null ? p.proj : '')}
              onChange={e => onOverride && onOverride(p.name, e.target.value)}
              onDoubleClick={() => onOverride && onOverride(p.name, null)}
              title={isOver ? 'Overridden — double-click to reset' : 'Click to edit projection'} />
          </span>
        </td>
        <td className="num"><span className={iv ? 'cell-top3' : ''}>{fmt(p.val, 2)}</span></td>
        <td className="num"><span className={is ? 'cell-top3' : ''}>{fmtPct(p.pStraight)}</span></td>
        <td className="num">{fmt(p.gw)}</td><td className="num muted">{fmt(p.gl)}</td><td className="num">{fmt(p.sw)}</td>
        <td className="num">{fmt(p.aces)}</td><td className="num muted">{fmt(p.dfs)}</td><td className="num">{fmt(p.breaks)}</td>
        <td className="muted">{fmtTime(p.startTime)}</td>
        <td style={{ textAlign: 'right', paddingRight: 10 }}><LockExcludeButtons name={p.name} isLocked={lockedPlayers.includes(p.name)} isExcluded={excludedPlayers.includes(p.name)} onToggleLock={onToggleLock} onToggleExclude={onToggleExclude} /></td>
      </tr>; })}</tbody></table></div>
  </>);
}

function PPTab({ rows }) {
  const [q, setQ] = useState('');
  const rowsFiltered = useMemo(() => rows.filter(r => matchesSearch(r, q, ['player', 'stat', 'opponent'])), [rows, q]);
  const { sorted, sortKey, sortDir, toggleSort } = useSort(rowsFiltered, 'ev', 'desc');
  const S = p => <SH {...p} sortKey={sortKey} sortDir={sortDir} onSort={toggleSort} />;
  const best = useMemo(() => [...rows].sort((a, b) => b.ev - a.ev).slice(0, 3), [rows]);
  const worst = useMemo(() => [...rows].sort((a, b) => a.ev - b.ev).slice(0, 3), [rows]);
  return (<>
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
        <div className="section-hero-sub">All plays sorted by edge · Edge = Projected − PP Line</div>
      </div>
    </div>
    <div style={{ background: 'rgba(245,197,24,0.06)', border: '1px solid rgba(245,197,24,0.25)', borderRadius: 8, padding: '10px 14px', marginBottom: 16, display: 'flex', alignItems: 'center', gap: 10, fontSize: 13 }}>
      <Icon name="trending-down" size={18} color="#F5C518"/>
      <span style={{ color: 'var(--text-muted)' }}><strong style={{ color: 'var(--primary)', fontWeight: 700 }}>Hint:</strong> PrizePicks bad value will typically reverse</span>
    </div>
    <div className="metrics">
      <div className="metric"><div className="metric-label"><Icon name="flame" size={13}/> Best Edge</div><div className="metric-value">{best.map((r, i) => <div key={i} style={{ fontSize: i === 0 ? '16px' : '13px', color: i === 0 ? 'var(--green-text)' : 'var(--text-muted)', fontWeight: i === 0 ? 700 : 500 }}>{r.player} · {r.stat} <span style={{fontSize:11, color: i === 0 ? undefined : 'var(--text-dim)'}}>{r.ev > 0 ? '+' : ''}{fmt(r.ev, 2)}</span>{r.mult && <span style={{fontSize:10,color: i === 0 ? 'var(--amber-text)' : 'var(--text-dim)',marginLeft:4}}>{r.mult}</span>}</div>)}</div></div>
      <div className="metric"><div className="metric-label"><Icon name="trending-down" size={13}/> Biggest "Fades"</div><div className="metric-value">{worst.map((r, i) => <div key={i} style={{ fontSize: i === 0 ? '16px' : '13px', color: i === 0 ? 'var(--red-text)' : 'var(--text-muted)', fontWeight: i === 0 ? 700 : 500 }}>{r.player} · {r.stat} <span style={{fontSize:11, color: i === 0 ? undefined : 'var(--text-dim)'}}>{fmt(r.ev, 2)}</span></div>)}</div></div>
    </div>
    <SearchBar value={q} onChange={setQ} placeholder="Search plays, players, stats" total={rows.length} filtered={rowsFiltered.length} />
    <div className="table-wrap"><table><thead><tr>
      <th>#</th><th></th><S label="Player" colKey="player" /><S label="Stat" colKey="stat" />
      <S label="PP Line" colKey="line" num /><S label="Projected" colKey="projected" num />
      <S label="Edge" colKey="ev" num /><S label="Play" colKey="direction" />
      <th>Mult</th><S label="Win%" colKey="wp" num /><S label="Opp" colKey="opponent" />
    </tr></thead>
    <tbody>{sorted.map((r, i) => {
      const isBest = best.some(t => t.player === r.player && t.stat === r.stat);
      const isWorst = worst.some(t => t.player === r.player && t.stat === r.stat);
      const playDir = r.direction;
      return <tr key={r.player + r.stat} className={isBest ? 'row-hl-green' : isWorst ? 'row-hl-red' : ''}>
        <td className="muted">{i+1}</td>
        <td>{isBest ? <Tip icon="flame" label="Best edge" /> : isWorst ? <Tip icon="trending-down" label="Fade" /> : ''}</td>
        <td className="name">{r.player}</td>
        <td style={{fontSize:11,color:'var(--text-muted)'}}>{r.stat}</td>
        <td className="num">{fmt(r.line, 1)}</td>
        <td className="num"><span className="cell-proj">{fmt(r.projected, 2)}</span></td>
        <td className="num"><span className={isBest ? 'cell-ev-top' : isWorst ? 'cell-ev-worst' : r.ev > 0 ? 'cell-ev-pos' : 'cell-ev-neg'}>{r.ev > 0 ? '+' : ''}{fmt(r.ev, 2)}</span></td>
        <td><span style={{color: playDir === 'MORE' ? 'var(--green-text)' : playDir === 'LESS' ? 'var(--red-text)' : 'var(--text-dim)', fontWeight:600}}>{playDir}</span></td>
        <td style={{color:'var(--amber-text)',fontSize:11}}>{r.mult || ''}</td>
        <td className="num muted">{fmtPct(r.wp)}</td>
        <td className="muted">{r.opponent}</td>
      </tr>;
    })}</tbody></table></div>
  </>);
}

// ═══════════════════════════════════════════════════════════════════════
// TENNIS OVEROWNED MODE — 16+ MATCH SLATE RULESET (v3.24.12)
// ═══════════════════════════════════════════════════════════════════════
// Completely isolated from the ≤15 match logic. Deep tennis slates have
// different structural dynamics — more chalk consolidation, more cheap
// dogs with real equity, more need for structural diversity — so they
// get their own rule pipeline. Do NOT mutate this without verifying
// ≤15 match slate behavior is unchanged.
//
// Exposure math convention: "X% leverage from simOwn" = multiplicative.
//   Negative leverage (fade): max = simOwn × (1 - X)
//   Positive leverage (boost): min = simOwn × (1 + X)
// Strength scaling: base spec is at strength 0.6. strengthFactor = strength/0.6.
// All percentages scale with strengthFactor; at strength 0 all contrarian
// rules become no-ops.
//
// Rule pipeline (order matters — later rules stack/override earlier ones):
//   (1)  Biggest Trap              -80% leverage from simOwn
//   (2)  Or Trap                   -75% leverage from simOwn
//   (3)  Hidden Gem Primary        min = 35% (Parks-style: cheap dog paired
//                                  with highest-owned chalk opponent in
//                                  trap's salary band) — v3.24.9: gem
//                                  candidates excluded from pool if they
//                                  are the opponent of a top-3 PP fade
//   (4)  Three PP Pivots           +100% / +90% / +80% leverage (top 3
//                                  PP fades, ppEdge ≤ -2). v3.24.11:
//                                  no max ceiling — user min overrides
//                                  are respected.
//   (5)  Pivot-Opponent Fade       opp of PP Pivot #1 capped at
//                                  simOwn × (1 - 0.5 × strengthFactor)
//   (6)  Extended PP Fade Floor    PP Pivots #2 & #3 get min = simOwn +
//                                  5pp × strengthFactor (floor-wins)
//   (7)  PP Fade Opponent Cap      opps of top 3 PP fades capped at
//                                  simOwn × (1 - 0.4 × strengthFactor),
//                                  only if opp has no existing signal
//   (7a) Gem Opp Fade              opps of Hidden Gem Primaries #1 & #2
//                                  capped at simOwn × (1 - 0.4 ×
//                                  strengthFactor) — closes the leak
//                                  where gem's opp (band chalk) was
//                                  falling through to Rule 11 (v3.24.10)
//   (8)  Top 3 Straight Sets       tiered fade by simOwn (v3.24.9): top
//                                  3 by pStraight, re-sorted simOwn
//                                  DESC, #1 max = simOwn × 0.20, #2 =
//                                  × 0.40, #3 = × 0.60 at base strength
//   (9)  Top 2 Val <$5,800 Opps    min = oppSimOwn × 1.5 (stacks
//                                  additively if opp already has signal)
//   (10) ≤$7,800 + >50% wp Fade    max = 5% + opp boost = oppSimOwn × 1.2
//                                  HARD OVERRIDE (unless PP Pivot — PP wins)
//   (11) Cheap Filler Hard Cap     (v3.24.8) tiered salary caps for
//                                  no-signal cheap plays:
//                                     • < $5,000      → max 4%
//                                     • $5,000-$5,699 → max 8%
//                                  Skips if player has any signal.
//   (12) Top 10 Val Tiered Cap     (v3.24.12) HARD OVERRIDE — sort by
//                                  val DESC, cap top 10 at tier max
//                                  (5 / 7 / 8 / 9 / 10 for #5-#10);
//                                  boost opps (if opp.val ≥ 5.50) to
//                                  tier min (50 / 40 / 37 / 35 / 30 for
//                                  #5-#10). Gems of any `_isGem` kind
//                                  bypass — otherwise wins over every
//                                  earlier rule.
function computeContrarianCaps16Plus(rp, ownership, contrarianStrength) {
  const withSal = rp.filter(p => p.salary > 0);
  if (withSal.length === 0) return {};
  const caps = {};
  const strengthFactor = contrarianStrength / 0.6;
  if (contrarianStrength === 0) return {};

  const own = (name) => ownership[name] || 0;
  // Clamp multiplicative scaling so stronger slider doesn't push caps negative.
  const scaledFadePct = (basePct) => Math.min(1, basePct * strengthFactor);
  const scaledBoostPct = (basePct) => basePct * strengthFactor;
  const roundInt = (x) => Math.max(0, Math.min(100, Math.round(x)));

  // ─────────────────────────────────────────────────────────────────
  // (1) BIGGEST TRAP — max (simOwn − wp×100), wp ≥ 30% gate
  // ─────────────────────────────────────────────────────────────────
  const trapActive = withSal.filter(p => (p.wp || 0) >= 0.30);
  let trap = null;
  if (trapActive.length > 0) {
    const scored = trapActive.map(p => ({
      p, score: own(p.name) - (p.wp || 0) * 100,
    })).sort((a, b) => b.score - a.score);
    trap = scored[0]?.p || null;
    if (trap && own(trap.name) === 0) {
      // No ownership data available — fall back to highest proj qualifier
      trap = [...trapActive].sort((a, b) => (b.proj || 0) - (a.proj || 0))[0] || null;
    }
  }
  if (trap) {
    const fieldOwn = own(trap.name);
    caps[trap.name] = {
      max: roundInt(fieldOwn * (1 - scaledFadePct(0.80))),
      _isTrap: true,
      _fieldOwn: Math.round(fieldOwn),
    };
  }

  // ─────────────────────────────────────────────────────────────────
  // (2) OR TRAP — #2 in the same (simOwn − wp×100) ranking, wp ≥ 30%
  // ─────────────────────────────────────────────────────────────────
  let orTrap = null;
  if (trapActive.length > 1 && trap) {
    const scored = trapActive
      .filter(p => p.name !== trap.name)
      .map(p => ({ p, score: own(p.name) - (p.wp || 0) * 100 }))
      .sort((a, b) => b.score - a.score);
    orTrap = scored[0]?.p || null;
  }
  if (orTrap) {
    const fieldOwn = own(orTrap.name);
    caps[orTrap.name] = {
      max: roundInt(fieldOwn * (1 - scaledFadePct(0.75))),
      _isOrTrap: true,
      _fieldOwn: Math.round(fieldOwn),
    };
  }

  // ─────────────────────────────────────────────────────────────────
  // (3) HIDDEN GEM PRIMARIES — (v3.24.3) run band search for EACH trap
  //     SEPARATELY, do NOT union bands. Sort band's opponents by VALUE
  //     descending, take top 2, the gem primaries are the band players
  //     paired with those top 2 opponents.
  //
  //     Biggest Trap band → Hidden Gem Primaries #1 and #2
  //     Or Trap band → "or Pivot" section (separate signal type)
  //
  //     Opponents excluded from the sort: any that are already a trap
  //     signal (Biggest Trap or Or Trap themselves).
  //     No 25% ownership gate. Salary of the trap doesn't matter —
  //     same logic regardless of whether trap is cheap or expensive.
  //
  //     EXPOSURE TIERING: tier the floor by the gem player's wp:
  //       • wp ≥ 30% → min = 35% (standard)
  //       • wp < 30% → min = 10% (hedged for longshots)
  //
  //     DISPLAY CONVENTION:
  //       Hidden Gem section displays band-player names (cheap dogs).
  //       or Pivot section displays opponent names (expensive pivots).
  //     Both come from the same formula, just different trap source.
  // ─────────────────────────────────────────────────────────────────
  // v3.24.9: Pre-compute PP fades BEFORE gem primaries so the gem
  // candidate pool can exclude any band player who is the opponent of a
  // top-3 PP fade. Without this, the gem primary could coincide with a
  // PP fade's opponent — creating a contradiction where Rule 3 boosts
  // that player (min 35%) while Rule 7 tries to fade them (max = simOwn
  // × 0.60). Rule 7 already skips if signal exists, so the previous
  // behavior silently dropped the Rule 7 leverage on that match. New
  // behavior: filter the gem candidate pool up front, letting Rule 7
  // fire against a non-gem opponent.
  const ppFades = withSal
    .filter(p => typeof p.ppLine === 'number' && typeof p.ppEdge === 'number' && p.ppEdge <= -2)
    .filter(p => !trap || p.name !== trap.name)
    .sort((a, b) => (a.ppEdge || 0) - (b.ppEdge || 0))
    .slice(0, 3);
  const ppFadeOppNames = new Set(ppFades.map(f => f.opponent).filter(Boolean));

  const findGemsForTrap = (trapObj) => {
    if (!trapObj) return [];
    const trapSal = trapObj.salary || 0;
    const inBand = (p, lo, hi) => {
      if (p.name === trapObj.name) return false;
      const diff = (p.salary || 0) - trapSal;
      return diff >= lo && diff <= hi;
    };
    let band = withSal.filter(p => inBand(p, -1000, 300));
    if (band.length === 0) band = withSal.filter(p => inBand(p, -2500, 1000));
    // For each band player, get their opponent. Build a candidate list
    // where each entry links a band player to their opponent, sorted by
    // opponent's val descending.
    const candidates = band.map(bp => {
      // v3.24.9: Exclude if this gem candidate is the opponent of a
      // top-3 PP fade — see ppFadeOppNames block above for rationale.
      if (ppFadeOppNames.has(bp.name)) return null;
      const opp = withSal.find(p => p.name === bp.opponent);
      if (!opp) return null;
      // Exclude if opp is already a trap signal
      const oppCap = caps[opp.name];
      if (oppCap && (oppCap._isTrap || oppCap._isOrTrap)) return null;
      return { bp, opp, oppVal: opp.val || 0 };
    }).filter(Boolean).sort((a, b) => b.oppVal - a.oppVal);
    return candidates.slice(0, 2);
  };

  // Hidden Gem Primaries — from Biggest Trap band
  const gemPrimaries = findGemsForTrap(trap);
  gemPrimaries.forEach(({ bp, opp }, rank) => {
    const fieldOwn = own(bp.name);
    const gemWp = bp.wp || 0;
    const basePct = gemWp >= 0.30 ? 35 : 10;
    const minVal = roundInt(basePct * strengthFactor);
    caps[bp.name] = {
      min: minVal,
      max: Math.min(100, minVal + 30),
      _isGem: true, _kind: 'primary',
      _gemPrimaryRank: rank + 1,
      _gemPrimaryTier: gemWp >= 0.30 ? 'standard' : 'hedged',
      _gemPrimaryFadedOpp: opp.name,
      _fieldOwn: Math.round(fieldOwn),
    };
  });

  // Or Pivots — from Or Trap band (same formula, displayed as opponent names)
  // The band players get boosted (same tier as gem primary). The opp names
  // are exposed via _orPivotOpp so the display layer can list them.
  const orPivotPairs = findGemsForTrap(orTrap);
  orPivotPairs.forEach(({ bp, opp }, rank) => {
    // Skip if the band player is already a gem primary from the other band
    if (caps[bp.name] && caps[bp.name]._isGem) return;
    const fieldOwn = own(bp.name);
    const gemWp = bp.wp || 0;
    const basePct = gemWp >= 0.30 ? 35 : 10;
    const minVal = roundInt(basePct * strengthFactor);
    caps[bp.name] = {
      min: minVal,
      max: Math.min(100, minVal + 30),
      _isGem: true, _kind: 'or-pivot',
      _orPivotRank: rank + 1,
      _orPivotTier: gemWp >= 0.30 ? 'standard' : 'hedged',
      _orPivotOpp: opp.name,  // The opponent name (for display)
      _fieldOwn: Math.round(fieldOwn),
    };
  });

  // ─────────────────────────────────────────────────────────────────
  // (4) THREE PP PIVOTS — top 3 PP LESS (ppEdge ≤ -2), excluding trap.
  //     Boost: #1 = +100%, #2 = +90%, #3 = +80% leverage from simOwn.
  //     v3.24.9: `ppFades` is computed earlier in the function (hoisted
  //     so Rule 3 can exclude their opponents from the gem pool).
  //     v3.24.11: REMOVED max ceiling (was boostedMin + 30). PP Pivots
  //     are our conviction plays — we want to run them as hard as the
  //     user asks. The ceiling was silently clobbering user min overrides
  //     (e.g. setting min 75% on a PP Pivot was being capped at ~50%
  //     because effMax = min(userMax, boostedMin+30) won the collision).
  //     Now PP Pivots set only a min floor; max defaults to 100%, and
  //     later cap-wins rules (Rule 8 straight sets, Rule 9 val cap,
  //     etc.) can still override if the player qualifies.
  // ─────────────────────────────────────────────────────────────────
  const pivotBoosts = [1.00, 0.90, 0.80];
  const gemPivots = [];
  ppFades.forEach((fade, idx) => {
    const fieldOwn = own(fade.name);
    const boostPct = scaledBoostPct(pivotBoosts[idx]);
    const boostedMin = roundInt(fieldOwn * (1 + boostPct));
    const existing = caps[fade.name] || {};
    const currMax = existing.max !== undefined ? existing.max : 100;
    // Preserve any existing max (from rare upstream rule), otherwise
    // omit the max key entirely so the combining logic defaults to 100.
    const nextCap = {
      ...existing,
      min: Math.min(boostedMin, currMax),
      _isGem: true, _kind: 'pivot',
      _ppPivotRank: idx + 1,
      _ppPivotEdge: fade.ppEdge || 0,
      _ppPivotLine: fade.ppLine || 0,
      _fieldOwn: existing._fieldOwn !== undefined ? existing._fieldOwn : Math.round(fieldOwn),
    };
    // Spread of `existing` already preserves existing.max if set.
    // If no existing max, delete the key so effMax defaults to 100 (no cap).
    if (existing.max === undefined) delete nextCap.max;
    caps[fade.name] = nextCap;
    gemPivots.push(fade);
  });

  // ─────────────────────────────────────────────────────────────────
  // (5) PIVOT-OPPONENT FADE — opponent of PP Pivot #1 only.
  //     Cap: simOwn × (1 - 0.5 × strengthFactor). Skip if opp has signal.
  // ─────────────────────────────────────────────────────────────────
  if (gemPivots.length > 0) {
    const pivotOpp = withSal.find(p => p.name === gemPivots[0].opponent);
    if (pivotOpp && !caps[pivotOpp.name]) {
      const oppFieldOwn = own(pivotOpp.name);
      const oppMax = roundInt(oppFieldOwn * (1 - scaledFadePct(0.50)));
      caps[pivotOpp.name] = {
        min: 0, max: oppMax,
        _isPivotOpponent: true,
        _fieldOwn: Math.round(oppFieldOwn),
      };
    }
  }

  // ─────────────────────────────────────────────────────────────────
  // (6) EXTENDED PP FADE FLOOR — PP Pivots #2 and #3.
  //     Existing pivot boost above already handles them, but ensure
  //     the floor simOwn + 5pp × strengthFactor is respected (floor-wins).
  // ─────────────────────────────────────────────────────────────────
  const extFloorPP = Math.round(5 * strengthFactor);
  [gemPivots[1], gemPivots[2]].forEach((fade, idx) => {
    if (!fade) return;
    const fieldOwn = own(fade.name);
    const existing = caps[fade.name] || {};
    const currMin = existing.min || 0;
    const currMax = existing.max !== undefined ? existing.max : 100;
    const simOwnFloor = roundInt(fieldOwn + extFloorPP);
    const newMin = Math.min(Math.max(currMin, simOwnFloor), currMax);
    caps[fade.name] = {
      ...existing,
      min: newMin,
      _isExtPpFade: true,
      _extPpFadeRank: idx + 2,
      _extPpFadeAddedMin: newMin - currMin,
    };
  });

  // ─────────────────────────────────────────────────────────────────
  // (7) PP FADE OPPONENT CAP — opponents of top 3 PP fades.
  //     Cap: simOwn × (1 - 0.4 × strengthFactor). Skip if opp has signal.
  // ─────────────────────────────────────────────────────────────────
  const fadeOppMult = scaledFadePct(0.40);
  gemPivots.forEach(fade => {
    const opp = withSal.find(p => p.name === fade.opponent);
    if (!opp) return;
    const existing = caps[opp.name] || {};
    const hasSignal = existing._isTrap || existing._isOrTrap || existing._isGem
                    || existing._isPivotOpponent;
    if (hasSignal) return;
    const oppFieldOwn = own(opp.name);
    const oppMax = roundInt(oppFieldOwn * (1 - fadeOppMult));
    const currMax = existing.max !== undefined ? existing.max : 100;
    caps[opp.name] = {
      ...existing,
      max: Math.min(currMax, oppMax),
      _isPpFadeOpponent: true,
      _fieldOwn: existing._fieldOwn !== undefined ? existing._fieldOwn : Math.round(oppFieldOwn),
    };
  });

  // ─────────────────────────────────────────────────────────────────
  // (7a) GEM OPP FADE (v3.24.10) — opponents of Hidden Gem Primaries.
  //      Cap: simOwn × (1 - 0.4 × strengthFactor), same strength as
  //      Rule 7. Skip if opp already has a signal so PP-related fades
  //      (Rule 5 at -0.50, Rule 7 at -0.40) always win on overlap.
  //
  //      Rationale: Rule 3 boosts the gem to min 35%, but the gem's
  //      opponent (the overbought chalk in the band) was otherwise
  //      falling through to Rule 11 (simOwn+10pp cap) and appearing
  //      in ~half of builds. Explicit fade closes that leak so the
  //      leverage Rule 3 was pricing in actually materializes.
  //      Applies to both Hidden Gem #1 and #2.
  // ─────────────────────────────────────────────────────────────────
  const gemOppFadeMult = scaledFadePct(0.40);
  gemPrimaries.forEach(({ opp }) => {
    if (!opp) return;
    const existing = caps[opp.name] || {};
    const hasSignal = existing._isTrap || existing._isOrTrap || existing._isGem
                    || existing._isPivotOpponent || existing._isPpFadeOpponent;
    if (hasSignal) return;
    const oppFieldOwn = own(opp.name);
    const oppMax = roundInt(oppFieldOwn * (1 - gemOppFadeMult));
    const currMax = existing.max !== undefined ? existing.max : 100;
    caps[opp.name] = {
      ...existing,
      max: Math.min(currMax, oppMax),
      _isGemOpponent: true,
      _fieldOwn: existing._fieldOwn !== undefined ? existing._fieldOwn : Math.round(oppFieldOwn),
    };
  });

  // ─────────────────────────────────────────────────────────────────
  // (8) TOP 3 STRAIGHT SETS — TIERED FADE (v3.24.9)
  //     Take top 3 by pStraight, re-sort those 3 by simOwn DESC, then
  //     apply graduated fade: #1 (most-owned) strongest, #3 lightest.
  //     At base strength 0.6:
  //       • #1  max = simOwn × 0.20   (e.g. 73% → ~15%)
  //       • #2  max = simOwn × 0.40
  //       • #3  max = simOwn × 0.60
  //     Scales with strengthFactor; at strength 0 these become simOwn
  //     × 1.0 (no-op). Cap-wins over any existing max.
  //     Replaces the old flat 10% cap, which fired hardest on the
  //     top straight-sets play regardless of how owned they actually
  //     were — the new rule concentrates leverage where the field
  //     over-commits while leaving lighter-owned straight-set plays
  //     with breathing room.
  // ─────────────────────────────────────────────────────────────────
  const topStraight = [...withSal]
    .sort((a, b) => (b.pStraight || 0) - (a.pStraight || 0))
    .slice(0, 3)
    .sort((a, b) => own(b.name) - own(a.name));
  const straightFadePcts = [0.80, 0.60, 0.40];  // #1 strongest → #3 lightest
  topStraight.forEach((p, idx) => {
    const existing = caps[p.name] || {};
    const currMax = existing.max !== undefined ? existing.max : 100;
    const fieldOwn = own(p.name);
    const newMax = roundInt(fieldOwn * (1 - scaledFadePct(straightFadePcts[idx])));
    caps[p.name] = {
      ...existing,
      max: Math.min(currMax, newMax),
      _isStraightSetsCap: true,
      _straightSetsRank: idx + 1,
      _fieldOwn: existing._fieldOwn !== undefined ? existing._fieldOwn : Math.round(fieldOwn),
    };
  });

  // ─────────────────────────────────────────────────────────────────
  // (9) TOP 3 VAL ≤$5,900 — CAP + OPP BOOST (v3.24.5)
  //     Find top 3 players by val with salary ≤ $5,900, EXCLUDING any
  //     player already flagged as a trap, gem primary, or or-pivot
  //     (traps and gems always have priority — their caps are the
  //     structural play, and Rule 9 shouldn't touch them).
  //
  //     • Cap the 3 value plays at max 1% (force the pivot to their opp)
  //     • Boost their opponents: min += oppSimOwn × 0.5 (additive stack)
  //
  //     Intent: cheap high-val dogs are where DK prices the "field
  //     trap" on the LOW side — the field happily rosters them at
  //     low ownership because they're salary-efficient. Capping forces
  //     our structural pivot UP to their opponent (the real leverage).
  // ─────────────────────────────────────────────────────────────────
  const topVal = withSal
    .filter(p => (p.salary || 0) <= 5900)
    .filter(p => {
      // Exclude traps, gems, and or-pivots from Rule 9 selection pool
      const c = caps[p.name];
      if (!c) return true;
      if (c._isTrap || c._isOrTrap) return false;
      if (c._isGem) return false;  // covers both _kind='primary' and _kind='or-pivot'
      return true;
    })
    .sort((a, b) => (b.val || 0) - (a.val || 0))
    .slice(0, 3);
  const valOppBoost = scaledBoostPct(0.50);
  topVal.forEach(valPlay => {
    // Cap the value play at max 1%
    const existing = caps[valPlay.name] || {};
    const valFieldOwn = own(valPlay.name);
    caps[valPlay.name] = {
      ...existing,
      min: 0,
      max: 1,
      _isValCap: true,
      _fieldOwn: existing._fieldOwn !== undefined ? existing._fieldOwn : Math.round(valFieldOwn),
    };
    // Boost the opponent
    const opp = withSal.find(p => p.name === valPlay.opponent);
    if (!opp) return;
    const oppExisting = caps[opp.name] || {};
    const oppFieldOwn = own(opp.name);
    const oppCurrMin = oppExisting.min || 0;
    const oppCurrMax = oppExisting.max !== undefined ? oppExisting.max : 100;
    const addedMin = Math.round(oppFieldOwn * valOppBoost);
    const newMin = Math.min(oppCurrMin + addedMin, oppCurrMax);
    caps[opp.name] = {
      ...oppExisting,
      min: Math.max(oppCurrMin, newMin),
      _isValOppBoost: true,
      _valOppBoostAddedMin: newMin - oppCurrMin,
      _fieldOwn: oppExisting._fieldOwn !== undefined ? oppExisting._fieldOwn : Math.round(oppFieldOwn),
    };
  });

  // ─────────────────────────────────────────────────────────────────
  // (10) ≤$7,800 + >50% WP FADE — hard override (except PP pivots).
  //      Target: max 5%. Opponent: min 10% flat floor (v3.24.6 — was
  //      oppSimOwn × 1.2 additive boost, simplified to flat floor).
  //      If target player is a PP Pivot (_isGem + _kind='pivot'), PP wins.
  // ─────────────────────────────────────────────────────────────────
  withSal.forEach(p => {
    if ((p.salary || 0) > 7800) return;
    if ((p.wp || 0) <= 0.50) return;
    const existing = caps[p.name] || {};
    // PP pivot wins over this hard-override
    if (existing._isGem && existing._kind === 'pivot') return;
    const fieldOwn = own(p.name);
    caps[p.name] = {
      // Fully replace (hard override), preserving only _fieldOwn for display
      min: 0,
      max: roundInt(5 * strengthFactor),
      _isHardFade: true,
      _fieldOwn: existing._fieldOwn !== undefined ? existing._fieldOwn : Math.round(fieldOwn),
    };
    // Opponent gets flat 10% min floor (floor-wins, cap-wins)
    const opp = withSal.find(x => x.name === p.opponent);
    if (opp) {
      const oppExisting = caps[opp.name] || {};
      const oppFieldOwn = own(opp.name);
      const oppCurrMin = oppExisting.min || 0;
      const oppCurrMax = oppExisting.max !== undefined ? oppExisting.max : 100;
      const flatFloor = roundInt(10 * strengthFactor);
      const oppNewMin = Math.min(Math.max(oppCurrMin, flatFloor), oppCurrMax);
      caps[opp.name] = {
        ...oppExisting,
        min: oppNewMin,
        _isHardFadeOpp: true,
        _hardFadeOppAddedMin: oppNewMin - oppCurrMin,
        _fieldOwn: oppExisting._fieldOwn !== undefined ? oppExisting._fieldOwn : Math.round(oppFieldOwn),
      };
    }
  });

  // Rule 11 (Strict Leverage Cap) DELETED in v3.24.5 — the simOwn+10pp
  // cap on no-signal players was interfering with the sim. No-signal
  // players now get the default max 100 (unrestricted).

  // ─────────────────────────────────────────────────────────────────
  // (11) CHEAP FILLER HARD CAP (v3.24.8) — tiered salary caps to
  //      prevent the optimizer from tunneling ultra-cheap filler
  //      plays into high exposure just because they fit salary
  //      geometry. Two tiers:
  //        • Salary < $5,000         → max 4%
  //        • Salary $5,000 – $5,699  → max 8%
  //        • Salary ≥ $5,700         → no cap from this rule
  //
  //      Skips if player has an active signal (Trap, Or Trap, Gem
  //      Primary, Or Pivot, PP Pivot) — traps/gems always have
  //      priority per established convention.
  // ─────────────────────────────────────────────────────────────────
  withSal.forEach(p => {
    const sal = p.salary || 0;
    if (sal >= 5700) return;
    const existing = caps[p.name] || {};
    // Skip if player has any active structural signal
    if (existing._isTrap || existing._isOrTrap) return;
    if (existing._isGem) return;  // covers primary, or-pivot, and pivot kinds
    const fieldOwn = own(p.name);
    // Tiered cap: under $5,000 → 4%, $5,000-$5,699 → 8%
    const baseCapPct = sal < 5000 ? 4 : 8;
    const hardCap = roundInt(baseCapPct * strengthFactor);
    const currMax = existing.max !== undefined ? existing.max : 100;
    const newMax = Math.min(currMax, hardCap);
    caps[p.name] = {
      ...existing,
      max: newMax,
      _isCheapCap: true,
      _cheapCapTier: sal < 5000 ? 'sub-5k' : 'sub-5.7k',
      _fieldOwn: existing._fieldOwn !== undefined ? existing._fieldOwn : Math.round(fieldOwn),
    };
  });

  // ─────────────────────────────────────────────────────────────────
  // (12) TOP 10 VAL TIERED CAP + OPP BOOST (v3.24.12) — HARD OVERRIDE.
  //      Runs LAST so it wins over every other rule except gems.
  //      Sort all players by val descending, take top 10. For each:
  //        • Cap the val play at tier max:
  //            #1 → 5%, #2 → 7%, #3 → 8%, #4 → 9%, #5-#10 → 10%
  //        • If opp.val ≥ 5.50, boost opp to tier min:
  //            #1 → 50%, #2 → 40%, #3 → 37%, #4 → 35%, #5-#10 → 30%
  //      If the val play OR the opp has any `_isGem` signal (primary,
  //      or-pivot, or PP pivot), skip — gems always win. If opp is
  //      also in the top 10, skip the opp boost (they'll get their
  //      own tier cap, avoiding a cyclic min/max contradiction).
  //      Cap-wins semantics on the val play max (stricter of existing
  //      or tier max). Floor-wins semantics on opp min (higher of
  //      existing or tier min); opp max raised to at least tier min
  //      to prevent contradiction.
  // ─────────────────────────────────────────────────────────────────
  const valMaxByRank = [5, 7, 8, 9, 10, 10, 10, 10, 10, 10];
  const oppMinByRank = [50, 40, 37, 35, 30, 30, 30, 30, 30, 30];
  const topVal10 = [...withSal]
    .sort((a, b) => (b.val || 0) - (a.val || 0))
    .slice(0, 10);
  const top10Names = new Set(topVal10.map(p => p.name));
  topVal10.forEach((valPlay, idx) => {
    const existing = caps[valPlay.name] || {};
    // Gem protection — skip this val play entirely (rule header)
    if (existing._isGem) return;
    const tierMax = valMaxByRank[idx];
    const existingMax = existing.max !== undefined ? existing.max : 100;
    const valFieldOwn = own(valPlay.name);
    caps[valPlay.name] = {
      ...existing,
      // Cap-wins: take stricter of existing and tier max.
      max: Math.min(existingMax, tierMax),
      // Ensure min doesn't exceed new max (would be infeasible).
      min: Math.min(existing.min || 0, tierMax),
      _isTop10Val: true,
      _top10ValRank: idx + 1,
      _fieldOwn: existing._fieldOwn !== undefined ? existing._fieldOwn : Math.round(valFieldOwn),
    };
    // Opp boost — gated by opp.val ≥ 5.50, not a gem, not already top-10.
    const opp = withSal.find(p => p.name === valPlay.opponent);
    if (!opp) return;
    if ((opp.val || 0) < 5.50) return;
    if (top10Names.has(opp.name)) return;  // avoid cyclic contradiction
    const oppExisting = caps[opp.name] || {};
    if (oppExisting._isGem) return;        // gem protection on opp
    const tierOppMin = oppMinByRank[idx];
    const oppCurrMin = oppExisting.min || 0;
    const oppCurrMax = oppExisting.max !== undefined ? oppExisting.max : 100;
    const newMin = Math.max(oppCurrMin, tierOppMin);
    const oppFieldOwn = own(opp.name);
    caps[opp.name] = {
      ...oppExisting,
      min: newMin,
      // Lift max to at least min so the range is feasible.
      max: Math.max(oppCurrMax, newMin),
      _isTop10ValOppBoost: true,
      _top10ValOppRank: idx + 1,
      _fieldOwn: oppExisting._fieldOwn !== undefined ? oppExisting._fieldOwn : Math.round(oppFieldOwn),
    };
  });

  return caps;
}

// ═══════════════════════════════════════════════════════════════════════
// TENNIS BUILDER — surgical additive change for contrarian mode
// ═══════════════════════════════════════════════════════════════════════
function BuilderTab({ players: rp, ownership, lockedPlayers = [], excludedPlayers = [], mc = 0, onGoToProjections }) {
  const [exp, setExp] = useState({}); const [res, setRes] = useState(null);
  const [prevRes, setPrevRes] = useState(null);              // v3.24.14: undo support for Rebuild
  const [isBuilding, setIsBuilding] = useState(false);       // v3.24.13: build animation gate
  const [nL, setNL] = useState(45);
  const [variance, setVariance] = useState(2);                // ±% jitter on projections per build — differentiates outputs between users
  const [globalMax, setGlobalMax] = useState(100); const [globalMin, setGlobalMin] = useState(0);
  const [poolQ, setPoolQ] = useState('');
  // Favorites — classic-only (tennis showdown intentionally skipped).
  // Stored as name-based tuples so they survive rebuilds.
  const [favoriteLineups, setFavoriteLineups] = useState([]);
  const favoriteKey = (fav) => [...(fav.players || [])].sort().join('|');
  const toggleFavoriteLineup = useCallback((lu, pData) => {
    // Ignore showdown lineups — favorites are classic-only for tennis
    if (lu.cpt !== undefined) return;
    const fav = { players: lu.players.map(i => pData[i].name), proj: lu.proj, sal: lu.sal };
    const key = favoriteKey(fav);
    setFavoriteLineups(prev => {
      const exists = prev.some(f => favoriteKey(f) === key);
      return exists ? prev.filter(f => favoriteKey(f) !== key) : [...prev, fav];
    });
  }, []);
  // NEW: contrarian state — OFF by default (behavior preserved when off)
  const [contrarianOn, setContrarianOn] = useState(false);
  const [contrarianStrength, setContrarianStrength] = useState(0.6);
  // NEW: value-aware contrarian — identical ref to rp when contrarian off (zero recompute)
  const avgVal = useMemo(() => {
    const vals = rp.filter(p => p.salary > 0).map(p => p.val || 0);
    return vals.length ? vals.reduce((s, v) => s + v, 0) / vals.length : 7;
  }, [rp]);
  // OVEROWNED MODE (TENNIS v3.19) — fully rewritten to mirror DK tab.
  // All signals (trap, gem, pivot) are computed using the exact same
  // formulas as the DK tab so the "Hidden Gem" / "Biggest Trap" displayed
  // in the DK tab ARE the players the Builder boosts / fades.
  //
  // Signals:
  //   (1) TRAP = max(simOwn − wp × 100) with wp ≥ 30% gate
  //       Cap:   max = fieldOwn − (strength × 35/0.6) pp
  //              → at base 0.6 = −35pp, at 1.0 = −58pp, at 0 = no cap
  //   (2) GEM PRIMARY = trap's opponent IF opponent wp ≥ 33.4%,
  //                     ELSE salary-band winner (val × proj × lev × upside)
  //       Boost: min = fieldOwn + (strength × 35/0.6) pp
  //              → at base 0.6 = +35pp, at 1.0 = +58pp
  //   (3) PIVOT = only when opponent qualifies for gem. Inherits the
  //       same +35pp leverage floor as primary gem.
  //   (4) PP BOOST = top 2 LESS edges (ppEdge ≤ -2), excluding primary
  //       trap. Stacks +15pp on top of whatever min the player already
  //       has (fresh if no prior signal). Cap-wins: clips to existing max.
  //   (5) GLOBAL FLOOR — all uncapped players get min 1-8% (scaled) so
  //       DK-salary tunneling is prevented.
  //   (6) LEVERAGE CAP — anyone ≥15% field gets max = field+30pp cap.
  const contrarianCaps = useMemo(() => {
    if (!contrarianOn) return {};
    // v3.24: 16+ match slates use a completely separate ruleset. Dispatch
    // early so none of the ≤15 match logic below executes on deep slates.
    // The new function is defined below with its own isolated rule pipeline.
    if (mc >= 16) {
      return computeContrarianCaps16Plus(rp, ownership, contrarianStrength);
    }
    const withSal = rp.filter(p => p.salary > 0);
    if (withSal.length === 0) return {};
    const caps = {};

    const LEV_CAP = 30;
    const levPP = Math.round(contrarianStrength * (35 / 0.6));  // 35pp @ 0.6, 58pp @ 1.0
    const globalFloor = Math.round(1 + contrarianStrength * 7);

    // (1) TRAP — mirror DK tab: simOwn − wp × 100, gate wp ≥ 30%.
    const trapActive = withSal.filter(p => (p.wp || 0) >= 0.30);
    const hasOwn = trapActive.some(p => (ownership[p.name] || 0) > 0);
    let trap = null;
    if (!hasOwn && trapActive.length > 0) {
      trap = [...trapActive].sort((a, b) => (b.proj || 0) - (a.proj || 0))[0];
    } else if (trapActive.length > 0) {
      const scored = trapActive.map(p => ({
        p, score: (ownership[p.name] || 0) - (p.wp || 0) * 100,
      })).sort((a, b) => b.score - a.score);
      trap = scored[0]?.p || null;
    }
    if (trap) {
      const fieldOwn = ownership[trap.name] || 0;
      caps[trap.name] = {
        max: Math.max(0, Math.round(fieldOwn - levPP)),
        _isTrap: true,
        _fieldOwn: Math.round(fieldOwn),
      };
    }

    // (2) + (3) GEM PRIMARY + PP-based PIVOT (v3.19) — mirror DK tab.
    //   Primary: opponent (if wp ≥ 33.4%) else salary-band winner.
    //   Pivot:   top PP LESS with ppEdge ≤ -2, excluding primary + trap.
    //            Falls back to salary-band winner if no PP data.
    let gemPrimary = null;
    let gemPivot   = null;
    if (trap) {
      const trapOwn = ownership[trap.name] || 0;
      const opponent = withSal.find(p => p.name === trap.opponent);
      const opponentQualifies = opponent && (opponent.wp || 0) >= 0.334;

      // Salary-band winner (same scoring as DK tab)
      const trapSal = trap.salary;
      const scoreBand = (lo, hi) => withSal.filter(p => {
        if (p.name === trap.name) return false;
        if (opponentQualifies && opponent && p.name === opponent.name) return false;
        const diff = p.salary - trapSal;
        return diff >= lo && diff <= hi;
      }).map(p => {
        const leverage = Math.max(0, trapOwn - (ownership[p.name] || 0));
        const levBoost = 1 + leverage * 0.012;
        const upsideBoost = 1 + (p.pStraight || 0.3) * 0.3;
        const score = (p.val || 0) * (p.proj || 0) * levBoost * upsideBoost;
        return { p, score };
      }).sort((a, b) => b.score - a.score);

      // Salary-band rankings — used for primary if opponent doesn't qualify
      // AND as pivot fallback when no PP data available.
      let bandRanked = scoreBand(-1000, 300);
      if (bandRanked.length === 0) bandRanked = scoreBand(-2500, 1000);
      if (bandRanked.length === 0) {
        const lev = withSal.filter(p => {
          if (p.name === trap.name) return false;
          if (opponentQualifies && opponent && p.name === opponent.name) return false;
          return (p.salary || 0) > 0;
        }).map(p => ({ p, score: (p.wp || 0) * 100 - (ownership[p.name] || 0) }))
          .sort((a, b) => b.score - a.score)
          .filter(x => x.score > 0);
        bandRanked = lev;
      }

      // Primary
      // v3.23: mirrors DK tab — ≥3% sim own gate on opponent path AND
      // bandRanked path. Opponent failing the gate falls through to bandRanked.
      if (opponentQualifies && (ownership[opponent.name] || 0) >= 3) {
        gemPrimary = opponent;
      }
      if (!gemPrimary) {
        gemPrimary = bandRanked.find(x => (ownership[x.p.name] || 0) >= 3)?.p || null;
      }

      // PIVOT — top PP LESS with ppEdge ≤ -2, excluding primary + trap
      const ppFades = withSal
        .filter(p => typeof p.ppLine === 'number' && typeof p.ppEdge === 'number' && p.ppEdge <= -2)
        .filter(p => p.name !== trap.name)
        .filter(p => !gemPrimary || p.name !== gemPrimary.name)
        .sort((a, b) => (a.ppEdge || 0) - (b.ppEdge || 0))
        .slice(0, 2);

      if (ppFades.length > 0) {
        gemPivot = ppFades[0];
      } else {
        // Fallback: salary-band winner (excluding primary)
        gemPivot = bandRanked.find(x => !gemPrimary || x.p.name !== gemPrimary.name)?.p || null;
      }
    }

    if (gemPrimary) {
      const fieldOwn = Math.round(ownership[gemPrimary.name] || 0);
      caps[gemPrimary.name] = {
        min: Math.min(95, fieldOwn + levPP),
        max: Math.min(100, fieldOwn + LEV_CAP + levPP),   // don't cap below the floor
        _isGem: true, _kind: 'primary',
        _fieldOwn: fieldOwn,
      };
    }
    if (gemPivot && !caps[gemPivot.name]) {
      const fieldOwn = Math.round(ownership[gemPivot.name] || 0);
      caps[gemPivot.name] = {
        min: Math.min(95, fieldOwn + levPP),
        max: Math.min(100, fieldOwn + LEV_CAP + levPP),
        _isGem: true, _kind: 'pivot',
        _fieldOwn: fieldOwn,
      };

    // PIVOT-OPPONENT FADE (v3.20) — the player opposite the gem pivot in their
    // match is a secondary trap: if the pivot hits, their opponent busts. Cap
    // at simOwn × (1 − multiplier) where multiplier = 0.50 at base strength
    // 0.6 (→ 0.83 at 1.0, 0 at 0). Safe to apply unconditionally — user
    // confirmed pivot's opponent is never already a trap or boosted gem.
    if (gemPivot) {
      const pivotOpponent = withSal.find(p => p.name === gemPivot.opponent);
      if (pivotOpponent && !caps[pivotOpponent.name]) {
        const oppFieldOwn = ownership[pivotOpponent.name] || 0;
        const fadeMultiplier = Math.min(1, contrarianStrength * (0.50 / 0.6));
        const oppMax = Math.max(0, Math.round(oppFieldOwn * (1 - fadeMultiplier)));
        caps[pivotOpponent.name] = {
          min: 0, max: oppMax,
          _isPivotOpponent: true,
          _fieldOwn: Math.round(oppFieldOwn),
        };
      }
    }
    }

    // (5) + (6) GLOBAL FLOOR + LEVERAGE CAP — everyone else
    withSal.forEach(p => {
      const fieldOwn = Math.round(ownership[p.name] || 0);
      if (caps[p.name]) return;  // already set by trap/gem/pivot
      const maxCap = fieldOwn >= 15 ? Math.min(95, fieldOwn + LEV_CAP) : 100;
      caps[p.name] = { min: globalFloor, max: maxCap, _isFloor: true };
    });

    // (4) PP BOOST — applied LAST so it stacks additively on top of any
    // signal the player may have (including gem primary or pivot). Top 2
    // LESS edges with ppEdge ≤ -2, excluding the primary trap.
    // Cap-wins: if boosted min would exceed existing max, clip to max.
    const ppFadeCandidates = withSal
      .filter(p => typeof p.ppLine === 'number' && typeof p.ppEdge === 'number' && p.ppEdge <= -2)
      .filter(p => !trap || p.name !== trap.name)
      .sort((a, b) => (a.ppEdge || 0) - (b.ppEdge || 0))
      .slice(0, 2);
    ppFadeCandidates.forEach((fade, idx) => {
      const existing = caps[fade.name] || {};
      const fieldOwn = Math.round(ownership[fade.name] || 0);
      const currMin = existing.min || 0;
      const currMax = existing.max !== undefined ? existing.max : 100;
      const newMin = Math.min(currMin + 15, currMax);   // +15pp, cap wins
      caps[fade.name] = {
        ...existing,
        min: newMin,
        max: existing.max !== undefined ? existing.max : Math.min(100, fieldOwn + LEV_CAP + 15),
        _isPpBoost: true,
        _ppBoostRank: idx + 1,
        _ppBoostEdge: fade.ppEdge || 0,
        _ppBoostLine: fade.ppLine || 0,
        _ppBoostOrigProj: fade.proj || 0,
        _fieldOwn: existing._fieldOwn !== undefined ? existing._fieldOwn : fieldOwn,
        _ppBoostAddedMin: newMin - currMin,
      };
    });

    // (4b) EXTENDED PP FADE FLOOR (v3.22) — on 10+ match slates, PP fades
    // ranked #2 and #3 get a min floor of simOwn + 5pp (conviction-play
    // insurance). #1 typically already has gem-pivot + PP-boost treatment
    // so this rule targets the next two to ensure they also land above field.
    // Baseline logic: min = max(currMin, simOwn + 5pp × strengthFactor).
    // Skipped if player is the primary trap (never boosted).
    if (mc >= 10) {
      const strengthFactor = contrarianStrength / 0.6;
      const extFloorPP = Math.round(5 * strengthFactor);
      const extFadeCandidates = withSal
        .filter(p => typeof p.ppLine === 'number' && typeof p.ppEdge === 'number' && p.ppEdge <= -2)
        .filter(p => !trap || p.name !== trap.name)
        .sort((a, b) => (a.ppEdge || 0) - (b.ppEdge || 0))
        .slice(1, 3);  // #2 and #3 in edge ranking
      extFadeCandidates.forEach((fade, idx) => {
        const existing = caps[fade.name] || {};
        const fieldOwn = Math.round(ownership[fade.name] || 0);
        const currMin = existing.min || 0;
        const currMax = existing.max !== undefined ? existing.max : 100;
        const simOwnFloor = Math.min(95, fieldOwn + extFloorPP);
        const newMin = Math.min(Math.max(currMin, simOwnFloor), currMax);
        caps[fade.name] = {
          ...existing,
          min: newMin,
          _isExtPpFade: true,
          _extPpFadeRank: idx + 2,   // fades 2 and 3
          _fieldOwn: existing._fieldOwn !== undefined ? existing._fieldOwn : fieldOwn,
          _extPpFadeAddedMin: newMin - currMin,
        };
      });
    }

    // (4c) PP FADE OPPONENT CAP (v3.22) — every tennis slate.
    // The opponents of the top 3 PP fades are "secondary traps" by proxy:
    // if our conviction plays hit, their opponents bust. Cap those opponents
    // at simOwn × (1 − multiplier) where multiplier = 0.40 at base 0.6
    // (→ 0.67 at 1.0, 0 at 0). Less aggressive than the gem pivot opponent
    // fade (0.50) since these are indirect signals.
    //
    // Rule gates:
    //   • exclude fades whose opponents already have a signal (trap/gem/
    //     pivot/pivot-opponent/pp-boost) — protect existing caps
    //   • apply only if the opponent exists on the slate
    {
      const fadeOppMultiplier = Math.min(1, contrarianStrength * (0.40 / 0.6));
      const topPpFades = withSal
        .filter(p => typeof p.ppLine === 'number' && typeof p.ppEdge === 'number' && p.ppEdge <= -2)
        .filter(p => !trap || p.name !== trap.name)
        .sort((a, b) => (a.ppEdge || 0) - (b.ppEdge || 0))
        .slice(0, 3);
      topPpFades.forEach(fade => {
        const opponent = withSal.find(p => p.name === fade.opponent);
        if (!opponent) return;
        const existing = caps[opponent.name] || {};
        // Skip if opponent already has any active signal (protect existing cap)
        const hasSignal = existing._isTrap || existing._isGem || existing._isPivotOpponent
                        || existing._isPpBoost || existing._isExtPpFade;
        if (hasSignal) return;
        const oppFieldOwn = ownership[opponent.name] || 0;
        const oppMax = Math.max(0, Math.round(oppFieldOwn * (1 - fadeOppMultiplier)));
        const currMax = existing.max !== undefined ? existing.max : 100;
        caps[opponent.name] = {
          ...existing,
          max: Math.min(currMax, oppMax),  // cap-wins
          _isPpFadeOpponent: true,
          _fieldOwn: existing._fieldOwn !== undefined ? existing._fieldOwn : Math.round(oppFieldOwn),
        };
      });
    }

    // (5) MID-TIER FADE + PIVOT (v3.21) — big-slate structural diversity rule.
    // Only fires on slates with 10+ matches (enough variance for the gamble).
    //   Fade pool:  salary ≤ $9,900, highest simOwn
    //   Pivot pool: salary $7,800-$9,900, highest simOwn (EXCLUDES the fade)
    //              → pivot must be meaningful mid-salary, not cheap filler
    //   #1 (Mid-Tier Fade):  max = round(simOwn × (1 - 0.50 × strengthFactor))
    //                        at base 0.6, simOwn × 0.50 cap (e.g., 45% → 22%)
    //   #2 (Mid-Tier Pivot): min = max(currMin, simOwn + 10pp × strengthFactor)
    //                        at base 0.6, +10pp above field (e.g., 36% → 46%)
    // STACKING: Pivot min takes the higher of existing min or simOwn+10pp.
    // strengthFactor: 0 when off, 1.0 at base 0.6, up to ~1.67 at max 1.0.
    // Excluded from selection: primary trap, gem primary, gem pivot, pivot-opponent.
    if (mc >= 10) {
      const strengthFactor = contrarianStrength / 0.6;
      const signalExcluded = (p) => {
        const c = caps[p.name];
        if (!c) return false;
        if (c._isTrap) return true;
        if (c._isGem) return true;           // primary + pivot
        if (c._isPivotOpponent) return true;
        return false;
      };
      // Fade pool: ≤ $9,900
      const fadePool = withSal
        .filter(p => (p.salary || 0) <= 9900)
        .filter(p => !signalExcluded(p))
        .sort((a, b) => (ownership[b.name] || 0) - (ownership[a.name] || 0));
      const midTierFade = fadePool[0];

      // Pivot pool: $7,800 - $9,900 (salary floor raised — the pivot must be
      // a meaningful mid-tier, not a sub-$7K filler). EXCLUDES the fade.
      const pivotPool = withSal
        .filter(p => (p.salary || 0) >= 7800 && (p.salary || 0) <= 9900)
        .filter(p => !signalExcluded(p))
        .filter(p => !midTierFade || p.name !== midTierFade.name)
        .sort((a, b) => (ownership[b.name] || 0) - (ownership[a.name] || 0));
      const midTierPivot = pivotPool[0];

      // #1: Mid-Tier Fade — cap at simOwn × (1 - 0.50 × strengthFactor)
      if (midTierFade) {
        const fieldOwn = Math.round(ownership[midTierFade.name] || 0);
        const fadeMultiplier = Math.min(1, 0.50 * strengthFactor);
        const newMax = Math.max(0, Math.round(fieldOwn * (1 - fadeMultiplier)));
        const existing = caps[midTierFade.name] || {};
        const currMax = existing.max !== undefined ? existing.max : 100;
        caps[midTierFade.name] = {
          ...existing,
          max: Math.min(currMax, newMax),  // cap-wins: take more restrictive
          _isMidTierFade: true,
          _fieldOwn: existing._fieldOwn !== undefined ? existing._fieldOwn : fieldOwn,
        };
      }

      // #2: Mid-Tier Pivot — min = simOwn + (10pp × strengthFactor).
      // Fix from v3.21 initial: previously stacked on currMin (which was
      // usually Global Floor's 5%), producing min 15 instead of 46.
      // New logic: baseline is simOwn itself. If the player already has a
      // higher min from another signal (PP Boost, etc.), we keep the higher.
      if (midTierPivot) {
        const fieldOwn = Math.round(ownership[midTierPivot.name] || 0);
        const boostPP = Math.round(10 * strengthFactor);
        const existing = caps[midTierPivot.name] || {};
        const currMin = existing.min || 0;
        const currMax = existing.max !== undefined ? existing.max : 100;
        const simOwnBoostedMin = Math.min(95, fieldOwn + boostPP);
        const boostedMin = Math.max(currMin, simOwnBoostedMin);
        const newMin = Math.min(boostedMin, currMax);  // cap-wins
        caps[midTierPivot.name] = {
          ...existing,
          min: newMin,
          _isMidTierPivot: true,
          _fieldOwn: existing._fieldOwn !== undefined ? existing._fieldOwn : fieldOwn,
          _midTierPivotAddedMin: newMin - currMin,
        };
      }
    }

    // (6) STRICT LEVERAGE CAP (v3.21) — tennis 10+ match slates only.
    // Any player WITHOUT an active signal gets capped at simOwn + 10pp max.
    // Prevents the cheap high-val tier (Montgomery, Begu, etc.) from running
    // to 50%+ exposure just because they fit salary geometry. Signal players
    // are excluded — their caps come from their own rule.
    //
    // "Signal" = any _is* flag set. Checked dynamically so future signals
    // are auto-included without code changes.
    if (mc >= 10) {
      withSal.forEach(p => {
        const fieldOwn = Math.round(ownership[p.name] || 0);
        const existing = caps[p.name] || {};
        // Skip if player has any active signal (trap, gem, pivot, pivot-opp,
        // pp-boost, mid-tier-fade, mid-tier-pivot, etc.)
        const hasSignal = existing._isTrap || existing._isGem || existing._isPivotOpponent
                        || existing._isPpBoost || existing._isMidTierFade || existing._isMidTierPivot
                        || existing._isExtPpFade || existing._isPpFadeOpponent;
        if (hasSignal) return;
        const strictMax = Math.min(95, fieldOwn + 10);
        const currMax = existing.max !== undefined ? existing.max : 100;
        const newMax = Math.min(currMax, strictMax);
        // Preserve existing min (Global Floor's 5%)
        caps[p.name] = {
          ...existing,
          max: newMax,
          _isStrictCap: true,
          _fieldOwn: existing._fieldOwn !== undefined ? existing._fieldOwn : fieldOwn,
        };
      });
    }

    return caps;
  }, [rp, ownership, contrarianOn, contrarianStrength, mc]);

  // Projections untouched when contrarian is on (caps do the work now)
  const adjRp = rp;

  const sp = useMemo(() => [...adjRp].filter(p => p.salary > 0).sort((a, b) => b.val - a.val), [adjRp]);
  const sE = (n, f, v) => setExp(p => ({ ...p, [n]: { ...p[n], [f]: v } }));
  const applyGlobal = () => { const ne = {}; sp.forEach(p => { ne[p.name] = { min: globalMin, max: globalMax, ...exp[p.name] }; }); setExp(ne); };
  const isShowdown = useMemo(() => sp.some(p => p.cpt_salary != null), [sp]);
  const run = () => {
    if (!canBuild || isBuilding) return;         // DK compliance gate — user must edit ≥2 projections
    // v3.24.13: surface build animation in the exposure-results slot.
    // CSS keyframes run on the compositor thread, so they stay smooth
    // even while `optimize()` blocks the main thread below.
    setIsBuilding(true);
    // v3.24.14: stash current res so the user can undo a Rebuild.
    if (res) setPrevRes(res);
    const start = performance.now();
    const finish = () => {
      const elapsed = performance.now() - start;
      const MIN_MS = 1400;                       // animation floor so fast slates don't flash
      setTimeout(() => setIsBuilding(false), Math.max(0, MIN_MS - elapsed));
    };
    // Yield two frames so the animation paints before we block on optimize.
    setTimeout(() => { try {
    // Variance jitter: each build applies a fresh ±variance% random multiplier to every player's projection.
    // Math.random() is unseeded, so two users clicking Build on the same slate get different rankings → different CSVs.
    const jitter = () => 1 + (Math.random() * 2 - 1) * variance / 100;
    // DK anti-abuse rule: submitted lineup CSVs can't be identical across entries. Even at variance=0
    // (deterministic builds), guarantee at least 2 players' projections differ from baseline by ≥0.01 —
    // enough to break ties in the optimizer without meaningfully altering rankings.
    const enforceMinNudge = (pd, basePd) => {
      const changed = pd.filter((p, i) => Math.abs(p.projection - basePd[i].proj) >= 0.01).length;
      if (changed >= 2) return;
      const idxs = [...Array(pd.length).keys()].sort(() => Math.random() - 0.5).slice(0, 2);
      idxs.forEach(i => {
        const sign = Math.random() < 0.5 ? -1 : 1;
        pd[i].projection = Math.round((pd[i].projection + sign * 0.01) * 1000) / 1000;
      });
    };
    if (isShowdown) {
      const pd = sp.map(p => {
        const cap = contrarianCaps[p.name] || {};
        const userSet = exp[p.name] || {};
        const userMin = userSet.min !== undefined ? userSet.min : globalMin;
        const userMax = userSet.max !== undefined ? userSet.max : globalMax;
        const effMin = Math.max(userMin, cap.min || 0);
        const effMax = Math.min(userMax, cap.max !== undefined ? cap.max : 100);
        return {
          name: p.name, projection: p.proj * jitter(), opponent: p.opponent,
          // Salary field kept for exposure table value calc (uses FLEX baseline)
          salary: p.flex_salary ?? p.salary, id: p.flex_id ?? p.id,
          cpt_salary: p.cpt_salary, acpt_salary: p.acpt_salary, flex_salary: p.flex_salary,
          cpt_id: p.cpt_id, acpt_id: p.acpt_id, flex_id: p.flex_id,
          maxExp: effMax, minExp: effMin,
        };
      });
      enforceMinNudge(pd, sp);
      const r = optimizeShowdown(pd, nL, 50000, 48000, { locked: new Set(lockedPlayers), excluded: new Set(excludedPlayers) });
      setRes({ ...r, pData: pd, isShowdown: true });
      return;
    }
    const pd = sp.map(p => {
      const cap = contrarianCaps[p.name] || {};
      const userSet = exp[p.name] || {};
      // Contrarian is MORE-RESTRICTIVE than user: cap.min lifts user.min, cap.max lowers user.max.
      // This way Apply Globals (which sets min:0/max:100 for all) can't silently override boosts/fades.
      const userMin = userSet.min !== undefined ? userSet.min : globalMin;
      const userMax = userSet.max !== undefined ? userSet.max : globalMax;
      const effMin = Math.max(userMin, cap.min || 0);
      const effMax = Math.min(userMax, cap.max !== undefined ? cap.max : 100);
      return { name: p.name, salary: p.salary, id: p.id, projection: p.proj * jitter(), opponent: p.opponent, maxExp: effMax, minExp: effMin };
    });
    enforceMinNudge(pd, sp);
    // v3.23: 18+ match tennis slates use a $49,200 minSalary floor (up from
    // $48,000). Forces meaningful cap utilization on deep slates where
    // cheap-filler lineups become structurally similar, improving lineup
    // diversity and paying a modest speed dividend from the smaller pool.
    // Threshold chosen empirically: cuts the valid-lineup pool ~62% on a
    // 24-match slate (1.36M → 517k) while preserving the top projection
    // (330.71 → 330.06, filler loss only). mc prop is slate match count.
    const minSal = mc >= 18 ? 49200 : 48000;
    const r = optimize(pd, nL, 50000, 6, minSal, { locked: new Set(lockedPlayers), excluded: new Set(excludedPlayers) });
    // Merge favorited classic lineups (remapped from name tuples to new indices).
    const favCls = [];
    const nameIdx = new Map(pd.map((p, i) => [p.name, i]));
    for (const fav of favoriteLineups) {
      const idxs = (fav.players || []).map(n => nameIdx.get(n));
      if (idxs.some(i => i === undefined)) continue;
      favCls.push({ players: idxs, proj: fav.proj, sal: fav.sal });
    }
    const favKeys = new Set(favCls.map(lu => [...lu.players].sort().join(',')));
    const deduped = r.lineups.filter(lu => !favKeys.has([...lu.players].sort().join(',')));
    const merged = [...favCls, ...deduped];
    const mergedCounts = new Array(pd.length).fill(0);
    for (const lu of merged) { lu.players.forEach(i => mergedCounts[i]++); }
    setRes({ ...r, lineups: merged, counts: mergedCounts, pData: pd, isShowdown: false });
    } finally { finish(); } }, 32);
  };
  const dl = (c, f) => { const b = new Blob([c], { type: 'text/csv' }); const a = document.createElement('a'); a.href = URL.createObjectURL(b); a.download = f; a.click(); URL.revokeObjectURL(a.href); };
  const exportDK = () => {
    if (!res) return;
    if (res.isShowdown) {
      let c = 'CPT,A-CPT,P\n';
      res.lineups.forEach(lu => {
        const cptP = res.pData[lu.cpt], acptP = res.pData[lu.acpt], flexP = res.pData[lu.flex];
        c += `${cptP.cpt_id},${acptP.acpt_id},${flexP.flex_id}\n`;
      });
      dl(c, 'dk_upload_showdown.csv');
      return;
    }
    let c = 'P,P,P,P,P,P\n';
    res.lineups.forEach(lu => { const ps = lu.players.map(i => res.pData[i]).sort((a, b) => b.salary - a.salary); c += ps.map(p => p.id).join(',') + '\n'; });
    dl(c, 'dk_upload.csv');
  };
  const exportReadable = () => {
    if (!res) return;
    if (res.isShowdown) {
      let c = 'Rank,Proj,Salary,CPT,A-CPT,FLEX\n';
      res.lineups.forEach((lu, i) => {
        const cptP = res.pData[lu.cpt], acptP = res.pData[lu.acpt], flexP = res.pData[lu.flex];
        c += `${i + 1},${lu.proj},${lu.sal},${cptP.name},${acptP.name},${flexP.name}\n`;
      });
      dl(c, 'lineups_showdown.csv');
      return;
    }
    let c = 'Rank,Proj,Salary,P1,P2,P3,P4,P5,P6\n';
    res.lineups.forEach((lu, i) => { const ps = lu.players.map(j => res.pData[j]).sort((a, b) => b.salary - a.salary); c += `${i + 1},${lu.proj},${lu.sal},${ps.map(p => p.name).join(',')}\n`; });
    dl(c, 'lineups.csv');
  };
  const exportProjections = () => { let c = 'Player,Salary,Win%,Proj,Value,GW,GL,SW,Aces,DFs,Breaks,P(2-0),Opp\n'; sp.forEach(p => { c += `${p.name},${p.salary},${(p.wp * 100).toFixed(0)}%,${p.proj},${p.val},${fmt(p.gw)},${fmt(p.gl)},${fmt(p.sw)},${fmt(p.aces)},${fmt(p.dfs)},${fmt(p.breaks)},${fmtPct(p.pStraight)},${p.opponent}\n`; }); dl(c, 'projections.csv'); };
  const overrideCount = useMemo(() => rp.filter(p => p._overridden).length, [rp]);
  const canBuild = overrideCount >= 2;
  useBuilderShortcuts({ run, canBuild, isBuilding }); // v3.24.14: B/R to build/rebuild
  // v3.24.14: empty state for malformed/empty slates. sp is the list of
  // players with salary>0 — if it's empty the builder panels render as
  // an empty ghost grid. This message makes the failure mode explicit.
  if (sp.length === 0) {
    return <>
      <div className="section-hero">
        <div className="section-hero-icon-wrap">
          <svg className="section-hero-icon" viewBox="0 0 24 24" fill="#F5C518" stroke="none">
            <path d="M13 2L4 14h7l-1 8 10-12h-7l1-8z"/>
          </svg>
        </div>
        <div className="section-hero-text">
          <h2 className="section-hero-title">Lineup Builder</h2>
          <div className="section-hero-sub">Set exposure %, build optimized lineups, export to DK</div>
        </div>
      </div>
      <div className="empty" style={{ padding: '60px 24px', background: 'var(--card)', border: '1px dashed var(--border-light)', borderRadius: 12 }}>
        <div style={{ fontSize: 42, marginBottom: 16, opacity: 0.4 }}>🎾</div>
        <h2 style={{ fontSize: 20, fontWeight: 700, color: 'var(--text)', marginBottom: 6 }}>No players on this slate yet</h2>
        <p style={{ color: 'var(--text-muted)', fontSize: 14, maxWidth: 420, margin: '0 auto' }}>
          The current slate has no priced players. Check back closer to lock, switch to a recent archive slate, or try a different sport.
        </p>
      </div>
    </>;
  }
  return (<>
    <div className="section-hero">
      <div className="section-hero-icon-wrap">
        <svg className="section-hero-icon" viewBox="0 0 24 24" fill="#F5C518" stroke="none">
          <path d="M13 2L4 14h7l-1 8 10-12h-7l1-8z"/>
        </svg>
      </div>
      <div className="section-hero-text">
        <h2 className="section-hero-title">Lineup Builder</h2>
        <div className="section-hero-sub">Set exposure %, build optimized lineups, export to DK</div>
      </div>
    </div>
    {!canBuild && (
      <div style={{ padding: '14px 18px', marginBottom: 16, background: 'rgba(245,197,24,0.08)', border: '1px solid rgba(245,197,24,0.35)', borderRadius: 10 }}>
        <div style={{ fontSize: 14, fontWeight: 700, color: 'var(--primary)', marginBottom: 4, display: 'flex', alignItems: 'center', gap: 6 }}><Icon name="warning" size={15} color="#F5C518"/> DraftKings Compliance Warning</div>
        <div style={{ fontSize: 13, color: 'var(--text-muted)', lineHeight: 1.5 }}>
          DraftKings policies require you to make some changes to our default projections before you can build lineups.
          Head to the <strong style={{ color: 'var(--text)' }}>DK Projections</strong> tab and edit at least <strong style={{ color: 'var(--primary)' }}>2 projections</strong> by any amount — this proves the lineups are your own work, not a shared export.
          <span style={{ color: 'var(--text-dim)' }}> Currently changed: <strong style={{ color: overrideCount >= 2 ? 'var(--green)' : 'var(--red)' }}>{overrideCount}</strong>/2</span>
        </div>
        {onGoToProjections && (
          <button
            onClick={onGoToProjections}
            className="oo-jump-btn"
            style={{
              marginTop: 10, padding: '7px 14px', fontSize: 12, fontWeight: 600,
              background: 'var(--primary)', color: '#0A1628', border: 'none',
              borderRadius: 6, cursor: 'pointer', display: 'inline-flex',
              alignItems: 'center', gap: 6
            }}
          >
            Jump to DK Projections
            <svg viewBox="0 0 24 24" width="12" height="12" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><path d="M5 12h14M13 6l6 6-6 6"/></svg>
          </button>
        )}
      </div>
    )}
    <ContrarianPanel enabled={contrarianOn} onToggle={setContrarianOn} strength={contrarianStrength} onStrengthChange={setContrarianStrength} />
    {contrarianOn && Object.keys(contrarianCaps).length > 0 && (() => {
      // OverOwned Mode display (v3.19/v3.24) — top 5 changes by |bound - fieldOwn|.
      const labelFor = (c) => {
        if (c._isTrap)   return { label: 'Trap',    icon: 'bomb',   color: 'var(--red)' };
        if (c._isOrTrap) return { label: 'Or Trap', icon: 'bomb',   color: 'var(--red)' };
        if (c._isGem && c._kind === 'primary') return { label: 'Gem',     icon: 'gem',    color: 'var(--green)' };
        if (c._isGem && c._kind === 'or-pivot') return { label: 'Or Pivot', icon: 'gem',   color: 'var(--green)' };
        if (c._isGem && c._kind === 'pivot')   return { label: `Pivot${c._ppPivotRank ? ' #' + c._ppPivotRank : ''}`, icon: 'gem', color: 'var(--green)' };
        if (c._isPivotOpponent) return { label: 'Pivot Opp', icon: 'bomb', color: 'var(--amber)' };
        if (c._isGemOpponent)   return { label: 'Gem Opp',   icon: 'bomb', color: 'var(--amber)' };
        if (c._isHardFade)      return { label: 'Hard Fade',  icon: 'bomb', color: 'var(--red)' };
        if (c._isHardFadeOpp)   return { label: 'Hard-Fade Opp Boost', icon: 'gem', color: 'var(--green)' };
        if (c._isValOppBoost)   return { label: 'Val-Opp Boost', icon: 'gem', color: 'var(--green)' };
        if (c._isValCap)        return { label: 'Val Cap', icon: 'bomb', color: 'var(--red)' };
        if (c._isTop10Val)      return { label: `Top Val #${c._top10ValRank || ''} Cap`, icon: 'bomb', color: 'var(--red)' };
        if (c._isTop10ValOppBoost) return { label: `Top Val #${c._top10ValOppRank || ''} Opp`, icon: 'gem', color: 'var(--green)' };
        if (c._isCheapCap)      return { label: 'Cheap Cap', icon: 'bomb', color: 'var(--amber)' };
        if (c._isStraightSetsCap) return { label: 'Straight-Sets Cap', icon: 'bomb', color: 'var(--amber)' };
        if (c._isPpFadeOpponent)  return { label: 'PP Fade Opp', icon: 'bomb', color: 'var(--amber)' };
        if (c._isMidTierFade)   return { label: 'Mid-Tier Fade', icon: 'bomb', color: 'var(--amber)' };
        if (c._isMidTierPivot)  return { label: 'Mid-Tier Pivot', icon: 'gem', color: 'var(--green)' };
        if (c._isPpBoost) return { label: 'PP Boost', icon: 'rocket', color: 'var(--primary)' };
        return null;
      };
      const describeChange = (c) => {
        if (c._isTrap || c._isOrTrap)   return { sym: `max ${c.max}%`, bound: c.max };
        if (c._isGem)    return { sym: `min ${c.min}%`, bound: c.min };
        if (c._isPivotOpponent) return { sym: `max ${c.max}%`, bound: c.max };
        if (c._isGemOpponent)   return { sym: `max ${c.max}%`, bound: c.max };
        if (c._isHardFade)      return { sym: `max ${c.max}%`, bound: c.max };
        if (c._isHardFadeOpp)   return { sym: `min ${c.min}% (+${c._hardFadeOppAddedMin || 0}pp)`, bound: c.min };
        if (c._isValOppBoost)   return { sym: `min ${c.min}% (+${c._valOppBoostAddedMin || 0}pp)`, bound: c.min };
        if (c._isValCap)        return { sym: `max ${c.max}%`, bound: c.max };
        if (c._isTop10Val)      return { sym: `max ${c.max}%`, bound: c.max };
        if (c._isTop10ValOppBoost) return { sym: `min ${c.min}%`, bound: c.min };
        if (c._isCheapCap)      return { sym: `max ${c.max}%`, bound: c.max };
        if (c._isStraightSetsCap) return { sym: `max ${c.max}%`, bound: c.max };
        if (c._isPpFadeOpponent)  return { sym: `max ${c.max}%`, bound: c.max };
        if (c._isMidTierFade)   return { sym: `max ${c.max}%`, bound: c.max };
        if (c._isMidTierPivot)  return { sym: `min ${c.min}% (+${c._midTierPivotAddedMin}pp stacked)`, bound: c.min };
        if (c._isPpBoost) return { sym: `+${c._ppBoostAddedMin}% min (stacked)`, bound: c.min };
        return { sym: '', bound: 0 };
      };
      const ranked = [];
      for (const [name, c] of Object.entries(contrarianCaps)) {
        const lbl = labelFor(c);
        if (!lbl) continue;
        const desc = describeChange(c);
        const field = ownership[name] || 0;
        const delta = desc.bound != null ? Math.abs(desc.bound - field) : 0;
        ranked.push({ name, lbl, desc, field, delta });
      }
      ranked.sort((a, b) => b.delta - a.delta);
      const top5 = ranked.slice(0, 5);
      const othersCount = ranked.length - top5.length;
      return (
        <div style={{ marginTop: -12, marginBottom: 16, padding: '10px 14px', background: 'rgba(245,197,24,0.06)', border: '1px solid rgba(245,197,24,0.2)', borderRadius: 8, fontSize: 12, color: 'var(--text-muted)', display: 'flex', gap: 16, flexWrap: 'wrap', alignItems: 'center' }}>
          <span style={{ color: 'var(--primary)', fontWeight: 700, textTransform: 'uppercase', fontSize: 10, letterSpacing: 0.5 }}>Top 5 Exposure Changes</span>
          {top5.map(({ name, lbl, desc, field }) => (
            <span key={name} title={signalTooltip(lbl.label)} style={{ display: 'inline-flex', alignItems: 'center', gap: 4, cursor: 'help' }}>
              <Icon name={lbl.icon} size={12} color={lbl.color}/>
              <span style={{ color: 'var(--text-dim)', fontSize: 11 }}>{lbl.label}:</span>
              <span style={{ color: lbl.color, fontWeight: 600 }}>{name}</span>
              <span style={{ color: 'var(--text-dim)' }}>· field {field.toFixed(1)}% →</span>
              <span style={{ color: 'var(--primary)', fontWeight: 600 }}>{desc.sym}</span>
            </span>
          ))}
          {othersCount > 0 && (
            <span style={{ color: 'var(--text-dim)', fontStyle: 'italic', fontSize: 11 }}>
              +{othersCount} more signals enforced
            </span>
          )}
        </div>
      );
    })()}
    <div style={{ display: 'flex', gap: 12, marginBottom: 16, flexWrap: 'wrap', alignItems: 'center' }}>
      <label style={{ fontSize: 13, color: 'var(--text-muted)' }}>Lineups: <input style={{ width: 60, background: 'var(--bg)', border: '1px solid var(--border)', borderRadius: 4, color: 'var(--text)', padding: '4px 8px', marginLeft: 4 }} type="number" min="1" step="1" value={nL} onChange={e => { const v = e.target.value; if (v === "") setNL(""); else setNL(Math.max(1, parseInt(v, 10) || 1)); }} onBlur={e => { if (e.target.value === "" || +e.target.value < 1) setNL(20); }} /></label>
      <label style={{ fontSize: 13, color: 'var(--text-muted)' }}>Global Min %: <input style={{ width: 50, background: 'var(--bg)', border: '1px solid var(--border)', borderRadius: 4, color: 'var(--text)', padding: '4px 8px', marginLeft: 4 }} type="number" min="0" max="100" step="1" value={globalMin} onChange={e => { const v = e.target.value; if (v === "") setGlobalMin(""); else setGlobalMin(Math.max(0, Math.min(100, parseInt(v, 10) || 0))); }} onBlur={e => { if (e.target.value === "") setGlobalMin(0); }} /></label>
      <label style={{ fontSize: 13, color: 'var(--text-muted)' }}>Global Max %: <input style={{ width: 50, background: 'var(--bg)', border: '1px solid var(--border)', borderRadius: 4, color: 'var(--text)', padding: '4px 8px', marginLeft: 4 }} type="number" min="0" max="100" step="1" value={globalMax} onChange={e => { const v = e.target.value; if (v === "") setGlobalMax(""); else setGlobalMax(Math.max(0, Math.min(100, parseInt(v, 10) || 0))); }} onBlur={e => { if (e.target.value === "") setGlobalMax(100); }} /></label>
      <label style={{ fontSize: 13, color: 'var(--text-muted)', display: 'flex', alignItems: 'center', gap: 6 }} title="Random ± shift applied to each player's projection per build. Ensures you and other users don't submit identical lineups on the same slate.">
        Variance
        <input type="range" min="0" max="25" step="1" value={variance} onChange={e => setVariance(+e.target.value)} style={{ width: 80, accentColor: 'var(--primary)' }} />
        <span style={{ fontWeight: 700, color: variance > 0 ? 'var(--primary)' : 'var(--text-dim)', minWidth: 28, textAlign: 'right' }}>{variance}%</span>
      </label>
      <button onClick={applyGlobal} style={{ background: 'var(--card)', border: '1px solid var(--border)', borderRadius: 6, color: 'var(--text-muted)', padding: '4px 12px', fontSize: 12, cursor: 'pointer' }}>Apply Global</button>
      <button onClick={exportProjections} style={{ background: 'var(--card)', border: '1px solid var(--border)', borderRadius: 6, color: 'var(--text-muted)', padding: '4px 12px', fontSize: 12, cursor: 'pointer', marginLeft: 'auto' }}><Icon name="download" size={12}/> Projections CSV</button>
    </div>
    <SearchBar value={poolQ} onChange={setPoolQ} placeholder="Search pool by player or opponent" total={sp.length} filtered={sp.filter(p => matchesSearch(p, poolQ)).length} />
    <div className="builder-controls">{sp.filter(p => matchesSearch(p, poolQ)).map(p => <div className="ctrl-row" key={p.name}><span className="ctrl-name" style={{ flex: '1 1 0', minWidth: 60, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{p.name}</span><span style={{ color: 'var(--text-dim)', fontSize: 11, width: 48, flexShrink: 0 }}>{fmtSal(p.salary)}</span><span className="ctrl-proj" style={{ flexShrink: 0, width: 38, textAlign: 'right' }}>{fmt(p.proj, 1)}</span><input type="number" min="0" max="100" step="1" value={exp[p.name]?.min ?? globalMin} onChange={e => { const v = e.target.value; if (v === '') sE(p.name, 'min', ''); else sE(p.name, 'min', Math.max(0, Math.min(100, parseInt(v, 10) || 0))); }} onBlur={e => { if (e.target.value === '') sE(p.name, 'min', globalMin); }} title="Min %" style={{ width: 32, flexShrink: 0 }} /><input type="number" min="0" max="100" step="1" value={exp[p.name]?.max ?? globalMax} onChange={e => { const v = e.target.value; if (v === '') sE(p.name, 'max', ''); else sE(p.name, 'max', Math.max(0, Math.min(100, parseInt(v, 10) || 0))); }} onBlur={e => { if (e.target.value === '') sE(p.name, 'max', globalMax); }} title="Max %" style={{ width: 32, flexShrink: 0 }} /></div>)}</div>
    <button className="btn btn-primary" onClick={run} disabled={!canBuild || isBuilding}
      title={canBuild ? '' : `Edit at least 2 projections on the DK Projections tab first (${overrideCount}/2 changed)`}
      style={(!canBuild || isBuilding) ? { opacity: 0.4, cursor: 'not-allowed' } : undefined}>
      <Icon name="bolt" size={14}/> Build {nL} {isShowdown ? 'Showdown' : ''} Lineups{contrarianOn ? ' (Contrarian)' : ''}
    </button>
    {isBuilding && <BuildAnimation count={nL} />}
    {!isBuilding && res && <ExposureResults res={res} ownership={ownership} onRebuild={run} onExportDK={exportDK} onExportReadable={exportReadable} nL={nL} canBuild={canBuild} overrideCount={overrideCount} favoriteLineups={favoriteLineups} onToggleFavorite={toggleFavoriteLineup} prevRes={prevRes} onRestorePrev={() => { setRes(prevRes); setPrevRes(null); }} />}
  </>);
}

function ExposureResults({ res, ownership, onRebuild, onExportDK, onExportReadable, nL, canBuild = true, overrideCount = 2, favoriteLineups = [], onToggleFavorite, prevRes, onRestorePrev }) {
  const [q, setQ] = useState('');
  // v3.24.14: MME-style metrics — lineup-set health stats that beginners
  // miss but power players optimize for. Median/stdev of cumulative
  // ownership quantifies how tightly clustered the set is around the
  // field consensus; unique-player count reveals if you're over-fitting
  // to the same 6-8 players across the full 45 lineups.
  const setMetrics = useMemo(() => {
    if (!res.lineups.length || !res.pData) return null;
    const cumOwns = res.lineups.map(lu =>
      lu.players.reduce((s, i) => s + (ownership[res.pData[i].name] || 0), 0)
    );
    const sorted = [...cumOwns].sort((a, b) => a - b);
    const mid = Math.floor(sorted.length / 2);
    const median = sorted.length % 2 === 0 ? (sorted[mid - 1] + sorted[mid]) / 2 : sorted[mid];
    const mean = cumOwns.reduce((s, v) => s + v, 0) / cumOwns.length;
    const variance = cumOwns.reduce((s, v) => s + (v - mean) ** 2, 0) / cumOwns.length;
    const stdev = Math.sqrt(variance);
    const uniqueCount = new Set(res.lineups.flatMap(lu => lu.players)).size;
    return { median, stdev, uniqueCount, mean };
  }, [res, ownership]);
  // Favorites + filter (classic-only; showdown branch below skips these controls)
  const favoriteKeySet = useMemo(() => {
    const s = new Set();
    for (const fav of favoriteLineups) s.add((fav.players || []).slice().sort().join('|'));
    return s;
  }, [favoriteLineups]);
  const lineupHash = (lu) => lu.players.map(i => res.pData[i].name).sort().join('|');
  const [lineupFilters, setLineupFilters] = useState([]);
  const toggleFilter = (name) => setLineupFilters(prev => {
    const ex = prev.findIndex(f => f.name === name);
    return ex >= 0 ? prev.filter((_, i) => i !== ex) : [...prev, { name }];
  });
  const clearFilters = () => setLineupFilters([]);
  const matchesFilters = (lu) => {
    if (lineupFilters.length === 0) return true;
    if (res.isShowdown) return true; // filters are classic-only
    const names = new Set(lu.players.map(i => res.pData[i].name));
    return lineupFilters.every(f => names.has(f.name));
  };
  const expData = useMemo(() => res.pData.map((p, i) => {
    const cnt = res.counts[i]; const pct = cnt / res.lineups.length * 100;
    const simOwn = ownership[p.name] || 0; const lev = Math.round((pct - simOwn) * 10) / 10;
    const val = p.projection / (p.salary / 1000);
    return { name: p.name, salary: p.salary, projection: p.projection, val, cnt, pct, simOwn, lev };
  }), [res, ownership]);
  const expFiltered = useMemo(() => expData.filter(p => matchesSearch(p, q)), [expData, q]);
  const avgSal = Math.round(res.lineups.reduce((s, lu) => s + lu.sal, 0) / res.lineups.length);
  const projMax = res.lineups.length ? Math.max(...res.lineups.map(lu => lu.proj)) : 0;
  const projMin = res.lineups.length ? Math.min(...res.lineups.map(lu => lu.proj)) : 0;
  const { sorted, sortKey, sortDir, toggleSort } = useSort(expFiltered, 'pct', 'desc');
  const S = p => <SH {...p} sortKey={sortKey} sortDir={sortDir} onSort={toggleSort} />;
  return (<>
    <div className="oo-summary-bar" style={{ marginTop: 20, padding: '10px 14px', background: 'var(--card)', border: '1px solid var(--border)', borderRadius: 8, fontSize: 13, color: 'var(--text-muted)', display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: '4px 12px' }}>
      <span><Icon name="check" size={14} color="#22C55E"/> Built <span style={{ color: 'var(--primary-glow)', fontWeight: 700 }}>{res.lineups.length}</span> lineups from {res.total.toLocaleString()} valid</span>
      <span className="oo-sum-sep" style={{ color: 'var(--text-dim)' }}>·</span>
      <span>Range: <span style={{ color: 'var(--green)' }}>{projMax}</span> → <span style={{ color: 'var(--text-dim)' }}>{projMin}</span></span>
      <span className="oo-sum-sep" style={{ color: 'var(--text-dim)' }}>·</span>
      <span>Avg Sal: <span style={{ color: 'var(--primary-glow)', fontWeight: 600 }}>${avgSal.toLocaleString()}</span></span>
      {setMetrics && <>
        <span className="oo-sum-sep" style={{ color: 'var(--text-dim)' }}>·</span>
        <span title="Median of sum-of-player-ownership across the lineup set. Lower = more contrarian field positioning.">Median Own: <span style={{ color: 'var(--text)', fontWeight: 600 }}>{setMetrics.median.toFixed(0)}%</span></span>
        <span className="oo-sum-sep" style={{ color: 'var(--text-dim)' }}>·</span>
        <span title="Standard deviation of cumulative ownership across the set. Higher = more diverse range of contrarian-to-chalky lineups.">Stdev: <span style={{ color: 'var(--text)', fontWeight: 600 }}>{setMetrics.stdev.toFixed(1)}</span></span>
        <span className="oo-sum-sep" style={{ color: 'var(--text-dim)' }}>·</span>
        <span title="Count of distinct players used across all lineups. Higher = broader exposure; lower = same core group recycled.">Unique: <span style={{ color: 'var(--text)', fontWeight: 600 }}>{setMetrics.uniqueCount}</span></span>
      </>}
    </div>
    <div className="oo-action-row" style={{ display: 'flex', gap: 10, marginTop: 10, flexWrap: 'wrap' }}>
      {onRebuild && <button className="btn btn-primary" onClick={onRebuild} disabled={!canBuild}
        title={canBuild ? '' : `Edit at least 2 projections first (${overrideCount}/2 changed)`}
        style={{ flex: '1 1 auto', width: 'auto', ...(canBuild ? {} : { opacity: 0.4, cursor: 'not-allowed' }) }}><Icon name="bolt" size={14}/> Rebuild {nL}</button>}
      {prevRes && onRestorePrev && <button
        onClick={onRestorePrev}
        className="oo-restore-btn"
        title="Restore the previous lineup set (undo Rebuild)"
        style={{ flex: '0 1 auto', padding: '10px 18px', fontSize: 13, fontWeight: 600,
          background: 'var(--card)', color: 'var(--text-muted)',
          border: '1px solid var(--border-light)', borderRadius: 8, cursor: 'pointer',
          display: 'inline-flex', alignItems: 'center', gap: 6 }}>
        <svg viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M3 7v6h6"/><path d="M21 17a9 9 0 0 0-15-6.7L3 13"/></svg>
        Restore previous
      </button>}
      {onExportDK && <button className="btn btn-primary" onClick={onExportDK} style={{ flex: '1 1 auto', width: 'auto', background: 'linear-gradient(135deg, #15803D, #22C55E)' }}><Icon name="download" size={14}/> Download DK Upload CSV</button>}
      {onExportReadable && <button className="btn btn-outline" onClick={onExportReadable} style={{ flex: '1 1 auto', width: 'auto', marginTop: 0 }}><Icon name="download" size={14}/> Readable CSV</button>}
    </div>
    <div className="section-head" style={{ marginTop: 20 }}><Icon name="chart" size={16} color="#F5C518"/> Exposure</div>
    <SearchBar value={q} onChange={setQ} placeholder="Search exposure" total={expData.length} filtered={expFiltered.length} />
    <div className="table-wrap" style={{ marginBottom: 20 }}><table><thead><tr>
      <S label="Player" colKey="name" /><S label="Salary" colKey="salary" num /><S label="Proj" colKey="projection" num /><S label="Value" colKey="val" num /><S label="Count" colKey="cnt" num /><S label="Exposure" colKey="pct" num /><S label="Sim Own" colKey="simOwn" num tip="Projected field ownership (imported from Pool Own, or simulated if unavailable)" /><S label="Leverage" colKey="lev" num />
      {!res.isShowdown && <th title="Filter displayed lineups by this player" style={{ textAlign: 'center' }}>Filter</th>}
    </tr></thead>
    <tbody>{sorted.map(p => {
      const filtered = lineupFilters.some(f => f.name === p.name);
      const btnStyle = (active) => ({ width: 24, height: 24, padding: 0, border: `1px solid ${active ? 'var(--primary)' : 'var(--border)'}`, background: active ? 'var(--primary)' : 'transparent', color: active ? '#0A1628' : 'var(--text-muted)', borderRadius: 4, cursor: 'pointer', fontSize: 11, fontWeight: 700, lineHeight: '22px' });
      return <tr key={p.name}>
        <td className="name">{p.name}</td><td className="num">${p.salary.toLocaleString()}</td><td className="num">{fmt(p.projection, 1)}</td><td className="num">{fmt(p.val, 2)}</td><td className="num">{p.cnt}</td>
        <td><span className="exp-bar-bg"><span className="exp-bar" style={{ width: Math.min(p.pct, 100) + '%' }} /></span>{fmt(p.pct, 1)}%</td>
        <td className="num muted">{fmt(p.simOwn, 1)}%</td>
        <td className="num"><span style={{ color: p.lev > 0 ? 'var(--green)' : p.lev < 0 ? 'var(--red)' : 'var(--text-dim)', fontWeight: Math.abs(p.lev) > 10 ? 700 : 400 }}>{p.lev > 0 ? '+' : ''}{fmt(p.lev, 1)}%</span></td>
        {!res.isShowdown && <td style={{ textAlign: 'center' }}><button style={btnStyle(filtered)} onClick={() => toggleFilter(p.name)} title={filtered ? 'Remove filter' : `Show only lineups containing ${p.name}`}>F</button></td>}
      </tr>;
    })}</tbody></table></div>

    {/* Filter chips — classic only. */}
    {!res.isShowdown && lineupFilters.length > 0 && (
      <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap', alignItems: 'center', padding: '8px 12px', background: 'var(--card)', border: '1px solid var(--border)', borderRadius: 8, marginBottom: 10 }}>
        <span style={{ fontSize: 11, fontWeight: 600, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: 0.5 }}>Filtering</span>
        {lineupFilters.map((f, i) => (
          <button key={i} onClick={() => toggleFilter(f.name)} style={{ padding: '3px 9px', background: 'transparent', border: '1px solid var(--primary)', color: 'var(--primary)', borderRadius: 999, fontSize: 11, fontWeight: 600, cursor: 'pointer', display: 'inline-flex', alignItems: 'center', gap: 5 }} title="Click to remove this filter">{f.name}<span style={{ opacity: 0.6 }}>✕</span></button>
        ))}
        <button onClick={clearFilters} style={{ marginLeft: 'auto', padding: '3px 9px', background: 'transparent', border: '1px solid var(--border)', color: 'var(--text-muted)', borderRadius: 4, fontSize: 11, fontWeight: 500, cursor: 'pointer' }}>Clear all</button>
      </div>
    )}

    <div className="section-head"><Icon name="target" size={16} color="#F5C518"/> Lineups{!res.isShowdown && lineupFilters.length > 0 && <span style={{ fontSize: 12, color: 'var(--text-dim)', fontWeight: 500, marginLeft: 8 }}>· {res.lineups.filter(matchesFilters).length} matching</span>}</div>
    <div className="lineup-grid">{res.lineups.filter(matchesFilters).slice(0, 30).map((lu, idx) => {
      if (res.isShowdown) {
        // Showdown: render in CPT → A-CPT → FLEX order with role badge + tier salary + multiplied projection
        const slots = [
          { role: 'CPT',   p: res.pData[lu.cpt],  sal: res.pData[lu.cpt].cpt_salary,   mult: 1.5,  color: '#F5C518' },
          { role: 'A-CPT', p: res.pData[lu.acpt], sal: res.pData[lu.acpt].acpt_salary, mult: 1.25, color: '#C084FC' },
          { role: 'FLEX',  p: res.pData[lu.flex], sal: res.pData[lu.flex].flex_salary, mult: 1.0,  color: 'var(--text-muted)' },
        ];
        return <div className="lu-card" key={idx}>
          <div className="lu-header"><span>#{idx + 1}</span><span className="lu-proj">{lu.proj} pts</span></div>
          {slots.map(s => <div className="lu-row" key={s.role}>
            <span style={{ fontSize: 10, fontWeight: 700, color: s.color, width: 44, flexShrink: 0, letterSpacing: 0.5 }}>{s.role}</span>
            <span className="lu-name">{s.p.name}</span>
            <span className="lu-opp">vs {s.p.opponent}</span>
            <span className="lu-sal">${s.sal.toLocaleString()}</span>
            <span className="lu-pts">{fmt(s.p.projection * s.mult, 1)}</span>
          </div>)}
          <div className="lu-footer"><span>${lu.sal.toLocaleString()}</span><span>{lu.proj}</span></div>
        </div>;
      }
      // Classic (with favorite star + gold border when favorited)
      const ps = lu.players.map(i => res.pData[i]).sort((a, b) => b.salary - a.salary);
      const isFav = favoriteKeySet.has(lineupHash(lu));
      return <div className="lu-card" key={idx} style={isFav ? { borderColor: '#F5C518', boxShadow: '0 0 0 1px #F5C518 inset, 0 2px 8px rgba(245,197,24,0.15)' } : undefined}>
        <div className="lu-header">
          <span style={{ display: 'inline-flex', alignItems: 'center', gap: 6 }}>
            <button
              onClick={() => onToggleFavorite && onToggleFavorite(lu, res.pData)}
              title={isFav ? 'Unfavorite — will no longer persist through rebuilds' : 'Favorite this lineup — it survives rebuilds and is always exported'}
              style={{ width: 20, height: 20, padding: 0, border: 'none', background: 'transparent', cursor: 'pointer', color: isFav ? '#F5C518' : 'var(--text-dim)', fontSize: 14, lineHeight: 1 }}>
              {isFav ? '★' : '☆'}
            </button>
            <span>#{idx + 1}</span>
          </span>
          <span className="lu-proj">{lu.proj} pts</span>
        </div>
        {ps.map(p => <div className="lu-row" key={p.name}><span className="lu-name">{p.name}</span><span className="lu-opp">vs {p.opponent}</span><span className="lu-sal">${p.salary.toLocaleString()}</span><span className="lu-pts">{fmt(p.projection, 1)}</span></div>)}
        <div className="lu-footer"><span>${lu.sal.toLocaleString()}</span><span>{lu.proj}</span></div>
      </div>;
    })}</div>
    {(() => { const vis = res.lineups.filter(matchesFilters).length; return vis > 30 && <div style={{ textAlign: 'center', color: 'var(--text-dim)', fontSize: 13, marginTop: 8 }}>+ {vis - 30} more{!res.isShowdown && lineupFilters.length > 0 && ' matching filters'}</div>; })()}
  </>);
}

function TrackRecordTab({ sport }) {
  const [data, setData] = useState(null);
  const [loadState, setLoadState] = useState('loading');
  useEffect(() => {
    setLoadState('loading');
    fetch(`/results/${sport}/aggregated.json`)
      .then(r => { if (!r.ok) throw new Error('no file'); return r.json(); })
      .then(j => { setData(j); setLoadState((j && j.slates_tracked > 0) ? 'loaded' : 'empty'); })
      .catch(() => setLoadState('empty'));
  }, [sport]);

  const hero = (
    <div className="section-hero">
      <div className="section-hero-icon-wrap">
        <svg className="section-hero-icon" viewBox="0 0 24 24" fill="none" stroke="#F5C518">
          <path d="M3 3v18h18"/>
          <path d="M7 15l4-5 4 3 6-8"/>
        </svg>
      </div>
      <div className="section-hero-text">
        <h2 className="section-hero-title">Track Record</h2>
        <div className="section-hero-sub">{loadState === 'loaded' ? `${data.slates_tracked} ${sport === 'tennis' ? 'tennis' : sport === 'mma' ? 'UFC' : sport === 'nba' ? 'NBA' : sport} slate${data.slates_tracked === 1 ? '' : 's'} tracked · sorted by profitability` : 'How each tag has performed across completed slates'}</div>
      </div>
    </div>
  );

  if (loadState === 'loading') {
    return <>{hero}<div style={{ padding: '40px 20px', textAlign: 'center', color: 'var(--text-dim)', fontSize: 13 }}>Loading track record...</div></>;
  }

  if (loadState === 'empty') {
    return (<>
      {hero}
      <div style={{ padding: '48px 24px', textAlign: 'center', background: 'var(--card)', border: '1px dashed var(--border)', borderRadius: 10 }}>
        <div style={{ display: 'inline-flex', marginBottom: 14, padding: 14, background: 'rgba(245,197,24,0.06)', border: '1px solid rgba(245,197,24,0.2)', borderRadius: '50%' }}>
          <Icon name="chart-line" size={28} color="#F5C518"/>
        </div>
        <div style={{ fontSize: 15, fontWeight: 600, color: 'var(--text)', marginBottom: 8 }}>No completed slates yet</div>
        <div style={{ fontSize: 13, maxWidth: 520, margin: '0 auto', lineHeight: 1.65, color: 'var(--text-muted)' }}>
          Track Record populates as you upload DK contest CSVs from completed slates. Every <strong style={{color:'var(--primary)'}}>Top Value</strong>, <strong style={{color:'var(--primary)'}}>Hidden Gem</strong>, <strong style={{color:'var(--primary)'}}>Biggest Trap</strong>, and <strong style={{color:'var(--primary)'}}>PP Edge/Fade</strong> call gets graded — hit rates compound into actionable patterns over time.
        </div>
        <div style={{ fontSize: 11, color: 'var(--text-dim)', marginTop: 18, fontFamily: 'monospace' }}>Expected path: <span style={{color:'var(--text-muted)'}}>public/results/{sport}/aggregated.json</span></div>
      </div>
    </>);
  }

  // LOADED — render categories table + insight
  const rateFor = c => {
    if (c.type === 'bust_rate') return c.bust_rate;
    if (c.type === 'reversal') return Math.max(c.hit_rate || 0, c.reverse_rate || 0);
    if (c.type === 'hit_rate_dual') return c.primary_hit_rate ?? c.hit_rate ?? 0;
    return c.hit_rate ?? 0;
  };
  const sorted = [...(data.categories || [])].sort((a, b) => rateFor(b) - rateFor(a));
  // v3.24.14: hide the Edge column entirely when every category reports
  // edge=0 or null. Showing "+0.00" on every row reads as broken, not as
  // "not yet computed". When real values exist, the column appears.
  const hasAnyEdge = sorted.some(c => {
    const e = c.counter_edge != null ? c.counter_edge : c.avg_edge;
    return typeof e === 'number' && e !== 0;
  });
  const sigColor = s => s === 'follow' ? 'var(--green-text)' : s === 'fade' ? 'var(--red-text)' : s === 'counter' ? 'var(--amber-text)' : 'var(--text-muted)';
  const sigBg = s => s === 'follow' ? 'rgba(74,222,128,0.12)' : s === 'fade' ? 'rgba(248,113,113,0.12)' : s === 'counter' ? 'rgba(251,191,36,0.12)' : 'rgba(156,163,175,0.1)';
  const sigLabel = s => s === 'follow' ? 'FOLLOW' : s === 'fade' ? 'FADE' : s === 'counter' ? 'COUNTER' : 'NEUTRAL';

  return (<>
    {hero}
    {data.data_status === 'preview' && (
      <div style={{ padding: '10px 14px', background: 'rgba(245,197,24,0.08)', border: '1px solid rgba(245,197,24,0.3)', borderRadius: 8, marginBottom: 16, fontSize: 12, color: 'var(--text-muted)', display: 'flex', alignItems: 'center', gap: 10 }}>
        <Icon name="warning" size={14} color="#F5C518"/>
        <span><strong style={{ color: 'var(--primary)' }}>Preview data</strong> — this is illustrative. Real tracking begins when you ship <code style={{ background: 'var(--bg)', padding: '2px 6px', borderRadius: 3, fontSize: 11 }}>public/results/{sport}/aggregated.json</code> with actual results.</span>
      </div>
    )}
    {data.featured_insight && (
      <div style={{ padding: '16px 18px', background: 'linear-gradient(135deg, rgba(245,197,24,0.1), rgba(245,197,24,0.02))', border: '1px solid rgba(245,197,24,0.35)', borderRadius: 10, marginBottom: 16 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 8 }}>
          <Icon name="flame" size={18} color="#F5C518"/>
          <span style={{ fontSize: 10, fontWeight: 700, letterSpacing: '0.12em', textTransform: 'uppercase', color: '#F5C518' }}>Pattern of Note</span>
        </div>
        <div style={{ fontSize: 16, fontWeight: 700, color: '#F8FAFC', marginBottom: 6, letterSpacing: '-0.01em', lineHeight: 1.3 }}>{data.featured_insight.headline}</div>
        <div style={{ fontSize: 13, color: 'var(--text-muted)', lineHeight: 1.55 }}>{data.featured_insight.detail}</div>
      </div>
    )}
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th style={{ width: 28 }}></th>
            <th>Tag</th>
            <th className="num">N</th>
            <th className="num">Rate</th>
            {hasAnyEdge && <th className="num">Edge</th>}
            <th>Signal</th>
          </tr>
        </thead>
        <tbody>
          {sorted.map(cat => {
            const primary = rateFor(cat);
            const rateLbl = cat.type === 'bust_rate' ? 'bust' : 'hit';
            const edge = cat.counter_edge != null ? cat.counter_edge : cat.avg_edge;
            const edgeLbl = cat.counter_edge != null ? `${cat.edge_units} (counter-play)` : cat.edge_units;
            return (
              <tr key={cat.key}>
                <td><Icon name={cat.icon} size={16} color="#F5C518"/></td>
                <td className="name">
                  <div>{cat.label}</div>
                  <div style={{ fontSize: 11, color: 'var(--text-dim)', fontWeight: 400, marginTop: 2 }}>{cat.description}</div>
                </td>
                <td className="num muted">{cat.n}</td>
                <td className="num">
                  <div style={{ fontWeight: 700, color: primary >= 0.55 ? 'var(--green-text)' : primary <= 0.45 ? 'var(--red-text)' : 'var(--text)' }}>
                    {(primary * 100).toFixed(1)}% <span style={{ fontSize: 10, color: 'var(--text-dim)', fontWeight: 400 }}>{rateLbl}</span>
                  </div>
                  {cat.type === 'reversal' && (
                    <div style={{ fontSize: 10, color: 'var(--amber-text)', marginTop: 2, fontWeight: 600 }}>
                      reverses {(cat.reverse_rate * 100).toFixed(0)}%
                    </div>
                  )}
                  {cat.type === 'hit_rate_dual' && (
                    <div style={{ fontSize: 10, color: 'var(--text-dim)', marginTop: 2, fontWeight: 500 }}>
                      pivot {((cat.pivot_hit_rate || 0) * 100).toFixed(0)}% · both {((cat.both_in_winner_rate || 0) * 100).toFixed(0)}%
                    </div>
                  )}
                </td>
                {hasAnyEdge && <td className="num">
                  <span style={{ fontWeight: 600, color: edge > 0 ? 'var(--green-text)' : edge < 0 ? 'var(--red-text)' : 'var(--text-muted)' }}>
                    {edge > 0 ? '+' : ''}{edge.toFixed(Math.abs(edge) < 1 ? 2 : 1)}
                  </span>
                  <div style={{ fontSize: 10, color: 'var(--text-dim)', fontWeight: 400, marginTop: 2 }}>{edgeLbl}</div>
                </td>}
                <td>
                  <span style={{ display: 'inline-block', padding: '3px 10px', borderRadius: 4, fontSize: 10, fontWeight: 700, letterSpacing: '0.05em', color: sigColor(cat.signal), background: sigBg(cat.signal), border: `1px solid ${sigColor(cat.signal)}` }}>
                    {sigLabel(cat.signal)}
                  </span>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  </>);
}

// v3.24.14: polished file-drop zone for Live Leverage. Supports click-
// to-browse AND drag-and-drop, shows clear validation state, and
// previews the uploaded filename + count. Replaces the bare
// <input type="file"> that was dropping quality vs. the rest of the app.
function CsvDropZone({ label, hint, onFile, filled, count, countLabel = 'players', sampleHref, sampleFilename }) {
  const inputRef = useRef(null);
  const [dragOver, setDragOver] = useState(false);
  // v3.24.15: detect coarse-pointer devices so the prompt says "Tap" not
  // "Click" and doesn't promise drag-and-drop that touchscreens can't do.
  const isTouch = typeof window !== 'undefined' && window.matchMedia && window.matchMedia('(hover: none)').matches;
  const handleFiles = (files) => {
    if (!files || !files.length) return;
    const f = files[0];
    if (!f.name.toLowerCase().endsWith('.csv')) return; // silent reject; parser will report
    // Synthesize an event-shape the existing parsers expect
    onFile({ target: { files: [f] } });
  };
  const onDrop = (e) => {
    e.preventDefault(); e.stopPropagation();
    setDragOver(false);
    handleFiles(e.dataTransfer.files);
  };
  return (
    <div
      onClick={() => inputRef.current?.click()}
      onDragOver={(e) => { e.preventDefault(); e.stopPropagation(); setDragOver(true); }}
      onDragLeave={(e) => { e.preventDefault(); e.stopPropagation(); setDragOver(false); }}
      onDrop={onDrop}
      style={{
        flex: '1 1 240px', minWidth: 200, minHeight: 90,
        background: filled ? 'rgba(74,222,128,0.06)' : dragOver ? 'rgba(245,197,24,0.10)' : 'var(--card)',
        border: `1px ${dragOver ? 'solid' : 'dashed'} ${filled ? 'var(--green)' : dragOver ? 'var(--primary)' : 'var(--border-light)'}`,
        borderRadius: 10, padding: '16px 18px', cursor: 'pointer',
        transition: 'background 0.15s, border-color 0.15s',
        display: 'flex', flexDirection: 'column', gap: 6,
        WebkitTapHighlightColor: 'transparent',
      }}
    >
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 8, flexWrap: 'wrap' }}>
        <div className="metric-label" style={{ color: filled ? 'var(--green)' : 'var(--text-muted)' }}>{label}</div>
        {sampleHref && <a
          href={sampleHref}
          download={sampleFilename}
          onClick={(e) => e.stopPropagation()}
          style={{ fontSize: 10, color: 'var(--text-dim)', textDecoration: 'underline', textDecorationColor: 'var(--border-light)' }}
        >Sample CSV</a>}
      </div>
      <div style={{ fontSize: 12, color: 'var(--text-muted)', marginTop: 2 }}>{hint}</div>
      {!filled && (
        <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginTop: 6, fontSize: 12, color: dragOver ? 'var(--primary)' : 'var(--text-dim)', fontWeight: 500 }}>
          <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/></svg>
          {dragOver ? 'Drop to upload' : isTouch ? 'Tap to choose a CSV file' : 'Click to choose a file or drag & drop'}
        </div>
      )}
      {filled && <div style={{ color: 'var(--green)', fontSize: 12, marginTop: 4, display: 'flex', alignItems: 'center', gap: 4 }}>
        <Icon name="check" size={12} color="var(--green)"/> {count} {countLabel}
      </div>}
      <input ref={inputRef} type="file" accept=".csv" onChange={onFile} style={{ display: 'none' }} />
    </div>
  );
}

function LeverageTab({ players: rp }) {
  const [cd, setCd] = useState(null); const [ul, setUl] = useState(null); const [err, setErr] = useState('');
  // Contest CSV parser — two paths:
  //
  //   Path 1 (preferred): DK standard contest export format. Columns 8-10
  //     contain Player / Roster Position / %Drafted. Each player appears
  //     twice on showdown (once CPT, once UTIL); we sum the two for total
  //     ownership. Works for all sports (tennis, MMA, NBA — any DK export
  //     that includes the player breakdown section).
  //
  //   Path 2 (fallback): legacy substring-count on lineup lines. Kept for
  //     backwards compat with older / non-standard CSVs that don't have
  //     the %Drafted breakdown section.
  //
  // The file has a BOM character and \r\n line endings in DK's export, so
  // we normalize both before parsing.
  const handleContest = e => {
    const f = e.target.files[0];
    if (!f) return;
    const r = new FileReader();
    r.onload = evt => {
      try {
        const raw = evt.target.result.replace(/^\uFEFF/, '');
        const lines = raw.split(/\r?\n/);
        // Minimal RFC4180 CSV line parser — handles quoted fields / commas in quotes
        const parseCsvLine = (line) => {
          const cols = [];
          let cur = '', inQuotes = false;
          for (let i = 0; i < line.length; i++) {
            const c = line[i];
            if (c === '"') {
              if (inQuotes && line[i + 1] === '"') { cur += '"'; i++; }
              else inQuotes = !inQuotes;
            } else if (c === ',' && !inQuotes) {
              cols.push(cur); cur = '';
            } else {
              cur += c;
            }
          }
          cols.push(cur);
          return cols;
        };
        // ─── PATH 1: DK %Drafted column (preferred) ────────────────────
        const own = {};
        let parsedPlayerRows = 0;
        for (let i = 1; i < lines.length; i++) {
          const line = lines[i];
          if (!line || !line.includes(',')) continue;
          const cols = parseCsvLine(line);
          if (cols.length < 10) continue;
          const playerName = (cols[7] || '').trim();
          const pctStr = (cols[9] || '').trim();
          if (!playerName || !pctStr) continue;
          const pct = parseFloat(pctStr.replace('%', ''));
          if (!Number.isFinite(pct)) continue;
          // Sum CPT% + UTIL% for total ownership (player can't be both
          // in the same lineup, so slot%'s are additive).
          own[playerName] = Math.round(((own[playerName] || 0) + pct) * 100) / 100;
          parsedPlayerRows++;
        }
        if (parsedPlayerRows > 0) {
          setCd(own);
          setErr('');
          return;
        }
        // ─── PATH 2: substring count fallback ──────────────────────────
        const ownFallback = {};
        let ec = 0;
        for (const line of lines) {
          if (line.includes(',') && rp.some(p => line.includes(p.name))) {
            ec++;
            for (const p of rp) {
              if (line.includes(p.name)) ownFallback[p.name] = (ownFallback[p.name] || 0) + 1;
            }
          }
        }
        if (ec > 0) {
          const op = {};
          for (const [n, c] of Object.entries(ownFallback)) op[n] = Math.round(c / ec * 1000) / 10;
          setCd(op);
          setErr('');
        } else {
          setErr('No player data found in CSV');
        }
      } catch (e) {
        setErr(e.message);
      }
    };
    r.readAsText(f);
  };
  const handleUser = e => { const f = e.target.files[0]; if (!f) return; const r = new FileReader(); r.onload = evt => { try { const lines = evt.target.result.split('\n'); const cnt = {}; let lc = 0; for (const line of lines) { if (!line.trim() || line.startsWith('P,') || line.startsWith('Rank')) continue; const hasP = rp.some(p => line.includes(p.name) || line.includes(String(p.id))); if (hasP) { lc++; for (const p of rp) { if (line.includes(p.name) || line.includes(String(p.id))) cnt[p.name] = (cnt[p.name] || 0) + 1; } } } if (lc > 0) { const ep = {}; for (const [n, c] of Object.entries(cnt)) ep[n] = Math.round(c / lc * 1000) / 10; setUl({ counts: ep, total: lc }); } } catch (e) { setErr(e.message); } }; r.readAsText(f); };
  const ld = useMemo(() => { if (!cd || !ul) return []; return rp.filter(p => p.salary > 0).map(p => ({ name: p.name, salary: p.salary, proj: p.proj, val: p.val, userExp: ul.counts[p.name] || 0, fieldOwn: cd[p.name] || 0, leverage: Math.round(((ul.counts[p.name] || 0) - (cd[p.name] || 0)) * 10) / 10, opponent: p.opponent })).sort((a, b) => b.leverage - a.leverage); }, [cd, ul, rp]);
  return (<>
    <div className="section-hero">
      <div className="section-hero-icon-wrap">
        <svg className="section-hero-icon" viewBox="0 0 24 24" fill="none" stroke="#F5C518">
          <path d="M3 12a9 9 0 0 1 15-6.7L21 8"/>
          <path d="M21 3v5h-5"/>
          <path d="M21 12a9 9 0 0 1-15 6.7L3 16"/>
          <path d="M3 21v-5h5"/>
        </svg>
      </div>
      <div className="section-hero-text">
        <h2 className="section-hero-title">Live Leverage</h2>
        <div className="section-hero-sub">Upload contest CSV + your lineups to compare vs the field</div>
      </div>
    </div>
    <div style={{ display: 'flex', gap: 16, marginBottom: 20, flexWrap: 'wrap' }}>
      <CsvDropZone
        label="Step 1: Contest CSV"
        hint="DK contest file after lock"
        onFile={handleContest}
        filled={!!cd}
        count={cd ? Object.keys(cd).length : 0}
        countLabel="players"
      />
      <CsvDropZone
        label="Step 2: Your Lineups"
        hint="Your DK upload or readable CSV"
        onFile={handleUser}
        filled={!!ul}
        count={ul ? ul.total : 0}
        countLabel="lineups"
      />
    </div>
    {err && <div style={{ color: 'var(--red)', fontSize: 13, marginBottom: 12, display: 'flex', alignItems: 'center', gap: 6 }}><Icon name="warning" size={14} color="var(--red)"/> {err}</div>}
    {ld.length > 0 && <>
      <div className="metrics">
        <div className="metric"><div className="metric-label"><Icon name="gem" size={13}/> Top Leverage</div><div className="metric-value" style={{ color: 'var(--green-text)' }}>{ld[0]?.name}</div><div className="metric-sub">You: {ld[0]?.userExp}% · Field: {ld[0]?.fieldOwn}% · +{ld[0]?.leverage}%</div></div>
        <div className="metric"><div className="metric-label"><Icon name="bomb" size={13}/> Most Underweight</div><div className="metric-value" style={{ color: 'var(--red-text)' }}>{ld[ld.length - 1]?.name}</div><div className="metric-sub">You: {ld[ld.length - 1]?.userExp}% · Field: {ld[ld.length - 1]?.fieldOwn}% · {ld[ld.length - 1]?.leverage}%</div></div>
      </div>
      <div className="table-wrap"><table><thead><tr><th>#</th><th></th><th>Player</th><th>Opp</th><th className="num">Proj</th><th className="num">Your Exp</th><th className="num">Field Own</th><th className="num">Leverage</th></tr></thead>
      <tbody>{ld.map((p, i) => <tr key={p.name} className={p.leverage > 10 ? 'row-hl-green' : p.leverage < -10 ? 'row-hl-red' : ''}><td className="muted">{i + 1}</td><td>{p.leverage > 10 ? <Tip icon="gem" label="Strong overweight" /> : p.leverage < -10 ? <Tip icon="bomb" label="Underweight" /> : ''}</td><td className="name">{p.name}</td><td className="muted">{p.opponent}</td><td className="num">{fmt(p.proj, 1)}</td><td className="num" style={{ color: 'var(--primary-glow)' }}>{fmt(p.userExp, 1)}%</td><td className="num muted">{fmt(p.fieldOwn, 1)}%</td><td className="num"><span style={{ color: p.leverage > 0 ? 'var(--green)' : p.leverage < 0 ? 'var(--red)' : 'var(--text-dim)', fontWeight: Math.abs(p.leverage) > 10 ? 700 : 500, background: Math.abs(p.leverage) > 15 ? (p.leverage > 0 ? 'rgba(34,197,94,0.15)' : 'rgba(239,68,68,0.12)') : 'transparent', padding: '2px 8px', borderRadius: 4 }}>{p.leverage > 0 ? '+' : ''}{fmt(p.leverage, 1)}%</span></td></tr>)}</tbody></table></div>
    </>}
    {!cd && !ul && <div style={{ textAlign: 'center', padding: '40px 20px', color: 'var(--text-dim)' }}><div style={{ marginBottom: 8, color: 'var(--text-muted)' }}><Icon name="refresh" size={32}/></div><div style={{ fontSize: 14 }}>Upload both CSVs to see leverage vs field</div></div>}
  </>);
}

// ═══════════════════════════════════════════════════════════════════════
// MMA COMPONENTS — NEW
// ═══════════════════════════════════════════════════════════════════════
function MMADKTab({ fighters, fc, own, onOverride, overrides, lockedPlayers = [], excludedPlayers = [], onToggleLock, onToggleExclude, onClearLocks, onClearExcludes }) {
  const [q, setQ] = useState('');
  const pw = useMemo(() => fighters.filter(p => p.salary > 0).map(p => ({ ...p, simOwn: own[p.name] || 0 })), [fighters, own]);
  const pwFiltered = useMemo(() => pw.filter(p => matchesSearch(p, q)), [pw, q]);
  const { sorted, sortKey, sortDir, toggleSort } = useSort(pwFiltered, 'proj', 'desc');
  const t3v = useMemo(() => [...fighters].sort((a, b) => b.val - a.val).slice(0, 3).map(p => p.name), [fighters]);
  const t3f = useMemo(() => [...fighters].sort((a, b) => b.finishUpside - a.finishUpside).slice(0, 3).map(p => p.name), [fighters]);
  // Biggest Trap — pp difference between ownership and win probability:
  //     trapScore = simOwn − wp × 100
  // Mirrors the tennis formula — UFC is also individual-athlete, binary
  // matchup. A fighter who can't realistically win can't be a "trap" no
  // matter how chalked up, so the 30% win-probability gate filters out
  // heavy dogs before the comparison runs.
  const trap = useMemo(() => {
    const active = pw.filter(p => p.salary > 0 && (p.wp || 0) >= 0.30);
    if (active.length === 0) return '';
    const hasOwn = active.some(p => (p.simOwn || 0) > 0);
    if (!hasOwn) return [...active].sort((a, b) => b.proj - a.proj)[0]?.name || '';
    const scored = active.map(p => {
      const own = p.simOwn || 0;
      const wp  = (p.wp || 0) * 100;
      return { name: p.name, score: own - wp };
    }).sort((a, b) => b.score - a.score);
    return scored[0]?.name || '';
  }, [pw]);

  // Hidden Gem — dual-track with primary + optional PP-based pivot (v3.19):
  //   PRIMARY 1 (preferred): trap's opponent, IF they're +199 or better
  //     (wp >= 33.4%). A fighter with a real win path can flip a close
  //     fight with a single finish, and comes with the exact salary swap.
  //   PRIMARY 2 (fallback): best fighter in trap's salary band
  //     (-$1000/+$300, widened to -$2500/+$1000 if empty, then
  //     highest (wp − simOwn) gap).
  //   PIVOT (v3.19): best PrizePicks LESS signal with ppEdge ≤ -2,
  //     excluding primary and trap. Falls back to salary-band winner
  //     (old pivot logic) when no PP data available.
  const gem = useMemo(() => {
    const trapPlayer = pw.find(p => p.name === trap);
    if (!trapPlayer) return { primary: null, pivot: null };
    const trapOwn = trapPlayer.simOwn || 0;

    const opponent = pw.find(p => p.name === trapPlayer.opponent);
    const opponentQualifies = opponent && (opponent.wp || 0) >= 0.334;

    const trapSal = trapPlayer.salary;
    const scoreBand = (lo, hi) => pw.filter(p => {
      if (p.name === trap) return false;
      if (opponentQualifies && opponent && p.name === opponent.name) return false;
      const diff = p.salary - trapSal;
      return diff >= lo && diff <= hi;
    }).map(p => {
      const ceil = p.ceil || p.proj || 0;
      const val  = p.val || (p.salary > 0 ? ceil / (p.salary / 1000) : 0);
      const leverage = Math.max(0, trapOwn - (p.simOwn || 0));
      const levBoost = 1 + leverage * 0.012;
      const upsideBoost = 1 + (p.finishProb || 0.2) * 0.3;
      const score = val * ceil * levBoost * upsideBoost;
      return { name: p.name, score };
    }).sort((a, b) => b.score - a.score);

    // Salary-band rankings — used for primary if opponent doesn't qualify
    // AND as pivot fallback when no PP data available.
    let bandRanked = scoreBand(-1000, 300);
    if (bandRanked.length === 0) bandRanked = scoreBand(-2500, 1000);
    if (bandRanked.length === 0) {
      bandRanked = pw.filter(p => {
        if (p.name === trap) return false;
        if (opponentQualifies && opponent && p.name === opponent.name) return false;
        return (p.salary || 0) > 0;
      }).map(p => ({ name: p.name, score: (p.wp || 0) * 100 - (p.simOwn || 0) }))
        .sort((a, b) => b.score - a.score)
        .filter(x => x.score > 0);
    }

    let primary;
    if (opponentQualifies) {
      primary = { name: opponent.name, kind: 'opponent', wp: opponent.wp };
    } else {
      const winner = bandRanked[0];
      primary = winner ? { name: winner.name, kind: 'value' } : null;
    }

    // PIVOT — top PP LESS with ppEdge ≤ -2, excluding primary + trap
    const ppFades = pw
      .filter(p => typeof p.ppLine === 'number' && typeof p.ppEdge === 'number' && p.ppEdge <= -2)
      .filter(p => p.name !== trap)
      .filter(p => !primary || p.name !== primary.name)
      .sort((a, b) => (a.ppEdge || 0) - (b.ppEdge || 0))
      .slice(0, 2);

    let pivot = null;
    if (ppFades.length > 0) {
      const top = ppFades[0];
      pivot = { name: top.name, kind: 'pp-fade', ppEdge: top.ppEdge };
    } else {
      const fallback = bandRanked.find(x => !primary || x.name !== primary.name);
      if (fallback) pivot = { name: fallback.name, kind: 'value' };
    }

    return { primary, pivot };
  }, [pw, trap]);
  const gemName = gem.primary?.name || '';
  const pivotName = gem.pivot?.name || '';
  const S = p => <SH {...p} sortKey={sortKey} sortDir={sortDir} onSort={toggleSort} />;
  return (<>
    <div className="metrics">
      <div className="metric"><div className="metric-label"><Icon name="trophy" size={13}/> Top Value</div><div className="metric-value">{t3v.map((n, i) => { const p = fighters.find(x => x.name === n); return <div key={i} style={{ fontSize: i === 0 ? '16px' : '13px', color: i === 0 ? undefined : 'var(--text-muted)' }}>{i + 1}. {n} <span style={{ fontSize: 11, color: 'var(--text-dim)' }}>{fmt(p?.val, 2)}</span></div>; })}</div></div>
      <div className="metric"><div className="metric-label"><Icon name="fist" size={13}/> Top Finish Path</div><div className="metric-value">{t3f.map((n, i) => { const p = fighters.find(x => x.name === n); return <div key={i} style={{ fontSize: i === 0 ? '16px' : '13px', color: i === 0 ? undefined : 'var(--text-muted)' }}>{n} <span style={{ fontSize: 11, color: 'var(--text-dim)' }}>{fmtPct(p?.finishProb)}</span></div>; })}</div></div>
      <div className="metric">
        <div className="metric-label"><Icon name="gem" size={13}/> Hidden Gem</div>
        <div className="metric-value" style={{ color: 'var(--green-text)' }}>{gem.primary?.name || '-'}</div>
        <div className="metric-sub">
          {gem.primary?.kind === 'opponent'
            ? `Close matchup · ${fmtPct(gem.primary.wp)} win prob`
            : gem.primary?.kind === 'value'
            ? "Overlooked value in trap's price range"
            : 'Low ownership, high ceiling'}
        </div>
        {gem.pivot && (
          <div style={{ marginTop: 6, paddingTop: 6, borderTop: '1px dashed var(--border)', fontSize: 11, color: 'var(--text-dim)' }}>
            or pivot: <span style={{ color: 'var(--text-muted)' }}>{gem.pivot.name}</span> <span style={{ fontSize: 10 }}>({gem.pivot.kind})</span>
          </div>
        )}
      </div>
      <div className="metric"><div className="metric-label"><Icon name="bomb" size={13}/> Biggest Trap</div><div className="metric-value" style={{ color: 'var(--red-text)' }}>{trap || '-'}</div><div className="metric-sub">Who the field needs most</div></div>
    </div>
    <SearchBar value={q} onChange={setQ} placeholder="Search fighters, opponents" total={pw.length} filtered={pwFiltered.length} />
    <LockBar lockedPlayers={lockedPlayers} excludedPlayers={excludedPlayers} onToggleLock={onToggleLock} onToggleExclude={onToggleExclude} onClearLocks={onClearLocks} onClearExcludes={onClearExcludes} />
    <div className="table-wrap"><table><thead><tr>
      <th>#</th><th></th><S label="Fighter" colKey="name" /><th>Opp</th>
      <S label="Sal" colKey="salary" num /><S label="Sim Own" colKey="simOwn" num tip="Projected field ownership (imported from Pool Own, or simulated if unavailable)" /><S label="Win%" colKey="wp" num />
      <S label="Proj" colKey="proj" num /><S label="Ceiling" colKey="ceil" num /><S label="Finish%" colKey="finishProb" />
      <S label="Val" colKey="val" num /><S label="CVal" colKey="cval" num />
      <S label="SS" colKey="sigStr" /><S label="TD" colKey="takedowns" /><S label="CT" colKey="ctMin" />
      <th>Time</th>
      <th></th>
    </tr></thead>
    <tbody>{sorted.map((p, i) => {
      const iv = t3v.includes(p.name), isf = t3f.includes(p.name);
      const ig = p.name === gemName;
      const ip = p.name === pivotName && pivotName !== '';
      const it = p.name === trap;
      const badges = [];
      if (iv)  badges.push({ icon: 'trophy', label: 'Top 3 Value' });
      if (isf) badges.push({ icon: 'fist',   label: 'Top 3 Finish Path' });
      if (ig)  badges.push({ icon: 'gem',    label: gem.primary?.kind === 'opponent' ? 'Hidden Gem (opp)' : 'Hidden Gem (value)' });
      if (ip)  badges.push({ icon: 'gem',    label: 'Gem pivot (value)' });
      if (it)  badges.push({ icon: 'bomb',   label: 'Trap' });
      const isOver = overrides && overrides[p.name] != null;
      return <tr key={p.name} className={ig ? 'row-hl-green' : ip ? 'row-hl-green' : it ? 'row-hl-red' : ''}>
        <td className="muted">{i + 1}</td>
        <td><span style={{ display: 'inline-flex', gap: 4, alignItems: 'center' }}>{badges.map((bd, j) => <Tip key={j} icon={bd.icon} label={bd.label} size={14} />)}</span></td>
        <td className="name">{p.name}</td><td className="muted">{p.opponent}</td>
        <td className="num">{fmtSal(p.salary)}</td>
        <td className="num" style={{ color: p.simOwn > 30 ? 'var(--amber)' : 'var(--text-muted)' }}>{fmt(p.simOwn, 1)}%</td>
        <td className="num">{fmtPct(p.wp)}</td>
        <td className="num">
          <span className={iv ? 'cell-top3' : 'cell-proj'}>
            <input type="number" step="0.1" className={`proj-edit ${isOver ? 'overridden' : ''}`}
              value={isOver ? overrides[p.name] : (p.proj != null ? p.proj : '')}
              onChange={e => onOverride && onOverride(p.name, e.target.value)}
              onDoubleClick={() => onOverride && onOverride(p.name, null)}
              title={isOver ? 'Overridden — double-click to reset. Ceiling scales proportionally.' : 'Click to edit projection'} />
          </span>
        </td>
        <td className="num"><span style={{ background: 'rgba(74,222,128,0.1)', color: 'var(--green)', padding: '4px 10px', borderRadius: 4, fontWeight: 600, minWidth: 50, display: 'inline-block', textAlign: 'center' }}>{fmt(p.ceil, 1)}</span></td>
        <td className="num" style={{ color: p.finishProb > 0.35 ? 'var(--primary)' : 'var(--text-muted)', fontWeight: p.finishProb > 0.35 ? 700 : 400 }}>{fmtPct(p.finishProb)}</td>
        <td className="num"><span className={iv ? 'cell-top3' : ''}>{fmt(p.val, 2)}</span></td>
        <td className="num" style={{ color: p.cval > 16 ? 'var(--green)' : undefined, fontWeight: p.cval > 16 ? 700 : 400 }}>{fmt(p.cval, 2)}</td>
        <td className="num">{fmt(p.sigStr)}</td><td className="num muted">{fmt(p.takedowns)}</td><td className="num">{fmt(p.ctMin)}</td>
        <td className="muted">{fmtTime(p.startTime)}</td>
        <td style={{ textAlign: 'right', paddingRight: 10 }}><LockExcludeButtons name={p.name} isLocked={lockedPlayers.includes(p.name)} isExcluded={excludedPlayers.includes(p.name)} onToggleLock={onToggleLock} onToggleExclude={onToggleExclude} /></td>
      </tr>; })}</tbody></table></div>
  </>);
}

function MMAPPTab({ rows }) {
  const [q, setQ] = useState('');
  const rowsFiltered = useMemo(() => rows.filter(r => matchesSearch(r, q, ['player', 'stat', 'opponent'])), [rows, q]);
  const { sorted, sortKey, sortDir, toggleSort } = useSort(rowsFiltered, 'ev', 'desc');
  const S = p => <SH {...p} sortKey={sortKey} sortDir={sortDir} onSort={toggleSort} />;
  const best = useMemo(() => [...rows].sort((a, b) => b.ev - a.ev).slice(0, 3), [rows]);
  const worst = useMemo(() => [...rows].sort((a, b) => a.ev - b.ev).slice(0, 3), [rows]);
  return (<>
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
        <div className="section-hero-sub">All plays sorted by edge · Edge = Projected − PP Line</div>
      </div>
    </div>
    <div style={{ background: 'rgba(245,197,24,0.06)', border: '1px solid rgba(245,197,24,0.25)', borderRadius: 8, padding: '10px 14px', marginBottom: 16, display: 'flex', alignItems: 'center', gap: 10, fontSize: 13 }}>
      <Icon name="trending-down" size={18} color="#F5C518"/>
      <span style={{ color: 'var(--text-muted)' }}><strong style={{ color: 'var(--primary)', fontWeight: 700 }}>Hint:</strong> PrizePicks bad value will typically reverse</span>
    </div>
    <div className="metrics">
      <div className="metric"><div className="metric-label"><Icon name="flame" size={13}/> Best Edge</div><div className="metric-value">{best.map((r, i) => <div key={i} style={{ fontSize: i === 0 ? '16px' : '13px', color: i === 0 ? 'var(--green-text)' : 'var(--text-muted)', fontWeight: i === 0 ? 700 : 500 }}>{r.player} · {r.stat} <span style={{fontSize:11, color: i === 0 ? undefined : 'var(--text-dim)'}}>{r.ev > 0 ? '+' : ''}{fmt(r.ev, 2)}</span>{r.mult && <span style={{fontSize:10,color: i === 0 ? 'var(--amber-text)' : 'var(--text-dim)',marginLeft:4}}>{r.mult}</span>}</div>)}</div></div>
      <div className="metric"><div className="metric-label"><Icon name="trending-down" size={13}/> Biggest "Fades"</div><div className="metric-value">{worst.map((r, i) => <div key={i} style={{ fontSize: i === 0 ? '16px' : '13px', color: i === 0 ? 'var(--red-text)' : 'var(--text-muted)', fontWeight: i === 0 ? 700 : 500 }}>{r.player} · {r.stat} <span style={{fontSize:11, color: i === 0 ? undefined : 'var(--text-dim)'}}>{fmt(r.ev, 2)}</span></div>)}</div></div>
    </div>
    <SearchBar value={q} onChange={setQ} placeholder="Search plays, fighters, stats" total={rows.length} filtered={rowsFiltered.length} />
    <div className="table-wrap"><table><thead><tr>
      <th>#</th><th></th><S label="Fighter" colKey="player" /><S label="Stat" colKey="stat" />
      <S label="PP Line" colKey="line" num /><S label="Projected" colKey="projected" num />
      <S label="Edge" colKey="ev" num /><S label="Play" colKey="direction" />
      <th>Mult</th><S label="Win%" colKey="wp" num /><S label="Opp" colKey="opponent" />
    </tr></thead>
    <tbody>{sorted.map((r, i) => {
      const isBest = best.some(t => t.player === r.player && t.stat === r.stat);
      const isWorst = worst.some(t => t.player === r.player && t.stat === r.stat);
      const playDir = r.direction;
      const isFT = r.stat === 'Fight Time';
      return <tr key={r.player + r.stat} className={isBest ? 'row-hl-green' : isWorst ? 'row-hl-red' : ''}>
        <td className="muted">{i+1}</td>
        <td>{isBest ? <Tip icon="flame" label="Best edge" /> : isWorst ? <Tip icon="trending-down" label="Fade" /> : ''}</td>
        <td className="name">{r.player}</td>
        <td style={{fontSize:11,color:'var(--text-muted)'}}>{r.stat}</td>
        <td className="num">{fmt(r.line, 2)}{isFT && <span style={{ fontSize: 10, color: 'var(--text-dim)', marginLeft: 2 }}>m</span>}</td>
        <td className="num"><span className="cell-proj">{fmt(r.projected, 2)}{isFT && <span style={{ fontSize: 10, color: 'var(--text-dim)', marginLeft: 2 }}>m</span>}</span></td>
        <td className="num"><span className={isBest ? 'cell-ev-top' : isWorst ? 'cell-ev-worst' : r.ev > 0 ? 'cell-ev-pos' : 'cell-ev-neg'}>{r.ev > 0 ? '+' : ''}{fmt(r.ev, 2)}</span></td>
        <td><span style={{color: playDir === 'MORE' ? 'var(--green-text)' : playDir === 'LESS' ? 'var(--red-text)' : 'var(--text-dim)', fontWeight:600}}>{playDir}</span></td>
        <td style={{color:'var(--amber-text)',fontSize:11}}>{r.mult || ''}</td>
        <td className="num muted">{fmtPct(r.wp)}</td>
        <td className="muted">{r.opponent}</td>
      </tr>;
    })}</tbody></table></div>
  </>);
}

function MMABuilderTab({ fighters: rp, ownership, lockedPlayers = [], excludedPlayers = [], onGoToProjections }) {
  const [exp, setExp] = useState({}); const [res, setRes] = useState(null);
  const [prevRes, setPrevRes] = useState(null);              // v3.24.14: undo support for Rebuild
  const [isBuilding, setIsBuilding] = useState(false);       // v3.24.13: build animation gate
  const [nL, setNL] = useState(150);
  const [variance, setVariance] = useState(2);                // ±% jitter on projections per build
  const [globalMax, setGlobalMax] = useState(100); const [globalMin, setGlobalMin] = useState(0);
  const [mode, setMode] = useState('ceiling');  // ceiling=GPP, proj=cash
  const [contrarianOn, setContrarianOn] = useState(false);
  const [contrarianStrength, setContrarianStrength] = useState(0.6);
  const [poolQ, setPoolQ] = useState('');
  // Favorites — classic-only (MMA is classic-only).
  const [favoriteLineups, setFavoriteLineups] = useState([]);
  const favoriteKey = (fav) => [...(fav.players || [])].sort().join('|');
  const toggleFavoriteLineup = useCallback((lu, pData) => {
    const fav = { players: lu.players.map(i => pData[i].name), proj: lu.proj, sal: lu.sal };
    const key = favoriteKey(fav);
    setFavoriteLineups(prev => {
      const exists = prev.some(f => favoriteKey(f) === key);
      return exists ? prev.filter(f => favoriteKey(f) !== key) : [...prev, fav];
    });
  }, []);

  const avgVal = useMemo(() => {
    const vals = rp.filter(p => p.salary > 0).map(p => (mode === 'ceiling' ? p.cval : p.val) || 0);
    return vals.length ? vals.reduce((s, v) => s + v, 0) / vals.length : 7;
  }, [rp, mode]);
  // OVEROWNED MODE (MMA v3.19) — fully rewritten to mirror DK tab.
  // Mirrors tennis exactly: trap = simOwn − wp × 100 with wp ≥ 30% gate,
  // gem = opponent-if-wp≥33.4% else salary-band winner, PP boost stacks
  // +15pp on top of existing min.
  const contrarianCaps = useMemo(() => {
    if (!contrarianOn) return {};
    const withSal = rp.filter(p => p.salary > 0);
    if (withSal.length === 0) return {};
    const caps = {};
    const projKey = mode === 'ceiling' ? 'ceil' : 'proj';
    const valKey  = mode === 'ceiling' ? 'cval' : 'val';

    const LEV_CAP = 30;
    const levPP = Math.round(contrarianStrength * (35 / 0.6));  // 35pp @ 0.6, 58pp @ 1.0
    const globalFloor = Math.round(1 + contrarianStrength * 7);

    // (1) TRAP — mirror DK tab: simOwn − wp × 100, gate wp ≥ 30%.
    const trapActive = withSal.filter(p => (p.wp || 0) >= 0.30);
    const hasOwn = trapActive.some(p => (ownership[p.name] || 0) > 0);
    let trap = null;
    if (!hasOwn && trapActive.length > 0) {
      trap = [...trapActive].sort((a, b) => (b[projKey] || 0) - (a[projKey] || 0))[0];
    } else if (trapActive.length > 0) {
      const scored = trapActive.map(p => ({
        p, score: (ownership[p.name] || 0) - (p.wp || 0) * 100,
      })).sort((a, b) => b.score - a.score);
      trap = scored[0]?.p || null;
    }
    if (trap) {
      const fieldOwn = ownership[trap.name] || 0;
      caps[trap.name] = {
        max: Math.max(0, Math.round(fieldOwn - levPP)),
        _isTrap: true,
        _fieldOwn: Math.round(fieldOwn),
      };
    }

    // (2) + (3) GEM PRIMARY + PP-based PIVOT (v3.19) — mirror MMA DK tab.
    //   Primary: opponent (if wp ≥ 33.4%) else salary-band winner.
    //   Pivot:   top PP LESS with ppEdge ≤ -2, excluding primary + trap.
    //            Falls back to salary-band winner if no PP data.
    let gemPrimary = null;
    let gemPivot   = null;
    if (trap) {
      const trapOwn = ownership[trap.name] || 0;
      const opponent = withSal.find(p => p.name === trap.opponent);
      const opponentQualifies = opponent && (opponent.wp || 0) >= 0.334;

      const trapSal = trap.salary;
      const scoreBand = (lo, hi) => withSal.filter(p => {
        if (p.name === trap.name) return false;
        if (opponentQualifies && opponent && p.name === opponent.name) return false;
        const diff = p.salary - trapSal;
        return diff >= lo && diff <= hi;
      }).map(p => {
        const ceil = p[projKey] || 0;
        const val  = p[valKey] || (p.salary > 0 ? ceil / (p.salary / 1000) : 0);
        const leverage = Math.max(0, trapOwn - (ownership[p.name] || 0));
        const levBoost = 1 + leverage * 0.012;
        const upsideBoost = 1 + (p.finishProb || 0.2) * 0.3;
        const score = val * ceil * levBoost * upsideBoost;
        return { p, score };
      }).sort((a, b) => b.score - a.score);

      // Salary-band rankings — used for primary if opponent doesn't qualify
      // AND as pivot fallback when no PP data available.
      let bandRanked = scoreBand(-1000, 300);
      if (bandRanked.length === 0) bandRanked = scoreBand(-2500, 1000);
      if (bandRanked.length === 0) {
        const lev = withSal.filter(p => {
          if (p.name === trap.name) return false;
          if (opponentQualifies && opponent && p.name === opponent.name) return false;
          return (p.salary || 0) > 0;
        }).map(p => ({ p, score: (p.wp || 0) * 100 - (ownership[p.name] || 0) }))
          .sort((a, b) => b.score - a.score)
          .filter(x => x.score > 0);
        bandRanked = lev;
      }

      // Primary
      if (opponentQualifies) {
        gemPrimary = opponent;
      } else {
        gemPrimary = bandRanked[0]?.p || null;
      }

      // PIVOT — top PP LESS with ppEdge ≤ -2, excluding primary + trap
      const ppFades = withSal
        .filter(p => typeof p.ppLine === 'number' && typeof p.ppEdge === 'number' && p.ppEdge <= -2)
        .filter(p => p.name !== trap.name)
        .filter(p => !gemPrimary || p.name !== gemPrimary.name)
        .sort((a, b) => (a.ppEdge || 0) - (b.ppEdge || 0))
        .slice(0, 2);

      if (ppFades.length > 0) {
        gemPivot = ppFades[0];
      } else {
        gemPivot = bandRanked.find(x => !gemPrimary || x.p.name !== gemPrimary.name)?.p || null;
      }
    }

    if (gemPrimary) {
      const fieldOwn = Math.round(ownership[gemPrimary.name] || 0);
      caps[gemPrimary.name] = {
        min: Math.min(95, fieldOwn + levPP),
        max: Math.min(100, fieldOwn + LEV_CAP + levPP),
        _isGem: true, _kind: 'primary',
        _fieldOwn: fieldOwn,
      };
    }
    if (gemPivot && !caps[gemPivot.name]) {
      const fieldOwn = Math.round(ownership[gemPivot.name] || 0);
      caps[gemPivot.name] = {
        min: Math.min(95, fieldOwn + levPP),
        max: Math.min(100, fieldOwn + LEV_CAP + levPP),
        _isGem: true, _kind: 'pivot',
        _fieldOwn: fieldOwn,
      };

    // PIVOT-OPPONENT FADE (v3.20) — the player opposite the gem pivot in their
    // match is a secondary trap: if the pivot hits, their opponent busts. Cap
    // at simOwn × (1 − multiplier) where multiplier = 0.50 at base strength
    // 0.6 (→ 0.83 at 1.0, 0 at 0). Safe to apply unconditionally — user
    // confirmed pivot's opponent is never already a trap or boosted gem.
    if (gemPivot) {
      const pivotOpponent = withSal.find(p => p.name === gemPivot.opponent);
      if (pivotOpponent && !caps[pivotOpponent.name]) {
        const oppFieldOwn = ownership[pivotOpponent.name] || 0;
        const fadeMultiplier = Math.min(1, contrarianStrength * (0.50 / 0.6));
        const oppMax = Math.max(0, Math.round(oppFieldOwn * (1 - fadeMultiplier)));
        caps[pivotOpponent.name] = {
          min: 0, max: oppMax,
          _isPivotOpponent: true,
          _fieldOwn: Math.round(oppFieldOwn),
        };
      }
    }
    }

    // GLOBAL FLOOR + LEVERAGE CAP — everyone else
    withSal.forEach(p => {
      const fieldOwn = Math.round(ownership[p.name] || 0);
      if (caps[p.name]) return;
      const maxCap = fieldOwn >= 15 ? Math.min(95, fieldOwn + LEV_CAP) : 100;
      caps[p.name] = { min: globalFloor, max: maxCap, _isFloor: true };
    });

    // (4) PP BOOST — top 2 LESS edges ≤ -2, exclude primary trap, +15pp min
    // stacked on existing. Cap-wins clips to existing max.
    const ppFadeCandidates = withSal
      .filter(p => typeof p.ppLine === 'number' && typeof p.ppEdge === 'number' && p.ppEdge <= -2)
      .filter(p => !trap || p.name !== trap.name)
      .sort((a, b) => (a.ppEdge || 0) - (b.ppEdge || 0))
      .slice(0, 2);
    ppFadeCandidates.forEach((fade, idx) => {
      const existing = caps[fade.name] || {};
      const fieldOwn = Math.round(ownership[fade.name] || 0);
      const currMin = existing.min || 0;
      const currMax = existing.max !== undefined ? existing.max : 100;
      const newMin = Math.min(currMin + 15, currMax);
      caps[fade.name] = {
        ...existing,
        min: newMin,
        max: existing.max !== undefined ? existing.max : Math.min(100, fieldOwn + LEV_CAP + 15),
        _isPpBoost: true,
        _ppBoostRank: idx + 1,
        _ppBoostEdge: fade.ppEdge || 0,
        _ppBoostLine: fade.ppLine || 0,
        _ppBoostOrigProj: fade.proj || 0,
        _fieldOwn: existing._fieldOwn !== undefined ? existing._fieldOwn : fieldOwn,
        _ppBoostAddedMin: newMin - currMin,
      };
    });

    return caps;
  }, [rp, ownership, contrarianOn, contrarianStrength, mode]);

  const adjRp = rp;

  const sortField = mode === 'ceiling' ? 'cval' : 'val';
  const sp = useMemo(() => [...adjRp].filter(p => p.salary > 0).sort((a, b) => b[sortField] - a[sortField]), [adjRp, sortField]);
  const sE = (n, f, v) => setExp(p => ({ ...p, [n]: { ...p[n], [f]: v } }));
  const applyGlobal = () => { const ne = {}; sp.forEach(p => { ne[p.name] = { min: globalMin, max: globalMax, ...exp[p.name] }; }); setExp(ne); };
  const run = () => {
    if (!canBuild || isBuilding) return;         // DK compliance gate
    // v3.24.13: build animation — see tennis run() comment for details.
    setIsBuilding(true);
    if (res) setPrevRes(res);                    // v3.24.14: stash prev res for undo
    const start = performance.now();
    const finish = () => {
      const elapsed = performance.now() - start;
      const MIN_MS = 1400;
      setTimeout(() => setIsBuilding(false), Math.max(0, MIN_MS - elapsed));
    };
    setTimeout(() => { try {
    // Variance jitter: each build applies a fresh ±variance% random multiplier. Same mult to proj AND ceiling
    // per fighter so their ratio stays consistent (matters because cash mode uses proj, GPP uses ceiling).
    const jitter = () => 1 + (Math.random() * 2 - 1) * variance / 100;
    // DK anti-abuse: guarantee ≥2 projections differ from baseline by ≥0.01 even at variance=0.
    const enforceMinNudge = (pd, basePd, key) => {
      const changed = pd.filter((p, i) => Math.abs(p[key] - basePd[i][key === 'projection' ? 'proj' : 'ceil']) >= 0.01).length;
      if (changed >= 2) return;
      const idxs = [...Array(pd.length).keys()].sort(() => Math.random() - 0.5).slice(0, 2);
      idxs.forEach(i => {
        const sign = Math.random() < 0.5 ? -1 : 1;
        pd[i][key] = Math.round((pd[i][key] + sign * 0.01) * 1000) / 1000;
      });
    };
    const pd = sp.map(p => {
      const cap = contrarianCaps[p.name] || {};
      const userSet = exp[p.name] || {};
      // Contrarian is MORE-RESTRICTIVE than user: lifts user.min, lowers user.max
      const userMin = userSet.min !== undefined ? userSet.min : globalMin;
      const userMax = userSet.max !== undefined ? userSet.max : globalMax;
      const effMin = Math.max(userMin, cap.min || 0);
      const effMax = Math.min(userMax, cap.max !== undefined ? cap.max : 100);
      const m = jitter();      // same multiplier for proj + ceiling per fighter
      return {
        name: p.name, salary: p.salary, id: p.id,
        projection: p.proj * m, ceiling: p.ceil * m,
        opponent: p.opponent,
        maxExp: effMax, minExp: effMin
      };
    });
    // Nudge whichever key the optimizer actually ranks on for this mode
    enforceMinNudge(pd, sp, mode === 'ceiling' ? 'ceiling' : 'projection');
    const r = optimizeMMA(pd, nL, 50000, 6, mode, 48000, { locked: new Set(lockedPlayers), excluded: new Set(excludedPlayers) });
    // Merge favorited lineups (remapped from name tuples to new indices).
    const favCls = [];
    const nameIdx = new Map(pd.map((p, i) => [p.name, i]));
    for (const fav of favoriteLineups) {
      const idxs = (fav.players || []).map(n => nameIdx.get(n));
      if (idxs.some(i => i === undefined)) continue;
      favCls.push({ players: idxs, proj: fav.proj, sal: fav.sal });
    }
    const favKeys = new Set(favCls.map(lu => [...lu.players].sort().join(',')));
    const deduped = r.lineups.filter(lu => !favKeys.has([...lu.players].sort().join(',')));
    const merged = [...favCls, ...deduped];
    const mergedCounts = new Array(pd.length).fill(0);
    for (const lu of merged) { lu.players.forEach(i => mergedCounts[i]++); }
    setRes({ ...r, lineups: merged, counts: mergedCounts, pData: pd, mode });
    } finally { finish(); } }, 32);
  };
  const overrideCount = useMemo(() => rp.filter(p => p._overridden).length, [rp]);
  const canBuild = overrideCount >= 2;
  useBuilderShortcuts({ run, canBuild, isBuilding }); // v3.24.14: B/R to build/rebuild
  const dl = (c, f) => { const b = new Blob([c], { type: 'text/csv' }); const a = document.createElement('a'); a.href = URL.createObjectURL(b); a.download = f; a.click(); URL.revokeObjectURL(a.href); };
  const exportDK = () => { if (!res) return; let c = 'F,F,F,F,F,F\n'; res.lineups.forEach(lu => { const ps = lu.players.map(i => res.pData[i]).sort((a, b) => b.salary - a.salary); c += ps.map(p => p.id).join(',') + '\n'; }); dl(c, 'dk_upload_ufc.csv'); };
  const exportReadable = () => { if (!res) return; let c = 'Rank,Score,Salary,F1,F2,F3,F4,F5,F6\n'; res.lineups.forEach((lu, i) => { const ps = lu.players.map(j => res.pData[j]).sort((a, b) => b.salary - a.salary); c += `${i + 1},${lu.proj},${lu.sal},${ps.map(p => p.name).join(',')}\n`; }); dl(c, 'lineups_ufc.csv'); };
  const exportProjections = () => { let c = 'Fighter,Salary,Win%,Median,Ceiling,Val,CVal,Finish%,SigStr,TDs,CT,Opp\n'; sp.forEach(p => { c += `${p.name},${p.salary},${(p.wp * 100).toFixed(0)}%,${p.proj},${p.ceil},${p.val},${p.cval},${(p.finishProb*100).toFixed(0)}%,${fmt(p.sigStr)},${fmt(p.takedowns)},${fmt(p.ctMin)},${p.opponent}\n`; }); dl(c, 'projections_ufc.csv'); };

  // v3.24.14: empty state for 0-fighter slates
  if (sp.length === 0) {
    return <>
      <div className="section-hero">
        <div className="section-hero-icon-wrap">
          <svg className="section-hero-icon" viewBox="0 0 24 24" fill="#F5C518" stroke="none">
            <path d="M13 2L4 14h7l-1 8 10-12h-7l1-8z"/>
          </svg>
        </div>
        <div className="section-hero-text">
          <h2 className="section-hero-title">Lineup Builder</h2>
          <div className="section-hero-sub">Set exposure %, build optimized lineups, export to DK</div>
        </div>
      </div>
      <div className="empty" style={{ padding: '60px 24px', background: 'var(--card)', border: '1px dashed var(--border-light)', borderRadius: 12 }}>
        <div style={{ fontSize: 42, marginBottom: 16, opacity: 0.4 }}>🥊</div>
        <h2 style={{ fontSize: 20, fontWeight: 700, color: 'var(--text)', marginBottom: 6 }}>No fighters on this slate yet</h2>
        <p style={{ color: 'var(--text-muted)', fontSize: 14, maxWidth: 420, margin: '0 auto' }}>
          The current card has no priced fighters. Check back closer to lock, or switch to a different slate.
        </p>
      </div>
    </>;
  }
  return (<>
    <div className="section-hero">
      <div className="section-hero-icon-wrap">
        <svg className="section-hero-icon" viewBox="0 0 24 24" fill="#F5C518" stroke="none">
          <path d="M13 2L4 14h7l-1 8 10-12h-7l1-8z"/>
        </svg>
      </div>
      <div className="section-hero-text">
        <h2 className="section-hero-title">Lineup Builder</h2>
        <div className="section-hero-sub">Set exposure %, build optimized lineups, export to DK</div>
      </div>
    </div>
    <div className="section-sub">UFC: 6 fighters, $50K cap · No opponent-vs-opponent enforced · Export to DK</div>
    {!canBuild && (
      <div style={{ padding: '14px 18px', marginBottom: 16, background: 'rgba(245,197,24,0.08)', border: '1px solid rgba(245,197,24,0.35)', borderRadius: 10 }}>
        <div style={{ fontSize: 14, fontWeight: 700, color: 'var(--primary)', marginBottom: 4, display: 'flex', alignItems: 'center', gap: 6 }}><Icon name="warning" size={15} color="#F5C518"/> DraftKings Compliance Warning</div>
        <div style={{ fontSize: 13, color: 'var(--text-muted)', lineHeight: 1.5 }}>
          DraftKings policies require you to make some changes to our default projections before you can build lineups.
          Head to the <strong style={{ color: 'var(--text)' }}>DK Projections</strong> tab and edit at least <strong style={{ color: 'var(--primary)' }}>2 projections</strong> by any amount — this proves the lineups are your own work, not a shared export.
          <span style={{ color: 'var(--text-dim)' }}> Currently changed: <strong style={{ color: overrideCount >= 2 ? 'var(--green)' : 'var(--red)' }}>{overrideCount}</strong>/2</span>
        </div>
        {onGoToProjections && (
          <button
            onClick={onGoToProjections}
            className="oo-jump-btn"
            style={{
              marginTop: 10, padding: '7px 14px', fontSize: 12, fontWeight: 600,
              background: 'var(--primary)', color: '#0A1628', border: 'none',
              borderRadius: 6, cursor: 'pointer', display: 'inline-flex',
              alignItems: 'center', gap: 6
            }}
          >
            Jump to DK Projections
            <svg viewBox="0 0 24 24" width="12" height="12" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><path d="M5 12h14M13 6l6 6-6 6"/></svg>
          </button>
        )}
      </div>
    )}
    <ContrarianPanel enabled={contrarianOn} onToggle={setContrarianOn} strength={contrarianStrength} onStrengthChange={setContrarianStrength} />
    {contrarianOn && Object.keys(contrarianCaps).length > 0 && (() => {
      // OverOwned Mode display (v3.19) — top 5 changes by |bound - fieldOwn|.
      const labelFor = (c) => {
        if (c._isTrap)   return { label: 'Trap',    icon: 'bomb',   color: 'var(--red)' };
        if (c._isGem && c._kind === 'primary') return { label: 'Gem',     icon: 'gem',    color: 'var(--green)' };
        if (c._isGem && c._kind === 'pivot')   return { label: 'Pivot',   icon: 'gem',    color: 'var(--green)' };
        if (c._isPivotOpponent) return { label: 'Pivot Opp', icon: 'bomb', color: 'var(--amber)' };
        if (c._isMidTierFade)   return { label: 'Mid-Tier Fade', icon: 'bomb', color: 'var(--amber)' };
        if (c._isMidTierPivot)  return { label: 'Mid-Tier Pivot', icon: 'gem', color: 'var(--green)' };
        if (c._isPpBoost) return { label: 'PP Boost', icon: 'rocket', color: 'var(--primary)' };
        return null;
      };
      const describeChange = (c) => {
        if (c._isTrap)   return { sym: `max ${c.max}%`, bound: c.max };
        if (c._isGem)    return { sym: `min ${c.min}%`, bound: c.min };
        if (c._isPivotOpponent) return { sym: `max ${c.max}%`, bound: c.max };
        if (c._isMidTierFade)   return { sym: `max ${c.max}%`, bound: c.max };
        if (c._isMidTierPivot)  return { sym: `min ${c.min}% (+${c._midTierPivotAddedMin}pp stacked)`, bound: c.min };
        if (c._isPpBoost) return { sym: `+${c._ppBoostAddedMin}% min (stacked)`, bound: c.min };
        return { sym: '', bound: 0 };
      };
      const ranked = [];
      for (const [name, c] of Object.entries(contrarianCaps)) {
        const lbl = labelFor(c);
        if (!lbl) continue;
        const desc = describeChange(c);
        const field = ownership[name] || 0;
        const delta = desc.bound != null ? Math.abs(desc.bound - field) : 0;
        ranked.push({ name, lbl, desc, field, delta });
      }
      ranked.sort((a, b) => b.delta - a.delta);
      const top5 = ranked.slice(0, 5);
      const othersCount = ranked.length - top5.length;
      return (
        <div style={{ marginTop: -12, marginBottom: 16, padding: '10px 14px', background: 'rgba(245,197,24,0.06)', border: '1px solid rgba(245,197,24,0.2)', borderRadius: 8, fontSize: 12, color: 'var(--text-muted)', display: 'flex', gap: 16, flexWrap: 'wrap', alignItems: 'center' }}>
          <span style={{ color: 'var(--primary)', fontWeight: 700, textTransform: 'uppercase', fontSize: 10, letterSpacing: 0.5 }}>Top 5 Exposure Changes</span>
          {top5.map(({ name, lbl, desc, field }) => (
            <span key={name} title={signalTooltip(lbl.label)} style={{ display: 'inline-flex', alignItems: 'center', gap: 4, cursor: 'help' }}>
              <Icon name={lbl.icon} size={12} color={lbl.color}/>
              <span style={{ color: 'var(--text-dim)', fontSize: 11 }}>{lbl.label}:</span>
              <span style={{ color: lbl.color, fontWeight: 600 }}>{name}</span>
              <span style={{ color: 'var(--text-dim)' }}>· field {field.toFixed(1)}% →</span>
              <span style={{ color: 'var(--primary)', fontWeight: 600 }}>{desc.sym}</span>
            </span>
          ))}
          {othersCount > 0 && (
            <span style={{ color: 'var(--text-dim)', fontStyle: 'italic', fontSize: 11 }}>
              +{othersCount} more signals enforced
            </span>
          )}
        </div>
      );
    })()}
    <div style={{ display: 'flex', gap: 12, marginBottom: 6, flexWrap: 'wrap', alignItems: 'center' }}>
      <div style={{ display: 'flex', background: 'var(--bg)', border: '1px solid var(--border-light)', borderRadius: 6, overflow: 'hidden' }}>
        <button onClick={() => setMode('proj')} style={{ background: mode === 'proj' ? 'var(--primary)' : 'transparent', color: mode === 'proj' ? '#0A1628' : 'var(--text-muted)', border: 'none', padding: '6px 14px', fontSize: 12, fontWeight: 600, cursor: 'pointer', display: 'inline-flex', alignItems: 'center', gap: 6 }}><Icon name="dollar" size={13}/> Cash (median)</button>
        <button onClick={() => setMode('ceiling')} style={{ background: mode === 'ceiling' ? 'var(--primary)' : 'transparent', color: mode === 'ceiling' ? '#0A1628' : 'var(--text-muted)', border: 'none', padding: '6px 14px', fontSize: 12, fontWeight: 600, cursor: 'pointer', display: 'inline-flex', alignItems: 'center', gap: 6 }}><Icon name="rocket" size={13}/> GPP (ceiling)</button>
      </div>
      <label style={{ fontSize: 13, color: 'var(--text-muted)' }}>Lineups: <input style={{ width: 60, background: 'var(--bg)', border: '1px solid var(--border)', borderRadius: 4, color: 'var(--text)', padding: '4px 8px', marginLeft: 4 }} type="number" min="1" step="1" value={nL} onChange={e => { const v = e.target.value; if (v === "") setNL(""); else setNL(Math.max(1, parseInt(v, 10) || 1)); }} onBlur={e => { if (e.target.value === "" || +e.target.value < 1) setNL(20); }} /></label>
      <label style={{ fontSize: 13, color: 'var(--text-muted)' }}>Min %: <input style={{ width: 50, background: 'var(--bg)', border: '1px solid var(--border)', borderRadius: 4, color: 'var(--text)', padding: '4px 8px', marginLeft: 4 }} type="number" min="0" max="100" step="1" value={globalMin} onChange={e => { const v = e.target.value; if (v === "") setGlobalMin(""); else setGlobalMin(Math.max(0, Math.min(100, parseInt(v, 10) || 0))); }} onBlur={e => { if (e.target.value === "") setGlobalMin(0); }} /></label>
      <label style={{ fontSize: 13, color: 'var(--text-muted)' }}>Max %: <input style={{ width: 50, background: 'var(--bg)', border: '1px solid var(--border)', borderRadius: 4, color: 'var(--text)', padding: '4px 8px', marginLeft: 4 }} type="number" min="0" max="100" step="1" value={globalMax} onChange={e => { const v = e.target.value; if (v === "") setGlobalMax(""); else setGlobalMax(Math.max(0, Math.min(100, parseInt(v, 10) || 0))); }} onBlur={e => { if (e.target.value === "") setGlobalMax(100); }} /></label>
      <label style={{ fontSize: 13, color: 'var(--text-muted)', display: 'flex', alignItems: 'center', gap: 6 }} title="Random ± shift applied to each fighter's projection & ceiling per build. Guarantees two users on the same slate don't submit identical lineups.">
        Variance
        <input type="range" min="0" max="25" step="1" value={variance} onChange={e => setVariance(+e.target.value)} style={{ width: 80, accentColor: 'var(--primary)' }} />
        <span style={{ fontWeight: 700, color: variance > 0 ? 'var(--primary)' : 'var(--text-dim)', minWidth: 28, textAlign: 'right' }}>{variance}%</span>
      </label>
      <button onClick={applyGlobal} style={{ background: 'var(--card)', border: '1px solid var(--border)', borderRadius: 6, color: 'var(--text-muted)', padding: '4px 12px', fontSize: 12, cursor: 'pointer' }}>Apply Global</button>
      <button onClick={exportProjections} style={{ background: 'var(--card)', border: '1px solid var(--border)', borderRadius: 6, color: 'var(--text-muted)', padding: '4px 12px', fontSize: 12, cursor: 'pointer', marginLeft: 'auto' }}><Icon name="download" size={12}/> Projections CSV</button>
    </div>
    <div style={{ fontSize: 11, color: 'var(--text-dim)', marginBottom: 14, marginLeft: 2 }}>
      {mode === 'ceiling'
        ? <><Icon name="rocket" size={14} color="var(--primary)"/> <strong style={{ color: 'var(--primary)' }}>GPP:</strong> Builds for ceiling — best for big tournaments with many entries (your 39.6K-entry contest is GPP)</>
        : <><Icon name="dollar" size={14} color="var(--primary)"/> <strong style={{ color: 'var(--primary)' }}>Cash:</strong> Builds for consistent median — best for 50/50s and head-to-heads</>
      }
    </div>
    <SearchBar value={poolQ} onChange={setPoolQ} placeholder="Search fighters, opponents" total={sp.length} filtered={sp.filter(p => matchesSearch(p, poolQ)).length} />
    <div className="builder-controls">{sp.filter(p => matchesSearch(p, poolQ)).map(p => <div className="ctrl-row" key={p.name}>
      <span className="ctrl-name" style={{ flex: '1 1 0', minWidth: 60, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{p.name}</span>
      <span style={{ color: 'var(--text-dim)', fontSize: 11, width: 48, flexShrink: 0 }}>{fmtSal(p.salary)}</span>
      <span className="ctrl-proj" style={{ flexShrink: 0, width: 38, textAlign: 'right' }}>{mode === 'ceiling' ? fmt(p.ceil, 1) : fmt(p.proj, 1)}</span>
      <span style={{ color: (ownership[p.name] || 0) > 35 ? 'var(--amber)' : 'var(--text-dim)', fontSize: 11, width: 30, textAlign: 'right', flexShrink: 0 }}>{fmt(ownership[p.name] || 0, 0)}%</span>
      <input type="number" min="0" max="100" step="1" value={exp[p.name]?.min ?? globalMin} onChange={e => { const v = e.target.value; if (v === '') sE(p.name, 'min', ''); else sE(p.name, 'min', Math.max(0, Math.min(100, parseInt(v, 10) || 0))); }} onBlur={e => { if (e.target.value === '') sE(p.name, 'min', globalMin); }} title="Min %" style={{ width: 32, flexShrink: 0 }} />
      <input type="number" min="0" max="100" step="1" value={exp[p.name]?.max ?? globalMax} onChange={e => { const v = e.target.value; if (v === '') sE(p.name, 'max', ''); else sE(p.name, 'max', Math.max(0, Math.min(100, parseInt(v, 10) || 0))); }} onBlur={e => { if (e.target.value === '') sE(p.name, 'max', globalMax); }} title="Max %" style={{ width: 32, flexShrink: 0 }} />
    </div>)}</div>
    <button className="btn btn-primary" onClick={run} disabled={!canBuild || isBuilding}
      title={canBuild ? '' : `Edit at least 2 projections on the DK Projections tab first (${overrideCount}/2 changed)`}
      style={(!canBuild || isBuilding) ? { opacity: 0.4, cursor: 'not-allowed' } : undefined}>
      <Icon name="bolt" size={14}/> Build {nL} {mode === 'ceiling' ? 'GPP' : 'Cash'} Lineups{contrarianOn ? ' (Contrarian)' : ''}
    </button>
    {isBuilding && <BuildAnimation count={nL} />}
    {!isBuilding && res && <MMAExposureResults res={res} ownership={ownership} onRebuild={run} onExportDK={exportDK} onExportReadable={exportReadable} nL={nL} mode={res.mode} canBuild={canBuild} overrideCount={overrideCount} favoriteLineups={favoriteLineups} onToggleFavorite={toggleFavoriteLineup} prevRes={prevRes} onRestorePrev={() => { setRes(prevRes); setPrevRes(null); }} />}
  </>);
}

function MMAExposureResults({ res, ownership, onRebuild, onExportDK, onExportReadable, nL, mode, canBuild = true, overrideCount = 2, favoriteLineups = [], onToggleFavorite, prevRes, onRestorePrev }) {
  const [q, setQ] = useState('');
  // v3.24.14: MME set metrics — median/stdev cumulative ownership + unique players
  const setMetrics = useMemo(() => {
    if (!res.lineups.length || !res.pData) return null;
    const cumOwns = res.lineups.map(lu =>
      lu.players.reduce((s, i) => s + (ownership[res.pData[i].name] || 0), 0)
    );
    const sorted = [...cumOwns].sort((a, b) => a - b);
    const mid = Math.floor(sorted.length / 2);
    const median = sorted.length % 2 === 0 ? (sorted[mid - 1] + sorted[mid]) / 2 : sorted[mid];
    const mean = cumOwns.reduce((s, v) => s + v, 0) / cumOwns.length;
    const stdev = Math.sqrt(cumOwns.reduce((s, v) => s + (v - mean) ** 2, 0) / cumOwns.length);
    const uniqueCount = new Set(res.lineups.flatMap(lu => lu.players)).size;
    return { median, stdev, uniqueCount };
  }, [res, ownership]);
  // Favorites + filter (MMA is classic-only)
  const favoriteKeySet = useMemo(() => {
    const s = new Set();
    for (const fav of favoriteLineups) s.add((fav.players || []).slice().sort().join('|'));
    return s;
  }, [favoriteLineups]);
  const lineupHash = (lu) => lu.players.map(i => res.pData[i].name).sort().join('|');
  const [lineupFilters, setLineupFilters] = useState([]);
  const toggleFilter = (name) => setLineupFilters(prev => {
    const ex = prev.findIndex(f => f.name === name);
    return ex >= 0 ? prev.filter((_, i) => i !== ex) : [...prev, { name }];
  });
  const clearFilters = () => setLineupFilters([]);
  const matchesFilters = (lu) => {
    if (lineupFilters.length === 0) return true;
    const names = new Set(lu.players.map(i => res.pData[i].name));
    return lineupFilters.every(f => names.has(f.name));
  };
  const expData = useMemo(() => res.pData.map((p, i) => {
    const cnt = res.counts[i]; const pct = cnt / res.lineups.length * 100;
    const simOwn = ownership[p.name] || 0; const lev = Math.round((pct - simOwn) * 10) / 10;
    const score = mode === 'ceiling' ? p.ceiling : p.projection;
    const val = score / (p.salary / 1000);
    return { name: p.name, salary: p.salary, score, val, cnt, pct, simOwn, lev };
  }), [res, ownership, mode]);
  const expFiltered = useMemo(() => expData.filter(p => matchesSearch(p, q)), [expData, q]);
  const avgSal = Math.round(res.lineups.reduce((s, lu) => s + lu.sal, 0) / res.lineups.length);
  const projMax = res.lineups.length ? Math.max(...res.lineups.map(lu => lu.proj)) : 0;
  const projMin = res.lineups.length ? Math.min(...res.lineups.map(lu => lu.proj)) : 0;
  const avgOwn = Math.round(res.lineups.reduce((s, lu) => {
    const lineupOwn = lu.players.reduce((ss, pi) => ss + (ownership[res.pData[pi].name] || 0), 0) / lu.players.length;
    return s + lineupOwn;
  }, 0) / res.lineups.length);
  const { sorted, sortKey, sortDir, toggleSort } = useSort(expFiltered, 'pct', 'desc');
  const S = p => <SH {...p} sortKey={sortKey} sortDir={sortDir} onSort={toggleSort} />;
  return (<>
    <div className="oo-summary-bar" style={{ marginTop: 20, padding: '10px 14px', background: 'var(--card)', border: '1px solid var(--border)', borderRadius: 8, fontSize: 13, color: 'var(--text-muted)', display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: '4px 12px' }}>
      <span><Icon name="check" size={14} color="#22C55E"/> Built <span style={{ color: 'var(--primary-glow)', fontWeight: 700 }}>{res.lineups.length}</span> lineups ({mode === 'ceiling' ? 'GPP/ceiling' : 'cash/median'}) from {res.total.toLocaleString()} valid</span>
      <span className="oo-sum-sep" style={{ color: 'var(--text-dim)' }}>·</span>
      <span>Range: <span style={{ color: 'var(--green)' }}>{projMax}</span> → <span style={{ color: 'var(--text-dim)' }}>{projMin}</span></span>
      <span className="oo-sum-sep" style={{ color: 'var(--text-dim)' }}>·</span>
      <span>Avg Sal: <span style={{ color: 'var(--primary-glow)', fontWeight: 600 }}>${avgSal.toLocaleString()}</span></span>
      <span className="oo-sum-sep" style={{ color: 'var(--text-dim)' }}>·</span>
      <span>Avg Own: <span style={{ color: avgOwn > 30 ? 'var(--amber)' : 'var(--green)', fontWeight: 600 }}>{avgOwn}%</span></span>
      {setMetrics && <>
        <span className="oo-sum-sep" style={{ color: 'var(--text-dim)' }}>·</span>
        <span title="Median of sum-of-fighter-ownership across the lineup set">Median Own: <span style={{ color: 'var(--text)', fontWeight: 600 }}>{setMetrics.median.toFixed(0)}%</span></span>
        <span className="oo-sum-sep" style={{ color: 'var(--text-dim)' }}>·</span>
        <span title="Stdev of cumulative ownership across the set">Stdev: <span style={{ color: 'var(--text)', fontWeight: 600 }}>{setMetrics.stdev.toFixed(1)}</span></span>
        <span className="oo-sum-sep" style={{ color: 'var(--text-dim)' }}>·</span>
        <span title="Distinct fighters used across all lineups">Unique: <span style={{ color: 'var(--text)', fontWeight: 600 }}>{setMetrics.uniqueCount}</span></span>
      </>}
    </div>
    <div className="oo-action-row" style={{ display: 'flex', gap: 10, marginTop: 10, flexWrap: 'wrap' }}>
      {onRebuild && <button className="btn btn-primary" onClick={onRebuild} disabled={!canBuild}
        title={canBuild ? '' : `Edit at least 2 projections first (${overrideCount}/2 changed)`}
        style={{ flex: '1 1 auto', width: 'auto', ...(canBuild ? {} : { opacity: 0.4, cursor: 'not-allowed' }) }}><Icon name="bolt" size={14}/> Rebuild {nL}</button>}
      {prevRes && onRestorePrev && <button
        onClick={onRestorePrev}
        className="oo-restore-btn"
        title="Restore the previous lineup set (undo Rebuild)"
        style={{ flex: '0 1 auto', padding: '10px 18px', fontSize: 13, fontWeight: 600,
          background: 'var(--card)', color: 'var(--text-muted)',
          border: '1px solid var(--border-light)', borderRadius: 8, cursor: 'pointer',
          display: 'inline-flex', alignItems: 'center', gap: 6 }}>
        <svg viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M3 7v6h6"/><path d="M21 17a9 9 0 0 0-15-6.7L3 13"/></svg>
        Restore previous
      </button>}
      {onExportDK && <button className="btn btn-primary" onClick={onExportDK} style={{ flex: '1 1 auto', width: 'auto', background: 'linear-gradient(135deg, #15803D, #22C55E)' }}><Icon name="download" size={14}/> Download DK Upload CSV</button>}
      {onExportReadable && <button className="btn btn-outline" onClick={onExportReadable} style={{ flex: '1 1 auto', width: 'auto', marginTop: 0 }}><Icon name="download" size={14}/> Readable CSV</button>}
    </div>
    <div className="section-head" style={{ marginTop: 20 }}><Icon name="chart" size={16} color="#F5C518"/> Exposure</div>
    <SearchBar value={q} onChange={setQ} placeholder="Search exposure" total={expData.length} filtered={expFiltered.length} />
    <div className="table-wrap" style={{ marginBottom: 20 }}><table><thead><tr>
      <S label="Fighter" colKey="name" /><S label="Salary" colKey="salary" num /><S label={mode === 'ceiling' ? 'Ceiling' : 'Proj'} colKey="score" /><S label="Val" colKey="val" num /><S label="Count" colKey="cnt" num /><S label="Exposure" colKey="pct" num /><S label="Sim Own" colKey="simOwn" num tip="Projected field ownership (imported from Pool Own, or simulated if unavailable)" /><S label="Leverage" colKey="lev" num />
      <th title="Filter displayed lineups by this fighter" style={{ textAlign: 'center' }}>Filter</th>
    </tr></thead>
    <tbody>{sorted.map(p => {
      const filtered = lineupFilters.some(f => f.name === p.name);
      const btnStyle = (active) => ({ width: 24, height: 24, padding: 0, border: `1px solid ${active ? 'var(--primary)' : 'var(--border)'}`, background: active ? 'var(--primary)' : 'transparent', color: active ? '#0A1628' : 'var(--text-muted)', borderRadius: 4, cursor: 'pointer', fontSize: 11, fontWeight: 700, lineHeight: '22px' });
      return <tr key={p.name}>
        <td className="name">{p.name}</td><td className="num">${p.salary.toLocaleString()}</td><td className="num">{fmt(p.score, 1)}</td><td className="num">{fmt(p.val, 2)}</td><td className="num">{p.cnt}</td>
        <td><span className="exp-bar-bg"><span className="exp-bar" style={{ width: Math.min(p.pct, 100) + '%' }} /></span>{fmt(p.pct, 1)}%</td>
        <td className="num muted">{fmt(p.simOwn, 1)}%</td>
        <td className="num"><span style={{ color: p.lev > 0 ? 'var(--green)' : p.lev < 0 ? 'var(--red)' : 'var(--text-dim)', fontWeight: Math.abs(p.lev) > 10 ? 700 : 400 }}>{p.lev > 0 ? '+' : ''}{fmt(p.lev, 1)}%</span></td>
        <td style={{ textAlign: 'center' }}><button style={btnStyle(filtered)} onClick={() => toggleFilter(p.name)} title={filtered ? 'Remove filter' : `Show only lineups containing ${p.name}`}>F</button></td>
      </tr>;
    })}</tbody></table></div>

    {/* Filter chips */}
    {lineupFilters.length > 0 && (
      <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap', alignItems: 'center', padding: '8px 12px', background: 'var(--card)', border: '1px solid var(--border)', borderRadius: 8, marginBottom: 10 }}>
        <span style={{ fontSize: 11, fontWeight: 600, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: 0.5 }}>Filtering</span>
        {lineupFilters.map((f, i) => (
          <button key={i} onClick={() => toggleFilter(f.name)} style={{ padding: '3px 9px', background: 'transparent', border: '1px solid var(--primary)', color: 'var(--primary)', borderRadius: 999, fontSize: 11, fontWeight: 600, cursor: 'pointer', display: 'inline-flex', alignItems: 'center', gap: 5 }} title="Click to remove this filter">{f.name}<span style={{ opacity: 0.6 }}>✕</span></button>
        ))}
        <button onClick={clearFilters} style={{ marginLeft: 'auto', padding: '3px 9px', background: 'transparent', border: '1px solid var(--border)', color: 'var(--text-muted)', borderRadius: 4, fontSize: 11, fontWeight: 500, cursor: 'pointer' }}>Clear all</button>
      </div>
    )}

    <div className="section-head"><Icon name="target" size={16} color="#F5C518"/> Lineups{lineupFilters.length > 0 && <span style={{ fontSize: 12, color: 'var(--text-dim)', fontWeight: 500, marginLeft: 8 }}>· {res.lineups.filter(matchesFilters).length} matching</span>}</div>
    <div className="lineup-grid">{res.lineups.filter(matchesFilters).slice(0, 30).map((lu, idx) => {
      const ps = lu.players.map(i => res.pData[i]).sort((a, b) => b.salary - a.salary);
      const lineupAvgOwn = Math.round(ps.reduce((s, p) => s + (ownership[p.name] || 0), 0) / ps.length);
      const isFav = favoriteKeySet.has(lineupHash(lu));
      return <div className="lu-card" key={idx} style={isFav ? { borderColor: '#F5C518', boxShadow: '0 0 0 1px #F5C518 inset, 0 2px 8px rgba(245,197,24,0.15)' } : undefined}>
        <div className="lu-header">
          <span style={{ display: 'inline-flex', alignItems: 'center', gap: 6 }}>
            <button
              onClick={() => onToggleFavorite && onToggleFavorite(lu, res.pData)}
              title={isFav ? 'Unfavorite — will no longer persist through rebuilds' : 'Favorite this lineup — it survives rebuilds and is always exported'}
              style={{ width: 20, height: 20, padding: 0, border: 'none', background: 'transparent', cursor: 'pointer', color: isFav ? '#F5C518' : 'var(--text-dim)', fontSize: 14, lineHeight: 1 }}>
              {isFav ? '★' : '☆'}
            </button>
            <span>#{idx + 1}</span>
          </span>
          <span className="lu-proj">{lu.proj} pts</span>
        </div>
        {ps.map(p => {
          const ownPct = ownership[p.name] || 0;
          const scoreShown = mode === 'ceiling' ? p.ceiling : p.projection;
          return <div className="lu-row" key={p.name}><span className="lu-name">{p.name}</span><span className="lu-opp">vs {p.opponent}</span><span className="lu-sal">${p.salary.toLocaleString()}</span><span className="lu-pts">{fmt(scoreShown, 1)}</span><span style={{ width: 36, textAlign: 'right', color: ownPct > 35 ? 'var(--amber)' : 'var(--text-dim)', fontSize: 11 }}>{fmt(ownPct, 0)}%</span></div>;
        })}
        <div className="lu-footer"><span>${lu.sal.toLocaleString()}</span><span style={{ color: lineupAvgOwn > 30 ? 'var(--amber)' : 'var(--green)' }}>Avg: {lineupAvgOwn}%</span></div>
      </div>;
    })}</div>
    {(() => { const vis = res.lineups.filter(matchesFilters).length; return vis > 30 && <div style={{ textAlign: 'center', color: 'var(--text-dim)', fontSize: 13, marginTop: 8 }}>+ {vis - 30} more{lineupFilters.length > 0 && ' matching filters'}</div>; })()}
  </>);
}

// ═══════════════════════════════════════════════════════════════════════
// NBA COMPONENTS
// ═══════════════════════════════════════════════════════════════════════

// Team color badge
// NBA team palette — each team gets bg (subtle tint), fg (legible on dark),
// and br (border matching bg). Values pulled from official team primary
// colors and lightened for contrast on the navy surface.
const NBA_TEAM_COLORS = {
  OKC: { bg: 'rgba(245,140,10,0.18)',  fg: '#FFB648', br: 'rgba(245,140,10,0.5)'  }, // sunset orange
  PHX: { bg: 'rgba(159,73,172,0.18)',  fg: '#C99AD4', br: 'rgba(159,73,172,0.5)'  }, // valley purple
  DET: { bg: 'rgba(200,16,46,0.18)',   fg: '#F0607A', br: 'rgba(200,16,46,0.5)'   }, // Pistons red
  ORL: { bg: 'rgba(0,119,192,0.20)',   fg: '#5DAFE0', br: 'rgba(0,119,192,0.55)'  }, // Magic blue
  BOS: { bg: 'rgba(0,122,51,0.20)',    fg: '#4ADE80', br: 'rgba(0,122,51,0.55)'   }, // Celtics green
  LAL: { bg: 'rgba(85,37,131,0.20)',   fg: '#B99AD9', br: 'rgba(85,37,131,0.55)'  }, // Lakers purple
  GSW: { bg: 'rgba(29,66,138,0.22)',   fg: '#FFC94D', br: 'rgba(253,185,39,0.55)' }, // Warriors blue/gold
  MIA: { bg: 'rgba(152,0,46,0.22)',    fg: '#F57AA0', br: 'rgba(152,0,46,0.55)'   }, // Heat red
  DEN: { bg: 'rgba(13,34,64,0.35)',    fg: '#9BB4D9', br: 'rgba(13,34,64,0.65)'   }, // Nuggets navy
  MIN: { bg: 'rgba(15,36,80,0.30)',    fg: '#78BE20', br: 'rgba(120,190,32,0.55)' }, // Wolves green/navy
  NYK: { bg: 'rgba(0,107,182,0.22)',   fg: '#F58426', br: 'rgba(245,132,38,0.55)' }, // Knicks orange
  CLE: { bg: 'rgba(134,0,56,0.22)',    fg: '#F5B04E', br: 'rgba(134,0,56,0.55)'   }, // Cavs wine
  MIL: { bg: 'rgba(0,71,27,0.24)',     fg: '#EEE1C6', br: 'rgba(0,71,27,0.6)'     }, // Bucks green
  DAL: { bg: 'rgba(0,83,188,0.22)',    fg: '#5DAFE0', br: 'rgba(0,83,188,0.55)'   }, // Mavs blue
  HOU: { bg: 'rgba(206,17,65,0.20)',   fg: '#F07A95', br: 'rgba(206,17,65,0.55)'  }, // Rockets red
  MEM: { bg: 'rgba(93,118,169,0.22)',  fg: '#B8C4D9', br: 'rgba(93,118,169,0.55)' }, // Grizzlies navy
  SAC: { bg: 'rgba(91,43,130,0.22)',   fg: '#C99AD4', br: 'rgba(91,43,130,0.55)'  }, // Kings purple
  LAC: { bg: 'rgba(200,16,46,0.18)',   fg: '#F0607A', br: 'rgba(200,16,46,0.5)'   }, // Clippers red
  PHI: { bg: 'rgba(0,107,182,0.22)',   fg: '#5DAFE0', br: 'rgba(0,107,182,0.55)'  }, // 76ers blue
  TOR: { bg: 'rgba(206,17,65,0.20)',   fg: '#F07A95', br: 'rgba(206,17,65,0.55)'  }, // Raptors red
  IND: { bg: 'rgba(253,187,48,0.18)',  fg: '#FFD73D', br: 'rgba(253,187,48,0.55)' }, // Pacers gold
  ATL: { bg: 'rgba(225,68,52,0.20)',   fg: '#F58A75', br: 'rgba(225,68,52,0.55)'  }, // Hawks red
  CHI: { bg: 'rgba(206,17,65,0.20)',   fg: '#F07A95', br: 'rgba(206,17,65,0.55)'  }, // Bulls red
  WAS: { bg: 'rgba(0,43,92,0.30)',     fg: '#9BB4D9', br: 'rgba(0,43,92,0.60)'    }, // Wizards navy
  CHA: { bg: 'rgba(29,17,96,0.30)',    fg: '#A797D4', br: 'rgba(29,17,96,0.60)'   }, // Hornets purple
  BKN: { bg: 'rgba(40,40,40,0.40)',    fg: '#CFCFCF', br: 'rgba(90,90,90,0.60)'   }, // Nets black
  POR: { bg: 'rgba(224,58,62,0.20)',   fg: '#F58486', br: 'rgba(224,58,62,0.55)'  }, // Blazers red
  NOP: { bg: 'rgba(0,43,92,0.30)',     fg: '#9BB4D9', br: 'rgba(0,43,92,0.60)'    }, // Pelicans navy
  SAS: { bg: 'rgba(110,110,110,0.25)', fg: '#D4D4D4', br: 'rgba(110,110,110,0.55)' }, // Spurs silver
  UTA: { bg: 'rgba(0,43,92,0.28)',     fg: '#F5B04E', br: 'rgba(253,185,39,0.55)' }, // Jazz navy/gold
};
function TeamBadge({ team }) {
  const c = NBA_TEAM_COLORS[team] || { bg: 'rgba(120,120,120,0.15)', fg: 'var(--text-muted)', br: 'var(--border)' };
  return (
    <span style={{
      display: 'inline-block', padding: '1px 7px', borderRadius: 4,
      fontSize: 10, fontWeight: 700, letterSpacing: '0.04em',
      color: c.fg, background: c.bg, border: `1px solid ${c.br}`,
    }}>{team}</span>
  );
}

// Status chip — click cycles ACTIVE → GTD → Q → OUT → ACTIVE
const NBA_STATUS_CYCLE = ['ACTIVE', 'GTD', 'Q', 'OUT'];
function StatusChip({ status, onCycle }) {
  const s = (status || 'ACTIVE').toUpperCase();
  const style = {
    ACTIVE: { bg: 'rgba(74,222,128,0.12)', fg: 'var(--green-text)', br: 'rgba(74,222,128,0.4)' },
    GTD:    { bg: 'rgba(251,191,36,0.12)', fg: 'var(--amber-text)', br: 'rgba(251,191,36,0.5)' },
    Q:      { bg: 'rgba(251,146,60,0.15)', fg: '#FB923C',            br: 'rgba(251,146,60,0.5)' },
    OUT:    { bg: 'rgba(239,68,68,0.18)',  fg: 'var(--red-text)',    br: 'rgba(239,68,68,0.6)' },
  }[s] || { bg: 'var(--card)', fg: 'var(--text-muted)', br: 'var(--border)' };
  return (
    <button onClick={onCycle} title="Click to cycle status" style={{
      padding: '2px 8px', borderRadius: 4, fontSize: 10, fontWeight: 700, letterSpacing: '0.04em',
      color: style.fg, background: style.bg, border: `1px solid ${style.br}`, cursor: 'pointer',
    }}>{s}</button>
  );
}

// NBA DK Tab — projections, value, ownership, status, mins/usg, cascade
function NBADKTab({ players, gameInfo, own, cptOwn = {}, onOverride, overrides, lockedPlayers = [], excludedPlayers = [], onToggleLock, onToggleExclude, onClearLocks, onClearExcludes, cptLockedPlayers = [], flexLockedPlayers = [], cptExcludedPlayers = [], flexExcludedPlayers = [], onToggleCptLock, onToggleCptExclude, onToggleFlexLock, onToggleFlexExclude }) {
  const [statusMap, setStatusMap] = useState({});
  const [q, setQ] = useState('');
  // Per-slot display mode for showdown: 'all' shows total ownership + UTIL-priced salary/proj,
  // 'cpt' shows CPT-specific ownership + CPT-priced salary (1.5×) and 1.5× projection,
  // 'flex' shows flex-only ownership (total − CPT) + UTIL-priced salary/proj.
  // Lock/exclude buttons operate on the corresponding per-slot set when scope ≠ 'all'.
  const [dkScope, setDkScope] = useState('all');
  const cycleStatus = (name) => {
    setStatusMap(prev => {
      const cur = prev[name] ?? (players.find(p => p.name === name)?.status || 'ACTIVE');
      const idx = NBA_STATUS_CYCLE.indexOf(cur);
      return { ...prev, [name]: NBA_STATUS_CYCLE[(idx + 1) % NBA_STATUS_CYCLE.length] };
    });
  };
  const effStatus = (p) => statusMap[p.name] ?? (p.status || 'ACTIVE');

  const pw = useMemo(() => players.filter(p => p.salary > 0).map(p => {
    const s = effStatus(p);
    return {
      ...p,
      simOwn: own[p.name] || 0,
      cptOwnPct: cptOwn[p.name] || 0,
      flexOwnPct: Math.max(0, (own[p.name] || 0) - (cptOwn[p.name] || 0)),
      effStatus: s,
      isOut: s === 'OUT',
    };
  }), [players, own, cptOwn, statusMap]);
  const pwFiltered = useMemo(() => pw.filter(p => matchesSearch(p, q, ['name', 'team', 'opponent', 'positions_str'])), [pw, q]);

  const { sorted, sortKey, sortDir, toggleSort } = useSort(pwFiltered, 'proj', 'desc');

  const t3v = useMemo(() => [...pw].filter(p => !p.isOut && p.projectable).sort((a, b) => b.val - a.val).slice(0, 3).map(p => p.name), [pw]);
  const t3c = useMemo(() => [...pw].filter(p => !p.isOut && p.projectable).sort((a, b) => b.ceil - a.ceil).slice(0, 3).map(p => p.name), [pw]);
  // TRAP = highest field-simulated ownership, period.
  // Per GPP leverage theory, the chalky play is what we fade — even if they
  // have "good value". Good-value chalk is exactly where the field converges,
  // which is precisely what we're trying to differentiate from.
  // ═══════════════════════════════════════════════════════════════════════
  // NBA TRAP ALGORITHM v3.1 — "Biggest Trap" via ownership × value product.
  //
  //     bustScore = simOwn × val
  //     Hard filter: salary > $6,000 (primary only, not flex trap)
  //
  // Philosophy shift from v2: DK prices players for a reason. When a player
  // looks like elite value AND the field is already piled on, that's not
  // an edge — it's a tournament trap. Best-value chalk provides zero
  // leverage in a GPP; everyone has them, so you can't differentiate.
  //
  // $6K salary floor: a sub-$6K punt being "best-value chalk" isn't
  // actionable as a primary fade — you can't pivot off a punt, and their
  // projections can't carry a 1.5× CPT multiplier. Primary trap is
  // restricted to captain-worthy salaries. The secondaryTrap below has
  // no salary floor — it surfaces the next-best chalk trap at any price.
  //
  // Gate: simOwn ≥ 25% (must be actual chalk — a 10%-owned 7.0 val isn't
  //       a "trap," it's an edge play the field missed).
  //
  // Fallback: if nobody clears the 25% gate, use the full active pool
  // (same product ranking) so we always return someone.
  // ═══════════════════════════════════════════════════════════════════════
  const trap = useMemo(() => {
    const active = pw.filter(p => !p.isOut && p.projectable && (p.salary || 0) > 6000);
    if (active.length === 0) return '';
    const hasOwn = active.some(p => p.simOwn > 0);
    if (!hasOwn) {
      // No ownership data yet — fall back to highest projection
      return [...active].sort((a, b) => b.proj - a.proj)[0]?.name || '';
    }
    // Chalk gate — only players the field is meaningfully on
    const gated = active.filter(p => p.simOwn >= 25);
    const pool = gated.length > 0 ? gated : active;
    // Rank by ownership × value (both must be high to score high)
    const sorted = [...pool].sort((a, b) => {
      const aScore = a.simOwn * (a.val || 0);
      const bScore = b.simOwn * (b.val || 0);
      return bScore - aScore;
    });
    return sorted[0]?.name || '';
  }, [pw]);
  // ═══════════════════════════════════════════════════════════════════════
  // SECONDARY TRAP (v3.3) — continues down the Primary Trap list, skipping
  // anyone already selected as Primary Trap / Gem Primary / Gem Pivot.
  // Same gates as Primary Trap: simOwn ≥ 25%, salary > $6000.
  // Formula: max(simOwn × val) among eligible remaining players.
  // Computed AFTER `gem` so we can exclude gem.primary / gem.pivot.
  // ═══════════════════════════════════════════════════════════════════════
  // ═══════════════════════════════════════════════════════════════════════
  // GEM ALGORITHM — NBA v3 (slate-type aware, dual-pivot)
  //
  // Showdown: position overlap is IGNORED because any player can be
  //   slotted anywhere (CPT + 5 FLEX). A center like Holmgren absorbs a
  //   PG's usage just as effectively as a wing via scoring output, since
  //   the roster has no positional constraint.
  // Classic (future): position overlap applies — the replacer has to
  //   actually slot into a position the trap would've filled.
  //
  // Pivot logic:
  //   - If #2 overall candidate scores within 10% of primary AND is same
  //     track, show it as pivot (two tied replacers or two tied values).
  //     This catches the Chet/Jalen situation.
  //   - Otherwise, show best candidate from the OTHER track as pivot
  //     (replacer-vs-value diversification), if ≥ 25% of primary.
  //   - If neither qualifies, no pivot — primary stands alone.
  // ═══════════════════════════════════════════════════════════════════════
  const gem = useMemo(() => {
    const trapP = pw.find(p => p.name === trap);
    if (!trapP) return { primary: null, pivots: [] };
    // Exclude only Primary Trap from gem consideration. Secondary Trap is
    // now computed AFTER gems (continues down the Primary Trap list past
    // whoever is already selected as Primary/Gem Primary/Gem Pivot).
    const active = pw.filter(p => p.projectable && !p.isOut && p.name !== trap);
    if (active.length === 0) return { primary: null, pivots: [] };

    // Default to showdown — when classic slates are built this flips based
    // on props / slate metadata.
    const slateType = 'showdown';
    const isShowdown = slateType === 'showdown';

    const trapUsageFactor = Math.max(0.3, Math.min(1.0, (trapP.proj || 0) / 35));
    const bucketOf = (s) => {
      const str = String(s || '');
      if (/C|PF/.test(str)) return 'big';
      if (/SF/.test(str)) return 'wing';
      return 'guard';
    };
    const positionOverlap = (a, b) => {
      const ab = bucketOf(a), bb = bucketOf(b);
      if (ab === bb) return 1.0;
      if ((ab === 'guard' && bb === 'big') || (ab === 'big' && bb === 'guard')) return 0.25;
      return 0.55;
    };

    // Showdown gem (v3): next-best-value player with salary > $6,000.
    // Simpler than the salary-band approach — just the highest-value captain-
    // worthy player who isn't the trap or stud. The $6,000 floor excludes
    // punt plays / min-salary fillers (which may have great raw val but aren't
    // genuine CPT pivots since their projections can't carry a 1.5× multiplier
    // alone). Validated across the 3 reference slates:
    //
    //   POR@SAS — Deni (~4.07 val) wins over Castle (~3.57) ✓ (winning CPT)
    //   PHX@OKC — Chet (~4.06 val) wins over Booker (~3.43)  ✓ (winning CPT)
    //   ORL@DET — highest-val >$6k among pivots
    //
    // Classic NBA stays on the legacy position-aware scoring below since
    // "pivot" there is position-constrained rather than CPT-focused.
    if (isShowdown) {
      const pool = active.filter(p => (p.val || 0) > 0 && p.salary > 6000);
      const byVal = [...pool].sort((a, b) => (b.val || 0) - (a.val || 0));
      if (byVal.length === 0) return { primary: null, pivots: [] };
      const isReplacerLabel = (p) => p.team === trapP.team && p.salary >= 3000;
      const primary = { name: byVal[0].name, kind: isReplacerLabel(byVal[0]) ? 'replacer' : 'value' };

      // Pivot (NBA v3.3): owned-but-not-chalky leverage play, ranked by
      // ceiling-value (ceil per $1K). Finds the best bang-for-buck ceiling
      // among players the field is dabbling on but hasn't piled into.
      //   • not a trap (handled by `active` filter upstream)
      //   • not the primary gem
      //   • 6% ≤ simOwn < 25% (on the field's radar but not chalk)
      //   • salary ≥ $1500 (nominal floor)
      //   • ranked by ceil / (salary/1000) desc — ceiling-adjusted value
      // If no player clears the ownership band, no pivot is shown (cleaner
      // than a silent fallback that obscures the intent).
      // Pivot #1: 6-34% simOwn pool (chalk-leaning leverage pivots allowed)
      // Pivot #2: narrower 6-27% simOwn pool (traditional low-owned leverage)
      const pivotPool1 = active.filter(p =>
        p.name !== primary.name &&
        (p.simOwn || 0) >= 6 &&
        (p.simOwn || 0) < 34 &&
        (p.salary || 0) >= 2700 &&
        (p.ceil || 0) > 0
      );
      const ceilVal = (p) => (p.ceil || 0) / Math.max(1, (p.salary || 0) / 1000);
      const pivot1 = [...pivotPool1].sort((a, b) => ceilVal(b) - ceilVal(a))[0];
      const pivotPool2 = active.filter(p =>
        p.name !== primary.name &&
        (!pivot1 || p.name !== pivot1.name) &&
        (p.simOwn || 0) >= 6 &&
        (p.simOwn || 0) < 27 &&
        (p.salary || 0) >= 2700 &&
        (p.ceil || 0) > 0
      );
      const pivot2 = [...pivotPool2].sort((a, b) => ceilVal(b) - ceilVal(a))[0];
      const pivots = [pivot1, pivot2].filter(Boolean).map(p => ({
        name: p.name,
        kind: isReplacerLabel(p) ? 'replacer' : 'value'
      }));
      return { primary, pivots };
    }

    const scored = active.map(p => {
      const leverage = Math.max(0, (trapP.simOwn || 0) - (p.simOwn || 0));
      const levBoost = 1 + leverage * 0.012;

      // Track A — value-adjacent (any team, rewarded for price proximity)
      const bandClose = p.salary >= trapP.salary - 2000 && p.salary <= trapP.salary + 500;
      const valueScore = (p.val || 0) * (p.ceil || 0) * (bandClose ? 1.0 : 0.55) * levBoost;

      // Track B — same-team replacer (position overlap gated by slate type)
      let replacerScore = 0;
      const isReplacer = p.team === trapP.team && p.salary >= 3000;
      if (isReplacer) {
        const overlap = isShowdown ? 1.0 : positionOverlap(p.positions_str, trapP.positions_str);
        const overlapBoost = isShowdown ? 1.0 : (1 + overlap * 0.55);
        replacerScore = (p.ceil || 0) * (p.val || 0)
          * overlapBoost
          * (1 + trapUsageFactor * 0.60)
          * levBoost;
      }
      return { name: p.name, valueScore, replacerScore, overall: Math.max(valueScore, replacerScore) };
    });

    const byOverall = [...scored].sort((a, b) => b.overall - a.overall);
    const top = byOverall[0];
    if (!top || top.overall <= 0) return { primary: null, pivots: [] };
    const primaryKind = top.replacerScore > top.valueScore ? 'replacer' : 'value';
    const primary = { name: top.name, kind: primaryKind, score: top.overall };

    // Pivot #1: 6-34% simOwn pool; Pivot #2: narrower 6-27% pool
    const pivotPool1 = active.filter(p =>
      p.name !== primary.name &&
      (p.simOwn || 0) >= 6 &&
      (p.simOwn || 0) < 34 &&
      (p.salary || 0) >= 2700 &&
      (p.ceil || 0) > 0
    );
    const ceilValC = (p) => (p.ceil || 0) / Math.max(1, (p.salary || 0) / 1000);
    const pivot1 = [...pivotPool1].sort((a, b) => ceilValC(b) - ceilValC(a))[0];
    const pivotPool2 = active.filter(p =>
      p.name !== primary.name &&
      (!pivot1 || p.name !== pivot1.name) &&
      (p.simOwn || 0) >= 6 &&
      (p.simOwn || 0) < 27 &&
      (p.salary || 0) >= 2700 &&
      (p.ceil || 0) > 0
    );
    const pivot2 = [...pivotPool2].sort((a, b) => ceilValC(b) - ceilValC(a))[0];
    const pivots = [pivot1, pivot2].filter(Boolean).map(p => {
      const pivotScored = scored.find(s => s.name === p.name);
      const nextKind = pivotScored && pivotScored.replacerScore > pivotScored.valueScore ? 'replacer' : 'value';
      return { name: p.name, kind: nextKind };
    });
    return { primary, pivots };
  }, [pw, trap]);
  const gemName = gem.primary?.name || '';
  const gemKind = gem.primary?.kind || '';
  const pivotName = gem.pivots?.[0]?.name || '';
  const pivotKind = gem.pivots?.[0]?.kind || '';
  const pivot2Name = gem.pivots?.[1]?.name || '';
  const pivot2Kind = gem.pivots?.[1]?.kind || '';

  // Secondary Trap — computed AFTER gem so we can skip players already
  // selected as Gem Primary / Gem Pivot (and the Primary Trap itself).
  // v3.15: simOwn ≥ 30% + max(simOwn × val). No salary gate — chalk at
  // any price can surface (e.g., sub-$6K mid-salary punts the field is
  // piling onto also qualify).
  const secondaryTrap = useMemo(() => {
    const excluded = new Set([trap, gem.primary?.name, gem.pivots?.[0]?.name, gem.pivots?.[1]?.name].filter(Boolean));
    const pool = pw.filter(p =>
      !p.isOut && p.projectable &&
      !excluded.has(p.name) &&
      (p.simOwn || 0) >= 30 &&
      (p.val || 0) > 0
    );
    if (pool.length === 0) return '';
    const sorted = [...pool].sort((a, b) => ((b.simOwn || 0) * (b.val || 0)) - ((a.simOwn || 0) * (a.val || 0)));
    return sorted[0]?.name || '';
  }, [pw, trap, gem]);

  const S = p => <SH {...p} sortKey={sortKey} sortDir={sortDir} onSort={toggleSort} />;

  // Cascade removed — projections come from static DK prop lines only.
  // Status cycler still exists so the user can flag players OUT to exclude
  // them from the builder / ownership sim.
  const unprojectablePlayers = pw.filter(p => !p.projectable && !p.isOut);

  return (<>
    <div className="metrics">
      <div className="metric"><div className="metric-label"><Icon name="trophy" size={13}/> Top Value</div><div className="metric-value">{t3v.map((n, i) => { const p = players.find(x => x.name === n); return <div key={i} style={{ fontSize: i === 0 ? '16px' : '13px', color: i === 0 ? undefined : 'var(--text-muted)' }}>{i + 1}. {n} <span style={{ fontSize: 11, color: 'var(--text-dim)' }}>{fmt(p?.val, 2)}</span></div>; })}</div></div>
      <div className="metric"><div className="metric-label"><Icon name="rocket" size={13}/> Top Ceiling</div><div className="metric-value">{t3c.map((n, i) => { const p = players.find(x => x.name === n); return <div key={i} style={{ fontSize: i === 0 ? '16px' : '13px', color: i === 0 ? undefined : 'var(--text-muted)' }}>{n} <span style={{ fontSize: 11, color: 'var(--text-dim)' }}>{fmt(p?.ceil, 1)}</span></div>; })}</div></div>
      <div className="metric">
        <div className="metric-label"><Icon name="gem" size={13}/> Hidden Gem</div>
        <div className="metric-value" style={{ color: 'var(--green-text)' }}>{gemName || '-'}</div>
        <div className="metric-sub">Next-best captain value (&gt; $6K)</div>
        {pivotName && (
          <div style={{ marginTop: 6, paddingTop: 6, borderTop: '1px dashed var(--border)', fontSize: 11, color: 'var(--text-dim)' }}>
            or pivot: <span style={{ color: 'var(--text-muted)' }}>{pivotName}</span>
            {pivot2Name && <> / <span style={{ color: 'var(--text-muted)' }}>{pivot2Name}</span></>}
          </div>
        )}
      </div>
      <div className="metric">
        <div className="metric-label"><Icon name="bomb" size={13}/> Biggest Trap</div>
        <div className="metric-value" style={{ color: 'var(--red-text)' }}>{trap || '-'}</div>
        <div className="metric-sub">Who the field needs most</div>
        {secondaryTrap && (
          <div style={{ marginTop: 6, paddingTop: 6, borderTop: '1px dashed var(--border)', fontSize: 11, color: 'var(--text-dim)' }}>
            or: <span style={{ color: 'var(--text-muted)' }}>{secondaryTrap}</span> <span style={{ fontSize: 10 }}>(sim ≥30%)</span>
          </div>
        )}
      </div>
    </div>
    {unprojectablePlayers.length > 0 && (
      <div style={{ padding: '10px 14px', marginBottom: 12, background: 'rgba(251,191,36,0.08)', border: '1px solid rgba(251,191,36,0.35)', borderRadius: 8, fontSize: 12, color: 'var(--text-muted)', display: 'flex', gap: 10, flexWrap: 'wrap', alignItems: 'center' }}>
        <Icon name="warning" size={14} color="#FBBF24"/>
        <strong style={{ color: 'var(--amber-text)' }}>{unprojectablePlayers.length} players have no DraftKings prop line</strong>
        <span style={{ color: 'var(--text-dim)' }}>— excluded from projections and builder. They remain visible below marked <span style={{ color: 'var(--text-muted)' }}>No Line</span>.</span>
      </div>
    )}
    {gameInfo && (() => {
      const hk = (gameInfo.home || '').toLowerCase();
      const ak = (gameInfo.away || '').toLowerCase();
      const spread   = gameInfo[`spread_${hk}`];
      const paceH    = gameInfo[`pace_${hk}`];
      const paceA    = gameInfo[`pace_${ak}`];
      const blowoutH = gameInfo[`blowout_risk_${hk}`];
      const fav      = Number.isFinite(spread) ? (spread < 0 ? gameInfo.home : gameInfo.away) : null;
      const hasSpread  = Number.isFinite(spread);
      const hasTotal   = Number.isFinite(gameInfo.total);
      const hasPace    = Number.isFinite(paceH) && Number.isFinite(paceA);
      const hasBlowout = Number.isFinite(blowoutH);
      return (
        <div style={{ padding: '8px 12px', marginBottom: 12, background: 'var(--card)', border: '1px solid var(--border)', borderRadius: 6, fontSize: 12, color: 'var(--text-muted)', display: 'flex', gap: 14, flexWrap: 'wrap' }}>
          <span>{gameInfo.away} @ {gameInfo.home}</span>
          <span className="oo-sum-sep" style={{ color: 'var(--text-dim)' }}>·</span>
          <span>Spread: <strong style={{ color: 'var(--text)' }}>{hasSpread ? `${fav} ${Math.abs(spread)}` : '—'}</strong></span>
          <span className="oo-sum-sep" style={{ color: 'var(--text-dim)' }}>·</span>
          <span>Total: <strong style={{ color: 'var(--text)' }}>{hasTotal ? gameInfo.total : '—'}</strong></span>
          <span className="oo-sum-sep" style={{ color: 'var(--text-dim)' }}>·</span>
          <span>Pace: <strong style={{ color: 'var(--text)' }}>{hasPace ? ((paceH + paceA) / 2).toFixed(1) : '—'}</strong></span>
          <span className="oo-sum-sep" style={{ color: 'var(--text-dim)' }}>·</span>
          <span title="Probability the game becomes a blowout — affects starter minutes">Blowout risk: <strong style={{ color: hasBlowout && blowoutH > 0.6 ? 'var(--amber)' : 'var(--text)' }}>{hasBlowout ? `${Math.round(blowoutH * 100)}%` : '—'}</strong></span>
        </div>
      );
    })()}
    <SearchBar value={q} onChange={setQ} placeholder="Search players, teams, positions" total={pw.length} filtered={pwFiltered.length} />

    {/* Scope tabs — All / CPT / FLEX. Mirrors the builder's exposure scope
        pattern. Switches which salary, projection, and ownership values are
        shown in the table, and which lock/exclude set is affected when the
        user clicks the lock or exclude button on a row. */}
    <div className="oo-nba-scope-wrap" style={{ display: 'flex', gap: 10, alignItems: 'center', marginBottom: 10, padding: '8px 12px', background: 'var(--card)', border: '1px solid var(--border)', borderRadius: 8 }}>
      <style>{`
        .oo-nba-dkscope-btn { padding: 5px 12px; background: var(--bg); border: 1px solid var(--border); color: var(--text-muted); border-radius: 4px; cursor: pointer; font-size: 12px; font-weight: 600; transition: all 0.15s; }
        .oo-nba-dkscope-btn:hover { color: var(--text); border-color: var(--border-light); }
        .oo-nba-dkscope-btn.active { background: var(--primary); color: #0A1628; border-color: var(--primary); }
      `}</style>
      <span style={{ fontSize: 11, fontWeight: 600, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: 0.5 }}>View</span>
      <div style={{ display: 'flex', gap: 4 }}>
        {[
          { k: 'all',  label: 'All' },
          { k: 'cpt',  label: 'Captain' },
          { k: 'flex', label: 'Flex' },
        ].map(({ k, label }) => (
          <button key={k} className={`oo-nba-dkscope-btn ${dkScope === k ? 'active' : ''}`} onClick={() => setDkScope(k)}>{label}</button>
        ))}
      </div>
      <span style={{ fontSize: 11, color: 'var(--text-dim)', marginLeft: 'auto' }}>
        {dkScope === 'all'  && 'All-slot view · lock/exclude applies to any slot'}
        {dkScope === 'cpt'  && 'Captain view — salary/proj ×1.5 · lock/exclude targets CPT slot only'}
        {dkScope === 'flex' && 'Flex view — flex-only ownership · lock/exclude targets UTIL slot only'}
      </span>
    </div>

    {/* Per-scope LockBar: shows the lock/exclude pills for the active scope.
        Helps users see at a glance what they've locked/excluded at each slot level. */}
    {dkScope === 'all' && <LockBar lockedPlayers={lockedPlayers} excludedPlayers={excludedPlayers} onToggleLock={onToggleLock} onToggleExclude={onToggleExclude} onClearLocks={onClearLocks} onClearExcludes={onClearExcludes} />}
    {dkScope === 'cpt'  && <LockBar lockedPlayers={cptLockedPlayers}  excludedPlayers={cptExcludedPlayers}  onToggleLock={onToggleCptLock}  onToggleExclude={onToggleCptExclude} />}
    {dkScope === 'flex' && <LockBar lockedPlayers={flexLockedPlayers} excludedPlayers={flexExcludedPlayers} onToggleLock={onToggleFlexLock} onToggleExclude={onToggleFlexExclude} />}

    <div className="table-wrap"><table><thead><tr>
      <th>#</th><th></th><S label="Player" colKey="name" /><th>Team</th><th>Pos</th>
      <S label="Sal" colKey="salary" num />
      <S label="Sim Own" colKey="simOwn" num tip="Projected field ownership (imported from Pool Own, or simulated if unavailable)" />
      <S label="CPT %" colKey="cptOwnPct" num />
      <S label="Proj" colKey="proj" num /><S label="Ceil" colKey="ceil" num />
      <S label="Val" colKey="val" num /><S label="CVal" colKey="cval" num />
      <S label="Min" colKey="projMins" num />
      <th title="Source of projection">Src</th>
      <th>Status</th>
      <th></th>
    </tr></thead>
    <tbody>{sorted.map((p, i) => {
      const iv = t3v.includes(p.name), ic = t3c.includes(p.name);
      const ig = p.name === gemName;
      const ip = (p.name === pivotName && pivotName !== '') || (p.name === pivot2Name && pivot2Name !== '');
      const it = p.name === trap;
      const badges = [];
      if (iv) badges.push({ icon: 'trophy', label: 'Top 3 Value' });
      if (ic) badges.push({ icon: 'rocket', label: 'Top 3 Ceiling' });
      if (ig) badges.push({ icon: 'gem',    label: 'Hidden Gem' });
      if (ip) badges.push({ icon: 'gem',    label: 'Gem pivot' });
      if (it) badges.push({ icon: 'bomb',   label: 'Trap' });
      const isOver = overrides && overrides[p.name] != null;
      const dimStyle = p.isOut || !p.projectable ? { opacity: p.isOut ? 0.4 : 0.6 } : {};
      const noLine = !p.projectable;
      return <tr key={p.name} className={ig ? 'row-hl-green' : ip ? 'row-hl-green' : it ? 'row-hl-red' : ''} style={dimStyle}>
        <td className="muted">{i + 1}</td>
        <td><span style={{ display: 'inline-flex', gap: 4, alignItems: 'center' }}>{badges.map((bd, j) => <Tip key={j} icon={bd.icon} label={bd.label} size={14} />)}</span></td>
        <td className="name">
          {p.name}
          {noLine && <span style={{ marginLeft: 6, padding: '1px 6px', borderRadius: 3, fontSize: 9, fontWeight: 700, background: 'rgba(251,191,36,0.15)', color: 'var(--amber-text)', border: '1px solid rgba(251,191,36,0.4)' }}>NO LINE</span>}
        </td>
        <td><TeamBadge team={p.team} /></td>
        <td style={{ fontSize: 11, color: 'var(--text-muted)' }}>{p.positions_str}</td>
        <td className="num">{dkScope === 'cpt' ? fmtSal(p.cpt_salary || p.salary * 1.5) : fmtSal(p.salary)}</td>
        <td className="num" style={{ color: p.simOwn > 40 ? 'var(--red-text)' : p.simOwn > 25 ? 'var(--amber)' : 'var(--text-muted)' }}>{noLine ? '—' : (dkScope === 'cpt' ? fmt(p.cptOwnPct, 1) : dkScope === 'flex' ? fmt(p.flexOwnPct, 1) : fmt(p.simOwn, 1)) + '%'}</td>
        <td className="num" title="Captain-specific ownership in top 1500 lineups" style={{ color: p.cptOwnPct > 30 ? 'var(--red-text)' : p.cptOwnPct > 15 ? 'var(--amber)' : 'var(--text-dim)', fontWeight: p.cptOwnPct > 20 ? 600 : 400 }}>{noLine ? '—' : fmt(p.cptOwnPct, 1) + '%'}</td>
        <td className="num">
          {noLine ? <span style={{ color: 'var(--text-dim)' }}>—</span> : (
            <span className={iv ? 'cell-top3' : 'cell-proj'}>
              <input type="number" step="0.1" className={`proj-edit ${isOver ? 'overridden' : ''}`}
                value={isOver ? overrides[p.name] : (p.proj != null ? (dkScope === 'cpt' ? (p.proj * 1.5).toFixed(1) : p.proj) : '')}
                onChange={e => onOverride && onOverride(p.name, e.target.value)}
                onDoubleClick={() => onOverride && onOverride(p.name, null)}
                title={isOver ? 'Overridden — double-click to reset' : dkScope === 'cpt' ? 'CPT projection shown as proj × 1.5 — editing overrides the base proj' : 'Click to edit'} />
            </span>
          )}
        </td>
        <td className="num">{noLine ? <span style={{ color: 'var(--text-dim)' }}>—</span> : <span style={{ background: 'rgba(74,222,128,0.1)', color: 'var(--green)', padding: '3px 8px', borderRadius: 4, fontWeight: 600, fontSize: 12 }}>{fmt(p.ceil, 1)}</span>}</td>
        <td className="num">{noLine ? '—' : <span className={iv ? 'cell-top3' : ''}>{fmt(p.val, 2)}</span>}</td>
        <td className="num" style={{ color: p.cval > 5 ? 'var(--green)' : undefined, fontWeight: p.cval > 5 ? 700 : 400 }}>{noLine ? '—' : fmt(p.cval, 2)}</td>
        <td className="num">{fmt(p.projMins, 1)}</td>
        <td className="num muted" title="Informational — minutes are not used in projections (DK lines already price them in)">{noLine ? '—' : 'DK'}</td>
        <td><StatusChip status={p.effStatus} onCycle={() => cycleStatus(p.name)} /></td>
        <td style={{ textAlign: 'right', paddingRight: 10 }}>
          <LockExcludeButtons
            name={p.name}
            isLocked={(dkScope === 'cpt' ? cptLockedPlayers : dkScope === 'flex' ? flexLockedPlayers : lockedPlayers).includes(p.name)}
            isExcluded={(dkScope === 'cpt' ? cptExcludedPlayers : dkScope === 'flex' ? flexExcludedPlayers : excludedPlayers).includes(p.name)}
            onToggleLock={dkScope === 'cpt' ? onToggleCptLock : dkScope === 'flex' ? onToggleFlexLock : onToggleLock}
            onToggleExclude={dkScope === 'cpt' ? onToggleCptExclude : dkScope === 'flex' ? onToggleFlexExclude : onToggleExclude}
          />
        </td>
      </tr>; })}</tbody></table></div>
  </>);
}

// NBA PP Tab — stat-by-stat EV vs PP lines
function NBAPPTab({ rows }) {
  const [q, setQ] = useState('');
  const rowsFiltered = useMemo(() => rows.filter(r => matchesSearch(r, q, ['player', 'team', 'opponent'])), [rows, q]);
  const { sorted, sortKey, sortDir, toggleSort } = useSort(rowsFiltered, 'ev', 'desc');
  const S = p => <SH {...p} sortKey={sortKey} sortDir={sortDir} onSort={toggleSort} />;
  const best = useMemo(() => [...rows].sort((a, b) => b.ev - a.ev).slice(0, 3), [rows]);
  const worst = useMemo(() => [...rows].sort((a, b) => a.ev - b.ev).slice(0, 3), [rows]);
  return (<>
    <div className="section-hero">
      <div className="section-hero-icon-wrap">
        <svg className="section-hero-icon" viewBox="0 0 24 24" fill="none" stroke="#F5C518">
          <circle cx="12" cy="12" r="9"/>
          <circle cx="12" cy="12" r="5"/>
          <circle cx="12" cy="12" r="1.5" fill="#F5C518" stroke="none"/>
        </svg>
      </div>
      <div className="section-hero-text">
        <h2 className="section-hero-title">PrizePicks Fantasy Score</h2>
        <div className="section-hero-sub">DK-devigged projection vs PP Fantasy Score line · Edge = Projected − PP Line</div>
      </div>
    </div>
    <div className="metrics">
      <div className="metric"><div className="metric-label"><Icon name="flame" size={13}/> Best Edge</div><div className="metric-value">{best.map((r, i) => <div key={i} style={{ fontSize: i === 0 ? '16px' : '13px', color: i === 0 ? 'var(--green-text)' : 'var(--text-muted)', fontWeight: i === 0 ? 700 : 500 }}>{r.player} <span style={{fontSize:11, color: i === 0 ? undefined : 'var(--text-dim)'}}>{r.ev > 0 ? '+' : ''}{fmt(r.ev, 2)}</span></div>)}</div></div>
      <div className="metric"><div className="metric-label"><Icon name="trending-down" size={13}/> Biggest Fades</div><div className="metric-value">{worst.map((r, i) => <div key={i} style={{ fontSize: i === 0 ? '16px' : '13px', color: i === 0 ? 'var(--red-text)' : 'var(--text-muted)', fontWeight: i === 0 ? 700 : 500 }}>{r.player} <span style={{fontSize:11, color: i === 0 ? undefined : 'var(--text-dim)'}}>{fmt(r.ev, 2)}</span></div>)}</div></div>
    </div>
    <SearchBar value={q} onChange={setQ} placeholder="Search players, teams" total={rows.length} filtered={rowsFiltered.length} />
    <div className="table-wrap"><table><thead><tr>
      <th>#</th><th></th><S label="Player" colKey="player" /><th>Team</th>
      <S label="PP Line" colKey="line" num />
      <S label="Our Proj" colKey="projected" num /><S label="Edge" colKey="ev" num />
      <S label="Play" colKey="direction" />
    </tr></thead>
    <tbody>{sorted.map((r, i) => {
      const isBest = best.some(t => t.player === r.player && t.stat === r.stat);
      const isWorst = worst.some(t => t.player === r.player && t.stat === r.stat);
      // Push-zone: |ev| < 1 → amber instead of hard green/red
      const pushZone = Math.abs(r.ev) < 1;
      return <tr key={r.player + r.stat} className={isBest ? 'row-hl-green' : isWorst ? 'row-hl-red' : ''}>
        <td className="muted">{i + 1}</td>
        <td>{isBest ? <Tip icon="flame" label="Best edge" /> : isWorst ? <Tip icon="trending-down" label="Fade" /> : ''}</td>
        <td className="name">{r.player}</td>
        <td>{r.team ? <TeamBadge team={r.team} /> : ''}</td>
        <td className="num">{fmt(r.line, 1)}</td>
        <td className="num"><span className="cell-proj">{fmt(r.projected, 2)}</span></td>
        <td className="num"><span className={isBest ? 'cell-ev-top' : isWorst ? 'cell-ev-worst' : pushZone ? 'cell-ev-push' : r.ev > 0 ? 'cell-ev-pos' : 'cell-ev-neg'} style={pushZone ? { color: 'var(--amber-text)' } : undefined}>{r.ev > 0 ? '+' : ''}{fmt(r.ev, 2)}</span></td>
        <td><span style={{ color: r.direction === 'MORE' ? 'var(--green-text)' : r.direction === 'LESS' ? 'var(--red-text)' : 'var(--text-dim)', fontWeight: 600 }}>{r.direction}</span></td>
      </tr>;
    })}</tbody></table></div>
  </>);
}

// NBA Builder Tab — contrarian lineup building for showdown or classic
function NBABuilderTab({ players: rp, ownership, cptOwnership = {}, slateType, gameInfo, lockedPlayers = [], excludedPlayers = [], cptLockedPlayers = [], flexLockedPlayers = [], cptExcludedPlayers = [], flexExcludedPlayers = [], onGoToProjections }) {
  const [exp, setExp] = useState({});
  const [res, setRes] = useState(null);
  const [prevRes, setPrevRes] = useState(null);              // v3.24.14: undo support for Rebuild
  const [isBuilding, setIsBuilding] = useState(false);       // v3.24.13: build animation gate
  const [nL, setNL] = useState(20);
  const [variance, setVariance] = useState(2);
  const [globalMax, setGlobalMax] = useState(100);
  const [globalMin, setGlobalMin] = useState(0);
  const [contrarianOn, setContrarianOn] = useState(false);
  const [contrarianStrength, setContrarianStrength] = useState(0.6);
  // 'all' | 'cpt' | 'flex' — which slot type the min/max inputs currently target
  const [expScope, setExpScope] = useState('all');
  const [poolQ, setPoolQ] = useState('');
  const isShowdown = (slateType || 'showdown') === 'showdown';

  // Favorited lineups — stored as name-based tuples so they survive rebuilds
  // (player indices change between builds when the active pool changes).
  // Each entry: { cpt, utils[], proj, sal } for showdown OR { players[], proj, sal } for classic.
  // After each Build, these are merged into the front of res.lineups so they
  // always appear in the display and get included in CSV exports.
  const [favoriteLineups, setFavoriteLineups] = useState([]);
  const favoriteKey = (fav) => fav.cpt !== undefined
    ? `CPT:${fav.cpt}|${[...fav.utils].sort().join('|')}`
    : [...(fav.players || [])].sort().join('|');
  const toggleFavoriteLineup = useCallback((lu, pData) => {
    const fav = isShowdown
      ? { cpt: pData[lu.cpt].name, utils: lu.utils.map(i => pData[i].name), proj: lu.proj, sal: lu.sal }
      : { players: lu.players.map(i => pData[i].name), proj: lu.proj, sal: lu.sal };
    const key = favoriteKey(fav);
    setFavoriteLineups(prev => {
      const exists = prev.some(f => favoriteKey(f) === key);
      return exists ? prev.filter(f => favoriteKey(f) !== key) : [...prev, fav];
    });
  }, [isShowdown]);

  // Per-scope field names in the `exp` state object
  const scopeField = (kind /* 'min' | 'max' */) => {
    if (expScope === 'cpt')  return kind === 'min' ? 'cptMin'  : 'cptMax';
    if (expScope === 'flex') return kind === 'min' ? 'flexMin' : 'flexMax';
    return kind; // 'all' uses base 'min' / 'max'
  };
  const getCap = (name, kind) => {
    const e = exp[name] || {};
    const f = scopeField(kind);
    return e[f];
  };
  const setCap = (name, kind, val) =>
    setExp(p => ({ ...p, [name]: { ...p[name], [scopeField(kind)]: val } }));

  const avgVal = useMemo(() => {
    const vals = rp.filter(p => p.salary > 0 && (p.status || 'ACTIVE').toUpperCase() !== 'OUT').map(p => p.val || 0);
    return vals.length ? vals.reduce((s, v) => s + v, 0) / vals.length : 4;
  }, [rp]);

  // CONTRARIAN MODE (NBA) — mirrors tennis/MMA with NBA-specific gem band
  const contrarianCaps = useMemo(() => {
    if (!contrarianOn) return {};
    // PP EXPOSURE BOOST (v3.19) — exposure-layer boost (replaces v3.18 proj boost).
    // Theory: PP's most-aggressive LESS lines signal PP-identified upside.
    // We don't touch projections — instead we stack +5pp cptMin and +15pp
    // flexMin on top of whatever caps the player already has from other
    // signals (pivot, sleeper, etc.). Multiple signals compound additively.
    // Selection (unchanged from v3.18): top-3 most-negative edges with
    // ppEdge ≤ -2, excluding the original Primary Trap (computed on original
    // projections — the trap is already chalky, compounding PP on top would
    // fight the fade).
    // Cap-win rule: if the boosted flexMin would exceed an existing flexMax
    // (e.g., Top Value Cap at 10%), the floor is clipped to the max. The
    // existing cap's fade intent wins.
    const origWithSal = rp.filter(p => p.salary > 0 && p.projectable && (p.status || 'ACTIVE').toUpperCase() !== 'OUT' && (p.proj || 0) >= 5);
    const origSalaryPool = origWithSal.filter(p => (p.salary || 0) > 6000);
    const origGated = origSalaryPool.filter(p => (ownership[p.name] || 0) >= 25);
    const origPool = origGated.length > 0 ? origGated : origSalaryPool;
    const origPrimaryTrapName = [...origPool].sort((a, b) =>
      ((ownership[b.name] || 0) * (b.val || 0)) - ((ownership[a.name] || 0) * (a.val || 0))
    )[0]?.name || null;
    const ppFadeCandidates = rp
      .filter(p => typeof p.ppLine === 'number' && typeof p.ppEdge === 'number' && p.ppEdge <= -2)
      .filter(p => p.name !== origPrimaryTrapName)
      .sort((a, b) => (a.ppEdge || 0) - (b.ppEdge || 0))
      .slice(0, 3);
    // Filter out low-projection players (proj < 5) — they're not viable
    // contrarian plays. Keeping them in meant the floor bucket was wasting
    // lineup slots on $1K players with 0.1-0.8 projections.
    const withSal = rp.filter(p => p.salary > 0 && p.projectable && (p.status || 'ACTIVE').toUpperCase() !== 'OUT' && (p.proj || 0) >= 5);
    if (withSal.length === 0) return {};
    const caps = {};
    // Stash the PP boost list for later — applied AFTER all other signals
    // assign their caps so the boost is additive on top of whatever the
    // player already got.
    const _ppBoostNames = ppFadeCandidates.map(p => p.name);

    const byProj = [...withSal].sort((a, b) => (b.ceil || b.proj || 0) - (a.ceil || a.proj || 0));
    const topProjN = Math.max(3, Math.ceil(withSal.length * 0.3));
    const topProjSet = new Set(byProj.slice(0, topProjN).map(p => p.name));

    const boostFloor = Math.round(10 + contrarianStrength * 10);
    const LEV_CAP = 30;

    // TRAP = "Biggest Trap" — ownership × value product (v3.1).
    // Mirrors NBA DK tab: bustScore = simOwn × val, gate ≥25%,
    // plus $6K salary floor on primary (punts excluded from being
    // the primary fade target — see NBADKTab comment for rationale).
    // Philosophy: DK prices for a reason. Best-value chalk = too good to
    // be true, leverage already dead, prime contrarian fade.
    const hasOwn = withSal.some(p => (ownership[p.name] || 0) > 0);
    let trap = null;
    if (!hasOwn) {
      trap = byProj[0];
    } else {
      const salaryPool = withSal.filter(p => (p.salary || 0) > 6000);
      const gated = salaryPool.filter(p => (ownership[p.name] || 0) >= 25);
      const pool = gated.length > 0 ? gated : salaryPool;
      trap = [...pool].sort((a, b) => {
        const aScore = (ownership[a.name] || 0) * (a.val || 0);
        const bScore = (ownership[b.name] || 0) * (b.val || 0);
        return bScore - aScore;
      })[0];
    }
    // Identify the slate's TOP VALUE player (highest val = proj / $K).
    // Used for two rules:
    //   1. If primary trap = top value → "wash mode" (match field, no fade)
    //   2. Otherwise cap top value player at 2% CPT / 10% FLEX
    const topValuePlayer = [...withSal]
      .filter(p => (p.val || 0) > 0)
      .sort((a, b) => (b.val || 0) - (a.val || 0))[0] || null;
    const isWashTrap = trap && topValuePlayer && trap.name === topValuePlayer.name;

    if (trap) {
      const trapFieldOwn = ownership[trap.name] || 0;
      if (isWashTrap) {
        // WASH MODE — primary trap IS the slate's top value. Rather than
        // fading them (which punishes our lineups since they're the best
        // pure value play), match field ownership exactly. Shoot for wash:
        // same exposure as field, leverage comes from elsewhere.
        const washExp = Math.round(trapFieldOwn);
        caps[trap.name] = {
          min: washExp,
          max: washExp,
          _isTrap: true, _isWashTrap: true,
          _fieldOwn: Math.round(trapFieldOwn),
        };
      } else {
        // Trap caps v2 — leverage-based fade anchored on field ownership.
        // Base values at strength 0.6:
        //   • CPT  max  = simOwn − 60pp (severe CPT fade)
        //   • FLEX min  = simOwn − 35pp (forced FLEX presence)
        //   • FLEX max  = simOwn − 30pp (FLEX cap)
        // → narrow 5pp FLEX exposure window between simOwn−35 and simOwn−30.
        // Replaces the old absolute-value caps (10 / 60 / 85) which ignored
        // how chalky the trap actually was. Now: chalkier trap = higher caps,
        // milder trap = lower caps (down to 0). More contrarian strength
        // widens the leverage gap (capped at 0% on the low end).
        const cptLeverage     = Math.round(60 + (contrarianStrength - 0.6) * 100);  // 60pp @ 0.6, 100pp @ 1.0
        const flexMaxLeverage = Math.round(30 + (contrarianStrength - 0.6) *  50);  // 30pp @ 0.6,  50pp @ 1.0
        const flexMinLeverage = Math.round(35 + (contrarianStrength - 0.6) *  50);  // 35pp @ 0.6,  55pp @ 1.0
        const cptMax  = Math.max(0, Math.round(trapFieldOwn - cptLeverage));
        let   flexMax = Math.max(0, Math.round(trapFieldOwn - flexMaxLeverage));
        let   flexMin = Math.max(0, Math.round(trapFieldOwn - flexMinLeverage));
        // PREMIUM-SALARY TRAP RULE (v3.6) — if trap salary > $10,000 they're
        // too salary-critical to fade at FLEX. Force flexMin to 80% at base
        // strength 0.6 (scales proportionally: 0 @ off, 80 @ 0.6, 95 @ 1.0,
        // capped at 95 so 5% of lineups can always skip them). CPT rules
        // unchanged — the fade still happens at the captain slot only.
        if ((trap.salary || 0) > 10000) {
          const premiumFlexMin = Math.min(95, Math.round(contrarianStrength * (80 / 0.6)));
          if (premiumFlexMin > flexMin) {
            flexMin = premiumFlexMin;
            // flexMax must be ≥ flexMin; if the leverage formula capped it
            // below the premium floor, open it up to 100 so the optimizer
            // has headroom to satisfy the floor.
            if (flexMax < flexMin) flexMax = 100;
          }
        }
        if (flexMin > flexMax) flexMin = flexMax;  // safety: floor never above cap
        caps[trap.name] = {
          cptMax, flexMin, flexMax,
          _isTrap: true,
          _fieldOwn: Math.round(trapFieldOwn),
        };
      }
    }

    // TOP VALUE CAP (v3.17) — if the slate's highest-val player is NOT the
    // primary trap (i.e., not in wash mode), cap their CPT + FLEX exposure.
    // Rationale: the top value is a chalk magnet — usually a cheap player
    // like Mike Conley ($1.6K / 5.07 val / 34.5% own) that the field piles
    // into. Heavy exposure to them kills leverage. Fixed cap at 2% CPT /
    // 10% FLEX when contrarian is on.
    if (topValuePlayer && !isWashTrap) {
      const existing = caps[topValuePlayer.name] || {};
      const fieldOwn = Math.round(ownership[topValuePlayer.name] || 0);
      caps[topValuePlayer.name] = {
        ...existing,
        cptMax:  Math.min(existing.cptMax  !== undefined ? existing.cptMax  : 100, 2),
        flexMax: Math.min(existing.flexMax !== undefined ? existing.flexMax : 100, 10),
        _isTopValueCap: true,
        _fieldOwn: fieldOwn,
      };
    }

    // STUD — overowned star with worst value
    const overownedStars = withSal.filter(p => {
      if (p.name === trap?.name) return false;
      if (!topProjSet.has(p.name)) return false;
      const fieldOwn = ownership[p.name] || 0;
      const fairOwn = computeFairOwn(p.val || 0, avgVal);
      return fieldOwn > fairOwn + 5 && fieldOwn >= 25;
    });
    const stud = overownedStars.sort((a, b) => (a.val || 0) - (b.val || 0))[0];
    if (stud) {
      const fieldOwn = Math.round(ownership[stud.name] || 0);
      // Stud leverage floor — scales linearly with contrarian strength:
      //   0pp @ strength 0 (no contrarian = match field exactly)
      //   +10pp @ strength 0.6 (base)
      //   +16-17pp @ strength 1.0 (max)
      // Stud is the chalk-adjacent high-projection player — we play them
      // slightly above field to leverage their certainty while still
      // differentiating enough to matter.
      const studLevPP = Math.round(contrarianStrength * (10 / 0.6));
      caps[stud.name] = {
        min: Math.min(95, fieldOwn + studLevPP),
        max: Math.min(95, fieldOwn + LEV_CAP),
        _isBoost: true, _leverage: studLevPP, _type: 'stud'
      };
    }

    // GEM v3 — dual-track (value-adjacent + same-team replacer). Now surfaces
    // BOTH primary gem and pivot gem (same tracks as NBADKTab's gem logic)
    // with MIN-exposure caps rather than a single pick. At base contrarian
    // strength 0.6: primary gem floor 50%, pivot gem floor 40% — scales
    // linearly with strength.
    const trapSal = trap?.salary ?? 0;
    const trapProj = trap?.proj ?? 0;
    const trapUsageFactor = Math.max(0.3, Math.min(1.0, trapProj / 35));
    const bucketOf = s => { const str = String(s || ''); if (/C|PF/.test(str)) return 'big'; if (/SF/.test(str)) return 'wing'; return 'guard'; };
    const positionOverlap = (a, b) => { const ab = bucketOf(a), bb = bucketOf(b); if (ab === bb) return 1.0; if ((ab === 'guard' && bb === 'big') || (ab === 'big' && bb === 'guard')) return 0.25; return 0.55; };

    // SECONDARY TRAP (builder v3.17) — defined BEFORE gem pivots (reorder
    // from v3.16) so mid-sal pivot can claim its slots ahead of gem pivots.
    // Gem pivots then exclude anyone already capped (secondary trap + mid-sal).
    // Placeholder here so gemPool can still reference it without ReferenceError.
    let secondaryTrap = null;

    const gemPool = trap ? withSal.filter(p =>
      p.name !== trap.name &&
      p.name !== stud?.name
    ) : [];

    // NBA SHOWDOWN — next-best-value pivot with salary > $6,000 (v3).
    // Mirrors the DK-tab gem logic: highest val (proj / $K) among captain-
    // worthy players excluding the trap and stud. $6K floor excludes punts
    // whose raw val looks great but can't carry a 1.5× multiplier alone.
    // See DK tab comment above for the slate-by-slate validation rationale.
    let gemScored;
    if (isShowdown) {
      const pool = gemPool.filter(p => (p.val || 0) > 0 && p.salary > 6000);
      gemScored = pool.map(p => ({
        p,
        score: p.val || 0,
        kind: (trap && p.team === trap.team && p.salary >= 3000) ? 'replacer' : 'value',
      })).sort((a, b) => b.score - a.score);
    } else {
      // Classic NBA — legacy score-combination (position-aware for replacer track).
      gemScored = gemPool.map(p => {
        const fieldOwn = ownership[p.name] || 0;
        const trapOwn = ownership[trap.name] || 0;
        const leverage = Math.max(0, trapOwn - fieldOwn);
        const levBoost = 1 + leverage * 0.012;
        const fairOwn = computeFairOwn(p.val || 0, avgVal);
        const underownedBonus = Math.max(0, fairOwn - fieldOwn) * 0.5;

        // Track A — value-adjacent
        const bandClose = (p.salary - trapSal) >= -2000 && (p.salary - trapSal) <= 500;
        const valueScore = ((p.val || 0) * (p.ceil || p.proj || 0) + underownedBonus)
          * (bandClose ? 1.0 : 0.55) * levBoost;

        // Track B — same-team replacer
        let replacerScore = 0;
        const isReplacer = trap && p.team === trap.team && p.salary >= 3000;
        if (isReplacer) {
          const overlap = positionOverlap(p.positions_str, trap.positions_str);
          replacerScore = ((p.ceil || p.proj || 0) * (p.val || 0) + underownedBonus)
            * (1 + overlap * 0.55)
            * (1 + trapUsageFactor * 0.60)
            * levBoost;
        }
        const score = Math.max(valueScore, replacerScore);
        const kind = replacerScore > valueScore ? 'replacer' : 'value';
        return { p, score, kind };
      }).sort((a, b) => b.score - a.score);
    }

    // Scaling: at strength 0.6 → primary 50%, pivot 40%
    const primaryMin = Math.max(5, Math.round(20 + contrarianStrength * 50));
    const pivotMin   = Math.max(5, Math.round(15 + contrarianStrength * 42));
    // CPT-specific floor for NBA SHOWDOWN primary gem — 40% fixed (not
    // strength-scaled). The primary gem is our strongest CPT-pivot conviction
    // off the trap (e.g., Deni off Wemby on POR@SAS); captaining them in at
    // least 40% of lineups aligns builds with winning outcomes. No flex cap
    // on the primary — the optimizer decides flex placement naturally based
    // on salary fit and projection, which gives the engine freedom to slot
    // the gem as either captain (40%+) or utility (the rest) as best serves
    // each individual lineup. For NBA CLASSIC the cptMin is irrelevant (no
    // captain concept) so we omit it entirely.
    //
    // Gem pivot (block below) uses a FLEX leverage min instead — +10pp over
    // field sim own — so the pivot is overexposed in UTIL without competing
    // with the primary for the captain slot.

    const gemPrimary = gemScored[0]?.p;
    const gemPrimaryKind = gemScored[0]?.kind || 'value';
    if (gemPrimary) {
      const fieldOwn = Math.round(ownership[gemPrimary.name] || 0);
      caps[gemPrimary.name] = {
        min: primaryMin,
        max: 100,
        ...(isShowdown ? { cptMin: 40 } : {}),
        _isGem: true, _kind: 'primary', _gemType: gemPrimaryKind,
        _fieldOwn: fieldOwn,
      };
    }

    // SECONDARY TRAP (v3.17) — moved BEFORE gem pivots so mid-sal pivot
    // (which depends on secondary trap being a premium chalk) can claim
    // its slots first. Gem pivots then exclude anyone already capped.
    // Formula: max(simOwn × val), simOwn ≥ 30%, no salary gate.
    // Caps: multiplicative leverage fade (split cpt/flex at premium salary).
    //   STANDARD (salary ≤ $10K) — unified cap:
    //     base 0.6: cap = simOwn × 0.55 (45% leverage fade)
    //   PREMIUM (salary > $10K) — split cpt/flex since salary-critical:
    //     base 0.6: cptMax = cptOwn × 0.45 (55% leverage fade, cpt-specific own)
    //                flexMax = simOwn × 0.90 (10% leverage fade)
    if (trap) {
      const stExcluded = new Set([trap.name, stud?.name, gemPrimary?.name].filter(Boolean));
      const stPool = withSal.filter(p =>
        !stExcluded.has(p.name) &&
        (ownership[p.name] || 0) >= 30 &&
        (p.val || 0) > 0
      );
      const stRanked = [...stPool].sort((a, b) =>
        ((ownership[b.name] || 0) * (b.val || 0)) - ((ownership[a.name] || 0) * (a.val || 0))
      );
      secondaryTrap = stRanked[0] || null;
      if (secondaryTrap) {
        const stFieldOwn = ownership[secondaryTrap.name] || 0;
        const stCptFieldOwn = cptOwnership[secondaryTrap.name] || 0;
        const isPremium = (secondaryTrap.salary || 0) > 10000;
        let stCptMax, stFlexMax;
        if (isPremium) {
          const cptLevFrac  = Math.min(1, Math.max(0, contrarianStrength * (0.55 / 0.6)));
          const flexLevFrac = Math.min(1, Math.max(0, contrarianStrength * (0.10 / 0.6)));
          stCptMax  = Math.max(0, Math.round(stCptFieldOwn * (1 - cptLevFrac)));
          stFlexMax = Math.max(0, Math.round(stFieldOwn    * (1 - flexLevFrac)));
        } else {
          const secondaryLevFrac = Math.min(1, Math.max(0, contrarianStrength * 0.75));
          stCptMax = stFlexMax = Math.max(0, Math.round(stFieldOwn * (1 - secondaryLevFrac)));
        }
        caps[secondaryTrap.name] = {
          cptMax: stCptMax,
          flexMax: stFlexMax,
          _isSecondaryTrap: true,
          _isPremium: isPremium,
          _fieldOwn: Math.round(stFieldOwn),
          _cptFieldOwn: Math.round(stCptFieldOwn),
        };
      }
    }

    // MID-SALARY CPT+FLEX PIVOT FLOOR (v3.12) — when secondary trap salary
    // > $10K (premium chalk), force top-2 val in $4K-$6.8K band to appear as
    // BOTH cptMin 12% / flexMin 10% at base 0.6. Selected BEFORE gem pivots
    // so these claim their slots first.
    if (secondaryTrap && (secondaryTrap.salary || 0) > 10000) {
      const cptPivotMinPct  = Math.round(contrarianStrength * (12 / 0.6));
      const flexPivotMinPct = Math.round(contrarianStrength * (10 / 0.6));
      const midSalValuePool = withSal
        .filter(p =>
          !caps[p.name] &&
          (p.salary || 0) >= 4000 &&
          (p.salary || 0) <= 6800 &&
          (p.val || 0) > 0
        )
        .sort((a, b) => (b.val || 0) - (a.val || 0))
        .slice(0, 2);
      midSalValuePool.forEach((p, idx) => {
        const fieldOwn = Math.round(ownership[p.name] || 0);
        caps[p.name] = {
          min: 0, max: 100,
          cptMin: cptPivotMinPct,
          flexMin: flexPivotMinPct,
          _isMidSalCptPivot: true, _midSalCptPivotRank: idx + 1,
          _fieldOwn: fieldOwn,
        };
      });
    }

    // Pivot (NBA v3.17): owned-but-not-chalky leverage plays, ranked by
    //   ceiling-value (ceil per $1K). Mirrors NBADKTab's pivot rule so
    //   DK tab display and Builder caps target the same players.
    //   • not already capped (primary trap, stud, gem primary,
    //     secondary trap, mid-sal pivots all claimed their slots first)
    //   • salary ≥ $2,700
    //   • ranked by ceil / (salary/1000) desc — ceiling-adjusted value
    //   • Pivot #1 pool: 6% ≤ simOwn < 34% (chalk-leaning pivots allowed)
    //   • Pivot #2 pool: 6% ≤ simOwn < 27% (traditional low-owned leverage,
    //     excluding Pivot #1 so the two picks are distinct)
    const pivotPool1 = gemPool.filter(p =>
      !caps[p.name] &&
      (ownership[p.name] || 0) >= 6 &&
      (ownership[p.name] || 0) < 34 &&
      (p.salary || 0) >= 2700 &&
      (p.ceil || 0) > 0
    );
    const ceilValB = (p) => (p.ceil || 0) / Math.max(1, (p.salary || 0) / 1000);
    const pivot1 = [...pivotPool1].sort((a, b) => ceilValB(b) - ceilValB(a))[0];
    const pivotPool2 = gemPool.filter(p =>
      !caps[p.name] &&
      (!pivot1 || p.name !== pivot1.name) &&
      (ownership[p.name] || 0) >= 6 &&
      (ownership[p.name] || 0) < 27 &&
      (p.salary || 0) >= 2700 &&
      (p.ceil || 0) > 0
    );
    const pivot2 = [...pivotPool2].sort((a, b) => ceilValB(b) - ceilValB(a))[0];
    const gemPivots = [pivot1, pivot2].filter(Boolean);
    // SLOT-TARGET POOL (v3.7/v3.15) — wider 6-35% simOwn band used only for
    // slot-based exposure targeting (the "+N pool" in the ribbon). Excludes
    // anyone already capped by earlier rules.
    const pivotSlotPool = gemPool.filter(p =>
      !caps[p.name] &&
      (!pivot1 || p.name !== pivot1.name) &&
      (!pivot2 || p.name !== pivot2.name) &&
      (ownership[p.name] || 0) >= 6 &&
      (ownership[p.name] || 0) < 35 &&
      (p.salary || 0) >= 2700 &&
      (p.ceil || 0) > 0
    );
    if (gemPivots.length > 0) {
      // GEM PIVOT DISPLAY FLOOR (v3.15) — top-2 pivots each get the same
      // individual caps:
      //   flexMin = fieldOwn + 10pp leverage @ base 0.6 (capped at 95)
      //   cptMin  = 2% @ base 0.6
      // Pool target (35% FLEX / 9% CPT at base, unchanged) lives on the
      // FIRST pivot entry; pool names exclude BOTH displayed pivots.
      const displayedNames = new Set(gemPivots.map(p => p.name));
      const poolNamesExDisplayed = pivotSlotPool
        .filter(p => !displayedNames.has(p.name))
        .map(p => p.name);
      const poolFlexTarget = Math.round(contrarianStrength * (35 / 0.6));
      const poolCptTarget  = Math.round(contrarianStrength * (9  / 0.6));
      gemPivots.forEach((gp, idx) => {
        if (caps[gp.name]) return;
        const fieldOwn = Math.round(ownership[gp.name] || 0);
        const nextKind = (trap && gp.team === trap.team && gp.salary >= 3000) ? 'replacer' : 'value';
        const displayedFlexLevPP = Math.round(contrarianStrength * (10 / 0.6));
        const displayedFlexMin = Math.min(95, fieldOwn + displayedFlexLevPP);
        const displayedCptMin  = Math.round(contrarianStrength * (2  / 0.6));
        const isFirst = idx === 0;
        caps[gp.name] = {
          min: 0, max: 100,
          flexMin: displayedFlexMin,
          cptMin:  displayedCptMin,
          _isGem: true, _kind: 'pivot', _gemType: nextKind, _pivotRank: idx + 1,
          _fieldOwn: fieldOwn,
          _displayedFlexMin: displayedFlexMin,
          _displayedCptMin:  displayedCptMin,
          _displayedFlexLev: displayedFlexLevPP,
          // Pool metadata only on the first entry — optimizer picks it via find()
          ...(isFirst ? {
            _pivotPoolNames: poolNamesExDisplayed,
            _pivotFlexTarget: poolFlexTarget,
            _pivotCptTarget:  poolCptTarget,
          } : {}),
        };
      });
    }


    // SLEEPER v3 — mirrors v3.3 Gem Pivot logic: owned-but-not-chalky plays
    // ranked by ceiling-value (ceil / $K). Effectively the "next best 2
    // pivots" after the Gem Pivot is pulled (already-capped check excludes
    // the gem pivot from this pool). Lower simOwn floor of 3% (vs gem
    // pivot's 6%) widens the pool to catch deeper sleepers.
    //   • Top 2: 3% ≤ simOwn < 25%, salary ≥ $1500
    //     – ranked by ceil / (salary/1000) desc — ceiling-adjusted value
    //     – floor: simOwn + 15pp (minimum 15-point leverage gap)
    //   • Extended pool: 3% ≤ simOwn < 25% AND proj > 5 AND salary ≥ $1500
    //     – floor: simOwn + 3pp (minimum 3-point leverage gap)
    // Excludes already-capped players (trap/stud/gem primary/gem pivot).
    const ceilValSleep = (p) => (p.ceil || 0) / Math.max(1, (p.salary || 0) / 1000);
    const sleeperCandidates = withSal.filter(p => {
      if (caps[p.name]) return false;                         // don't double up trap/stud/gem/pivot
      if (trap && p.name === trap.name) return false;
      if ((p.salary || 0) < 1500) return false;
      const own = ownership[p.name] || 0;
      if (own < 3 || own >= 25) return false;                 // 3% ≤ simOwn < 25%
      if ((p.ceil || 0) <= 0) return false;
      return true;
    }).sort((a, b) => ceilValSleep(b) - ceilValSleep(a))
      .slice(0, 2);

    sleeperCandidates.forEach((p, idx) => {
      const fieldOwn = Math.round(ownership[p.name] || 0);
      // Sleeper leverage floor (v3.19) — simplified to +5pp at base 0.6.
      // Scales linearly: 0pp @ strength 0, +5pp @ 0.6, ~8pp @ 1.0.
      // Reduced from prior +10pp base — sleepers now get a lighter nudge
      // (PP Boost on top adds another +15pp flex if the sleeper is also a
      // PP fade target; the two boosts compound additively).
      const slpLevPP = Math.round(contrarianStrength * (5 / 0.6));
      caps[p.name] = {
        min: Math.min(95, fieldOwn + slpLevPP),
        max: 100,
        _isSleeper: true, _sleeperRank: idx + 1,
        _fieldOwn: fieldOwn,
      };
    });

    // EXTENDED SLEEPER POOL — every other player in the 3-25% simOwn band
    // with proj > 5 gets a +3pp leverage floor. Catches viable low-owned
    // pieces below the top-2 ceil-value cut so the user always has some
    // exposure to potential winners.
    withSal.forEach(p => {
      if (caps[p.name]) return;                               // already capped
      if (trap && p.name === trap.name) return;
      if ((p.proj || 0) <= 5) return;                         // proj > 5
      if ((p.salary || 0) < 1500) return;
      const own = ownership[p.name] || 0;
      if (own < 3 || own >= 25) return;                       // 3% ≤ simOwn < 25%
      const fieldOwn = Math.round(own);
      caps[p.name] = {
        min: Math.min(95, fieldOwn + 3),                      // +3pp leverage floor
        max: 100,
        _isExtSleeper: true,
        _fieldOwn: fieldOwn,
      };
    });

    // MID-CHALK — legit chalk plays (field 25-65%) that aren't trap/stud/gem/sleeper.
    // Winning lineups often include mid-chalk non-trap pieces (e.g., Deni Avdija
    // 59.7% field won POR as CPT, Franz Wagner 39% field won ORL, Tobias Harris
    // 55.5% field won ORL, Dillon Brooks 26.5% won PHX, Robert Williams 28.5% won POR).
    // Without this tier, these plays get globalFloor 5% and get obliterated in
    // contrarian builds. Matching field ownership on them ensures we include
    // them as the field does. Lower bound 25% catches "tweener" conviction
    // plays the field is moderately on.
    withSal.forEach(p => {
      if (caps[p.name]) return;
      const fieldOwn = ownership[p.name] || 0;
      if (fieldOwn < 25 || fieldOwn > 65) return;
      const minExp = Math.round(fieldOwn);
      const maxExp = Math.min(95, minExp + LEV_CAP);
      caps[p.name] = {
        min: minExp,
        max: maxExp,
        _isMidChalk: true,
        _fieldOwn: Math.round(fieldOwn),
      };
    });

    // GLOBAL FLOOR — small min exposure for rest, +30pp cap for ≥15% field plays
    const globalFloor = Math.round(1 + contrarianStrength * 7);
    withSal.forEach(p => {
      if (caps[p.name]) return;
      const fieldOwn = Math.round(ownership[p.name] || 0);
      const maxCap = fieldOwn >= 15 ? Math.min(95, fieldOwn + LEV_CAP) : 100;
      caps[p.name] = { min: globalFloor, max: maxCap, _isFloor: true };
    });

    // ─── CPT PIVOT (utility-chalk captain leverage) ───────────────────────
    // Identifies players the field heavily plays at UTIL but rarely captains.
    // When the chalk captain is a mega-trap (Wemby 53% CPT), the leverage pivot
    // isn't always the gem or sleeper — often it's the 2nd-most-owned overall
    // player who the field almost never captains. Captaining them = 1.5× their
    // points while the field has them at 1× UTIL.
    //
    // Validation against Seth's 3 winning lineups:
    //   POR@SAS  — Winning CPT: Deni Avdija (59.7% total, 10.4% CPT, flex 49%)
    //              → #1 utility-chalk CPT pivot ✓
    //   PHX@OKC  — Winning CPT: Chet Holmgren (gem territory, trap=SGA)
    //              → Booker surfaces here (harmless; gem logic handles Chet)
    //   ORL@DET  — Winning CPT: Cade Cunningham (trap territory)
    //              → Harris/Paolo surface here (harmless; trap logic handles Cade)
    //
    // This tier runs LAST and only ADDS cptMin to whatever was already set by
    // prior tiers, so it's purely additive — never downgrades an existing cap.
    // Gate: flexOwn ≥ 35% (field's UTIL pick), cptOwn ≤ 15% (not already chalk
    // CPT), projection in top 30% (captain-worthy), not trap or stud.
    const cptPivots = withSal.filter(p => {
      if (p.name === trap?.name || p.name === stud?.name) return false;
      const totalOwn = ownership[p.name] || 0;
      const cptOwn = cptOwnership[p.name] || 0;
      const flexOwn = Math.max(0, totalOwn - cptOwn);
      if (flexOwn < 35) return false;
      if (cptOwn > 15) return false;
      if (!topProjSet.has(p.name)) return false;
      return true;
    }).sort((a, b) => {
      // Rank by flexOwn × ceiling — highest = best CPT-pivot candidate
      const aFlex = Math.max(0, (ownership[a.name] || 0) - (cptOwnership[a.name] || 0));
      const bFlex = Math.max(0, (ownership[b.name] || 0) - (cptOwnership[b.name] || 0));
      return bFlex * (b.ceil || b.proj || 0) - aFlex * (a.ceil || a.proj || 0);
    });

    // Primary floor 15% @ 0.6 (→ 23 @ 1.0); secondary 8% @ 0.6 (→ 12 @ 1.0).
    // Primary receives full floor; a close 2nd candidate gets a lighter floor
    // so we don't over-commit the CPT slot across multiple pivots in a 20-lineup build.
    const primCptPivotMin = Math.max(5, Math.round(3 + contrarianStrength * 20));
    const secCptPivotMin  = Math.max(3, Math.round(2 + contrarianStrength * 10));
    cptPivots.slice(0, 2).forEach((p, idx) => {
      const existing = caps[p.name] || {};
      const flr = idx === 0 ? primCptPivotMin : secCptPivotMin;
      const totalOwn = ownership[p.name] || 0;
      const cptOwn = cptOwnership[p.name] || 0;
      caps[p.name] = {
        ...existing,
        cptMin: Math.max(existing.cptMin || 0, flr),
        _isCptPivot: true,
        _cptPivotRank: idx + 1,
        _cptPivotFlexOwn: Math.round(Math.max(0, totalOwn - cptOwn)),
        _cptPivotCptOwn: Math.round(cptOwn),
      };
    });

    // PP EXPOSURE BOOST (v3.19) — applied LAST, after all other signals have
    // assigned their caps. For each top-3 PP fade target (identified above
    // in `_ppBoostNames`, excluding the primary trap), add +5pp cptMin and
    // +15pp flexMin additively on top of whatever the player already has.
    //
    // Cap-win rule: if the boosted floor would exceed an existing MAX cap
    // (e.g., Top Value flexMax = 10%, boosted flexMin wants 20%), the floor
    // is clipped down to the max. This protects fade intent — a player the
    // Top Value rule wants capped at 10% stays capped, PP signal notwithstanding.
    // Cap-raise rule for MIN: if existing cptMax/flexMax are undefined (no
    // fade cap in place), the boost applies cleanly.
    _ppBoostNames.forEach((name, idx) => {
      const existing = caps[name] || {};
      const fieldOwn = Math.round(ownership[name] || 0);
      const currCptMin  = existing.cptMin  || 0;
      const currFlexMin = existing.flexMin || 0;
      const currCptMax  = existing.cptMax  !== undefined ? existing.cptMax  : 100;
      const currFlexMax = existing.flexMax !== undefined ? existing.flexMax : 100;
      // +5pp CPT, +15pp FLEX (fixed, not strength-scaled — user spec)
      const newCptMin  = Math.min(currCptMin  + 5,  currCptMax);   // cap wins
      const newFlexMin = Math.min(currFlexMin + 15, currFlexMax);  // cap wins
      const fade = ppFadeCandidates.find(p => p.name === name);
      caps[name] = {
        ...existing,
        cptMin:  newCptMin,
        flexMin: newFlexMin,
        _isPpBoost: true,
        _ppBoostRank: idx + 1,
        _ppBoostEdge: fade?.ppEdge || 0,
        _ppBoostLine: fade?.ppLine || 0,
        _ppBoostOrigProj: fade?.proj || 0,
        _fieldOwn: existing._fieldOwn !== undefined ? existing._fieldOwn : fieldOwn,
        _ppBoostAddedCptMin:  newCptMin  - currCptMin,
        _ppBoostAddedFlexMin: newFlexMin - currFlexMin,
      };
    });

    return caps;
  }, [rp, ownership, cptOwnership, contrarianOn, contrarianStrength, avgVal]);

  const sp = useMemo(() =>
    [...rp].filter(p => p.salary > 0 && p.projectable && (p.status || 'ACTIVE').toUpperCase() !== 'OUT')
           .sort((a, b) => b.val - a.val),
    [rp]);

  const sE = (n, f, v) => setExp(p => ({ ...p, [n]: { ...p[n], [f]: v } }));
  const applyGlobal = () => { const ne = {}; sp.forEach(p => { ne[p.name] = { min: globalMin, max: globalMax, ...exp[p.name] }; }); setExp(ne); };

  const run = () => {
    if (!canBuild || isBuilding) return;
    // v3.24.13: build animation — see tennis run() comment for details.
    setIsBuilding(true);
    if (res) setPrevRes(res);                    // v3.24.14: stash prev res for undo
    const start = performance.now();
    const finish = () => {
      const elapsed = performance.now() - start;
      const MIN_MS = 1400;
      setTimeout(() => setIsBuilding(false), Math.max(0, MIN_MS - elapsed));
    };
    setTimeout(() => { try {
    const jitter = () => 1 + (Math.random() * 2 - 1) * variance / 100;
    const enforceMinNudge = (pd, baseProjs) => {
      const changed = pd.filter((p, i) => Math.abs(p.projection - baseProjs[i]) >= 0.01).length;
      if (changed >= 2) return;
      const idxs = [...Array(pd.length).keys()].sort(() => Math.random() - 0.5).slice(0, 2);
      idxs.forEach(i => {
        const sign = Math.random() < 0.5 ? -1 : 1;
        pd[i].projection = Math.round((pd[i].projection + sign * 0.01) * 1000) / 1000;
      });
    };
    if (isShowdown) {
      const baseProjs = sp.map(p => p.proj);
      const pd = sp.map(p => {
        const cap = contrarianCaps[p.name] || {};
        const userSet = exp[p.name] || {};
        const userMin = userSet.min !== undefined ? userSet.min : globalMin;
        const userMax = userSet.max !== undefined ? userSet.max : globalMax;
        // Hard-exclude players projected under 5 FS — they shouldn't compete
        // for pool slots against real contrarian plays. Setting maxExp = 0
        // keeps them in the `pd` array (preserves indexing) but blocks all
        // lineup inclusion.
        const lowProj = (p.proj || 0) < 5;
        const effMin = lowProj ? 0 : Math.max(userMin, cap.min || 0);
        const effMax = lowProj ? 0 : Math.min(userMax, cap.max !== undefined ? cap.max : 100);
        // Per-slot caps — merge contrarian caps (e.g., trap CPT max 10 /
        // FLEX min 60) with user-set caps by taking the MORE-RESTRICTIVE
        // of each bound, consistent with how all/min/max merge works.
        const userCptMin  = userSet.cptMin  !== undefined ? userSet.cptMin  : 0;
        const userCptMax  = userSet.cptMax  !== undefined ? userSet.cptMax  : 100;
        const userFlexMin = userSet.flexMin !== undefined ? userSet.flexMin : 0;
        const userFlexMax = userSet.flexMax !== undefined ? userSet.flexMax : 100;
        return {
          name: p.name, team: p.team, projection: p.proj * jitter(),
          util_salary: p.util_salary || p.salary, cpt_salary: p.cpt_salary,
          util_id: p.util_id || p.id, cpt_id: p.cpt_id,
          salary: p.util_salary || p.salary, id: p.util_id || p.id,
          positions: p.positions || [], status: p.status,
          maxExp: effMax, minExp: effMin,
          cptMinExp:  Math.max(userCptMin,  cap.cptMin  !== undefined ? cap.cptMin  : 0),
          cptMaxExp:  Math.min(userCptMax,  cap.cptMax  !== undefined ? cap.cptMax  : 100),
          flexMinExp: Math.max(userFlexMin, cap.flexMin !== undefined ? cap.flexMin : 0),
          flexMaxExp: Math.min(userFlexMax, cap.flexMax !== undefined ? cap.flexMax : 100),
        };
      });
      enforceMinNudge(pd, baseProjs);
      // Extract gem pivot pool info (v3.4 slot-based exposure) from
      // contrarianCaps. The pivot pool drives slot reservation in the
      // optimizer: pivotFlexTarget% of lineups contain ≥1 pool player in
      // FLEX, pivotCptTarget% have a pool player as CPT.
      const pivotPoolCap = Object.values(contrarianCaps).find(c => c._isGem && c._kind === 'pivot');
      const pivotPoolSet = new Set(pivotPoolCap?._pivotPoolNames || []);
      const pivotFlexFrac = (pivotPoolCap?._pivotFlexTarget || 0) / 100;
      const pivotCptFrac  = (pivotPoolCap?._pivotCptTarget  || 0) / 100;
      const r = nbaOptimizeShowdown(pd, nL, 50000, 48000, {
        locked: new Set(lockedPlayers),
        excluded: new Set(excludedPlayers),
        cptLocked: new Set(cptLockedPlayers),
        flexLocked: new Set(flexLockedPlayers),
        cptExcluded: new Set(cptExcludedPlayers),
        flexExcluded: new Set(flexExcludedPlayers),
        pivotPool: pivotPoolSet,
        pivotFlexTarget: pivotFlexFrac,
        pivotCptTarget: pivotCptFrac,
      });
      // Merge favorited lineups: remap name-based favorites to the new pData
      // indices, prepend to r.lineups, and recompute counts so the exposure
      // tallies include favorited lineups. Favorites with missing players
      // (e.g. marked OUT since favoriting) are dropped silently.
      const favShow = [];
      const nameIdx = new Map(pd.map((p, i) => [p.name, i]));
      for (const fav of favoriteLineups) {
        if (fav.cpt === undefined) continue; // classic favorite, skip in showdown mode
        const cptIdx = nameIdx.get(fav.cpt);
        if (cptIdx === undefined) continue;
        const utilIdxs = fav.utils.map(n => nameIdx.get(n));
        if (utilIdxs.some(i => i === undefined)) continue;
        favShow.push({ cpt: cptIdx, utils: utilIdxs, players: [cptIdx, ...utilIdxs], proj: fav.proj, sal: fav.sal, _fav: true });
      }
      // De-dupe: if a favorite also got generated by the optimizer, keep the fav
      // entry and drop the optimizer's copy so we don't have two cards for one lineup.
      const favKeys = new Set(favShow.map(lu => `${lu.cpt}|${[...lu.utils].sort().join(',')}`));
      const dedupedR = r.lineups.filter(lu => !favKeys.has(`${lu.cpt}|${[...lu.utils].sort().join(',')}`));
      const mergedLineups = [...favShow, ...dedupedR];
      const mergedCounts = new Array(pd.length).fill(0);
      for (const lu of mergedLineups) { mergedCounts[lu.cpt]++; lu.utils.forEach(i => mergedCounts[i]++); }
      setRes({ ...r, lineups: mergedLineups, counts: mergedCounts, pData: pd, isShowdown: true });
      return;
    }
    // Classic
    const baseProjs = sp.map(p => p.proj);
    const pd = sp.map(p => {
      const cap = contrarianCaps[p.name] || {};
      const userSet = exp[p.name] || {};
      const userMin = userSet.min !== undefined ? userSet.min : globalMin;
      const userMax = userSet.max !== undefined ? userSet.max : globalMax;
      const effMin = Math.max(userMin, cap.min || 0);
      const effMax = Math.min(userMax, cap.max !== undefined ? cap.max : 100);
      return {
        name: p.name, team: p.team, projection: p.proj * jitter(),
        salary: p.salary, id: p.id, positions: p.positions || [],
        status: p.status, maxExp: effMax, minExp: effMin,
      };
    });
    enforceMinNudge(pd, baseProjs);
    const r = nbaOptimizeClassic(pd, nL, 50000, 48000, { locked: new Set(lockedPlayers), excluded: new Set(excludedPlayers) });
    // Merge classic favorites the same way
    const favCls = [];
    const nameIdxC = new Map(pd.map((p, i) => [p.name, i]));
    for (const fav of favoriteLineups) {
      if (fav.cpt !== undefined) continue; // showdown favorite
      const idxs = (fav.players || []).map(n => nameIdxC.get(n));
      if (idxs.some(i => i === undefined)) continue;
      favCls.push({ players: idxs, proj: fav.proj, sal: fav.sal, _fav: true });
    }
    const favKeysC = new Set(favCls.map(lu => [...lu.players].sort().join(',')));
    const dedupedRC = r.lineups.filter(lu => !favKeysC.has([...lu.players].sort().join(',')));
    const mergedC = [...favCls, ...dedupedRC];
    const mergedCountsC = new Array(pd.length).fill(0);
    for (const lu of mergedC) { lu.players.forEach(i => mergedCountsC[i]++); }
    setRes({ ...r, lineups: mergedC, counts: mergedCountsC, pData: pd, isShowdown: false });
    } finally { finish(); } }, 32);
  };

  const dl = (c, f) => { const b = new Blob([c], { type: 'text/csv' }); const a = document.createElement('a'); a.href = URL.createObjectURL(b); a.download = f; a.click(); URL.revokeObjectURL(a.href); };

  const exportDK = () => {
    if (!res) return;
    if (res.isShowdown) {
      let c = 'CPT,UTIL,UTIL,UTIL,UTIL,UTIL\n';
      res.lineups.forEach(lu => {
        const cptP = res.pData[lu.cpt];
        const utilPs = lu.utils.map(i => res.pData[i]);
        c += `${cptP.cpt_id},${utilPs.map(p => p.util_id).join(',')}\n`;
      });
      dl(c, 'dk_upload_nba_showdown.csv');
      return;
    }
    let c = 'PG,SG,SF,PF,C,G,F,UTIL\n';
    res.lineups.forEach(lu => {
      const ps = lu.players.map(i => res.pData[i]);
      c += ps.map(p => p.id).join(',') + '\n';
    });
    dl(c, 'dk_upload_nba_classic.csv');
  };

  const exportReadable = () => {
    if (!res) return;
    if (res.isShowdown) {
      let c = 'Rank,Proj,Salary,CPT,U1,U2,U3,U4,U5\n';
      res.lineups.forEach((lu, i) => {
        const cptP = res.pData[lu.cpt];
        const utilPs = lu.utils.map(j => res.pData[j]);
        c += `${i + 1},${lu.proj},${lu.sal},${cptP.name},${utilPs.map(p => p.name).join(',')}\n`;
      });
      dl(c, 'nba_lineups_showdown.csv');
      return;
    }
    let c = 'Rank,Proj,Salary,P1,P2,P3,P4,P5,P6,P7,P8\n';
    res.lineups.forEach((lu, i) => {
      const ps = lu.players.map(j => res.pData[j]);
      c += `${i + 1},${lu.proj},${lu.sal},${ps.map(p => p.name).join(',')}\n`;
    });
    dl(c, 'nba_lineups_classic.csv');
  };

  const exportProjections = () => {
    let c = 'Player,Team,Sal,Proj,Ceil,Val,Mins,Usg,Status\n';
    sp.forEach(p => { c += `${p.name},${p.team},${p.salary},${p.proj},${p.ceil},${p.val},${fmt(p.projMins, 1)},${fmt(p.usg, 1)},${p.status}\n`; });
    dl(c, 'nba_projections.csv');
  };

  const overrideCount = useMemo(() => rp.filter(p => p._overridden).length, [rp]);
  const canBuild = overrideCount >= 2;
  useBuilderShortcuts({ run, canBuild, isBuilding }); // v3.24.14: B/R to build/rebuild

  // v3.24.14: empty state for 0-player slates
  if (sp.length === 0) {
    return <>
      <div className="section-hero">
        <div className="section-hero-icon-wrap">
          <svg className="section-hero-icon" viewBox="0 0 24 24" fill="#F5C518" stroke="none">
            <path d="M13 2L4 14h7l-1 8 10-12h-7l1-8z"/>
          </svg>
        </div>
        <div className="section-hero-text">
          <h2 className="section-hero-title">Lineup Builder</h2>
          <div className="section-hero-sub">Set exposure %, build optimized lineups, export to DK</div>
        </div>
      </div>
      <div className="empty" style={{ padding: '60px 24px', background: 'var(--card)', border: '1px dashed var(--border-light)', borderRadius: 12 }}>
        <div style={{ fontSize: 42, marginBottom: 16, opacity: 0.4 }}>🏀</div>
        <h2 style={{ fontSize: 20, fontWeight: 700, color: 'var(--text)', marginBottom: 6 }}>No players on this slate yet</h2>
        <p style={{ color: 'var(--text-muted)', fontSize: 14, maxWidth: 420, margin: '0 auto' }}>
          The current slate has no priced players. Check back closer to lock, or switch to a different slate.
        </p>
      </div>
    </>;
  }

  return (<>
    <div className="section-hero">
      <div className="section-hero-icon-wrap">
        <svg className="section-hero-icon" viewBox="0 0 24 24" fill="#F5C518" stroke="none">
          <path d="M13 2L4 14h7l-1 8 10-12h-7l1-8z"/>
        </svg>
      </div>
      <div className="section-hero-text">
        <h2 className="section-hero-title">Lineup Builder</h2>
        <div className="section-hero-sub">{isShowdown ? 'NBA Showdown · 1 CPT + 5 UTIL · $50K cap' : 'NBA Classic · PG/SG/SF/PF/C/G/F/UTIL · $50K cap'}</div>
      </div>
    </div>
    {!canBuild && (
      <div style={{ padding: '14px 18px', marginBottom: 16, background: 'rgba(245,197,24,0.08)', border: '1px solid rgba(245,197,24,0.35)', borderRadius: 10 }}>
        <div style={{ fontSize: 14, fontWeight: 700, color: 'var(--primary)', marginBottom: 4, display: 'flex', alignItems: 'center', gap: 6 }}><Icon name="warning" size={15} color="#F5C518"/> DraftKings Compliance Warning</div>
        <div style={{ fontSize: 13, color: 'var(--text-muted)', lineHeight: 1.5 }}>
          DraftKings requires you to edit our default projections before building. Head to the <strong style={{ color: 'var(--text)' }}>DK Projections</strong> tab and change at least <strong style={{ color: 'var(--primary)' }}>2 projections</strong>.
          <span style={{ color: 'var(--text-dim)' }}> Currently changed: <strong style={{ color: overrideCount >= 2 ? 'var(--green)' : 'var(--red)' }}>{overrideCount}</strong>/2</span>
        </div>
        {onGoToProjections && (
          <button
            onClick={onGoToProjections}
            className="oo-jump-btn"
            style={{
              marginTop: 10, padding: '7px 14px', fontSize: 12, fontWeight: 600,
              background: 'var(--primary)', color: '#0A1628', border: 'none',
              borderRadius: 6, cursor: 'pointer', display: 'inline-flex',
              alignItems: 'center', gap: 6
            }}
          >
            Jump to DK Projections
            <svg viewBox="0 0 24 24" width="12" height="12" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><path d="M5 12h14M13 6l6 6-6 6"/></svg>
          </button>
        )}
      </div>
    )}
    <ContrarianPanel enabled={contrarianOn} onToggle={setContrarianOn} strength={contrarianStrength} onStrengthChange={setContrarianStrength} />
    {contrarianOn && Object.keys(contrarianCaps).length > 0 && (() => {
      // OverOwned Mode display (v3.19) — simplified to show only the top 5
      // biggest exposure changes (ranked by |newBound − fieldOwn|). Players
      // not shown still have their caps enforced by the optimizer.
      //
      // For each capped player, compute a "primary change" — the single
      // bound that represents the rule's intent (trap → max, gem → min,
      // pivot → flexMin, top value → flexMax, PP boost → flexMin, etc.).
      const labelFor = (c) => {
        if (c._isWashTrap) return { label: 'Wash Trap', icon: 'bomb', color: 'var(--amber)' };
        if (c._isTrap) return { label: 'Trap', icon: 'bomb', color: 'var(--red)' };
        if (c._isTopValueCap) return { label: 'Top Value Cap', icon: 'trophy', color: 'var(--amber)' };
        if (c._isSecondaryTrap) return { label: 'Secondary Trap', icon: 'bomb', color: 'var(--amber)' };
        if (c._isMidSalCptPivot) return { label: 'Mid-sal Pivot', icon: 'trophy', color: 'var(--green)' };
        if (c._isGem && c._kind === 'primary') return { label: 'Gem', icon: 'gem', color: 'var(--green)' };
        if (c._isGem && c._kind === 'pivot') return { label: `Pivot ${c._pivotRank || ''}`.trim(), icon: 'gem', color: 'var(--green)' };
        if (c._isBoost && c._type === 'stud') return { label: 'Stud', icon: 'trophy', color: 'var(--green)' };
        if (c._isPpBoost) return { label: 'PP Boost', icon: 'rocket', color: 'var(--primary)' };
        if (c._isSleeper) return { label: `Sleeper ${c._sleeperRank || ''}`.trim(), icon: 'sleeper', color: 'var(--green)' };
        if (c._isCptPivot) return { label: 'CPT Pivot', icon: 'trophy', color: 'var(--green)' };
        return null;
      };
      const describeChange = (c, name) => {
        // Returns { sym, bound, fieldRef }
        // fieldRef: which ownership scope the bound is compared against
        //   'total' = total simOwn (default)
        //   'cpt'   = cptOwnership[name]
        if (c._isWashTrap) return { sym: `match ${c.max}%`, bound: c.max, fieldRef: 'total' };
        if (c._isTrap) {
          const bound = c.flexMax != null ? c.flexMax : c.cptMax;
          return { sym: `CPT max ${c.cptMax}% / FLEX ${c.flexMin}-${c.flexMax}%`, bound, fieldRef: 'total' };
        }
        if (c._isTopValueCap) return { sym: `CPT max ${c.cptMax}% / FLEX max ${c.flexMax}%`, bound: c.flexMax, fieldRef: 'total' };
        if (c._isSecondaryTrap) {
          return { sym: c._isPremium
            ? `CPT max ${c.cptMax}% / FLEX max ${c.flexMax}% ★$10K+`
            : `max ${c.cptMax}%`,
            bound: c.flexMax != null ? c.flexMax : c.cptMax, fieldRef: 'total' };
        }
        if (c._isMidSalCptPivot) return { sym: `CPT ${c.cptMin}% / FLEX ${c.flexMin}%`, bound: c.flexMin, fieldRef: 'total' };
        if (c._isGem && c._kind === 'primary') return { sym: `min ${c.min}%`, bound: c.min, fieldRef: 'total' };
        if (c._isGem && c._kind === 'pivot') return { sym: `FLEX ${c._displayedFlexMin}% / CPT ${c._displayedCptMin}%`, bound: c._displayedFlexMin, fieldRef: 'total' };
        if (c._isBoost && c._type === 'stud') return { sym: `min ${c.min}%`, bound: c.min, fieldRef: 'total' };
        if (c._isPpBoost) return { sym: `+${c._ppBoostAddedCptMin}% CPT / +${c._ppBoostAddedFlexMin}% FLEX (stacked)`, bound: c.flexMin, fieldRef: 'total' };
        if (c._isSleeper) return { sym: `min ${c.min}%`, bound: c.min, fieldRef: 'total' };
        // CPT Pivot rule operates on the CPT slot specifically — compare
        // cptMin to cptOwnership (not total) so the delta reflects the
        // actual CPT-slot change, not inflated by flex ownership.
        if (c._isCptPivot) return { sym: `CPT min ${c.cptMin}%`, bound: c.cptMin, fieldRef: 'cpt' };
        return { sym: '', bound: 0, fieldRef: 'total' };
      };
      // Build list of (name, cap, labelInfo, desc, delta) — split into DK tab
      // "headliners" (always shown) and auxiliaries (ranked by delta to fill
      // any remaining slots). The headliners match exactly what the NBA DK
      // tab displays: Primary Trap, Gem Primary, Gem Pivots 1 & 2, Secondary.
      const headlinerEntries = [];
      const auxEntries = [];
      const isHeadliner = (c) =>
        c._isTrap ||                                  // primary trap (incl. wash)
        c._isSecondaryTrap ||                         // secondary trap
        (c._isGem && c._kind === 'primary') ||        // gem primary
        (c._isGem && c._kind === 'pivot');            // gem pivots 1 & 2
      for (const [name, c] of Object.entries(contrarianCaps)) {
        const lbl = labelFor(c);
        if (!lbl) continue;
        const desc = describeChange(c, name);
        const fieldTotal = ownership[name] || 0;
        const fieldCpt   = (cptOwnership || {})[name] || 0;
        const fieldForDelta = desc.fieldRef === 'cpt' ? fieldCpt : fieldTotal;
        const delta = desc.bound != null ? Math.abs(desc.bound - fieldForDelta) : 0;
        const row = { name, cap: c, lbl, desc, field: fieldTotal, fieldRef: desc.fieldRef, delta };
        (isHeadliner(c) ? headlinerEntries : auxEntries).push(row);
      }
      // Headliner order: trap → gem primary → gem pivot 1 → gem pivot 2 → secondary
      const headlinerOrder = (r) => {
        if (r.cap._isTrap && !r.cap._isSecondaryTrap) return 0;
        if (r.cap._isGem && r.cap._kind === 'primary') return 1;
        if (r.cap._isGem && r.cap._kind === 'pivot') return 2 + (r.cap._pivotRank || 1) - 1;
        if (r.cap._isSecondaryTrap) return 4;
        return 99;
      };
      headlinerEntries.sort((a, b) => headlinerOrder(a) - headlinerOrder(b));
      auxEntries.sort((a, b) => b.delta - a.delta);
      const top5 = [...headlinerEntries, ...auxEntries].slice(0, 5);
      const othersCount = (headlinerEntries.length + auxEntries.length) - top5.length;
      return (
        <div style={{ marginTop: -12, marginBottom: 16, padding: '10px 14px', background: 'rgba(245,197,24,0.06)', border: '1px solid rgba(245,197,24,0.2)', borderRadius: 8, fontSize: 12, color: 'var(--text-muted)', display: 'flex', gap: 16, flexWrap: 'wrap', alignItems: 'center' }}>
          <span style={{ color: 'var(--primary)', fontWeight: 700, textTransform: 'uppercase', fontSize: 10, letterSpacing: 0.5 }}>Top 5 Exposure Changes</span>
          {top5.map(({ name, lbl, desc, field, fieldRef }) => {
            // Display field ownership in the scope that matches the rule.
            // CPT Pivot → show CPT-specific ownership. Everything else → total.
            const displayField = fieldRef === 'cpt' ? ((cptOwnership || {})[name] || 0) : field;
            const fieldLabel = fieldRef === 'cpt' ? 'cpt own' : 'field';
            return (
              <span key={name} title={signalTooltip(lbl.label)} style={{ display: 'inline-flex', alignItems: 'center', gap: 4, cursor: 'help' }}>
                <Icon name={lbl.icon} size={12} color={lbl.color}/>
                <span style={{ color: 'var(--text-dim)', fontSize: 11 }}>{lbl.label}:</span>
                <span style={{ color: lbl.color, fontWeight: 600 }}>{name}</span>
                <span style={{ color: 'var(--text-dim)' }}>· {fieldLabel} {displayField.toFixed(1)}% →</span>
                <span style={{ color: 'var(--primary)', fontWeight: 600 }}>{desc.sym}</span>
              </span>
            );
          })}
          {othersCount > 0 && (
            <span style={{ color: 'var(--text-dim)', fontStyle: 'italic', fontSize: 11 }}>
              +{othersCount} more signals enforced
            </span>
          )}
        </div>
      );
    })()}
    <div style={{ display: 'flex', gap: 12, marginBottom: 16, flexWrap: 'wrap', alignItems: 'center' }}>
      <label style={{ fontSize: 13, color: 'var(--text-muted)' }}>Lineups: <input style={{ width: 60, background: 'var(--bg)', border: '1px solid var(--border)', borderRadius: 4, color: 'var(--text)', padding: '4px 8px', marginLeft: 4 }} type="number" min="1" step="1" value={nL} onChange={e => { const v = e.target.value; if (v === "") setNL(""); else setNL(Math.max(1, parseInt(v, 10) || 1)); }} onBlur={e => { if (e.target.value === "" || +e.target.value < 1) setNL(20); }} /></label>
      <label style={{ fontSize: 13, color: 'var(--text-muted)' }}>Min %: <input style={{ width: 50, background: 'var(--bg)', border: '1px solid var(--border)', borderRadius: 4, color: 'var(--text)', padding: '4px 8px', marginLeft: 4 }} type="number" min="0" max="100" step="1" value={globalMin} onChange={e => { const v = e.target.value; if (v === "") setGlobalMin(""); else setGlobalMin(Math.max(0, Math.min(100, parseInt(v, 10) || 0))); }} onBlur={e => { if (e.target.value === "") setGlobalMin(0); }} /></label>
      <label style={{ fontSize: 13, color: 'var(--text-muted)' }}>Max %: <input style={{ width: 50, background: 'var(--bg)', border: '1px solid var(--border)', borderRadius: 4, color: 'var(--text)', padding: '4px 8px', marginLeft: 4 }} type="number" min="0" max="100" step="1" value={globalMax} onChange={e => { const v = e.target.value; if (v === "") setGlobalMax(""); else setGlobalMax(Math.max(0, Math.min(100, parseInt(v, 10) || 0))); }} onBlur={e => { if (e.target.value === "") setGlobalMax(100); }} /></label>
      <label style={{ fontSize: 13, color: 'var(--text-muted)', display: 'flex', alignItems: 'center', gap: 6 }} title="Random ± shift applied to each player's projection per build.">
        Variance
        <input type="range" min="0" max="25" step="1" value={variance} onChange={e => setVariance(+e.target.value)} style={{ width: 80, accentColor: 'var(--primary)' }} />
        <span style={{ fontWeight: 700, color: variance > 0 ? 'var(--primary)' : 'var(--text-dim)', minWidth: 28, textAlign: 'right' }}>{variance}%</span>
      </label>
      <button onClick={applyGlobal} style={{ background: 'var(--card)', border: '1px solid var(--border)', borderRadius: 6, color: 'var(--text-muted)', padding: '4px 12px', fontSize: 12, cursor: 'pointer' }}>Apply Global</button>
      <button onClick={exportProjections} style={{ background: 'var(--card)', border: '1px solid var(--border)', borderRadius: 6, color: 'var(--text-muted)', padding: '4px 12px', fontSize: 12, cursor: 'pointer', marginLeft: 'auto' }}><Icon name="download" size={12}/> Projections CSV</button>
    </div>
    <style>{`
      /* Scoped NBA builder pool — 2-row card layout so player names always
         fit. Names + team on row 1; salary / proj / own% / min / max on
         row 2. Fully scoped class names avoid any collision with the
         legacy .builder-controls / .ctrl-row rules in styles.css. */
      .oo-nba-pool {
        display: grid !important;
        grid-template-columns: repeat(auto-fill, minmax(260px, 1fr)) !important;
        gap: 8px !important;
        padding: 0 !important;
        margin: 0 0 12px 0 !important;
        list-style: none !important;
      }
      .oo-nba-card {
        display: flex !important;
        flex-direction: column !important;
        gap: 6px !important;
        background: var(--card) !important;
        border: 1px solid var(--border) !important;
        border-radius: 8px !important;
        padding: 9px 11px !important;
        min-width: 0 !important;
      }
      .oo-nba-card:hover { border-color: var(--border-light) !important; }
      .oo-nba-card > * { min-width: 0 !important; }
      .oo-nba-r1 {
        display: flex !important;
        align-items: center !important;
        gap: 8px !important;
        min-width: 0 !important;
      }
      .oo-nba-name {
        flex: 1 1 auto !important;
        min-width: 0 !important;
        overflow: hidden !important;
        text-overflow: ellipsis !important;
        white-space: nowrap !important;
        font-weight: 600 !important;
        font-size: 13px !important;
        color: var(--text) !important;
        letter-spacing: -0.01em !important;
      }
      .oo-nba-team {
        flex-shrink: 0 !important;
        font-size: 10px !important;
        font-weight: 700 !important;
        letter-spacing: 0.04em !important;
        padding: 2px 6px !important;
        border-radius: 4px !important;
        background: rgba(255, 255, 255, 0.04) !important;
      }
      .oo-nba-r2 {
        display: grid !important;
        grid-template-columns: 1fr 1fr 1fr 46px 46px !important;
        align-items: center !important;
        gap: 6px !important;
        font-size: 11px !important;
        font-variant-numeric: tabular-nums !important;
      }
      .oo-nba-r2 > span { min-width: 0 !important; overflow: hidden !important; }
      .oo-nba-sal { color: var(--text-dim) !important; }
      .oo-nba-proj { color: var(--primary) !important; font-weight: 700 !important; font-size: 12px !important; }
      .oo-nba-own { color: var(--text-muted) !important; }
      .oo-nba-card input[type="number"] {
        width: 100% !important;
        min-width: 0 !important;
        padding: 4px 2px !important;
        background: var(--bg) !important;
        border: 1px solid var(--border) !important;
        border-radius: 4px !important;
        color: var(--text) !important;
        font-size: 11px !important;
        text-align: center !important;
        font-weight: 600 !important;
        box-sizing: border-box !important;
        -moz-appearance: textfield !important;
      }
      .oo-nba-card input[type="number"]::-webkit-outer-spin-button,
      .oo-nba-card input[type="number"]::-webkit-inner-spin-button { -webkit-appearance: none !important; margin: 0 !important; }
      .oo-nba-card input[type="number"]:focus { outline: none !important; border-color: var(--primary) !important; }
      .oo-nba-legend {
        display: flex !important; flex-wrap: wrap !important; gap: 12px !important;
        font-size: 10px !important; color: var(--text-dim) !important;
        padding: 0 6px 6px !important; letter-spacing: 0.03em !important;
        text-transform: uppercase !important; font-weight: 600 !important;
      }
      .oo-nba-legend b { color: var(--primary) !important; font-weight: 700 !important; }
      @media (max-width: 600px) {
        .oo-nba-pool { grid-template-columns: 1fr !important; gap: 6px !important; }
        .oo-nba-card { padding: 10px 12px !important; }
        .oo-nba-r2 { grid-template-columns: 1fr 1fr 1fr 52px 52px !important; gap: 8px !important; }
        .oo-nba-r2 input { font-size: 12px !important; padding: 6px 2px !important; }
      }
    `}</style>
    <SearchBar value={poolQ} onChange={setPoolQ} placeholder="Search pool by player, team, or position" total={sp.length} filtered={sp.filter(p => matchesSearch(p, poolQ, ['name', 'team', 'positions_str'])).length} />
    {isShowdown && (
      <div className="oo-nba-scope">
        <style>{`
          .oo-nba-scope {
            display: flex; align-items: center; gap: 8px; flex-wrap: wrap;
            padding: 8px 10px; margin-bottom: 10px;
            background: var(--card); border: 1px solid var(--border);
            border-radius: 8px;
          }
          .oo-nba-scope-label {
            font-size: 11px; color: var(--text-dim); font-weight: 600;
            text-transform: uppercase; letter-spacing: 0.04em;
            margin-right: 4px;
          }
          .oo-nba-scope-btns { display: flex; gap: 4px; }
          .oo-nba-scope-btn {
            padding: 6px 14px; font-size: 12px; font-weight: 700; border-radius: 6px;
            border: 1px solid var(--border); background: var(--bg);
            color: var(--text-muted); cursor: pointer; transition: all 0.12s;
            letter-spacing: 0.03em; text-transform: uppercase;
          }
          .oo-nba-scope-btn:hover { border-color: var(--border-light); color: var(--text); }
          .oo-nba-scope-btn.active {
            border-color: var(--primary); background: rgba(245,197,24,0.14);
            color: var(--primary);
          }
          .oo-nba-scope-hint {
            font-size: 11px; color: var(--text-dim); margin-left: auto;
            font-style: italic;
          }
          @media (max-width: 600px) {
            .oo-nba-scope { padding: 8px; }
            .oo-nba-scope-btn { padding: 7px 10px; font-size: 11px; flex: 1; }
            .oo-nba-scope-btns { flex: 1 1 100%; }
            .oo-nba-scope-hint { margin-left: 0; flex-basis: 100%; text-align: center; margin-top: 4px; }
          }
        `}</style>
        <span className="oo-nba-scope-label">Exposure scope</span>
        <div className="oo-nba-scope-btns">
          {[
            { k: 'all',  label: 'All' },
            { k: 'cpt',  label: 'Captain' },
            { k: 'flex', label: 'Flex' },
          ].map(({ k, label }) => (
            <button key={k}
              className={`oo-nba-scope-btn ${expScope === k ? 'active' : ''}`}
              onClick={() => setExpScope(k)}>{label}</button>
          ))}
        </div>
        <span className="oo-nba-scope-hint">
          {expScope === 'all'  && 'Min/max below cap total exposure across all slots'}
          {expScope === 'cpt'  && 'Min/max below cap captain-slot exposure only'}
          {expScope === 'flex' && 'Min/max below cap flex-slot exposure only'}
        </span>
      </div>
    )}
    <div className="oo-nba-legend">
      <span>Each card shows:</span>
      <span>Player</span>
      <span>Team</span>
      <span>$Sal</span>
      <span>Proj</span>
      <span>Own%</span>
      <b>{expScope === 'cpt' ? 'CPT Min%' : expScope === 'flex' ? 'FLEX Min%' : 'Min%'}</b>
      <b>{expScope === 'cpt' ? 'CPT Max%' : expScope === 'flex' ? 'FLEX Max%' : 'Max%'}</b>
    </div>
    <ul className="oo-nba-pool">{sp.filter(p => matchesSearch(p, poolQ, ['name', 'team', 'positions_str'])).map(p => {
      const ownPct = ownership[p.name] || 0;
      const teamColor = (NBA_TEAM_COLORS[p.team] || {}).fg || 'var(--text-muted)';
      const minVal = getCap(p.name, 'min');
      const maxVal = getCap(p.name, 'max');
      // In 'all' scope, default to globalMin/Max. In cpt/flex scope, default
      // to the unconstrained range 0/100 so nothing is enforced until the
      // user explicitly sets a per-slot cap.
      const minDefault = expScope === 'all' ? globalMin : 0;
      const maxDefault = expScope === 'all' ? globalMax : 100;
      return <li className="oo-nba-card" key={p.name}>
        <div className="oo-nba-r1">
          <span className="oo-nba-name" title={p.name}>{p.name}</span>
          <span className="oo-nba-team" style={{ color: teamColor }}>{p.team}</span>
        </div>
        <div className="oo-nba-r2">
          <span className="oo-nba-sal" title="Salary">{fmtSal(p.salary)}</span>
          <span className="oo-nba-proj" title="DK Fantasy Projection">{fmt(p.proj, 1)}</span>
          <span className="oo-nba-own" title="Simulated ownership" style={ownPct > 35 ? { color: 'var(--amber)', fontWeight: 600 } : {}}>{fmt(ownPct, 0)}%</span>
          <input type="number" min="0" max="100" value={minVal ?? minDefault}
            onChange={e => setCap(p.name, 'min', +e.target.value)}
            title={`${expScope === 'cpt' ? 'Captain' : expScope === 'flex' ? 'Flex' : 'Total'} min exposure % — ${p.name}`} />
          <input type="number" min="0" max="100" value={maxVal ?? maxDefault}
            onChange={e => setCap(p.name, 'max', +e.target.value)}
            title={`${expScope === 'cpt' ? 'Captain' : expScope === 'flex' ? 'Flex' : 'Total'} max exposure % — ${p.name}`} />
        </div>
      </li>;
    })}</ul>
    <div style={{ fontSize: 11, color: 'var(--text-dim)', marginBottom: 14, paddingLeft: 4 }}>
      {sp.length} players in pool
      {sp.length < rp.filter(p => p.salary > 0).length && (
        <span style={{ marginLeft: 10, color: 'var(--amber-text)' }}>
          · {rp.filter(p => p.salary > 0 && !p.projectable).length} excluded (no DK line, no manual projection)
        </span>
      )}
    </div>
    <button className="btn btn-primary" onClick={run} disabled={!canBuild || isBuilding}
      title={canBuild ? '' : `Edit at least 2 projections first (${overrideCount}/2 changed)`}
      style={(!canBuild || isBuilding) ? { opacity: 0.4, cursor: 'not-allowed' } : undefined}>
      <Icon name="bolt" size={14}/> Build {nL} {isShowdown ? 'Showdown' : 'Classic'} Lineups{contrarianOn ? ' (Contrarian)' : ''}
    </button>
    {isBuilding && <BuildAnimation count={nL} />}
    {!isBuilding && res && <NBAExposureResults res={res} ownership={ownership} cptOwnership={cptOwnership} onRebuild={run} onExportDK={exportDK} onExportReadable={exportReadable} nL={nL} canBuild={canBuild} overrideCount={overrideCount} favoriteLineups={favoriteLineups} onToggleFavorite={toggleFavoriteLineup} prevRes={prevRes} onRestorePrev={() => { setRes(prevRes); setPrevRes(null); }} />}
  </>);
}

function NBAExposureResults({ res, ownership, cptOwnership = {}, onRebuild, onExportDK, onExportReadable, nL, canBuild, overrideCount, favoriteLineups = [], onToggleFavorite, prevRes, onRestorePrev }) {
  const isShowdown = res.isShowdown;
  const [view, setView] = useState('all');   // 'all' | 'cpt' | 'flex'
  const [q, setQ] = useState('');
  // v3.24.14: MME set metrics — median/stdev cumulative ownership + unique players
  const setMetrics = useMemo(() => {
    if (!res.lineups.length || !res.pData) return null;
    const cumOwns = res.lineups.map(lu => {
      if (isShowdown) {
        const cptOwn = cptOwnership[res.pData[lu.cpt].name] || 0;
        const utilSum = lu.utils.reduce((s, i) => s + (ownership[res.pData[i].name] || 0), 0);
        return cptOwn + utilSum;
      }
      return lu.players.reduce((s, i) => s + (ownership[res.pData[i].name] || 0), 0);
    });
    const sorted = [...cumOwns].sort((a, b) => a - b);
    const mid = Math.floor(sorted.length / 2);
    const median = sorted.length % 2 === 0 ? (sorted[mid - 1] + sorted[mid]) / 2 : sorted[mid];
    const mean = cumOwns.reduce((s, v) => s + v, 0) / cumOwns.length;
    const stdev = Math.sqrt(cumOwns.reduce((s, v) => s + (v - mean) ** 2, 0) / cumOwns.length);
    const playerIdxs = isShowdown
      ? res.lineups.flatMap(lu => [lu.cpt, ...lu.utils])
      : res.lineups.flatMap(lu => lu.players);
    const uniqueCount = new Set(playerIdxs).size;
    return { median, stdev, uniqueCount };
  }, [res, ownership, cptOwnership, isShowdown]);

  // Favorites are managed by the parent (NBABuilderTab) so they persist across
  // Build clicks. Here we derive the set of favorited lineup hashes for quick
  // lookup in the card renderer.
  const favoriteKeySet = useMemo(() => {
    const s = new Set();
    for (const fav of favoriteLineups) {
      if (fav.cpt !== undefined) s.add(`CPT:${fav.cpt}|${[...fav.utils].sort().join('|')}`);
      else s.add((fav.players || []).slice().sort().join('|'));
    }
    return s;
  }, [favoriteLineups]);
  const lineupHash = (lu) => {
    if (isShowdown) {
      const cpt = res.pData[lu.cpt].name;
      const utils = lu.utils.map(i => res.pData[i].name).sort();
      return `CPT:${cpt}|${utils.join('|')}`;
    }
    return lu.players.map(i => res.pData[i].name).sort().join('|');
  };

  // Player filter — when enabled on a player, only lineups containing that
  // player (in the specified slot) are shown in the lineups grid.
  // For showdown: filter entries are { name, slot: 'cpt'|'flex'|'any' }.
  // Multiple active filters AND together (all must match).
  const [lineupFilters, setLineupFilters] = useState(() => []);
  const toggleFilter = (name, slot = 'any') => {
    setLineupFilters(prev => {
      const existing = prev.findIndex(f => f.name === name && f.slot === slot);
      if (existing >= 0) return prev.filter((_, i) => i !== existing);
      return [...prev, { name, slot }];
    });
  };
  const clearFilters = () => setLineupFilters([]);
  const matchesFilters = (lu) => {
    if (lineupFilters.length === 0) return true;
    for (const f of lineupFilters) {
      if (isShowdown) {
        const cptName = res.pData[lu.cpt].name;
        const utilNames = new Set(lu.utils.map(i => res.pData[i].name));
        if (f.slot === 'cpt'  && cptName !== f.name)      return false;
        if (f.slot === 'flex' && !utilNames.has(f.name))  return false;
        if (f.slot === 'any'  && cptName !== f.name && !utilNames.has(f.name)) return false;
      } else {
        const names = new Set(lu.players.map(i => res.pData[i].name));
        if (!names.has(f.name)) return false;
      }
    }
    return true;
  };

  // Tally CPT vs FLEX occurrences across the user's built lineups
  const { cptCnt, flexCnt } = useMemo(() => {
    const c = new Array(res.pData.length).fill(0);
    const f = new Array(res.pData.length).fill(0);
    if (isShowdown) {
      res.lineups.forEach(lu => {
        c[lu.cpt]++;
        lu.utils.forEach(i => f[i]++);
      });
    }
    return { cptCnt: c, flexCnt: f };
  }, [res, isShowdown]);

  // Per-player breakdown — each row has both the total / CPT / FLEX numbers
  // and the corresponding sim ownership from the top-1500 pool.
  const expRows = useMemo(() => {
    const N = res.lineups.length || 1;
    return res.pData.map((p, i) => {
      const total = res.counts[i];
      const cpt = cptCnt[i];
      const flex = flexCnt[i];
      const sal = p.util_salary || p.salary;
      const val = sal > 0 ? p.projection / (sal / 1000) : 0;
      const simOverall = ownership[p.name] || 0;
      const simCpt = cptOwnership[p.name] || 0;
      const simFlex = Math.max(0, simOverall - simCpt);
      return {
        name: p.name, team: p.team, salary: sal, projection: p.projection, val,
        totalCnt: total, cptCnt: cpt, flexCnt: flex,
        totalPct: total / N * 100,
        cptPct:   cpt   / N * 100,
        flexPct:  flex  / N * 100,
        simOverall, simCpt, simFlex,
      };
    });
  }, [res, cptCnt, flexCnt, ownership, cptOwnership]);

  // Apply view filter — compute pct/simOwn/cnt/lev relative to the chosen slot
  const displayRows = useMemo(() => {
    return expRows.map(p => {
      let pct, simOwn, cnt;
      if (isShowdown && view === 'cpt')        { pct = p.cptPct;   simOwn = p.simCpt;     cnt = p.cptCnt; }
      else if (isShowdown && view === 'flex')  { pct = p.flexPct;  simOwn = p.simFlex;    cnt = p.flexCnt; }
      else                                     { pct = p.totalPct; simOwn = p.simOverall; cnt = p.totalCnt; }
      const lev = Math.round((pct - simOwn) * 10) / 10;
      return { ...p, pct, simOwn, cnt, lev };
    });
  }, [expRows, view, isShowdown]);
  const filteredRows = useMemo(() => displayRows.filter(p => matchesSearch(p, q)), [displayRows, q]);

  const { sorted, sortKey, sortDir, toggleSort } = useSort(filteredRows, 'pct', 'desc');
  const S = p => <SH {...p} sortKey={sortKey} sortDir={sortDir} onSort={toggleSort} />;

  const avgSal = res.lineups.length ? Math.round(res.lineups.reduce((s, lu) => s + lu.sal, 0) / res.lineups.length) : 0;
  const projMax = res.lineups.length ? Math.max(...res.lineups.map(lu => lu.proj)) : 0;
  const projMin = res.lineups.length ? Math.min(...res.lineups.map(lu => lu.proj)) : 0;
  const avgOwn = res.lineups.length ? Math.round(res.lineups.reduce((s, lu) => {
    const all = isShowdown ? [lu.cpt, ...lu.utils] : lu.players;
    const lineupOwn = all.reduce((ss, pi) => ss + (ownership[res.pData[pi].name] || 0), 0) / all.length;
    return s + lineupOwn;
  }, 0) / res.lineups.length) : 0;

  const viewLabel = view === 'cpt' ? 'Captain' : view === 'flex' ? 'Flex' : 'All Positions';
  const pctColLabel = view === 'cpt' ? 'CPT Exposure' : view === 'flex' ? 'FLEX Exposure' : 'Exposure';
  const simColLabel = view === 'cpt' ? 'Sim CPT %'    : view === 'flex' ? 'Sim FLEX %'   : 'Sim Own %';

  const viewTabStyle = (active) => ({
    padding: '6px 14px', fontSize: 12, fontWeight: 700, borderRadius: 6,
    border: `1px solid ${active ? 'var(--primary)' : 'var(--border)'}`,
    background: active ? 'rgba(245,197,24,0.14)' : 'var(--card)',
    color: active ? 'var(--primary)' : 'var(--text-muted)',
    cursor: 'pointer', transition: 'all 0.12s',
    letterSpacing: '0.03em', textTransform: 'uppercase',
  });

  return (<>
    <div className="oo-summary-bar" style={{ marginTop: 20, padding: '10px 14px', background: 'var(--card)', border: '1px solid var(--border)', borderRadius: 8, fontSize: 13, color: 'var(--text-muted)', display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: '4px 12px' }}>
      <span><Icon name="check" size={14} color="#22C55E"/> Built <span style={{ color: 'var(--primary-glow)', fontWeight: 700 }}>{res.lineups.length}</span> lineups from {res.total.toLocaleString()} valid</span>
      <span className="oo-sum-sep" style={{ color: 'var(--text-dim)' }}>·</span>
      <span>Range: <span style={{ color: 'var(--green)' }}>{projMax}</span> → <span style={{ color: 'var(--text-dim)' }}>{projMin}</span></span>
      <span className="oo-sum-sep" style={{ color: 'var(--text-dim)' }}>·</span>
      <span>Avg Sal: <span style={{ color: 'var(--primary-glow)', fontWeight: 600 }}>${avgSal.toLocaleString()}</span></span>
      <span className="oo-sum-sep" style={{ color: 'var(--text-dim)' }}>·</span>
      <span>Avg Own: <span style={{ color: avgOwn > 35 ? 'var(--amber)' : 'var(--green)', fontWeight: 600 }}>{avgOwn}%</span></span>
      {setMetrics && <>
        <span className="oo-sum-sep" style={{ color: 'var(--text-dim)' }}>·</span>
        <span title="Median of sum-of-player-ownership across the lineup set">Median Own: <span style={{ color: 'var(--text)', fontWeight: 600 }}>{setMetrics.median.toFixed(0)}%</span></span>
        <span className="oo-sum-sep" style={{ color: 'var(--text-dim)' }}>·</span>
        <span title="Stdev of cumulative ownership across the set">Stdev: <span style={{ color: 'var(--text)', fontWeight: 600 }}>{setMetrics.stdev.toFixed(1)}</span></span>
        <span className="oo-sum-sep" style={{ color: 'var(--text-dim)' }}>·</span>
        <span title="Distinct players used across all lineups">Unique: <span style={{ color: 'var(--text)', fontWeight: 600 }}>{setMetrics.uniqueCount}</span></span>
      </>}
    </div>
    <div className="oo-action-row" style={{ display: 'flex', gap: 10, marginTop: 10, flexWrap: 'wrap' }}>
      {onRebuild && <button className="btn btn-primary" onClick={onRebuild} disabled={!canBuild} style={{ flex: '1 1 auto', width: 'auto', ...(canBuild ? {} : { opacity: 0.4, cursor: 'not-allowed' }) }}><Icon name="bolt" size={14}/> Rebuild {nL}</button>}
      {prevRes && onRestorePrev && <button
        onClick={onRestorePrev}
        className="oo-restore-btn"
        title="Restore the previous lineup set (undo Rebuild)"
        style={{ flex: '0 1 auto', padding: '10px 18px', fontSize: 13, fontWeight: 600,
          background: 'var(--card)', color: 'var(--text-muted)',
          border: '1px solid var(--border-light)', borderRadius: 8, cursor: 'pointer',
          display: 'inline-flex', alignItems: 'center', gap: 6 }}>
        <svg viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M3 7v6h6"/><path d="M21 17a9 9 0 0 0-15-6.7L3 13"/></svg>
        Restore previous
      </button>}
      {onExportDK && <button className="btn btn-primary" onClick={onExportDK} style={{ flex: '1 1 auto', width: 'auto', background: 'linear-gradient(135deg, #15803D, #22C55E)' }}><Icon name="download" size={14}/> Download DK Upload CSV</button>}
      {onExportReadable && <button className="btn btn-outline" onClick={onExportReadable} style={{ flex: '1 1 auto', width: 'auto', marginTop: 0 }}><Icon name="download" size={14}/> Readable CSV</button>}
    </div>

    <div className="section-head" style={{ marginTop: 22, display: 'flex', alignItems: 'center', gap: 10, flexWrap: 'wrap' }}>
      <span><Icon name="chart" size={16} color="#F5C518"/> Exposure <span style={{ fontSize: 12, color: 'var(--text-dim)', fontWeight: 500, marginLeft: 4 }}>· {viewLabel}</span></span>
      {isShowdown && (
        <div style={{ display: 'flex', gap: 4, marginLeft: 'auto' }}>
          <button style={viewTabStyle(view === 'all')}  onClick={() => setView('all')}>All</button>
          <button style={viewTabStyle(view === 'cpt')}  onClick={() => setView('cpt')}>Captain</button>
          <button style={viewTabStyle(view === 'flex')} onClick={() => setView('flex')}>Flex</button>
        </div>
      )}
    </div>
    {isShowdown && (
      <div style={{ fontSize: 11, color: 'var(--text-dim)', marginBottom: 8, paddingLeft: 2 }}>
        {view === 'all'  && 'Total exposure across all roster spots · Leverage compared to overall sim ownership'}
        {view === 'cpt'  && 'How often each player was your captain · Leverage compared to captain-slot sim ownership'}
        {view === 'flex' && 'How often each player was in your flex/utility slots · Leverage compared to flex-slot sim ownership'}
      </div>
    )}

    <SearchBar value={q} onChange={setQ} placeholder="Search exposure" total={displayRows.length} filtered={filteredRows.length} />
    <div className="table-wrap" style={{ marginBottom: 20 }}><table><thead><tr>
      <S label="Player" colKey="name" /><th>Team</th>
      <S label="Salary" colKey="salary" num />
      <S label="Proj" colKey="projection" num />
      <S label="Val" colKey="val" num />
      <S label="Count" colKey="cnt" num />
      <S label={pctColLabel} colKey="pct" />
      <S label={simColLabel} colKey="simOwn" />
      <S label="Leverage" colKey="lev" num />
      <th title="Filter displayed lineups by this player" style={{ textAlign: 'center' }}>Filter</th>
    </tr></thead>
    <tbody>{sorted.map(p => {
      const filteredAny  = lineupFilters.some(f => f.name === p.name && f.slot === 'any');
      const filteredCpt  = lineupFilters.some(f => f.name === p.name && f.slot === 'cpt');
      const filteredFlex = lineupFilters.some(f => f.name === p.name && f.slot === 'flex');
      const btnStyle = (active, color) => ({
        width: 24, height: 24, padding: 0, border: `1px solid ${active ? color : 'var(--border)'}`, background: active ? color : 'transparent',
        color: active ? '#0A1628' : 'var(--text-muted)', borderRadius: 4, cursor: 'pointer', fontSize: 11, fontWeight: 700, lineHeight: '22px',
      });
      return <tr key={p.name}>
        <td className="name">{p.name}</td>
        <td>{p.team ? <TeamBadge team={p.team} /> : ''}</td>
        <td className="num">${p.salary.toLocaleString()}</td>
        <td className="num">{fmt(p.projection, 1)}</td>
        <td className="num">{fmt(p.val, 2)}</td>
        <td className="num">{p.cnt}</td>
        <td><span className="exp-bar-bg"><span className="exp-bar" style={{ width: Math.min(p.pct, 100) + '%' }} /></span>{fmt(p.pct, 1)}%</td>
        <td className="num muted">{fmt(p.simOwn, 1)}%</td>
        <td className="num"><span style={{ color: p.lev > 0 ? 'var(--green)' : p.lev < 0 ? 'var(--red)' : 'var(--text-dim)', fontWeight: Math.abs(p.lev) > 10 ? 700 : 400 }}>{p.lev > 0 ? '+' : ''}{fmt(p.lev, 1)}%</span></td>
        <td style={{ textAlign: 'center', whiteSpace: 'nowrap' }}>
          {isShowdown ? (
            <span style={{ display: 'inline-flex', gap: 3 }}>
              <button style={btnStyle(filteredCpt,  '#F5C518')} onClick={() => toggleFilter(p.name, 'cpt')}  title={filteredCpt  ? 'Remove CPT filter'  : `Filter lineups with ${p.name} as CPT`}>C</button>
              <button style={btnStyle(filteredFlex, 'var(--primary)')} onClick={() => toggleFilter(p.name, 'flex')} title={filteredFlex ? 'Remove FLEX filter' : `Filter lineups with ${p.name} in FLEX`}>F</button>
            </span>
          ) : (
            <button style={btnStyle(filteredAny, 'var(--primary)')} onClick={() => toggleFilter(p.name, 'any')} title={filteredAny ? 'Remove filter' : `Show only lineups containing ${p.name}`}>F</button>
          )}
        </td>
      </tr>;
    })}</tbody></table></div>

    {/* Active filter chips — click a chip to remove the filter. */}
    {lineupFilters.length > 0 && (
      <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap', alignItems: 'center', padding: '8px 12px', background: 'var(--card)', border: '1px solid var(--border)', borderRadius: 8, marginBottom: 10 }}>
        <span style={{ fontSize: 11, fontWeight: 600, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: 0.5 }}>Filtering</span>
        {lineupFilters.map((f, i) => {
          const color = f.slot === 'cpt' ? '#F5C518' : f.slot === 'flex' ? 'var(--primary)' : 'var(--primary)';
          const slotLabel = f.slot === 'cpt' ? 'CPT' : f.slot === 'flex' ? 'FLEX' : '';
          return <button key={i}
            onClick={() => toggleFilter(f.name, f.slot)}
            style={{ padding: '3px 9px', background: 'transparent', border: `1px solid ${color}`, color, borderRadius: 999, fontSize: 11, fontWeight: 600, cursor: 'pointer', display: 'inline-flex', alignItems: 'center', gap: 5 }}
            title="Click to remove this filter">
            {slotLabel && <span style={{ fontSize: 9, opacity: 0.8 }}>{slotLabel}</span>}
            {f.name}
            <span style={{ opacity: 0.6 }}>✕</span>
          </button>;
        })}
        <button onClick={clearFilters} style={{ marginLeft: 'auto', padding: '3px 9px', background: 'transparent', border: '1px solid var(--border)', color: 'var(--text-muted)', borderRadius: 4, fontSize: 11, fontWeight: 500, cursor: 'pointer' }}>Clear all</button>
      </div>
    )}

    <div className="section-head"><Icon name="target" size={16} color="#F5C518"/> Lineups{lineupFilters.length > 0 && <span style={{ fontSize: 12, color: 'var(--text-dim)', fontWeight: 500, marginLeft: 8 }}>· {res.lineups.filter(matchesFilters).length} matching</span>}</div>
    <div className="lineup-grid">{res.lineups.filter(matchesFilters).slice(0, 30).map((lu, idx) => {
      const luHash = lineupHash(lu);
      const isFav = favoriteKeySet.has(luHash);
      if (res.isShowdown) {
        const cpt = res.pData[lu.cpt];
        const utils = lu.utils.map(i => res.pData[i]);
        // Cumulative ownership using CPT-specific own% for captain slot + FLEX-only
        // own% (total − cpt) for UTIL slots. This reflects what % of the field
        // will actually have each player in the SAME slot. Captain ownership is
        // structurally lower (only 1 captain per lineup), so using total own for
        // a captain would overstate the chalk burden.
        const cptOwnOnly = cptOwnership[cpt.name] || 0;
        const utilCumOwn = utils.reduce((s, p) => s + Math.max(0, (ownership[p.name] || 0) - (cptOwnership[p.name] || 0)), 0);
        const cumOwn = Math.round(cptOwnOnly + utilCumOwn);
        const cumColor = cumOwn > 220 ? 'var(--amber)' : cumOwn > 180 ? 'var(--primary)' : 'var(--green)';
        const cumLabel = `Cumulative ownership — sum of CPT% (${cptOwnOnly.toFixed(1)}%) and each UTIL player's FLEX-only% (total own − CPT own). Lower = more unique vs field. Typical range: 150 super-contrarian → 280+ mega-chalk.`;
        return <div className="lu-card" key={idx} style={isFav ? { borderColor: '#F5C518', boxShadow: '0 0 0 1px #F5C518 inset, 0 2px 8px rgba(245,197,24,0.15)' } : undefined}>
          <div className="lu-header">
            <span style={{ display: 'inline-flex', alignItems: 'center', gap: 6 }}>
              <button
                onClick={() => onToggleFavorite && onToggleFavorite(lu, res.pData)}
                title={isFav ? 'Unfavorite — will no longer persist through rebuilds' : 'Favorite this lineup — it survives rebuilds and is always exported'}
                style={{ width: 20, height: 20, padding: 0, border: 'none', background: 'transparent', cursor: 'pointer', color: isFav ? '#F5C518' : 'var(--text-dim)', fontSize: 14, lineHeight: 1 }}>
                {isFav ? '★' : '☆'}
              </button>
              <span>#{idx + 1}</span>
            </span>
            <span className="lu-proj">{lu.proj} pts</span>
          </div>
          <div className="lu-row">
            <span style={{ fontSize: 10, fontWeight: 700, color: '#F5C518', width: 44, flexShrink: 0, letterSpacing: 0.5 }}>CPT</span>
            <span className="lu-name">{cpt.name}</span>
            <span className="lu-opp"><TeamBadge team={cpt.team} /></span>
            <span className="lu-sal">${(cpt.cpt_salary || 0).toLocaleString()}</span>
            <span className="lu-pts">{fmt(cpt.projection * 1.5, 1)}</span>
            <span style={{ width: 36, textAlign: 'right', color: cptOwnOnly > 20 ? 'var(--amber)' : 'var(--text-dim)', fontSize: 11 }} title={`${cpt.name} is captained in ${cptOwnOnly.toFixed(1)}% of field lineups`}>{fmt(cptOwnOnly, 0)}%</span>
          </div>
          {utils.map((p) => {
            const flexOwn = Math.max(0, (ownership[p.name] || 0) - (cptOwnership[p.name] || 0));
            return <div className="lu-row" key={p.name}>
              <span style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-muted)', width: 44, flexShrink: 0, letterSpacing: 0.5 }}>UTIL</span>
              <span className="lu-name">{p.name}</span>
              <span className="lu-opp"><TeamBadge team={p.team} /></span>
              <span className="lu-sal">${(p.util_salary || p.salary).toLocaleString()}</span>
              <span className="lu-pts">{fmt(p.projection, 1)}</span>
              <span style={{ width: 36, textAlign: 'right', color: flexOwn > 50 ? 'var(--amber)' : 'var(--text-dim)', fontSize: 11 }} title={`${p.name} is in UTIL in ${flexOwn.toFixed(1)}% of field lineups`}>{fmt(flexOwn, 0)}%</span>
            </div>;
          })}
          <div className="lu-footer">
            <span>${lu.sal.toLocaleString()}</span>
            <span title={cumLabel} style={{ color: cumColor, cursor: 'help' }}>Own: {cumOwn}%</span>
          </div>
        </div>;
      }
      // Classic
      const ps = lu.players.map(i => res.pData[i]).sort((a, b) => b.salary - a.salary);
      const cumClassic = Math.round(ps.reduce((s, p) => s + (ownership[p.name] || 0), 0));
      const classicColor = cumClassic > 120 ? 'var(--amber)' : cumClassic > 90 ? 'var(--primary)' : 'var(--green)';
      const classicLabel = `Cumulative field ownership — sum of each player's total own%. Lower = more unique vs field.`;
      return <div className="lu-card" key={idx} style={isFav ? { borderColor: '#F5C518', boxShadow: '0 0 0 1px #F5C518 inset, 0 2px 8px rgba(245,197,24,0.15)' } : undefined}>
        <div className="lu-header">
          <span style={{ display: 'inline-flex', alignItems: 'center', gap: 6 }}>
            <button
              onClick={() => onToggleFavorite && onToggleFavorite(lu, res.pData)}
              title={isFav ? 'Unfavorite — will no longer persist through rebuilds' : 'Favorite this lineup — it survives rebuilds and is always exported'}
              style={{ width: 20, height: 20, padding: 0, border: 'none', background: 'transparent', cursor: 'pointer', color: isFav ? '#F5C518' : 'var(--text-dim)', fontSize: 14, lineHeight: 1 }}>
              {isFav ? '★' : '☆'}
            </button>
            <span>#{idx + 1}</span>
          </span>
          <span className="lu-proj">{lu.proj} pts</span>
        </div>
        {ps.map(p => {
          const ownPct = ownership[p.name] || 0;
          return <div className="lu-row" key={p.name}>
            <span className="lu-name">{p.name}</span>
            <span className="lu-opp"><TeamBadge team={p.team} /></span>
            <span className="lu-sal">${p.salary.toLocaleString()}</span>
            <span className="lu-pts">{fmt(p.projection, 1)}</span>
            <span style={{ width: 36, textAlign: 'right', color: ownPct > 35 ? 'var(--amber)' : 'var(--text-dim)', fontSize: 11 }}>{fmt(ownPct, 0)}%</span>
          </div>;
        })}
        <div className="lu-footer">
          <span>${lu.sal.toLocaleString()}</span>
          <span title={classicLabel} style={{ color: classicColor, cursor: 'help' }}>Own: {cumClassic}%</span>
        </div>
      </div>;
    })}</div>
    {(() => { const vis = res.lineups.filter(matchesFilters).length; return vis > 30 && <div style={{ textAlign: 'center', color: 'var(--text-dim)', fontSize: 13, marginTop: 8 }}>+ {vis - 30} more{lineupFilters.length > 0 && ' matching filters'}</div>; })()}
  </>);
}
