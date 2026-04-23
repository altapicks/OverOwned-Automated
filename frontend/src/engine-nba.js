// ============================================================
// OverOwned — NBA ENGINE
// Mirrors the engine.js / engine-mma.js interface: pure functions,
// no React, no imports. Consumed by App.jsx for NBA slates.
//
// Key concepts:
//  - Minute projection blends rotation data (L3/L5/L10/season)
//  - Pace factor adjusts counting stats by game tempo
//  - Blowout risk fades starters' minutes and boosts bench
//  - PP stat lines (where available) are used as the "truth" projection,
//    then DK/PP Fantasy Scores are DERIVED from those stat projections
//    so both tabs compute from one shared stat model.
//  - Injury cascade: OUT/DOUBTFUL player's minutes and usage get
//    redistributed to positional backups (60/30/10 mins, 50/25 usg).
// ============================================================

function round2(n) { return Math.round(n * 100) / 100; }
function clamp(n, lo, hi) { return Math.max(lo, Math.min(hi, n)); }

// ------------------------------------------------------------
// ODDS → PROBABILITY helpers (for DD/TD odds lines)
// ------------------------------------------------------------
function americanToProb(odds) {
  if (!odds || odds === 0) return 0;
  return odds > 0 ? 100 / (odds + 100) : Math.abs(odds) / (Math.abs(odds) + 100);
}

// ------------------------------------------------------------
// DEVIG — remove bookmaker vig from a two-way market.
// Input: over/under American odds.
// Output: { pOver, pUnder, vig } where pOver + pUnder === 1.
// ------------------------------------------------------------
export function devig(overOdds, underOdds) {
  const pOverRaw  = americanToProb(overOdds);
  const pUnderRaw = americanToProb(underOdds);
  const total = pOverRaw + pUnderRaw;
  if (total <= 0) return { pOver: 0.5, pUnder: 0.5, vig: 0 };
  return {
    pOver:  pOverRaw / total,
    pUnder: pUnderRaw / total,
    vig:    total - 1,
  };
}

// ------------------------------------------------------------
// Beasley-Springer-Moro approximation for the inverse standard normal CDF.
// Accurate to ~1e-5 across the full (0,1) range; used to convert
// fair over-probability into z-scores for line→projection conversion.
// ------------------------------------------------------------
function invNormal(p) {
  if (p <= 0) return -10;
  if (p >= 1) return 10;
  const a = [-39.6968302866538, 220.946098424521, -275.928510446969,
              138.357751867269, -30.6647980661472, 2.50662827745924];
  const b = [-54.4760987982241, 161.585836858041, -155.698979859887,
              66.8013118877197, -13.2806815528857];
  const c = [-0.00778489400243029, -0.322396458041136, -2.40075827716184,
             -2.54973253934373, 4.37466414146497, 2.93816398269878];
  const d = [0.00778469570904146, 0.32246712907004, 2.445134137143, 3.75440866190742];
  const pLow = 0.02425, pHigh = 1 - pLow;
  if (p < pLow) {
    const q = Math.sqrt(-2 * Math.log(p));
    return (((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5])
         / ((((d[0]*q+d[1])*q+d[2])*q+d[3])*q+1);
  } else if (p <= pHigh) {
    const q = p - 0.5;
    const r = q * q;
    return (((((a[0]*r+a[1])*r+a[2])*r+a[3])*r+a[4])*r+a[5]) * q
         / (((((b[0]*r+b[1])*r+b[2])*r+b[3])*r+b[4])*r+1);
  } else {
    const q = Math.sqrt(-2 * Math.log(1 - p));
    return -(((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5])
         / ((((d[0]*q+d[1])*q+d[2])*q+d[3])*q+1);
  }
}

// Per-stat standard deviation heuristics (NBA empirical). Used to convert
// devigged pOver → projected mean via the normal-approximation inverse.
//   projection = line + sigma * Φ⁻¹(pOverFair)
// Lines come in half-integers so the continuity correction is already
// baked into the half-line; we treat line as continuous for the normal approx.
function sigmaForStat(stat, line) {
  const m = Math.max(line, 1);
  switch (String(stat).toLowerCase()) {
    case 'points':    return Math.max(4.0, m * 0.33);
    case 'rebounds':  return Math.max(1.8, m * 0.35);
    case 'assists':   return Math.max(1.3, m * 0.40);
    case 'threes':
    case '3pm':       return 1.25;
    case 'stls_blks':
    case 'stls+blks': return Math.max(0.9, m * 0.45);
    case 'pra':       return Math.max(5.5, m * 0.28);
    default:          return Math.max(1.5, m * 0.35);
  }
}

// Convert a single devigged prop (line + fair over prob) to a projected
// mean using the normal approximation. Falls back gracefully when only
// one side of the market is offered.
export function lineToProjection(line, pOverFair, stat) {
  if (pOverFair == null || !isFinite(pOverFair)) return line;
  const p = clamp(pOverFair, 0.001, 0.999);
  const z = invNormal(p);
  const sigma = sigmaForStat(stat, line);
  return round2(line + sigma * z);
}

// ------------------------------------------------------------
// MINUTES PROJECTION
// Blend of recent games + season. Playoff game 1 weighting:
//   20% L3, 50% L5, 20% L10, 10% season.
// This de-emphasizes garbage-time L3 spikes while still capturing
// recent lineup changes.
// ------------------------------------------------------------
export function projectMinutes(minsObj) {
  if (!minsObj) return 0;
  const L3  = Number(minsObj.L3)  || 0;
  const L5  = Number(minsObj.L5)  || 0;
  const L10 = Number(minsObj.L10) || 0;
  const All = Number(minsObj.All) || 0;
  // Only All available (e.g. playoff slate where rest/DNPs skew L3/L5/L10)
  if (L3 === 0 && L5 === 0 && L10 === 0 && All > 0) {
    return All;
  }
  // If L3/L5 are both zero (rest/injury return), fall back to L10/All weighted
  if (L3 === 0 && L5 === 0 && (L10 > 0 || All > 0)) {
    return 0.55 * L10 + 0.45 * All;
  }
  return 0.20 * L3 + 0.50 * L5 + 0.20 * L10 + 0.10 * All;
}

// Turnover estimate used only for the DK -0.5 scoring slot. Pure creation-load
// proxy from devigged projections; no minute dependency.
function estimateTurnovers(assists, points) {
  const creation = (assists || 0) + (points || 0) * 0.08;
  return round2(clamp(creation * 0.22, 0.3, 4.0));
}

// Gaussian approximation for DD/TD fallback — retained for the rare case
// a player has stat projections but no DD/TD odds (not used on current slate
// since all meaningful DD/TD markets are provided via odds).
function probOverTen(mean) {
  if (mean < 4) return 0;
  if (mean >= 15) return 0.95;
  const sigma = Math.max(1.5, mean * 0.35);
  const z = (10 - mean) / sigma;
  const t = 1 / (1 + 0.2316419 * Math.abs(z));
  const d = 0.3989423 * Math.exp(-z * z / 2);
  let p = d * t * (0.3193815 + t * (-0.3565638 + t * (1.781478 + t * (-1.821256 + t * 1.330274))));
  if (z >= 0) p = 1 - p;
  return clamp(1 - p, 0, 1);
}
function stlBlkSplit(positions) {
  const pos = Array.isArray(positions) ? positions.join('/') : String(positions || '');
  const hasC = /C/.test(pos) || /PF/.test(pos);
  const hasG = /PG/.test(pos) || /SG/.test(pos);
  if (hasC && !hasG) return { stlPct: 0.35, blkPct: 0.65 };   // big
  if (hasG && !hasC) return { stlPct: 0.70, blkPct: 0.30 };   // guard
  return { stlPct: 0.55, blkPct: 0.45 };                       // wing / hybrid
}

// Share of (rebounds + assists) combined, by position. Used when DK prices
// the PRA composite but not the individual Rebounds / Assists markets.
// Bigs rebound more and pass less; PGs pass more and rebound less.
function rebAstSplit(positions) {
  const pos = Array.isArray(positions) ? positions.join('/') : String(positions || '');
  if (/C/i.test(pos))  return { rebShare: 0.75, astShare: 0.25 };   // center
  if (/PF/i.test(pos)) return { rebShare: 0.65, astShare: 0.35 };   // power forward
  if (/SF/i.test(pos)) return { rebShare: 0.55, astShare: 0.45 };   // small forward / wing
  if (/PG/i.test(pos)) return { rebShare: 0.30, astShare: 0.70 };   // point guard
  return { rebShare: 0.40, astShare: 0.60 };                        // shooting guard / default
}

// Build a player stats object ENTIRELY from devigged DraftKings prop lines.
// No minute scaling, pace factor, blowout adjustment, cascade multiplier,
// or fallback estimation. If a stat has no DK line, it is treated as 0
// (and flagged via `hasStatData` so the UI can show which stats are missing).
// If a player has zero DK prop lines at all, `projectable` is false and the
// caller should exclude them from the showdown pool.
//
//   player  — slate.dk_players entry
//   ctx     — accepted for interface compatibility; all fields ignored
//
// Returns: { projectable, status, pts, reb, ast, threesM, stl, blk, to,
//            pDD, pTD, projMins (display only), usg, hasStatData }
export function buildPlayerStats(player, /* ctx */ _ctx = {}) {
  const status = (player.status || 'ACTIVE').toUpperCase();
  if (status === 'OUT') {
    return {
      projectable: false, status,
      pts: 0, reb: 0, ast: 0, threesM: 0, stl: 0, blk: 0, to: 0,
      pDD: 0, pTD: 0, projMins: 0, usg: 0,
      hasStatData: {},
    };
  }

  const props = player.dk_props || {};

  function readProp(key) {
    const pr = props[key];
    if (!pr || pr.line == null) return null;
    if (pr.over != null && pr.under != null) {
      const { pOver } = devig(pr.over, pr.under);
      return lineToProjection(pr.line, pOver, key);
    }
    // Single-sided market — treat line as the projection
    return pr.line;
  }

  const pts       = readProp('points');
  let   reb       = readProp('rebounds');
  let   ast       = readProp('assists');
  let   threesM   = readProp('threes');
  // Steals/blocks are NOT devigged from DK's stls_blks market anymore.
  // DK's combined line forces a position-based 55/45-ish split that loses
  // accuracy vs a per-player projection. We read player.stl / player.blk
  // from the slate directly (typically supplied by a projection CSV).
  const pra       = readProp('pra');      // DK Points+Rebounds+Assists market

  // ─────────────────────────────────────────────────────────────────────
  // PRA BACK-CALC — when DK offers a PRA market but is missing one or
  // both of the component individual markets, back-calculate the missing
  // component(s). This is still PURE DK-market data (nothing fabricated),
  // just cross-sourced from the composite prop when the individual isn't
  // priced. Without this, players like Grayson Allen (PRA 13.5, Points
  // 8.5, but no Rebounds/Assists market) get projected using only their
  // Points line and badly underprojected.
  // ─────────────────────────────────────────────────────────────────────
  let rebFromPra = false, astFromPra = false;
  if (pra != null && pts != null && pra > pts) {
    const ra = Math.max(0, pra - pts);     // projected rebounds + assists
    if (reb == null && ast == null) {
      // Split the composite by position archetype
      const split = rebAstSplit(player.positions);
      reb = ra * split.rebShare;
      ast = ra * split.astShare;
      rebFromPra = true;
      astFromPra = true;
    } else if (reb == null && ast != null) {
      reb = Math.max(0, ra - ast);
      rebFromPra = true;
    } else if (ast == null && reb != null) {
      ast = Math.max(0, ra - reb);
      astFromPra = true;
    }
    // If both individual lines existed, we don't overwrite — those are the
    // most precise numbers. PRA just confirms the sum roughly matches.
  }

  // ─────────────────────────────────────────────────────────────────────
  // CSV FALLBACK — fill any stat that DK didn't price (and PRA back-calc
  // couldn't derive) from the projection CSV. Preference order:
  //   1. DK direct market (most accurate for that specific stat)
  //   2. DK PRA back-calc (derived from DK composite — still DK-sourced)
  //   3. CSV projection (fallback when DK doesn't price the market)
  // Without this step, players DK only prices for Points + Threes (e.g.
  // Booker, Green, Brooks on PHX@OKC) project at 0 for reb/ast and get
  // near-zero exposure despite being obvious core plays.
  // ─────────────────────────────────────────────────────────────────────
  let rebFromCsv = false, astFromCsv = false, threesFromCsv = false;
  if (reb     == null && player.reb    != null) { reb     = Number(player.reb);    rebFromCsv    = true; }
  if (ast     == null && player.ast    != null) { ast     = Number(player.ast);    astFromCsv    = true; }
  if (threesM == null && player.threes != null) { threesM = Number(player.threes); threesFromCsv = true; }

  const hasStatData = {
    points:    pts       != null,
    rebounds:  reb       != null,
    assists:   ast       != null,
    threes:    threesM   != null,
    // stls_blks = "do we have stl+blk values for this player" — now sourced
    // from the projection CSV (player.stl / player.blk) instead of DK's
    // combined market. Kept under the legacy key so the UI's stat-coverage
    // badge keeps working.
    stls_blks: (player.stl != null) || (player.blk != null),
    rebFromPra, astFromPra,
    rebFromCsv, astFromCsv, threesFromCsv,
    pra:       pra       != null,
  };

  // Needs Points at minimum to be "projectable" via DK prop devig.
  const projectableViaProps = hasStatData.points === true;

  // Manual projection fallback: a user-supplied DK fantasy score for
  // bench players DK hasn't priced with prop lines. This is the ONLY way
  // a no-DK-line player becomes projectable. If neither DK props nor a
  // manual projection exists, the player stays unprojectable (→ proj 0,
  // excluded from builder and ownership sim).
  const manualDk = (!projectableViaProps && player.manual_proj != null && player.manual_proj > 0)
    ? Number(player.manual_proj)
    : null;

  if (!projectableViaProps && manualDk == null) {
    return {
      projectable: false, status,
      pts: 0, reb: 0, ast: 0, threesM: 0, stl: 0, blk: 0, to: 0,
      pDD: 0, pTD: 0,
      projMins: round2(projectMinutes(player.mins)),   // informational
      usg: 0,
      hasStatData: {},
    };
  }

  if (manualDk != null) {
    // User-supplied DK fantasy value. Synthesize a minimal stats object
    // so scoring functions know to return it directly.
    return {
      projectable: true, status,
      manual: true, manualDk,
      pts: 0, reb: 0, ast: 0, threesM: 0, stl: 0, blk: 0, to: 0,
      pDD: 0, pTD: 0,
      projMins: round2(projectMinutes(player.mins)),
      usg: 0,
      hasStatData: {},
    };
  }

  // Projectable via DK props.

  // Steals and blocks come straight from the slate (supplied by the projection
  // CSV). We no longer split a devigged DK stls_blks sum by position —
  // per-player projections from a stats model are more accurate than an
  // archetype split of a combined market.
  const stl = (player.stl != null) ? Number(player.stl) : 0;
  const blk = (player.blk != null) ? Number(player.blk) : 0;

  // Turnovers: prefer explicit per-player value from the slate (user-supplied
  // rotation data); fall back to the creation-load estimator when absent.
  // DK scoring: -0.5 × TO, PP scoring: -1 × TO.
  const to = (player.turnovers != null)
    ? Number(player.turnovers)
    : estimateTurnovers(ast || 0, pts || 0);

  // DD/TD probabilities — direct from odds when present, else 0.
  // These also come from DraftKings markets (Same Game Parlay screen).
  const pDD = player.dd_odds ? americanToProb(player.dd_odds) : 0;
  const pTD = player.td_odds ? americanToProb(player.td_odds) : 0;

  // Minutes: INFORMATIONAL ONLY. Not used for scaling stats.
  const projMins = round2(projectMinutes(player.mins));

  return {
    projectable: true, status,
    pts: round2(pts || 0),
    reb: round2(reb || 0),
    ast: round2(ast || 0),
    threesM: round2(threesM || 0),
    stl: round2(stl),
    blk: round2(blk),
    to: round2(to),
    pDD: round2(pDD),
    pTD: round2(pTD),
    projMins,
    usg: 0,
    hasStatData,
  };
}

// Simplified TO estimate — kept as alias for back-compat with any older imports
function estimateTurnoversSimple(assists, points) {
  return estimateTurnovers(assists, points);
}

// ------------------------------------------------------------
// DK NBA SCORING
//  +1 pt, +0.5 3PM, +1.25 reb, +1.5 ast, +2 stl, +2 blk, −0.5 TO
//  +1.5 DD, +3 TD
//
// If stats came from a manual projection (no DK prop lines available),
// we return that manual value directly — no scoring recomputation.
// ------------------------------------------------------------
export function dkProjection(stats) {
  if (!stats) return 0;
  if (stats.manual && stats.manualDk != null) return round2(stats.manualDk);
  return round2(
    (stats.pts       || 0) * 1
    + (stats.threesM || 0) * 0.5
    + (stats.reb     || 0) * 1.25
    + (stats.ast     || 0) * 1.5
    + (stats.stl     || 0) * 2
    + (stats.blk     || 0) * 2
    - (stats.to      || 0) * 0.5
    + (stats.pDD     || 0) * 1.5
    + (stats.pTD     || 0) * 3
  );
}

// Ceiling projection: what an 85th-percentile night looks like.
// Applies +25% to counting stats + doubles DD/TD bonus frequency.
// For manual projections, scale 1.25× as a reasonable ceiling.
export function dkCeiling(stats) {
  if (!stats) return 0;
  if (stats.manual && stats.manualDk != null) return round2(stats.manualDk * 1.25);
  return round2(
    (stats.pts       || 0) * 1.25
    + (stats.threesM || 0) * 0.5 * 1.3
    + (stats.reb     || 0) * 1.25 * 1.2
    + (stats.ast     || 0) * 1.5 * 1.2
    + (stats.stl     || 0) * 2 * 1.3
    + (stats.blk     || 0) * 2 * 1.3
    - (stats.to      || 0) * 0.5 * 0.9
    + Math.min(1, (stats.pDD || 0) * 2) * 1.5
    + Math.min(1, (stats.pTD || 0) * 2.5) * 3
  );
}

// ------------------------------------------------------------
// PP NBA FANTASY SCORE
//  +1 pt, +1.2 reb, +1.5 ast, +3 blk, +3 stl, −1 TO
//  (3PM is NOT included; DD/TD bonuses are NOT included)
//
// For manual DK projections, approximate PP FS as 0.95× DK. This is a
// rough typical ratio for low-variance bench players where neither
// scoring formula gets big bonuses.
// ------------------------------------------------------------
export function ppProjection(stats) {
  if (!stats) return 0;
  if (stats.manual && stats.manualDk != null) return round2(stats.manualDk * 0.95);
  return round2(
    (stats.pts || 0) * 1
    + (stats.reb || 0) * 1.2
    + (stats.ast || 0) * 1.5
    + (stats.stl || 0) * 3
    + (stats.blk || 0) * 3
    - (stats.to  || 0) * 1
  );
}

// Simple EV: projection − PP line
export function ppEV(projection, line) {
  return round2(projection - line);
}

// Computes projected value for an individual PP stat category.
export function projectPPStat(stats, statName) {
  if (!stats) return 0;
  const s = statName.toLowerCase();
  if (s === 'points' || s === 'pts') return stats.pts || 0;
  if (s === 'rebounds' || s === 'reb') return stats.reb || 0;
  if (s === 'assists' || s === 'ast') return stats.ast || 0;
  if (s === '3pm' || s === 'threes' || s === '3-pt made') return stats.threesM || 0;
  if (s === 'stls+blks' || s === 'blks+stls' || s === 'defense') return (stats.stl || 0) + (stats.blk || 0);
  if (s === 'pts+reb+ast' || s === 'pra') return (stats.pts || 0) + (stats.reb || 0) + (stats.ast || 0);
  if (s === 'fantasy score' || s === 'fs') return ppProjection(stats);
  if (s === 'doubledouble' || s === 'double-double' || s === 'dd') return stats.pDD || 0;
  if (s === 'tripledouble' || s === 'triple-double' || s === 'td') return stats.pTD || 0;
  if (s === 'turnovers' || s === 'to') return stats.to || 0;
  return 0;
}

// ------------------------------------------------------------
// INJURY / STATUS PASSTHROUGH
// Previously this function redistributed minutes and usage from OUT
// players to their positional backups. Under the pure-DK-devig model,
// projections come directly from DraftKings' prop lines (which already
// reflect DK's view of who's playing when the lines were set), so we
// no longer mutate projections based on user-set statuses. The function
// is kept for API compatibility and simply clones the input so callers
// that mutate the result don't accidentally mutate the slate file.
// ------------------------------------------------------------
export function applyInjuryAdjustments(players) {
  return players.map(p => ({ ...p }));
}

// ============================================================
// FIELD OWNERSHIP SIMULATOR (Showdown)
// Weighted random sampling that models how the PUBLIC builds — chalk gravitates
// to high projection but not perfectly. Produces realistic ownership
// percentages (studs 40-65% rather than 95%+ from pure top-N enumeration).
//
// How it differs from optimizeShowdown:
//   - optimizeShowdown = top-N by projection → used for the USER'S builder
//   - simulateFieldShowdown = random weighted → used for ownership estimates
// ============================================================
export function simulateFieldShowdown(players, nSims = 1500, salaryCap = 50000) {
  const valid = players.filter(p =>
    p.projection > 0 &&
    p.util_salary > 0 &&
    p.cpt_salary > 0 &&
    (p.status || 'ACTIVE').toUpperCase() !== 'OUT'
  );
  if (valid.length < 6) return { counts: players.map(() => 0), lineups: 0 };

  // CPT weight: heavy emphasis on raw ceiling — field captains the best scorer
  // the vast majority of the time. Exponent 3.2 pushes the top stud to ~75%+
  // CPT frequency on showdown slates with a dominant player (matches observed
  // public ownership in contests like PHX@OKC Game 1).
  const cptWeights = valid.map(p => Math.pow(p.projection * 1.5, 3.2));
  const cptTotal = cptWeights.reduce((a, b) => a + b, 0);

  // UTIL weight: projection-dominated with a value tilt. Field chases pts
  // but still has some value awareness. Exponent on projection is higher
  // than before (1.7 vs 1.3) to match the fact that dominant players end
  // up in ~80% of field lineups once you add CPT + UTIL rosters together.
  const utilWeights = valid.map(p => {
    const v = p.projection / Math.max(p.util_salary / 1000, 1);
    return Math.pow(p.projection, 1.7) * Math.pow(v, 0.6);
  });

  const counts = new Array(valid.length).fill(0);
  let successes = 0;
  const maxAttempts = nSims * 6;

  function pickWeighted(wArr, total, blocked) {
    if (total <= 0) return -1;
    let r = Math.random() * total;
    for (let i = 0; i < wArr.length; i++) {
      if (blocked.has(i)) continue;
      r -= wArr[i];
      if (r <= 0) return i;
    }
    // Fallback: return first unblocked
    for (let i = 0; i < wArr.length; i++) if (!blocked.has(i)) return i;
    return -1;
  }

  for (let attempt = 0; attempt < maxAttempts && successes < nSims; attempt++) {
    // Pick CPT
    const cptIdx = pickWeighted(cptWeights, cptTotal, new Set());
    if (cptIdx < 0) continue;
    const cpt = valid[cptIdx];
    let salUsed = cpt.cpt_salary;
    if (salUsed > salaryCap - 5000) continue;   // impossible to fill 5 UTIL at $1k min

    // Pick 5 UTIL players
    const used = new Set([cptIdx]);
    const utils = [];
    let failed = false;
    for (let slot = 0; slot < 5; slot++) {
      // Remaining salary per slot
      const remainingSlots = 5 - slot;
      const maxPerSlot = (salaryCap - salUsed) - (remainingSlots - 1) * 1000;
      // Build a filtered weight list: only players with util_salary ≤ maxPerSlot
      let subTotal = 0;
      const effW = utilWeights.map((w, i) => {
        if (used.has(i) || valid[i].util_salary > maxPerSlot) return 0;
        subTotal += w;
        return w;
      });
      const pick = pickWeighted(effW, subTotal, used);
      if (pick < 0) { failed = true; break; }
      used.add(pick);
      utils.push(pick);
      salUsed += valid[pick].util_salary;
    }
    if (failed) continue;
    if (salUsed > salaryCap) continue;

    // Both-teams constraint
    const teams = new Set([cpt.team, ...utils.map(i => valid[i].team)]);
    if (teams.size < 2) continue;

    counts[cptIdx]++;
    utils.forEach(i => counts[i]++);
    successes++;
  }

  // Map counts back to original players array (not filtered)
  const nameIdx = {}; players.forEach((p, i) => { nameIdx[p.name] = i; });
  const fullCounts = new Array(players.length).fill(0);
  valid.forEach((p, i) => { fullCounts[nameIdx[p.name]] = counts[i]; });

  return { counts: fullCounts, lineups: successes };
}


// CPT = 1.5× projection, 1.5× salary. $50K cap. Both teams required.
//
// Strategy:
//   1. Greedy build top-N by projection, respecting min-salary floor ($45K)
//      and both-teams constraint
//   2. Exposure caps via maxExp/minExp (percentages)
//   3. Phase-1 min-exposure fill with urgency weighting (mirror tennis)
// ============================================================
export function optimizeShowdown(players, nLineups = 150, salaryCap = 50000, minSalary = 48000, opts = {}) {
  // opts: {
  //   locked:       Set<string>  — must appear somewhere in lineup (CPT or UTIL)
  //   excluded:     Set<string>  — removed from pool entirely
  //   cptLocked:    Set<string>  — must be the CPT of every lineup (effectively size ≤ 1)
  //   flexLocked:   Set<string>  — must appear in a UTIL slot of every lineup
  //   cptExcluded:  Set<string>  — may appear as UTIL but never as CPT
  //   flexExcluded: Set<string>  — may appear as CPT but never as UTIL
  //   pivotPool:    Set<string>  — gem pivot pool (6-25% simOwn band) for slot-based exposure
  //   pivotFlexTarget: number    — fraction of lineups that should contain ≥1 pool player in FLEX (0..1)
  //   pivotCptTarget:  number    — fraction of lineups that should have a pool player as CPT (0..1)
  // }
  const lockedSet       = opts.locked        instanceof Set ? opts.locked        : new Set(opts.locked        || []);
  const excludedSet     = opts.excluded      instanceof Set ? opts.excluded      : new Set(opts.excluded      || []);
  const cptLockedSet    = opts.cptLocked     instanceof Set ? opts.cptLocked     : new Set(opts.cptLocked     || []);
  const flexLockedSet   = opts.flexLocked    instanceof Set ? opts.flexLocked    : new Set(opts.flexLocked    || []);
  const cptExcludedSet  = opts.cptExcluded   instanceof Set ? opts.cptExcluded   : new Set(opts.cptExcluded   || []);
  const flexExcludedSet = opts.flexExcluded  instanceof Set ? opts.flexExcluded  : new Set(opts.flexExcluded  || []);
  const pivotPoolSet    = opts.pivotPool     instanceof Set ? opts.pivotPool     : new Set(opts.pivotPool     || []);
  const pivotFlexFrac   = Math.max(0, Math.min(1, opts.pivotFlexTarget || 0));
  const pivotCptFrac    = Math.max(0, Math.min(1, opts.pivotCptTarget  || 0));
  const pivotFlexMin    = Math.round(pivotFlexFrac * nLineups);
  const pivotCptMin     = Math.round(pivotCptFrac  * nLineups);

  const valid = players.filter(p =>
    p.projection > 0 &&
    p.util_salary > 0 &&
    p.cpt_salary > 0 &&
    (p.status || 'ACTIVE').toUpperCase() !== 'OUT' &&
    !excludedSet.has(p.name)
  );
  if (valid.length < 6) return { lineups: [], counts: [], total: 0 };

  const teams = [...new Set(valid.map(p => p.team))];
  const needBothTeams = teams.length >= 2;
  const byName = {}; valid.forEach((p, i) => { byName[p.name] = i; });

  // Build candidate pool: for each possible CPT, find top UTIL combinations.
  // Enumeration space is huge (C(36, 5) × 36), so we sample intelligently:
  //   - For each CPT, take top K UTIL by projection (K = 18) then combine
  const allLineups = [];
  const K = Math.min(18, valid.length - 1);

  for (let c = 0; c < valid.length; c++) {
    const cpt = valid[c];
    // CPT-specific exclude: never use this player as captain
    if (cptExcludedSet.has(cpt.name)) continue;
    // CPT-specific lock: if user has locked a specific player as CPT, only allow that player
    if (cptLockedSet.size > 0 && !cptLockedSet.has(cpt.name)) continue;
    const cptProj = 1.5 * cpt.projection;
    const cptSal = cpt.cpt_salary;
    if (cptSal > salaryCap - 5 * 1000) continue;

    // When there are locked players, ensure the UTIL pool includes them
    // regardless of value rank — otherwise they'd get filtered out by top-K.
    // Locked players must appear across the roster (CPT + 5 UTIL slots).
    const utilPool = valid
      .map((p, i) => ({ p, i }))
      .filter(({ p, i }) => {
        if (i === c) return false;
        // FLEX-specific exclude: this player is not allowed in UTIL slots
        if (flexExcludedSet.has(p.name)) return false;
        return true;
      })
      .sort((a, b) => {
        // Boost locked candidates (any-slot OR flex-specific) to the front so
        // they're always present in the top-K UTIL pool.
        const aLocked = (lockedSet.has(a.p.name) || flexLockedSet.has(a.p.name)) ? 1 : 0;
        const bLocked = (lockedSet.has(b.p.name) || flexLockedSet.has(b.p.name)) ? 1 : 0;
        if (aLocked !== bLocked) return bLocked - aLocked;
        const va = a.p.projection / Math.max(a.p.util_salary, 1);
        const vb = b.p.projection / Math.max(b.p.util_salary, 1);
        return vb - va;
      })
      .slice(0, K);

    const pool = utilPool;
    for (let a = 0; a < pool.length - 4; a++) {
      const pa = pool[a].p;
      for (let b = a + 1; b < pool.length - 3; b++) {
        const pb = pool[b].p;
        for (let d = b + 1; d < pool.length - 2; d++) {
          const pd = pool[d].p;
          for (let e = d + 1; e < pool.length - 1; e++) {
            const pe = pool[e].p;
            for (let f = e + 1; f < pool.length; f++) {
              const pf = pool[f].p;
              const totalSal = cptSal + pa.util_salary + pb.util_salary + pd.util_salary + pe.util_salary + pf.util_salary;
              if (totalSal > salaryCap || totalSal < minSalary) continue;

              // Check both-teams constraint
              if (needBothTeams) {
                const teamSet = new Set([cpt.team, pa.team, pb.team, pd.team, pe.team, pf.team]);
                if (teamSet.size < 2) continue;
              }

              // Lock check — every locked name must be in this 6-player lineup
              if (lockedSet.size > 0) {
                const luNames = new Set([cpt.name, pa.name, pb.name, pd.name, pe.name, pf.name]);
                let allLocked = true;
                for (const ln of lockedSet) { if (!luNames.has(ln)) { allLocked = false; break; } }
                if (!allLocked) continue;
              }
              // FLEX-specific lock — every flex-locked name must be in a UTIL slot
              // (not as CPT). If a flexLocked player happened to be captain, fail.
              if (flexLockedSet.size > 0) {
                const utilNames = new Set([pa.name, pb.name, pd.name, pe.name, pf.name]);
                let allFlexLocked = true;
                for (const fn of flexLockedSet) {
                  if (!utilNames.has(fn)) { allFlexLocked = false; break; }
                }
                if (!allFlexLocked) continue;
              }

              const totalProj = cptProj + pa.projection + pb.projection + pd.projection + pe.projection + pf.projection;
              allLineups.push({
                proj: round2(totalProj),
                sal: totalSal,
                cpt: c,
                utils: [pool[a].i, pool[b].i, pool[d].i, pool[e].i, pool[f].i],
                players: [c, pool[a].i, pool[b].i, pool[d].i, pool[e].i, pool[f].i],
              });
            }
          }
        }
      }
    }
  }

  if (allLineups.length === 0) return { lineups: [], counts: new Array(valid.length).fill(0), total: 0 };

  allLineups.sort((x, y) => y.proj - x.proj);

  // ─── Exposure caps — three independent dimensions ────────────────────
  // 1. Total:   max/min times the player appears in ANY slot
  // 2. CPT:     max/min times the player is the captain specifically
  // 3. FLEX:    max/min times the player is in a UTIL slot specifically
  // Each player carries {maxExp, minExp, cptMaxExp, cptMinExp, flexMaxExp, flexMinExp}
  // (all as percentages of nLineups).
  const toCap = (pct) => (pct == null ? null : Math.max(1, Math.round(nLineups * pct / 100)));
  const toMin = (pct) => (pct == null || pct <= 0 ? 0 : Math.max(1, Math.round(nLineups * pct / 100)));
  const caps = valid.map(p => ({
    max:     p.maxExp     != null ? toCap(p.maxExp)     : nLineups,
    min:     toMin(p.minExp),
    cptMax:  p.cptMaxExp  != null ? toCap(p.cptMaxExp)  : nLineups,
    cptMin:  toMin(p.cptMinExp),
    flexMax: p.flexMaxExp != null ? toCap(p.flexMaxExp) : nLineups,
    flexMin: toMin(p.flexMinExp),
  }));

  const counts = new Array(valid.length).fill(0);
  const cptCounts = new Array(valid.length).fill(0);
  const flexCounts = new Array(valid.length).fill(0);
  // Pivot pool tracking — how many selected lineups contain ≥1 pool player
  // in the specified slot. Used to enforce slot-based exposure targets
  // instead of forcing a single named player (see pivotPool opts above).
  let poolFlexCount = 0;
  let poolCptCount  = 0;
  const pivotPoolIdx = new Set();
  valid.forEach((p, i) => { if (pivotPoolSet.has(p.name)) pivotPoolIdx.add(i); });
  const selected = [];
  const usedKeys = new Set();

  function canAdd(lu) {
    // CPT caps for the captain player
    const cc = caps[lu.cpt];
    if (counts[lu.cpt] + 1 > cc.max) return false;
    if (cptCounts[lu.cpt] + 1 > cc.cptMax) return false;
    // FLEX caps for each utility player
    for (const pid of lu.utils) {
      const fc = caps[pid];
      if (counts[pid] + 1 > fc.max) return false;
      if (flexCounts[pid] + 1 > fc.flexMax) return false;
    }
    return true;
  }

  function keyOf(lu) { return lu.players.join(','); }

  function addLU(lu) {
    const k = keyOf(lu);
    if (usedKeys.has(k)) return;
    selected.push(lu); usedKeys.add(k);
    lu.players.forEach(pid => counts[pid]++);
    cptCounts[lu.cpt]++;
    lu.utils.forEach(pid => flexCounts[pid]++);
    // Track pivot pool exposure — at most +1 per lineup per dimension
    if (pivotPoolIdx.has(lu.cpt)) poolCptCount++;
    if (lu.utils.some(pid => pivotPoolIdx.has(pid))) poolFlexCount++;
  }

  // Phase 1: min-exposure fill (covers total, cpt, and flex mins)
  function hasUnmetMins() {
    for (let i = 0; i < valid.length; i++) {
      if (counts[i]     < caps[i].min)     return true;
      if (cptCounts[i]  < caps[i].cptMin)  return true;
      if (flexCounts[i] < caps[i].flexMin) return true;
    }
    // Pivot pool slot targets
    if (poolFlexCount < pivotFlexMin) return true;
    if (poolCptCount  < pivotCptMin)  return true;
    return false;
  }
  while (hasUnmetMins() && selected.length < nLineups) {
    let best = null, bestScore = 0, bestProj = -Infinity;
    for (const lu of allLineups) {
      if (usedKeys.has(keyOf(lu)) || !canAdd(lu)) continue;
      let score = 0;
      // Reward addressing unmet mins
      const cc = caps[lu.cpt];
      if (counts[lu.cpt]    < cc.min)    score += (cc.min    - counts[lu.cpt])    / nLineups;
      if (cptCounts[lu.cpt] < cc.cptMin) score += (cc.cptMin - cptCounts[lu.cpt]) / nLineups;
      for (const pid of lu.utils) {
        const fc = caps[pid];
        if (counts[pid]     < fc.min)     score += (fc.min     - counts[pid])     / nLineups;
        if (flexCounts[pid] < fc.flexMin) score += (fc.flexMin - flexCounts[pid]) / nLineups;
      }
      // Pivot pool slot rewards — prefer lineups that add pool-slot exposure
      // when under target. Projection tiebreaker (below) naturally favors the
      // highest-ceiling pool member affordable within the lineup's salary.
      const cptInPool = pivotPoolIdx.has(lu.cpt);
      const anyUtilInPool = lu.utils.some(pid => pivotPoolIdx.has(pid));
      if (cptInPool && poolCptCount < pivotCptMin) {
        score += (pivotCptMin - poolCptCount) / nLineups;
      }
      if (anyUtilInPool && poolFlexCount < pivotFlexMin) {
        score += (pivotFlexMin - poolFlexCount) / nLineups;
      }
      if (score === 0) continue;
      if (score > bestScore + 1e-9 || (Math.abs(score - bestScore) < 1e-9 && lu.proj > bestProj)) {
        best = lu; bestScore = score; bestProj = lu.proj;
      }
    }
    if (!best) break;
    addLU(best);
  }

  // Phase 2: greedy fill by projection
  for (const lu of allLineups) {
    if (selected.length >= nLineups) break;
    if (usedKeys.has(keyOf(lu)) || !canAdd(lu)) continue;
    addLU(lu);
  }

  // ─── Phase 3: ADDITIVE SWAP-UP PASS ───────────────────────────────────
  // For each lineup in `selected`, compute its best single-swap upgrade as a
  // NEW lineup rather than mutating in place. Rationale: the contrarian /
  // min-fill lineup has strategic value even though its projection is lower
  // — it hits a low-owned player (like Camara at ext-sleeper min) that the
  // field rarely has, creating leverage in tail outcomes. The swap-up
  // variant (like Fox replacing Camara) has higher expected projection and
  // wins in standard outcomes. Both deserve a spot in the output.
  //
  // Strategy: generate upgrades as candidate new lineups. If selected is
  // under nLineups, add directly. If at nLineups, displace the LOWEST-
  // projection lineup whose removal doesn't violate any min-exposure cap.
  // Sort the final set by projection descending so the max-EV variant
  // appears at #1, with the contrarian original still present further down.
  //
  // Example — POR@SAS contrarian build:
  //   Original lineup: Deni CPT + Wemby + Camara + Scoot + R.Williams +
  //                    Kornet = 197 pts (hits Camara ext-sleeper min)
  //   Variant generated: Camara → Fox (same lineup otherwise). Fox has
  //                    higher proj + remaining exposure headroom (mid-chalk
  //                    min only puts him at 42%, max is 72%).
  //   Both end up in selected; sort places the 203-pt Fox lineup at #1,
  //   197-pt Camara lineup further down for contrarian diversification.

  function generateBestSwap(lu) {
    let best = null;
    for (let slotIdx = 0; slotIdx < lu.utils.length; slotIdx++) {
      const curIdx = lu.utils[slotIdx];
      const cur = valid[curIdx];
      for (let altIdx = 0; altIdx < valid.length; altIdx++) {
        if (altIdx === lu.cpt || lu.utils.includes(altIdx)) continue;
        const alt = valid[altIdx];
        if (alt.projection <= cur.projection) continue;
        // Salary
        const newSal = lu.sal - cur.util_salary + alt.util_salary;
        if (newSal > salaryCap || newSal < minSalary) continue;
        // Both-teams (if applicable to slate)
        if (needBothTeams) {
          const teams = new Set([valid[lu.cpt].team]);
          for (let i = 0; i < lu.utils.length; i++) {
            teams.add(i === slotIdx ? alt.team : valid[lu.utils[i]].team);
          }
          if (teams.size < 2) continue;
        }
        // Per-slot lock/exclude invariants
        if (flexExcludedSet.has(alt.name)) continue;
        // Alt's max exposure — we're ADDING a lineup, so alt's count goes up by 1
        if (counts[altIdx] + 1 > caps[altIdx].max) continue;
        if (flexCounts[altIdx] + 1 > caps[altIdx].flexMax) continue;
        // Uniqueness against both existing selected and already-generated variants
        const newUtils = [...lu.utils];
        newUtils[slotIdx] = altIdx;
        const newKey = [lu.cpt, ...newUtils].sort().join(',');
        if (usedKeys.has(newKey)) continue;

        const gain = alt.projection - cur.projection;
        if (!best || gain > best.gain) {
          best = { altIdx, slotIdx, gain, newSal, newUtils, newKey };
        }
      }
    }
    return best;
  }

  // Helper: can we safely remove this lineup without dropping anyone below min?
  function safeToRemove(lu) {
    if (counts[lu.cpt]    <= caps[lu.cpt].min)    return false;
    if (cptCounts[lu.cpt] <= caps[lu.cpt].cptMin) return false;
    for (const ui of lu.utils) {
      if (counts[ui]     <= caps[ui].min)     return false;
      if (flexCounts[ui] <= caps[ui].flexMin) return false;
    }
    return true;
  }
  function removeLU(idx) {
    const lu = selected[idx];
    usedKeys.delete(keyOf(lu));
    counts[lu.cpt]--; cptCounts[lu.cpt]--;
    lu.utils.forEach(ui => { counts[ui]--; flexCounts[ui]--; });
    selected.splice(idx, 1);
  }

  // Iterate a snapshot of originals so we don't try to upgrade our own variants.
  const originals = [...selected];
  const candidateVariants = [];
  for (const lu of originals) {
    const swap = generateBestSwap(lu);
    if (!swap) continue;
    candidateVariants.push({
      cpt: lu.cpt,
      utils: swap.newUtils,
      players: [lu.cpt, ...swap.newUtils],
      sal: swap.newSal,
      proj: round2(lu.proj + swap.gain),
      _variantKey: swap.newKey,
    });
  }
  // Process highest-gain variants first so the best upgrades claim displacement slots.
  candidateVariants.sort((a, b) => b.proj - a.proj);

  for (const v of candidateVariants) {
    if (usedKeys.has(v._variantKey)) continue;   // another variant already claimed this combo
    // Re-check alt caps against current counts (may have changed as variants were added)
    if (!canAdd(v)) continue;
    if (selected.length < nLineups) {
      addLU(v);
      continue;
    }
    // At capacity — try to displace the worst lineup we can safely remove.
    let worstIdx = -1, worstProj = v.proj;
    for (let i = 0; i < selected.length; i++) {
      if (selected[i].proj >= worstProj) continue;
      if (!safeToRemove(selected[i])) continue;
      worstIdx = i;
      worstProj = selected[i].proj;
    }
    if (worstIdx < 0) continue;   // nothing displaceable below this variant's proj
    const saved = selected[worstIdx];
    removeLU(worstIdx);
    if (canAdd(v)) {
      addLU(v);
    } else {
      // Restore the displaced lineup if variant can't be added after all
      counts[saved.cpt]++; cptCounts[saved.cpt]++;
      saved.utils.forEach(ui => { counts[ui]++; flexCounts[ui]++; });
      usedKeys.add(keyOf(saved));
      selected.splice(worstIdx, 0, saved);
    }
  }

  // Final sort: highest-projection variant first. The contrarian original
  // stays in the pool and still exports in CSV — it just ranks below the
  // max-EV upgrade in the UI.
  selected.sort((a, b) => b.proj - a.proj);

  return { lineups: selected, counts, cptCounts, flexCounts, total: allLineups.length };
}

// ============================================================
// CLASSIC OPTIMIZER (PG/SG/SF/PF/C/G/F/UTIL)
// Single-game slates don't need this, but provided for future.
// Randomized greedy with position-eligibility check.
// ============================================================
export function optimizeClassic(players, nLineups = 500, salaryCap = 50000, minSalary = 48000, opts = {}) {
  // opts: { locked: Set<string>, excluded: Set<string> }
  // Excluded players are filtered out of the `valid` pool up front.
  // Locked players get forcibly placed at the best eligible slot at the start
  // of each lineup attempt (before randomized greedy fill of remaining slots).
  const lockedSet = opts.locked instanceof Set ? opts.locked : new Set(opts.locked || []);
  const excludedSet = opts.excluded instanceof Set ? opts.excluded : new Set(opts.excluded || []);

  const valid = players.filter(p =>
    p.projection > 0 &&
    p.salary > 0 &&
    (p.status || 'ACTIVE').toUpperCase() !== 'OUT' &&
    !excludedSet.has(p.name)
  );
  if (valid.length < 8) return { lineups: [], counts: [], total: 0 };

  const ELIG = {
    PG:   p => p.positions?.includes('PG'),
    SG:   p => p.positions?.includes('SG'),
    SF:   p => p.positions?.includes('SF'),
    PF:   p => p.positions?.includes('PF'),
    C:    p => p.positions?.includes('C'),
    G:    p => p.positions?.some(x => ['PG','SG'].includes(x)),
    F:    p => p.positions?.some(x => ['SF','PF'].includes(x)),
    UTIL: () => true,
  };
  const SLOTS = ['PG','SG','SF','PF','C','G','F','UTIL'];

  // Resolve locked player indices + validate they exist in the pool
  // (locked-but-filtered-out players would make every lineup impossible).
  const lockedIndices = [];
  for (const name of lockedSet) {
    const i = valid.findIndex(p => p.name === name);
    if (i >= 0) lockedIndices.push(i);
  }
  if (lockedIndices.length > SLOTS.length) return { lineups: [], counts: [], total: 0 };

  const counts = new Array(valid.length).fill(0);
  const selected = [];
  const usedKeys = new Set();

  // Weighted random attempts
  const weights = valid.map(p => Math.pow(p.projection, 1.8));
  let total = weights.reduce((a, b) => a + b, 0);

  function pick(elig, blocked) {
    let tries = 0;
    while (tries < 25) {
      let r = Math.random() * total;
      for (let i = 0; i < valid.length; i++) {
        r -= weights[i];
        if (r <= 0) {
          if (!blocked.has(i) && elig(valid[i])) return i;
          break;
        }
      }
      tries++;
    }
    // Fallback: linear scan
    for (let i = 0; i < valid.length; i++) if (!blocked.has(i) && elig(valid[i])) return i;
    return -1;
  }

  // Place locked players into the best-fit slot from SLOTS — greedy assignment
  // that tries stricter slots first (e.g. C before UTIL for a Center-only player).
  function placeLocked(locked, slots) {
    const assignments = new Map();  // slotIdx → playerIdx
    const remaining = [...locked];
    // Sort by slot strictness — PG/SG/SF/PF/C first, then G/F, then UTIL
    const slotOrder = slots.map((s, i) => i);
    for (const pid of remaining) {
      const p = valid[pid];
      let placed = false;
      for (const si of slotOrder) {
        if (assignments.has(si)) continue;
        if (ELIG[slots[si]](p)) { assignments.set(si, pid); placed = true; break; }
      }
      if (!placed) return null;  // this locked player can't fit anywhere — abort
    }
    return assignments;
  }

  let attempts = 0;
  const MAX_ATTEMPTS = nLineups * 30;
  while (selected.length < nLineups && attempts < MAX_ATTEMPTS) {
    attempts++;
    const picks = new Array(SLOTS.length).fill(-1);
    const blocked = new Set();
    let sal = 0;
    let ok = true;

    // Step 1: slot locked players first
    if (lockedIndices.length > 0) {
      const assign = placeLocked(lockedIndices, SLOTS);
      if (!assign) { ok = false; }
      else {
        for (const [slotIdx, playerIdx] of assign) {
          picks[slotIdx] = playerIdx;
          blocked.add(playerIdx);
          sal += valid[playerIdx].salary;
          if (sal > salaryCap) { ok = false; break; }
        }
      }
    }
    if (!ok) continue;

    // Step 2: fill remaining slots with weighted random picks
    for (let si = 0; si < SLOTS.length; si++) {
      if (picks[si] !== -1) continue;  // already filled by a lock
      const idx = pick(ELIG[SLOTS[si]], blocked);
      if (idx < 0) { ok = false; break; }
      picks[si] = idx;
      blocked.add(idx);
      sal += valid[idx].salary;
      if (sal > salaryCap) { ok = false; break; }
    }
    if (!ok) continue;
    if (sal > salaryCap) continue;
    if (sal < minSalary) continue;
    const sortedPlayers = [...picks].sort((a, b) => a - b);
    const key = sortedPlayers.join(',');
    if (usedKeys.has(key)) continue;
    usedKeys.add(key);
    const proj = round2(picks.reduce((s, i) => s + valid[i].projection, 0));
    selected.push({ proj, sal, players: sortedPlayers });
    picks.forEach(i => counts[i]++);
  }

  selected.sort((a, b) => b.proj - a.proj);
  return { lineups: selected, counts, total: selected.length };
}
