// ============================================================
// OVEROWNED — MMA ENGINE
// Completely separate from tennis engine.js. Shares only pure helpers.
// ============================================================

// ---------- ODDS HELPERS ----------
export function americanToProb(odds) {
  if (!odds || odds === 0) return 0.5;
  return odds > 0 ? 100 / (odds + 100) : Math.abs(odds) / (Math.abs(odds) + 100);
}

export function removeVig(probs) {
  const sum = probs.reduce((a, b) => a + b, 0);
  if (sum === 0) return probs.map(() => 1 / probs.length);
  return probs.map(p => p / sum);
}

function round2(n) { return Math.round(n * 100) / 100; }
function round1(n) { return Math.round(n * 10) / 10; }

// ---------- METHOD & ROUND PARSERS ----------
// Devig 7-way method market: KO_a, Sub_a, Dec_a, KO_b, Sub_b, Dec_b, Draw
function parseMethodOdds(o) {
  const raw = [
    americanToProb(o.method_a_ko),
    americanToProb(o.method_a_sub),
    americanToProb(o.method_a_dec),
    americanToProb(o.method_b_ko),
    americanToProb(o.method_b_sub),
    americanToProb(o.method_b_dec),
    americanToProb(o.method_draw || 8000),
  ];
  const [ka, sa, da, kb, sb, db, dr] = removeVig(raw);
  return {
    a: { ko: ka, sub: sa, dec: da, win: ka + sa + da },
    b: { ko: kb, sub: sb, dec: db, win: kb + sb + db },
    draw: dr,
  };
}

// Devig round market. 3-round fights have R1/R2/R3 for each side + Points (=decision).
// 5-round fights add R4/R5.
function parseRoundOdds(o, rounds) {
  const aRounds = [o.a_r1, o.a_r2, o.a_r3];
  const bRounds = [o.b_r1, o.b_r2, o.b_r3];
  if (rounds === 5) {
    aRounds.push(o.a_r4, o.a_r5);
    bRounds.push(o.b_r4, o.b_r5);
  }
  const aPts = o.a_points;  // A wins by decision
  const bPts = o.b_points;
  const draw = o.method_draw || 8000;

  const raw = [
    ...aRounds.map(americanToProb),
    ...bRounds.map(americanToProb),
    americanToProb(aPts),
    americanToProb(bPts),
    americanToProb(draw),
  ];
  const devigged = removeVig(raw);
  const n = aRounds.length;
  return {
    a: {
      rounds: devigged.slice(0, n),
      dec: devigged[2 * n],
      win: devigged.slice(0, n).reduce((s, p) => s + p, 0) + devigged[2 * n],
    },
    b: {
      rounds: devigged.slice(n, 2 * n),
      dec: devigged[2 * n + 1],
      win: devigged.slice(n, 2 * n).reduce((s, p) => s + p, 0) + devigged[2 * n + 1],
    },
  };
}

// ---------- EXPECTED VALUE FROM TIER LADDER ----------
// Bet365 sig-strike tier odds: [[25, -235], [50, -105], [75, +240], ...]
// We compute the MEDIAN of the distribution (50th percentile), which is what
// O/U lines are set to. Mean would be higher due to right-skew from long
// decisions, but median matches how books price these props.
function expectedFromTiers(tiers) {
  if (!tiers || tiers.length === 0) return null;
  const survival = tiers.map(([k, odds]) => [k, americanToProb(odds)]);
  survival.sort((a, b) => a[0] - b[0]);

  // Find where P(X >= x) = 0.5 by linear interpolation between tiers
  // Tier 0 is (0, 1) implicitly — P(X >= 0) = 1
  let median = null;
  let prevK = 0, prevP = 1.0;
  for (const [k, p] of survival) {
    if (prevP >= 0.5 && p < 0.5) {
      // Interpolate between (prevK, prevP) and (k, p) where survival crosses 0.5
      median = prevK + (k - prevK) * (prevP - 0.5) / (prevP - p);
      break;
    }
    prevK = k;
    prevP = p;
  }
  // Fallback: if all tiers are still >= 0.5, extrapolate
  if (median === null) {
    const [lastK, lastP] = survival[survival.length - 1];
    // Assume exponential tail decay from last tier
    median = lastP >= 0.5 ? lastK + 10 : lastK;
  }

  // 80th percentile for DK ceiling calc: where P(X >= k) drops below 0.2
  let p80 = survival[survival.length - 1][0];
  for (let i = 0; i < survival.length; i++) {
    if (survival[i][1] < 0.2) {
      p80 = i > 0 ? survival[i - 1][0] + 5 : survival[i][0];
      break;
    }
    p80 = survival[i][0] + 10;
  }
  return { ev: round1(median), p80: round1(p80), tiers: survival };
}

// ---------- TAKEDOWN PARSER ----------
// Fighter TD O/U line + over odds → expected TDs
function parseTDs(line, overOdds, underOdds) {
  if (line == null) return null;
  const pOver = americanToProb(overOdds);
  const pUnder = americanToProb(underOdds);
  const total = pOver + pUnder;
  const trueOver = total > 0 ? pOver / total : pOver;
  // EV approximation: line + 0.6 if over is favored, line - 0.4 if under
  if (trueOver > 0.55) return round1(line + 0.7);
  if (trueOver < 0.45) return round1(line - 0.3);
  return round1(line + 0.2);
}

// ---------- PROCESS FIGHT ----------
export function processFight(fight) {
  const o = fight.odds;
  const rounds = fight.rounds || 3;
  const method = parseMethodOdds(o);
  const round = parseRoundOdds(o, rounds);

  // Reconcile win% from ML vs round market vs method (use ML as anchor)
  const mlProbs = removeVig([americanToProb(o.ml_a), americanToProb(o.ml_b)]);
  const wpA = mlProbs[0];
  const wpB = mlProbs[1];

  // Rebase round probabilities to match ML
  const roundProbsA = round.a.win > 0
    ? round.a.rounds.map(p => p * (wpA / round.a.win))
    : round.a.rounds;
  const decA = round.a.win > 0 ? round.a.dec * (wpA / round.a.win) : round.a.dec;
  const roundProbsB = round.b.win > 0
    ? round.b.rounds.map(p => p * (wpB / round.b.win))
    : round.b.rounds;
  const decB = round.b.win > 0 ? round.b.dec * (wpB / round.b.win) : round.b.dec;

  // Method probs rebased to ML
  const methodA = method.a.win > 0 ? {
    ko: method.a.ko * (wpA / method.a.win),
    sub: method.a.sub * (wpA / method.a.win),
    dec: method.a.dec * (wpA / method.a.win),
  } : method.a;
  const methodB = method.b.win > 0 ? {
    ko: method.b.ko * (wpB / method.b.win),
    sub: method.b.sub * (wpB / method.b.win),
    dec: method.b.dec * (wpB / method.b.win),
  } : method.b;

  // Sig strikes + total strikes
  // NEW RULE: explicit O/U line (Bet365 direct > PP line > Underdog) takes PRIORITY.
  // Tier ladder is only a fallback if no explicit line exists anywhere.
  const ssA = o.ss_a_tiers ? expectedFromTiers(o.ss_a_tiers) : null;
  const ssB = o.ss_b_tiers ? expectedFromTiers(o.ss_b_tiers) : null;
  const tsA = o.ts_a_tiers ? expectedFromTiers(o.ts_a_tiers) : null;
  const tsB = o.ts_b_tiers ? expectedFromTiers(o.ts_b_tiers) : null;

  const sigA = fight.ss_line_a ?? ssA?.ev ?? 40;
  const sigB = fight.ss_line_b ?? ssB?.ev ?? 40;
  const sig80A = ssA?.p80 ?? (sigA * 1.45);
  const sig80B = ssB?.p80 ?? (sigB * 1.45);

  // Total strikes (DK only — NOT sig strikes). Explicit TS line > tier median > 1.5×sig fallback.
  const totA = fight.ts_line_a ?? tsA?.ev ?? sigA * 1.5;
  const totB = fight.ts_line_b ?? tsB?.ev ?? sigB * 1.5;
  const tot80A = tsA?.p80 ?? (totA * 1.4);
  const tot80B = tsB?.p80 ?? (totB * 1.4);

  // Takedowns
  const tdA = parseTDs(o.td_a_line, o.td_a_over, o.td_a_under);
  const tdB = parseTDs(o.td_b_line, o.td_b_over, o.td_b_under);
  // Fallback: derive from CT + ML (grapplers with high CT get more TDs)
  const tdEstA = tdA ?? round1(Math.min((fight.ct_a || 1.5) / 2 + wpA * 0.5, 3));
  const tdEstB = tdB ?? round1(Math.min((fight.ct_b || 1.5) / 2 + wpB * 0.5, 3));

  // Control time (seconds)
  const ctSecA = (fight.ct_a || 1.5) * 60;
  const ctSecB = (fight.ct_b || 1.5) * 60;

  // Knockdowns: ~0.4 per KO win (not every KO has a KD, some have multiple)
  const kdA = round1(methodA.ko * 0.6);
  const kdB = round1(methodB.ko * 0.6);

  // Submission attempts: ~2.5 per submission win + ~0.5 baseline if grappler
  const subAttA = round1(methodA.sub * 3 + (tdEstA > 1 ? 0.5 : 0));
  const subAttB = round1(methodB.sub * 3 + (tdEstB > 1 ? 0.5 : 0));

  return {
    fighter_a: buildStats({
      name: fight.fighter_a, wp: wpA, method: methodA, rounds: roundProbsA, dec: decA,
      sig: sigA, sig80: sig80A, tot: totA, tot80: tot80A,
      td: tdEstA, td80: tdEstA + 1, ct: ctSecA, kd: kdA, subAtt: subAttA,
      adj: fight.adj_a || 0, roundsMax: rounds,
    }),
    fighter_b: buildStats({
      name: fight.fighter_b, wp: wpB, method: methodB, rounds: roundProbsB, dec: decB,
      sig: sigB, sig80: sig80B, tot: totB, tot80: tot80B,
      td: tdEstB, td80: tdEstB + 1, ct: ctSecB, kd: kdB, subAtt: subAttB,
      adj: fight.adj_b || 0, roundsMax: rounds,
    }),
  };
}

function buildStats({ name, wp, method, rounds, dec, sig, sig80, tot, tot80, td, td80, ct, kd, subAtt, adj, roundsMax }) {
  return {
    name, wp, adj, roundsMax,
    pKO: method.ko, pSub: method.sub, pDec: dec,
    pR1: rounds[0] || 0,
    pR2: rounds[1] || 0,
    pR3: rounds[2] || 0,
    pR4: rounds[3] || 0,
    pR5: rounds[4] || 0,
    pFinish: method.ko + method.sub,
    sigStr: sig, sigStr80: sig80,
    totStr: tot, totStr80: tot80,
    takedowns: td, takedowns80: td80,
    ctSec: ct,
    knockdowns: kd,
    subAttempts: subAtt,
  };
}

// ============================================================
// DK SCORING (MMA Classic: 6 fighters, $50K cap)
// ============================================================
// Strike: +0.2 | Sig strike stacks +0.2 (total 0.4) | CT: +0.03/sec
// TD: +5 | Reversal: +5 | KD: +10
// R1 win: +90 | R2: +70 | R3: +45 | R4: +40 | R5: +40 | Dec: +30
// Quick win (R1 ≤ 60s): +25
// ============================================================
export function dkMMAProjection(s) {
  const roundBonus =
    90 * s.pR1 + 70 * s.pR2 + 45 * s.pR3 + 40 * s.pR4 + 40 * s.pR5 + 30 * s.pDec;
  const strikeScore = 0.2 * s.totStr + 0.2 * s.sigStr;  // sig stacks
  const ctScore = 0.03 * s.ctSec;
  const quickWin = 25 * s.pR1 * 0.08;  // ~8% of R1 wins are ≤60s
  return round2(
    roundBonus
    + strikeScore
    + ctScore
    + 5 * s.takedowns
    + 10 * s.knockdowns
    + quickWin
    + s.adj
  );
}

// Ceiling: best-case realistic outcome for GPP tournament play
// If fighter has real finish prob → finish path (R1 or R2 bonus + strikes before finish + KD + TDs)
// Else → dominant decision path (80th %ile strikes, TDs, full CT)
export function dkMMACeiling(s) {
  // Decision ceiling — full fight, 80th percentile everything
  const decCeil =
    30  // decision bonus (assume win)
    + 0.2 * s.totStr80 + 0.2 * s.sigStr80
    + 0.03 * s.ctSec * 1.3  // 80th %ile CT
    + 5 * s.takedowns80
    + 10 * Math.max(s.knockdowns, s.pFinish > 0.2 ? 1 : 0)
    + s.adj;

  // Finish ceiling — R1 or R2 finish (use most likely finish round)
  const finishProb = s.pFinish;
  let finishCeil = 0;
  if (finishProb > 0.1) {
    // Weight R1 vs R2 finish by their relative likelihood
    const pR1Finish = s.pR1 * (s.pKO + s.pSub) / Math.max(s.pR1 + s.pR2, 0.01);
    const pR2Finish = s.pR2 * (s.pKO + s.pSub) / Math.max(s.pR1 + s.pR2, 0.01);
    const bestBonus = pR1Finish > pR2Finish ? 90 : 70;
    const bestCtFrac = pR1Finish > pR2Finish ? 0.3 : 0.6;  // partial fight time

    finishCeil =
      bestBonus
      + 0.2 * s.totStr80 * 0.55  // partial strikes before finish
      + 0.2 * s.sigStr80 * 0.55
      + 0.03 * s.ctSec * bestCtFrac
      + 5 * Math.min(s.takedowns80, 2)  // harder to get multiple TDs in short fight
      + 10 * (s.pKO > 0.15 ? 1.3 : 0.3)  // high KO prob → likely a KD too
      + (bestBonus === 90 ? 25 * 0.15 : 0)  // small quick-win contribution
      + s.adj;
  }

  return round2(Math.max(decCeil, finishCeil));
}

// ============================================================
// PRIZEPICKS SCORING
// ============================================================
// Sig strike: +0.5 | Sub att: +4 | TD: +5 | KD: +10
// R1 win: +50 | R2: +40 | R3: +30 | R4: +20 | R5: +20 | Dec: +10
// ============================================================
export function ppMMAProjection(s) {
  const roundBonus =
    50 * s.pR1 + 40 * s.pR2 + 30 * s.pR3 + 20 * s.pR4 + 20 * s.pR5 + 10 * s.pDec;
  return round2(
    roundBonus
    + 0.5 * s.sigStr
    + 4 * s.subAttempts
    + 5 * s.takedowns
    + 10 * s.knockdowns
    + s.adj
  );
}

export function ppMMACeiling(s) {
  const decCeil =
    10
    + 0.5 * s.sigStr80
    + 4 * s.subAttempts * 1.4
    + 5 * s.takedowns80
    + 10 * Math.max(s.knockdowns, s.pFinish > 0.2 ? 1 : 0)
    + s.adj;

  const finishProb = s.pFinish;
  let finishCeil = 0;
  if (finishProb > 0.1) {
    const pR1Finish = s.pR1 * (s.pKO + s.pSub) / Math.max(s.pR1 + s.pR2, 0.01);
    const pR2Finish = s.pR2 * (s.pKO + s.pSub) / Math.max(s.pR1 + s.pR2, 0.01);
    const bestBonus = pR1Finish > pR2Finish ? 50 : 40;

    finishCeil =
      bestBonus
      + 0.5 * s.sigStr80 * 0.55
      + 4 * Math.max(s.subAttempts, s.pSub > 0.15 ? 2 : 0)
      + 5 * Math.min(s.takedowns80, 2)
      + 10 * (s.pKO > 0.15 ? 1.3 : 0.3)
      + s.adj;
  }

  return round2(Math.max(decCeil, finishCeil));
}

// ============================================================
// PP EDGE (projected vs line)
// ============================================================
export function ppMMAEdge(projected, ppLine, mult) {
  const edge = projected - ppLine;
  // Demon multipliers pay more but require over; normal is straight over/under
  return round2(edge);
}

// ============================================================
// LINEUP OPTIMIZER — same logic as tennis (one side per match, no opp vs opp)
// ============================================================
export function optimizeMMA(fighters, nLineups = 150, salaryCap = 50000, rosterSize = 6, mode = 'ceiling', minSalary = 0, opts = {}) {
  // opts: { locked: Set<string>, excluded: Set<string> }
  // Same semantics as tennis: locked names MUST appear in every lineup;
  // excluded names MUST NOT appear in any lineup.
  const lockedSet = opts.locked instanceof Set ? opts.locked : new Set(opts.locked || []);
  const excludedSet = opts.excluded instanceof Set ? opts.excluded : new Set(opts.excluded || []);

  const idx = {};
  fighters.forEach((f, i) => { idx[f.name] = i; });

  // Build fight pairs (one side each)
  const seen = new Set();
  const fights = [];
  fighters.forEach(f => {
    if (seen.has(f.name)) return;
    if (f.opponent && idx[f.opponent] !== undefined) {
      fights.push([f.name, f.opponent]);
      seen.add(f.name); seen.add(f.opponent);
    }
  });

  // Pick the score field: ceiling for GPP, median for cash
  const scoreKey = mode === 'ceiling' ? 'ceiling' : 'projection';

  const fightOpts = fights.map(([a, b]) => [
    { idx: idx[a], sal: fighters[idx[a]].salary, proj: fighters[idx[a]][scoreKey] },
    { idx: idx[b], sal: fighters[idx[b]].salary, proj: fighters[idx[b]][scoreKey] },
  ]);

  // Generate all valid lineups (same comb logic as tennis)
  const combos = combinations(fights.length, rosterSize);
  const allLineups = [];
  for (const fc of combos) {
    const bits = 1 << rosterSize;
    for (let b = 0; b < bits; b++) {
      let ts = 0, tp = 0;
      const fidxs = [];
      let hasExcluded = false;
      for (let i = 0; i < rosterSize; i++) {
        const side = (b >> i) & 1;
        const opt = fightOpts[fc[i]][side];
        const name = fighters[opt.idx].name;
        if (excludedSet.has(name)) { hasExcluded = true; break; }
        ts += opt.sal; tp += opt.proj; fidxs.push(opt.idx);
      }
      if (hasExcluded) continue;
      if (ts <= salaryCap && ts >= minSalary) {
        if (lockedSet.size > 0) {
          const luNames = new Set(fidxs.map(fi => fighters[fi].name));
          let allLocked = true;
          for (const ln of lockedSet) { if (!luNames.has(ln)) { allLocked = false; break; } }
          if (!allLocked) continue;
        }
        allLineups.push({ proj: round2(tp), sal: ts, players: fidxs });
      }
    }
  }
  allLineups.sort((a, b) => b.proj - a.proj);

  // Exposure caps (same logic as tennis)
  const maxCaps = {}, minCaps = {};
  const defCap = nLineups;
  fighters.forEach(f => {
    if (f.maxExp != null) maxCaps[f.name] = Math.max(1, Math.round(nLineups * f.maxExp / 100));
    if (f.minExp != null && f.minExp > 0) minCaps[f.name] = Math.max(1, Math.round(nLineups * f.minExp / 100));
  });

  const counts = new Array(fighters.length).fill(0);
  const selected = [];
  const usedKeys = new Set();

  function canAdd(fidxs) {
    for (const fid of fidxs) {
      const cap = maxCaps[fighters[fid].name] ?? defCap;
      if (counts[fid] + 1 > cap) return false;
    }
    return true;
  }
  function addLU(lu) {
    const key = lu.players.join(',');
    selected.push(lu); usedKeys.add(key);
    lu.players.forEach(fid => counts[fid]++);
  }

  // Phase 1: satisfy mins with URGENCY-WEIGHTED MULTI-CONSTRAINT PAIRING
  const minNames = Object.keys(minCaps);
  while (minNames.some(name => counts[idx[name]] < minCaps[name]) && selected.length < nLineups) {
    const urgency = new Map();
    for (const name of minNames) {
      const pid = idx[name];
      const needed = minCaps[name] - counts[pid];
      if (needed <= 0) continue;
      urgency.set(pid, needed / nLineups);
    }
    if (urgency.size === 0) break;
    let best = null, bestScore = 0, bestProj = -Infinity;
    for (const lu of allLineups) {
      const key = lu.players.join(',');
      if (usedKeys.has(key) || !canAdd(lu.players)) continue;
      let score = 0;
      for (const pid of lu.players) if (urgency.has(pid)) score += urgency.get(pid);
      if (score === 0) continue;
      if (score > bestScore + 1e-9 || (Math.abs(score - bestScore) < 1e-9 && lu.proj > bestProj)) {
        best = lu; bestScore = score; bestProj = lu.proj;
      }
    }
    if (best) addLU(best);
    else break;
  }

  // Phase 2: greedy fill
  for (const lu of allLineups) {
    if (selected.length >= nLineups) break;
    const key = lu.players.join(',');
    if (usedKeys.has(key) || !canAdd(lu.players)) continue;
    addLU(lu);
  }

  // Keep phase 1 boost-pairing order at top (no re-sort — boosted lineups should be #1).
  return { lineups: selected, counts, total: allLineups.length, mode };
}

function combinations(n, k) {
  const result = [];
  const combo = [];
  function gen(start) {
    if (combo.length === k) { result.push([...combo]); return; }
    for (let i = start; i < n; i++) { combo.push(i); gen(i + 1); combo.pop(); }
  }
  gen(0);
  return result;
}
