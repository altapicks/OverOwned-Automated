// ============================================================
// ODDS HELPERS
// ============================================================
export function americanToProb(odds) {
  if (!odds || odds === 0) return 0.5;
  return odds > 0 ? 100 / (odds + 100) : Math.abs(odds) / (Math.abs(odds) + 100);
}

export function removeVig(p1, p2) {
  const t = p1 + p2;
  return t === 0 ? [0.5, 0.5] : [p1 / t, p2 / t];
}

export function poissonEV(odds, milestone) {
  const p = americanToProb(odds);
  if (p <= 0 || p >= 1) return milestone;
  let lam = milestone;
  for (let iter = 0; iter < 50; iter++) {
    let cdf = 0;
    for (let i = 0; i < milestone; i++) {
      cdf += Math.exp(-lam) * Math.pow(lam, i) / factorial(i);
    }
    const target = 1 - p;
    if (Math.abs(cdf - target) < 0.001) break;
    let dcdf = 0;
    for (let i = 0; i < milestone; i++) {
      dcdf += Math.exp(-lam) * (i * Math.pow(lam, i - 1) / factorial(i) - Math.pow(lam, i) / factorial(i));
    }
    if (Math.abs(dcdf) < 1e-10) break;
    lam -= (cdf - target) / dcdf;
    lam = Math.max(0.1, Math.min(lam, 30));
  }
  return lam;
}

function factorial(n) {
  if (n <= 1) return 1;
  let r = 1;
  for (let i = 2; i <= n; i++) r *= i;
  return r;
}

// ============================================================
// BASELINE STATS FROM WIN PROBABILITY ONLY
// ------------------------------------------------------------
// When we only have Kalshi match-winner data (no set betting, game totals,
// or ace/DF props), we still need sensible per-player stat projections.
// This function takes a single winner probability and produces a plausible
// stat profile calibrated from ATP/WTA best-of-3 averages.
//
// Not as sharp as having real prop lines, but directionally correct and
// useful until manual stat overrides are applied. Used by processMatch()
// automatically when the richer betting-line fields are missing.
// ============================================================
function clamp(n, lo, hi) { return Math.max(lo, Math.min(hi, n)); }

// v6.0c: When the manual upload pipeline writes posted_lines (CSV-supplied
// per-player stat values), prefer those over engine-computed defaults. The
// posted_lines come from Underdog raw lines for aces/dfs/breaks/gw/gl, and
// from Cloudbet correct-score devig for sw/sl. Either way they're more
// market-informative than baseline-from-wp computations.
//
// Schema: match.odds.posted_lines.{a|b}.{aces, dfs, breaks, games_won,
// games_lost, sets_won, sets_lost} — written by manual_slate_ingest.py.
//
// Applied AFTER the engine produces its own values, so anything the CSV
// doesn't supply still falls back to the computed default.
function applyPostedLineOverrides(playerStats, postedSide) {
  if (!postedSide || typeof postedSide !== 'object') return playerStats;
  const out = { ...playerStats };
  if (postedSide.aces       != null) out.aces       = postedSide.aces;
  if (postedSide.dfs        != null) out.dfs        = postedSide.dfs;
  if (postedSide.breaks     != null) out.breaks     = postedSide.breaks;
  if (postedSide.games_won  != null) out.gw         = postedSide.games_won;
  if (postedSide.games_lost != null) out.gl         = postedSide.games_lost;
  if (postedSide.sets_won   != null) out.setsWon    = postedSide.sets_won;
  if (postedSide.sets_lost  != null) out.setsLost   = postedSide.sets_lost;
  return out;
}

export function baselineStatsFromWp(wp_a, wp_b) {
  // Probability of straight-sets winners vs 3-set matches.
  // Sigmoid'd so:
  //   0.50 wp → ~30% straight-set prob
  //   0.75 wp → ~50%
  //   0.90 wp → ~65%
  //   0.95 wp → ~72%
  const pStraightWinA = clamp(0.5 * Math.pow(wp_a, 1.4), 0.05, 0.75);
  const pStraightWinB = clamp(0.5 * Math.pow(wp_b, 1.4), 0.05, 0.75);
  const p3set = clamp(1 - pStraightWinA - pStraightWinB, 0.15, 0.65);

  // Expected sets.
  // In a best-of-3: winner plays 2 sets (if 2-0) or 3 (if 2-1), loser same.
  // E[sets played] per player = 2 + p3set
  const eSetsPlayed = 2 + p3set;

  // Games won per player. Baseline: winner ~12-14 games, loser ~7-9 games.
  // Scale with competitiveness (closer matches = more total games).
  // Favorite wins ~65% of the games they play; that ratio shifts toward
  // 55% in close matches (more holds-of-serve parity in close matches).
  const tightness = 1 - Math.abs(wp_a - 0.5) * 2; // 0=blowout, 1=pickem
  const totalGames = 18 + 4 * tightness; // 18 for blowout, 22 for pickem
  const favoriteGameShare = 0.68 - 0.10 * tightness; // 0.68 blowout, 0.58 pickem
  const gw_a = wp_a >= 0.5
    ? totalGames * favoriteGameShare
    : totalGames * (1 - favoriteGameShare);
  const gw_b = totalGames - gw_a;
  const gl_a = gw_b;
  const gl_b = gw_a;

  // Expected sets won per player.
  // Winner's average sets-won in bo3 = 2 (always wins 2 sets).
  // Loser's average sets-won depends on match competitiveness.
  const setsWonA = wp_a * 2 + (1 - wp_a) * (0.3 + 0.4 * p3set);
  const setsWonB = wp_b * 2 + (1 - wp_b) * (0.3 + 0.4 * p3set);
  const setsLostA = eSetsPlayed - setsWonA;
  const setsLostB = eSetsPlayed - setsWonB;

  // Aces per player. ATP/WTA average ~5 per player in bo3.
  // Favorites serve marginally better (+1 for clear favorite, +2 heavy fav).
  // Scale gently so we don't over-correlate skill with serve.
  const aceEdgeA = (wp_a - 0.5) * 2; // [-1, 1]
  const aces_a = clamp(5 + aceEdgeA * 1.5, 2, 9);
  const aces_b = clamp(5 - aceEdgeA * 1.5, 2, 9);

  // DFs per player. ATP/WTA average ~3 per player.
  // Favorites DF slightly less (better serve mechanics).
  const dfs_a = clamp(3 - aceEdgeA * 0.5, 1.5, 4.5);
  const dfs_b = clamp(3 + aceEdgeA * 0.5, 1.5, 4.5);

  // Breaks per player. Total ~4-5 per match in bo3, split toward underdog
  // (they get broken more often than they break).
  const breaksPerMatch = 4 + 2 * tightness; // more breaks in close matches
  const favoriteBreakShare = 0.62 - 0.1 * tightness;
  const breaks_a = wp_a >= 0.5
    ? breaksPerMatch * favoriteBreakShare
    : breaksPerMatch * (1 - favoriteBreakShare);
  const breaks_b = breaksPerMatch - breaks_a;

  // Ace milestone probabilities (10+ aces rare for baseline average ~5)
  // Use Poisson tail.
  const p10ace_a = poissonTail(aces_a, 10);
  const p10ace_b = poissonTail(aces_b, 10);

  // No-DF probability (0 DFs in the match). Low average DF count → ~0.05
  const pNoDF_a = Math.exp(-dfs_a);
  const pNoDF_b = Math.exp(-dfs_b);

  // "Clean sets" — sets won with no games dropped. Baseline: higher for
  // bigger favorites. Reasonable range 0.1-0.5 clean sets per match.
  const cleanSetsA = setsWonA * (0.05 + 0.15 * wp_a);
  const cleanSetsB = setsWonB * (0.05 + 0.15 * wp_b);

  return {
    player_a: {
      wp: wp_a, pStraightWin: pStraightWinA, p3set,
      gw: gw_a, gl: gl_a,
      setsWon: setsWonA, setsLost: setsLostA, setsPlayed: eSetsPlayed,
      aces: aces_a, dfs: dfs_a, breaks: breaks_a,
      p10ace: p10ace_a, pNoDF: pNoDF_a,
      adj: 0, cleanSets: cleanSetsA,
    },
    player_b: {
      wp: wp_b, pStraightWin: pStraightWinB, p3set,
      gw: gw_b, gl: gl_b,
      setsWon: setsWonB, setsLost: setsLostB, setsPlayed: eSetsPlayed,
      aces: aces_b, dfs: dfs_b, breaks: breaks_b,
      p10ace: p10ace_b, pNoDF: pNoDF_b,
      adj: 0, cleanSets: cleanSetsB,
    },
  };
}

// Poisson P(X >= k). Used for ace milestones in baseline mode.
function poissonTail(lambda, k) {
  if (lambda <= 0) return 0;
  let cdf = 0;
  for (let i = 0; i < k; i++) {
    cdf += Math.exp(-lambda) * Math.pow(lambda, i) / factorial(i);
  }
  return clamp(1 - cdf, 0.01, 0.95);
}

// v6.2: Per-player breaks count derived from the two Pinnacle Games-Won
// lines. Used as the third-tier fallback in sharp mode when NEITHER
// the sportsbook nor PP posts a per-player Break Points Won line for
// this match. Sharper than the wp-only sigmoid baseline because it
// consumes the actual Pinnacle gw market (which the engine already
// trusts everywhere else in sharp mode).
//
// Math: in tennis, total games won = holds + breaks. Since players
// alternate serve roughly evenly, gw_a − gw_b ≈ 2·(breaks_a − breaks_b).
// That gives the break differential. For total breaks, we use the tour-
// average rate of ~22% of games being breaks, scaled up slightly in
// tighter matches (more break-back exchanges per game).
//
// Worked example — Sinner vs Moller, gw_a=12.5, gw_b=5.5:
//   T          = 18
//   tightness  = 1 − 7/18 = 0.61
//   total_brk  = 18 × (0.20 + 0.05 × 0.61) ≈ 4.15
//   diff       = 7/2 = 3.5
//   breaks_a   = (4.15 + 3.5) / 2 = 3.83
//   breaks_b   = (4.15 − 3.5) / 2 = 0.33
//
// Vs. the wp-only fallback which would give Sinner 2.53 and Moller 1.59
// (badly underweighting the favorite's break dominance against an
// underdog who can't realistically break a 97% favorite 1.6 times).
//
// Returns null on bad inputs so the caller can fall back further down
// the tier chain. Clamped at 0 — never returns negative breaks even if
// the line differential is so wide it implies negative break counts
// for the underdog.
function breaksFromGames(gw_a, gw_b) {
  if (gw_a == null || gw_b == null) return null;
  const T = gw_a + gw_b;
  if (T <= 0) return null;
  const diff = gw_a - gw_b;
  const tightness = clamp(1 - Math.abs(diff) / T, 0, 1);
  const totalBreaks = T * (0.20 + 0.05 * tightness);
  const breaksDiff = diff / 2;
  const breaks_a = Math.max(0, (totalBreaks + breaksDiff) / 2);
  const breaks_b = Math.max(0, (totalBreaks - breaksDiff) / 2);
  return { breaks_a, breaks_b };
}

// Derive match win probabilities from the two Games Won lines.
// Tennis game-share → match-win-prob is non-linear: games compound through
// sets, and small edges in games-won correspond to larger edges in match-win
// probability. Empirical calibration against ATP/WTA historical data:
//   55/45 game share → ~65/35 match wp
//   60/40            → ~78/22
//   65/35            → ~87/13
//   70/30            → ~93/7
// Sigmoid parameterization on game-share deviation from 50/50.
function wpFromGamesLines(gw_a_line, gw_b_line) {
  const total = gw_a_line + gw_b_line;
  if (total <= 0) return [0.5, 0.5];
  const share_a = gw_a_line / total;
  // Sigmoid on (share - 0.5) with gain ~15 hits the calibration targets above.
  const deviation = share_a - 0.5;
  const wp_a_raw = 1 / (1 + Math.exp(-15 * deviation));
  // Clamp to plausible tennis range — avoid 99%/1% from line noise
  const wp_a = Math.max(0.05, Math.min(0.95, wp_a_raw));
  return [wp_a, 1 - wp_a];
}

// Expose as export for the hasRichOdds check to use the same signal
function hasWpFromGames(o) {
  return o.gw_a_line != null && o.gw_b_line != null;
}

// v6.1: Sharp-mode gate decomposed into three named helpers for clarity.
// Sharp mode runs whenever we have a wp source + a set market + a games
// market. Ace/DF props are NOT required — Pinnacle (and most non-Underdog
// books) don't post serve props, so requiring them would force every
// Pinnacle-only slate into baseline mode and produce 48%-style straight-set
// probs for 97.5% Kalshi favorites. Per-stat ace/DF/break fallbacks inside
// processMatch() now handle the missing-prop case via posted_lines →
// baseline-from-wp three-tier chain.
function hasWpSource(o) {
  return (o.kalshi_prob_a != null && o.kalshi_prob_b != null) ||
         (o.ml_a != null && o.ml_b != null) ||
         hasWpFromGames(o);
}

function hasSetMarket(o) {
  return (o.set_a_20 != null && o.set_a_21 != null &&
          o.set_b_20 != null && o.set_b_21 != null) ||
         o.p3set != null;
}

function hasGamesMarket(o) {
  return o.gw_a_line != null && o.gw_b_line != null;
}

// True if a match row has enough market data to run sharp mode.
// Sharp mode needs: a wp source (Kalshi OR ml OR GW-derived), a set market
// (4-way OR p3set), and a games market (both gw lines). Ace/DF/break props
// are nice-to-have but not gating — see helpers above.
function hasRichOdds(o) {
  if (!o || typeof o !== 'object') return false;
  return hasWpSource(o) && hasSetMarket(o) && hasGamesMarket(o);
}

// ============================================================
// PROCESS MATCH ODDS → PLAYER STATS
// ============================================================
export function processMatch(match) {
  const o = match.odds || {};

  // Determine win probabilities — three-tier fallback chain:
  //   (1) Kalshi (market-sourced, no vig) — preferred
  //   (2) Odds API ml_a/ml_b (vig-removed) — designed backstop
  //   (3) Games Won lines — derived approximation when neither market is live
  //       (Kalshi's tennis coverage varies by tour stop; Odds API silently fails
  //        on some stops. The GW-derived fallback keeps sharp mode running
  //        instead of reverting to a 50/50 pickem that would break downstream
  //        set-betting + projection math.)
  let wp_a, wp_b;
  if (o.kalshi_prob_a != null && o.kalshi_prob_b != null) {
    wp_a = o.kalshi_prob_a;
    wp_b = o.kalshi_prob_b;
  } else if (o.ml_a != null && o.ml_b != null) {
    [wp_a, wp_b] = removeVig(americanToProb(o.ml_a), americanToProb(o.ml_b));
  } else if (o.gw_a_line != null && o.gw_b_line != null) {
    [wp_a, wp_b] = wpFromGamesLines(o.gw_a_line, o.gw_b_line);
  } else {
    wp_a = 0.5;
    wp_b = 0.5;
  }

  // BASELINE MODE: no rich stat-prop data at all.
  if (!hasRichOdds(o)) {
    const base = baselineStatsFromWp(wp_a, wp_b);
    const posted = o.posted_lines || {};
    return {
      player_a: applyPostedLineOverrides(
        { ...base.player_a, adj: match.adj_a || 0 },
        posted.a
      ),
      player_b: applyPostedLineOverrides(
        { ...base.player_b, adj: match.adj_b || 0 },
        posted.b
      ),
    };
  }

  // SHARP MODE: stat-prop data available. Use sportsbook math.

  // v6.1: Compute the wp-derived baseline ONCE at the top of sharp mode and
  // reuse it as the final fallback for any per-stat read where neither the
  // sportsbook prop nor the PrizePicks posted_line is available. This keeps
  // a Pinnacle-only slate (no ace/DF/break props) projectable instead of
  // bottoming out to 0.0 on missing fields.
  const sharpBaseline = baselineStatsFromWp(wp_a, wp_b);

  // Set betting (4-way). Two paths:
  //   (a) Full 4-way market present (bet365-style): use directly, normalize.
  //   (b) Only p3set available (Underdog-style): derive from wp + p3set.
  //       P(A 2-0) = wp_a × (1 - p3set),  P(A 2-1) = wp_a × p3set
  //       P(B 2-0) = wp_b × (1 - p3set),  P(B 2-1) = wp_b × p3set
  //       Sum = 1. Exact when outcome is independent of set count.
  let p_a20, p_a21, p_b20, p_b21;
  if (o.set_a_20 != null && o.set_a_21 != null && o.set_b_20 != null && o.set_b_21 != null) {
    const rawSet = [o.set_a_20, o.set_a_21, o.set_b_20, o.set_b_21].map(americanToProb);
    const setTotal = rawSet.reduce((a, b) => a + b, 0);
    [p_a20, p_a21, p_b20, p_b21] = rawSet.map(p => p / setTotal);
  } else {
    const p3 = o.p3set;
    p_a20 = wp_a * (1 - p3);
    p_a21 = wp_a * p3;
    p_b20 = wp_b * (1 - p3);
    p_b21 = wp_b * p3;
  }

  // Games won (adjust by over lean)
  const gw_a = adjustLine(o.gw_a_line, o.gw_a_over);
  const gw_b = adjustLine(o.gw_b_line, o.gw_b_over);

  // v6.2: Derive a games-won-based breaks fallback up front so both sides
  // can use it. Only consulted when neither sportsbook nor PP has a per-
  // player breaks line for the match (e.g. Sinner-tier favorites where
  // PP routinely omits the BP prop). See breaksFromGames() for the math.
  const gwDerivedBreaks = breaksFromGames(gw_a, gw_b);

  // v6.1: Per-stat three-tier fallback chain (sharp prop → posted_line →
  // wp-derived baseline). Was two-tier before, which meant Pinnacle-only
  // matches without sportsbook ace/DF/break props bottomed out to 0.0 and
  // broke every break-related gem/fade signal. Posted_lines come from the
  // PrizePicks/Underdog ingest and reflect the real market when available.
  //
  // v6.2: Breaks specifically gets a NEW third tier — gw-derived breaks
  // from breaksFromGames() — slotted between posted_lines and the
  // wp-baseline. Aces and DFs keep their original 3-tier chain (no
  // games-margin analog for serve stats).

  // Breaks
  let brk_a, brk_b;
  if (o.brk_a_line != null && o.brk_a_over != null) {
    brk_a = poissonEV(o.brk_a_over, Math.ceil(o.brk_a_line));
  } else if (o.posted_lines?.a?.breaks != null) {
    brk_a = o.posted_lines.a.breaks;
  } else if (gwDerivedBreaks) {
    brk_a = gwDerivedBreaks.breaks_a;
  } else {
    brk_a = sharpBaseline.player_a.breaks;
  }
  if (o.brk_b_line != null && o.brk_b_over != null) {
    brk_b = poissonEV(o.brk_b_over, Math.ceil(o.brk_b_line));
  } else if (o.posted_lines?.b?.breaks != null) {
    brk_b = o.posted_lines.b.breaks;
  } else if (gwDerivedBreaks) {
    brk_b = gwDerivedBreaks.breaks_b;
  } else {
    brk_b = sharpBaseline.player_b.breaks;
  }

  // Aces (Pinnacle does not post ace markets — PP posted_line is often the
  // only signal, so the second tier is the operative one for most slates).
  let ace_a, ace_b;
  if (o.ace_a_5plus != null) {
    ace_a = poissonEV(o.ace_a_5plus, 5);
  } else if (o.posted_lines?.a?.aces != null) {
    ace_a = o.posted_lines.a.aces;
  } else {
    ace_a = sharpBaseline.player_a.aces;
  }
  if (o.ace_b_5plus != null) {
    ace_b = poissonEV(o.ace_b_5plus, 5);
  } else if (o.posted_lines?.b?.aces != null) {
    ace_b = o.posted_lines.b.aces;
  } else {
    ace_b = sharpBaseline.player_b.aces;
  }

  // DFs (same reasoning as aces — Pinnacle does not post DF markets).
  let df_a, df_b;
  if (o.df_a_3plus != null) {
    df_a = poissonEV(o.df_a_3plus, 3);
  } else if (o.posted_lines?.a?.dfs != null) {
    df_a = o.posted_lines.a.dfs;
  } else {
    df_a = sharpBaseline.player_a.dfs;
  }
  if (o.df_b_3plus != null) {
    df_b = poissonEV(o.df_b_3plus, 3);
  } else if (o.posted_lines?.b?.dfs != null) {
    df_b = o.posted_lines.b.dfs;
  } else {
    df_b = sharpBaseline.player_b.dfs;
  }

  // Milestone probs — when the sportsbook milestone prop is missing,
  // derive from the per-stat projection center using a Poisson model.
  // 10+ ace prob: tail of Poisson(ace_x) at k=10.
  // No-DF prob:   P(X=0) = exp(-df_x) for Poisson(df_x).
  const p10ace_a = o.ace_a_10plus != null
    ? americanToProb(o.ace_a_10plus)
    : poissonTail(ace_a, 10);
  const p10ace_b = o.ace_b_10plus != null
    ? americanToProb(o.ace_b_10plus)
    : poissonTail(ace_b, 10);
  const pnodf_a = o.df_a_2plus != null
    ? Math.max(0, 1 - americanToProb(o.df_a_2plus))
    : Math.exp(-df_a);
  const pnodf_b = o.df_b_2plus != null
    ? Math.max(0, 1 - americanToProb(o.df_b_2plus))
    : Math.exp(-df_b);

  const posted = o.posted_lines || {};
  return {
    player_a: applyPostedLineOverrides(
      buildPlayerStats(wp_a, p_a20, p_b20, p_a21, p_b21, gw_a, gw_b, ace_a, df_a, brk_a, p10ace_a, pnodf_a, match.adj_a || 0),
      posted.a
    ),
    player_b: applyPostedLineOverrides(
      buildPlayerStats(wp_b, p_b20, p_a20, p_b21, p_a21, gw_b, gw_a, ace_b, df_b, brk_b, p10ace_b, pnodf_b, match.adj_b || 0),
      posted.b
    ),
  };
}

function adjustLine(line, overOdds) {
  const p = americanToProb(overOdds);
  if (p > 0.55) return line + 0.5;
  if (p < 0.45) return line - 0.3;
  return line;
}

function buildPlayerStats(wp, pStraightWin, pStraightLoss, pWin3, pLose3, gw, gl, aces, dfs, breaks, p10ace, pNoDF, adj) {
  const p3set = 1 - pStraightWin - pStraightLoss;
  const eSP = 2 * (1 - p3set) + 3 * p3set;
  const eSW = 2 * pStraightWin + 2 * (wp - pStraightWin) + 1 * ((1 - wp) - pStraightLoss);
  const eSL = eSP - eSW;
  const cleanRate = 0.05 + 0.15 * wp;

  return {
    wp, pStraightWin, p3set, gw, gl, aces, dfs, breaks, p10ace, pNoDF, adj,
    setsWon: eSW, setsLost: eSL, setsPlayed: eSP,
    cleanSets: eSW * cleanRate,
  };
}

// ============================================================
// DK SCORING (Best of 3)
// ============================================================
export function dkProjection(stats) {
  return round2(
    30                              // match played
    + 6 * stats.wp                  // match won
    + 6 * stats.setsWon             // sets won
    - 3 * stats.setsLost            // sets lost
    + 2.5 * stats.gw                // games won
    - 2 * stats.gl                  // games lost
    + 0.4 * stats.aces              // aces
    - 1 * stats.dfs                 // DFs
    + 0.75 * stats.breaks           // breaks
    + 6 * stats.pStraightWin        // straight sets bonus
    + 4 * stats.cleanSets           // clean set bonus
    + 2.5 * stats.pNoDF             // no DF bonus
    + 2 * stats.p10ace              // 10+ ace bonus
    + stats.adj                     // user adjustment
  );
}

// ============================================================
// PRIZEPICKS SCORING
// ============================================================
export function ppProjection(stats) {
  return round2(
    10                              // match played
    + 1 * stats.gw                  // game win
    - 1 * stats.gl                  // game loss
    + 3 * stats.setsWon             // set won
    - 3 * stats.setsLost            // set lost
    + 0.5 * stats.aces              // ace
    - 0.5 * stats.dfs               // double fault
  );
}

// ============================================================
// EV CALCULATION (PP)
// ============================================================
export function ppEV(projectedScore, ppLine) {
  // Simple EV: projected - line. Positive = MORE has edge
  return round2(projectedScore - ppLine);
}

// ============================================================
// SHOWDOWN OPTIMIZER (DK Captain Mode)
// ============================================================
// Builds 3-player lineups: CPT (1.5x proj, CPT salary), A-CPT (1.25x proj, A-CPT salary), FLEX (1x proj, FLEX salary)
// Requires each player to have: flex_salary, acpt_salary, cpt_salary, flex_id, acpt_id, cpt_id, projection
// Mirrors optimize()'s urgency-weighted min satisfaction + greedy fill pattern.
export function optimizeShowdown(players, nLineups = 20, salaryCap = 50000, minSalary = 0, opts = {}) {
  // opts: { locked: Set<string>, excluded: Set<string> }
  const lockedSet = opts.locked instanceof Set ? opts.locked : new Set(opts.locked || []);
  const excludedSet = opts.excluded instanceof Set ? opts.excluded : new Set(opts.excluded || []);

  const N = players.length;
  const idx = {};
  players.forEach((p, i) => { idx[p.name] = i; });

  // Enumerate all valid (CPT, A-CPT, FLEX) triples with distinct players
  // Rule: no two players in the same lineup may face each other (no opponent-vs-opponent)
  const allLineups = [];
  for (let c = 0; c < N; c++) {
    const cp = players[c];
    if (excludedSet.has(cp.name)) continue;
    const cSal = cp.cpt_salary, cProj = 1.5 * cp.projection;
    for (let a = 0; a < N; a++) {
      if (a === c) continue;
      const ap = players[a];
      if (excludedSet.has(ap.name)) continue;
      if (ap.opponent === cp.name) continue;
      const partial = cSal + ap.acpt_salary;
      if (partial > salaryCap) continue;
      const aProj = 1.25 * ap.projection;
      for (let f = 0; f < N; f++) {
        if (f === c || f === a) continue;
        const fp = players[f];
        if (excludedSet.has(fp.name)) continue;
        if (fp.opponent === cp.name || fp.opponent === ap.name) continue;
        const ts = partial + fp.flex_salary;
        if (ts > salaryCap || ts < minSalary) continue;
        // Lock check — all locked names must be in this lineup (at any slot)
        if (lockedSet.size > 0) {
          const luNames = new Set([cp.name, ap.name, fp.name]);
          let allLocked = true;
          for (const ln of lockedSet) { if (!luNames.has(ln)) { allLocked = false; break; } }
          if (!allLocked) continue;
        }
        const tp = cProj + aProj + fp.projection;
        allLineups.push({
          proj: round2(tp),
          sal: ts,
          players: [c, a, f],
          roles: ['CPT', 'A-CPT', 'FLEX'],
          cpt: c, acpt: a, flex: f,
        });
      }
    }
  }
  allLineups.sort((a, b) => b.proj - a.proj);

  // Exposure caps (identical pattern to classic optimize)
  const maxCaps = {}, minCaps = {};
  const defCap = nLineups;
  players.forEach(p => {
    if (p.maxExp != null) maxCaps[p.name] = Math.max(1, Math.round(nLineups * p.maxExp / 100));
    if (p.minExp != null && p.minExp > 0) minCaps[p.name] = Math.max(1, Math.round(nLineups * p.minExp / 100));
  });

  const counts = new Array(N).fill(0);
  const cptCounts = new Array(N).fill(0);       // tracks CPT-specific usage for downstream diagnostics
  const selected = [];
  const usedKeys = new Set();

  function canAdd(lu) {
    for (const pid of lu.players) {
      const cap = maxCaps[players[pid].name] ?? defCap;
      if (counts[pid] + 1 > cap) return false;
    }
    return true;
  }

  function addLU(lu) {
    const key = `${lu.cpt}|${lu.acpt}|${lu.flex}`;
    selected.push(lu); usedKeys.add(key);
    lu.players.forEach(pid => counts[pid]++);
    cptCounts[lu.cpt]++;
  }

  // Phase 1: urgency-weighted min satisfaction (same logic as classic)
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
      const key = `${lu.cpt}|${lu.acpt}|${lu.flex}`;
      if (usedKeys.has(key) || !canAdd(lu)) continue;
      let score = 0;
      for (const pid of lu.players) {
        if (urgency.has(pid)) score += urgency.get(pid);
      }
      if (score === 0) continue;
      if (score > bestScore + 1e-9 || (Math.abs(score - bestScore) < 1e-9 && lu.proj > bestProj)) {
        best = lu; bestScore = score; bestProj = lu.proj;
      }
    }
    if (best) addLU(best);
    else break;
  }

  // Phase 2: greedy fill from highest projection
  for (const lu of allLineups) {
    if (selected.length >= nLineups) break;
    const key = `${lu.cpt}|${lu.acpt}|${lu.flex}`;
    if (usedKeys.has(key) || !canAdd(lu)) continue;
    addLU(lu);
  }

  return { lineups: selected, counts, cptCounts, total: allLineups.length };
}

// ============================================================
// LINEUP OPTIMIZER
// ============================================================
export function optimize(players, nLineups = 45, salaryCap = 50000, rosterSize = 6, minSalary = 0, opts = {}) {
  // opts: { locked: Set<string>, excluded: Set<string> }
  //  - locked: player names that MUST appear in every generated lineup
  //  - excluded: player names that MUST NOT appear in any lineup
  // Tennis note: because rosters are pair-picks (player vs opponent from each match),
  // locking/excluding one side of a match forces the other side out/in.
  const lockedSet = opts.locked instanceof Set ? opts.locked : new Set(opts.locked || []);
  const excludedSet = opts.excluded instanceof Set ? opts.excluded : new Set(opts.excluded || []);

  // Build match pairs
  const idx = {};
  players.forEach((p, i) => { idx[p.name] = i; });
  const seen = new Set();
  const matches = [];
  players.forEach(p => {
    if (seen.has(p.name)) return;
    if (p.opponent && idx[p.opponent] !== undefined) {
      matches.push([p.name, p.opponent]);
      seen.add(p.name); seen.add(p.opponent);
    }
  });

  const matchOpts = matches.map(([a, b]) => [
    { idx: idx[a], sal: players[idx[a]].salary, proj: players[idx[a]].projection },
    { idx: idx[b], sal: players[idx[b]].salary, proj: players[idx[b]].projection },
  ]);

  // Generate all valid lineups
  const combos = combinations(matches.length, rosterSize);
  const allLineups = [];
  for (const mc of combos) {
    const bits = 1 << rosterSize;
    for (let b = 0; b < bits; b++) {
      let ts = 0, tp = 0;
      const pidxs = [];
      let hasExcluded = false;
      for (let i = 0; i < rosterSize; i++) {
        const side = (b >> i) & 1;
        const opt = matchOpts[mc[i]][side];
        const name = players[opt.idx].name;
        if (excludedSet.has(name)) { hasExcluded = true; break; }
        ts += opt.sal; tp += opt.proj; pidxs.push(opt.idx);
      }
      if (hasExcluded) continue;
      if (ts <= salaryCap && ts >= minSalary) {
        // Lock check — all locked names must appear in this lineup
        if (lockedSet.size > 0) {
          const luNames = new Set(pidxs.map(pi => players[pi].name));
          let allLocked = true;
          for (const ln of lockedSet) { if (!luNames.has(ln)) { allLocked = false; break; } }
          if (!allLocked) continue;
        }
        allLineups.push({ proj: round2(tp), sal: ts, players: pidxs });
      }
    }
  }
  allLineups.sort((a, b) => b.proj - a.proj);

  // Exposure caps
  const maxCaps = {}, minCaps = {};
  const defCap = nLineups; // 100% default
  players.forEach(p => {
    if (p.maxExp != null) maxCaps[p.name] = Math.max(1, Math.round(nLineups * p.maxExp / 100));
    if (p.minExp != null && p.minExp > 0) minCaps[p.name] = Math.max(1, Math.round(nLineups * p.minExp / 100));
  });

  const counts = new Array(players.length).fill(0);
  const selected = [];
  const usedKeys = new Set();

  function canAdd(pidxs) {
    for (const pid of pidxs) {
      const cap = maxCaps[players[pid].name] ?? defCap;
      if (counts[pid] + 1 > cap) return false;
    }
    return true;
  }

  function addLU(lu) {
    const key = lu.players.join(',');
    selected.push(lu); usedKeys.add(key);
    lu.players.forEach(pid => counts[pid]++);
  }

  // Phase 1: Satisfy mins with URGENCY-WEIGHTED MULTI-CONSTRAINT PAIRING
  // For each candidate lineup, score it by summing urgency weights of unmet-min players
  // it contains. Urgency = remaining_count / total_count (high-min binding constraints
  // have more remaining slots → higher weight). This naturally pairs stud+gem in the
  // same lineups because their urgency dominates small floor mins.
  const minNames = Object.keys(minCaps);
  while (minNames.some(name => counts[idx[name]] < minCaps[name]) && selected.length < nLineups) {
    // Build urgency map: pid → remaining weight
    const urgency = new Map();
    for (const name of minNames) {
      const pid = idx[name];
      const needed = minCaps[name] - counts[pid];
      if (needed <= 0) continue;
      urgency.set(pid, needed / nLineups);                        // absolute share of slate
    }
    if (urgency.size === 0) break;

    let best = null, bestScore = 0, bestProj = -Infinity;
    for (const lu of allLineups) {
      const key = lu.players.join(',');
      if (usedKeys.has(key) || !canAdd(lu.players)) continue;
      let score = 0;
      for (const pid of lu.players) {
        if (urgency.has(pid)) score += urgency.get(pid);
      }
      if (score === 0) continue;
      if (score > bestScore + 1e-9 || (Math.abs(score - bestScore) < 1e-9 && lu.proj > bestProj)) {
        best = lu; bestScore = score; bestProj = lu.proj;
      }
    }
    if (best) addLU(best);
    else break;
  }

  // Phase 2: Greedy fill
  for (const lu of allLineups) {
    if (selected.length >= nLineups) break;
    const key = lu.players.join(',');
    if (usedKeys.has(key) || !canAdd(lu.players)) continue;
    addLU(lu);
  }

  // Keep phase 1 boost-pairing order at top; phase 2 greedy-proj lineups fill bottom.
  // (No final re-sort — user wants to see Ben+Cobolli lineups as their top lineups.)
  return { lineups: selected, counts, total: allLineups.length };
}

// ============================================================
// HELPERS
// ============================================================
function round2(n) { return Math.round(n * 100) / 100; }

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

// ============================================================
// PP FANTASY SCORE — SCENARIO DISTRIBUTION
// ============================================================
export function ppScenarios(stats, ppLine) {
  const aceNet = 0.5 * stats.aces - 0.5 * stats.dfs;

  // Estimate conditional game distributions per outcome
  // Scale based on player's actual games won line
  const gwBase = stats.gw;
  const glBase = stats.gl;
  const gameRatio = gwBase / (gwBase + glBase); // player's share of games

  // 2-0 win: ~20 total games, winner dominates
  const tot20 = 19;
  const gw20 = Math.round(tot20 * Math.max(gameRatio, 0.58) * 10) / 10;
  const gl20 = tot20 - gw20;

  // 2-1 win: ~32 total games, closer split
  const tot21 = 32;
  const gw21 = Math.round(tot21 * Math.min(Math.max(gameRatio, 0.52), 0.58) * 10) / 10;
  const gl21 = tot21 - gw21;

  // 0-2 loss: ~20 total games, player loses
  const gw02 = tot20 - Math.round(tot20 * Math.max(1 - gameRatio, 0.58) * 10) / 10;
  const gl02 = tot20 - gw02;

  // 1-2 loss: ~32 total games
  const gw12 = tot21 - Math.round(tot21 * Math.min(Math.max(1 - gameRatio, 0.52), 0.58) * 10) / 10;
  const gl12 = tot21 - gw12;

  // PP scores per outcome: 10 + GW - GL + 3*(SW - SL) + aceNet
  const outcomes = [
    { label: 'Win 2-0', prob: stats.pStraightWin, gw: gw20, gl: gl20, sw: 2, sl: 0 },
    { label: 'Win 2-1', prob: Math.max(0, stats.wp - stats.pStraightWin), gw: gw21, gl: gl21, sw: 2, sl: 1 },
    { label: 'Lose 0-2', prob: Math.max(0, (1 - stats.wp) - ((1 - stats.wp) - stats.pStraightWin > 0 ? 0 : 0)), gw: gw02, gl: gl02, sw: 0, sl: 2 },
    { label: 'Lose 1-2', prob: 0, gw: gw12, gl: gl12, sw: 1, sl: 2 },
  ];

  // Fix lose probabilities using set betting
  const pLoss = 1 - stats.wp;
  // Approximate: P(lose 0-2) ≈ pLoss * (1 - stats.p3set) if player is underdog
  // More accurately from the set betting data we already have
  const pStraightLoss = Math.max(0, pLoss - Math.max(0, stats.p3set - (stats.wp - stats.pStraightWin)));
  outcomes[2].prob = Math.max(0, pLoss * 0.6); // rough: 60% of losses are in straights
  outcomes[3].prob = Math.max(0, pLoss - outcomes[2].prob);

  // Actually use the simpler approach from set betting
  // p3set = 1 - pStraightWin - pStraightLoss
  // We have pStraightWin and p3set, so pStraightLoss = 1 - pStraightWin - p3set
  const pSL = Math.max(0, 1 - stats.pStraightWin - stats.p3set);
  const pW3 = Math.max(0, stats.wp - stats.pStraightWin);
  const pL3 = Math.max(0, (1 - stats.wp) - pSL);

  outcomes[0].prob = stats.pStraightWin;
  outcomes[1].prob = pW3;
  outcomes[2].prob = pSL;
  outcomes[3].prob = pL3;

  // Compute PP score for each
  outcomes.forEach(o => {
    o.ppScore = round2(10 + o.gw - o.gl + 3 * (o.sw - o.sl) + aceNet);
  });

  // Expected PP score
  const expectedPP = round2(outcomes.reduce((sum, o) => sum + o.prob * o.ppScore, 0));

  // Conditional winning PP score
  const winProb = outcomes[0].prob + outcomes[1].prob;
  const winningPP = winProb > 0
    ? round2((outcomes[0].prob * outcomes[0].ppScore + outcomes[1].prob * outcomes[1].ppScore) / winProb)
    : 0;

  // P(over line)
  const pOver = round2(outcomes.reduce((sum, o) => sum + (o.ppScore > ppLine ? o.prob : 0), 0));

  // Edge vs 50/50
  const edge = round2(pOver - 0.5);

  return { outcomes, expectedPP, winningPP, pOver, edge, ppLine, aceNet: round2(aceNet) };
}

// ============================================================
// BREAK POINTS — DEVIG BET365 vs PP IMPLIED
// ============================================================
export function bpComparison(bet365Over, bet365Under, ppLine, ppMult) {
  // Devig bet365
  const rawOver = bet365Over > 0 ? 100 / (bet365Over + 100) : Math.abs(bet365Over) / (Math.abs(bet365Over) + 100);
  const rawUnder = bet365Under > 0 ? 100 / (bet365Under + 100) : Math.abs(bet365Under) / (Math.abs(bet365Under) + 100);
  const total = rawOver + rawUnder;
  const b365pOver = round2(rawOver / total);

  // PP implied from multiplier (1/mult is rough implied probability)
  let ppImplied = 0.5; // default for normal
  if (ppMult && ppMult > 1) {
    ppImplied = round2(1 / ppMult);
  }

  // Edge: bet365 true prob - PP implied prob
  const edge = round2(b365pOver - ppImplied);

  return {
    b365pOver,
    ppImplied,
    edge,
    play: edge > 0.03 ? 'MORE ✅' : edge < -0.03 ? 'LESS ✅' : 'SKIP',
  };
}
