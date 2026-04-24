# Piece #2 — Deploy & Wire-Up

Everything below is in the zip. Ship this as one PR.

## New Python deps

Added to `backend/pyproject.toml`:
- `cryptography>=42.0.0` — for Kalshi RSA-PSS signing

## New env vars

Add to Railway (both API and worker services):

```
ODDS_API_KEY=<your the-odds-api.com key>
KALSHI_KEY_ID=<your UUID>
KALSHI_PRIVATE_KEY=<full PEM block, multi-line>
KALSHI_API_BASE=https://trading-api.kalshi.com/trade-api/v2
```

**Critical:** `KALSHI_API_BASE` must be `trading-api.kalshi.com`, NOT `api.elections.kalshi.com`. Tennis lives on the general markets base. Elections endpoints will 401 on tennis calls. Confirmed from Kalshi's official docs at trading-api.readme.io.

## Kalshi auth correction

The original brief specified JWT RS256 signing with Authorization: Bearer. **That's wrong.** Kalshi uses per-request RSA-PSS signing with three custom headers:

- `KALSHI-ACCESS-KEY`: your key UUID
- `KALSHI-ACCESS-TIMESTAMP`: milliseconds since epoch
- `KALSHI-ACCESS-SIGNATURE`: base64 of RSA-PSS(SHA256) signature over `timestamp + METHOD + path_without_query`

Implemented correctly in `backend/app/services/kalshi.py`.

## Run migration 003

Supabase SQL editor → paste entire contents of `backend/migrations/003_odds_and_prizepicks.sql` → Run.

Creates: `matches.odds` column, `odds_history`, `prizepicks_lines`, `line_movements`, `admin_users`, `is_admin()` function, movement trigger, RLS policies, Realtime subscriptions.

## Seed yourself as admin

**After** migration 003 applies, run once in Supabase SQL editor:

```sql
insert into admin_users (user_id, notes)
select id, 'founder' from auth.users where email = '<YOUR LOGIN EMAIL>';
```

Replace `<YOUR LOGIN EMAIL>` with whatever you used to sign into the live site via magic link.

## 3-line App.jsx wire-up for the PrizePicks tab

In `frontend/src/App.jsx`:

**1. Import the component** (add near other component imports, around line 19):

```jsx
import { PrizePicksTab } from './components/PrizePicksTab';
```

**2. Add a tab button** — search for the tabs array (they're defined as objects like `{ key: 'dk', label: 'DK Lineups' }`). Add:

```jsx
{ key: 'pp', label: 'PrizePicks' }
```

**3. Render the tab content** — near where other tabs render (`tab === 'dk' && <DkTab .../>` pattern):

```jsx
{tab === 'pp' && <PrizePicksTab slateId={data?.meta?.id || data?.slate_id} />}
```

The exact `slateId` you pass depends on how your `data` object exposes the slate UUID — could be `data.meta.id`, `data.id`, or `data.meta.dk_draft_group_id` (the API returns all of these). Check the network tab to confirm the key; adjust if needed.

## Acceptance criteria (verify in order)

1. ✅ Migration 003 applies cleanly
2. ✅ Railway deploys without error (check worker logs for `Slate watcher started` + no Python import errors)
3. ✅ Within 15 min: `SELECT count(*) FROM odds_history WHERE fetched_at > now() - interval '20 min'` > 0
4. ✅ Within 15 min: `SELECT * FROM matches WHERE odds::text <> '{}' LIMIT 5` returns matches with populated odds keys (ml_a, ml_b at minimum)
5. ✅ Frontend player cards now show real WIN% values (not flat zeros) for matches where odds landed
6. ✅ PrizePicks tab loads empty, "Add Line" + "Paste CSV" buttons visible only after signing in with admin account
7. ✅ Add a line → row appears; open another browser tab → it appears there within 2s via Realtime
8. ✅ Edit line inline → row flashes yellow in all open tabs; `SELECT * FROM line_movements ORDER BY detected_at DESC LIMIT 5` shows new row
9. ✅ CSV paste of 10 lines creates 10 rows; re-pasting with 1 changed line produces 1 new movement row
10. ✅ Non-admin signed-in user: reads work, writes return 403 in DevTools Network tab
11. ✅ Odds API credits remaining (log line in worker): starts near 20K, drops by ≤2/tick

## Known limitations (out of scope for piece #2)

- **Tennis props from Odds API are limited to h2h + totals.** No set-winner, aces, DFs, or breaks in their tennis markets. Those fields stay unpopulated from Odds API — engine.js computes PROJ on partial data. WIN% lights up fully; PROJ less comprehensive than archive slates that had manual bet365 prop data. Closing this gap = piece #3 (Pinnacle or tennis-prop-specific book integration).
- **Kalshi tennis coverage is variable.** Their general markets sometimes have tennis match winners (Slams, Masters 1000), sometimes don't (250s, Challengers). When no markets match, logs INFO and writes nothing — doesn't error.
- **CSV paste uses simple comma split.** If a player name contains a comma (rare), it'll parse wrong. Improvement on demand.

## Test results

Ran `PYTHONPATH=. pytest backend/tests/` — **36/36 tests pass.**

Includes:
- `test_normalizer.py`: 6 tests
- `test_slate_classifier.py`: 21 tests (including Short slate regression tests)
- `test_piece2.py`: 9 tests (Odds API transformation, Kalshi signing, event parsing)
