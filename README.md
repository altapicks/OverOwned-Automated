# OverOwned

Tennis-first DFS analytics and automation platform. Full monorepo with
production-ready frontend (React + Vite → Netlify) and backend (FastAPI +
Supabase → Railway).

## What's in this repo

```
overowned/
├── frontend/                    React + Vite, deploys to Netlify
│   ├── src/
│   │   ├── App.jsx              Main app (already integrated with API client)
│   │   ├── main.jsx             Entrypoint with AuthProvider wrapper
│   │   ├── styles.css
│   │   ├── engine.js            Tennis projection engine
│   │   ├── engine-mma.js        (hidden behind feature flag)
│   │   ├── engine-nba.js        (hidden behind feature flag)
│   │   ├── lib/
│   │   │   ├── api.js           Backend API client (Railway), static fallback
│   │   │   ├── auth-context.jsx Supabase auth + subscriptions
│   │   │   ├── supabase.js      Supabase client singleton
│   │   │   └── checkout.js      Stripe checkout / billing portal calls
│   │   └── components/
│   │       ├── UserMenu.jsx     Topbar sign-in state + subscribe button
│   │       └── SignInPrompt.jsx Magic-link sign-in screen
│   ├── public/
│   │   ├── _redirects           Netlify SPA routing
│   │   ├── slate.json           Placeholder fallback (real data via API)
│   │   └── slates/tennis/       Archive fallback
│   │       ├── manifest.json
│   │       ├── 2026-04-18.json
│   │       └── 2026-04-19.json
│   ├── scripts/                 Legacy manual-build scripts (no longer needed with backend)
│   ├── index.html
│   ├── package.json
│   ├── vite.config.js
│   ├── netlify.toml
│   └── .env.example
└── backend/                     Python + FastAPI, deploys to Railway
    ├── app/
    │   ├── main.py              FastAPI entrypoint
    │   ├── routes/              /api/slates, /api/players, /health
    │   ├── services/            DK client, normalizer, Discord, slate builder
    │   └── workers/             APScheduler DK poller
    ├── migrations/
    │   └── 001_initial.sql      Run once in Supabase SQL editor
    ├── Dockerfile
    ├── Procfile
    ├── railway.json
    ├── pyproject.toml
    └── .env.example
```

---

## One manual step you need to do after extracting

You'll need to drop your logo files into `frontend/public/`:

- `logo.png` (used by topbar and sign-in prompt)
- `favicon.ico`
- `apple-touch-icon.png`

Copy these from your old live-site repo's `public/` folder. Without them,
image tags in the UI will show broken. Everything else works fine without them.

---

## Deploy sequence

### 1. Push to GitHub

```bash
cd overowned
git init
git add .
git commit -m "Complete monorepo: frontend + backend + real auth integration"
git branch -M main
git remote add origin https://github.com/altapicks/overowned.git
git push -u origin main
```

(If pushing to the existing `OverOwned-Automated` repo, you'll want to
force-push over the partial state there: add `--force` to the last command.
Only do this if you're sure the current repo state is one you're willing
to overwrite.)

### 2. Run Supabase migration

Supabase dashboard → SQL Editor → paste `backend/migrations/001_initial.sql` → Run.

This creates the `players`, `slates`, `slate_players`, `matches`,
`ingestion_log`, and `unmatched_names` tables. It does **not** touch your
existing `subscriptions` or auth tables.

### 3. Deploy backend to Railway (already done if you got this far)

**API service:**
- Root directory: `backend`
- Env vars: `SUPABASE_URL`, `SUPABASE_SERVICE_KEY`, `CORS_ORIGINS`, `ENVIRONMENT=production`, `DK_SPORTS=TEN`
- Generate Domain → this is your `VITE_API_URL`

**Worker service:**
- Same repo, root `backend`
- Custom start command: `python -m app.workers.slate_watcher`
- Unset config-as-code (so the API's healthcheck doesn't kill the worker)
- Same env vars

### 4. Deploy frontend to Netlify

- Link GitHub repo
- Base directory: `frontend`
- Build command: `npm run build`
- Publish directory: `dist`
- Env vars:
  - `VITE_API_URL=https://<your-railway-api-url>`
  - `VITE_USE_API=true`
  - `VITE_SHOW_MMA=false`
  - `VITE_SHOW_NBA=false`
  - `VITE_SUPABASE_URL=https://<your-supabase>.supabase.co`
  - `VITE_SUPABASE_ANON_KEY=eyJ...` (anon/publishable key, NOT service_role)

Clear cache and deploy.

### 5. Verify

- Site loads, users can sign in via magic link as before
- F12 Network tab: request to Railway `/api/slates/today?sport=tennis`
- Railway worker logs show `DK lobby: sport=TEN draft_groups=X`
- Discord ping fires when a real (non-empty) slate is detected

---

## Daily workflow

1. Worker auto-ingests DK tennis slate → Discord pings you
2. Open site → slate already populated with salaries and pairings
3. Fill in odds / PP lines / adjustments via existing UI
4. Engine runs on auto-ingested data

No more `convert.py`. No more manual slate.json editing.

---

## What's next

The schema is ready for each of these without changes:

1. **ML odds ingestion** — DK Sportsbook + Odds API → `match_odds` table
2. **Kalshi websocket** — live probability, line movement detection
3. **Pickfinder Discord bot** — PP lines auto-ingested from your existing webhook
4. **Signals table** — every edge call logged with frozen state, auto-graded against results
5. **Contest results ingestion** — optimal lineup + realized ownership attribution

Each = one new service + one new table + one new API route.
