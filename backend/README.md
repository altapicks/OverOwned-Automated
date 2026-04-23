# OverOwned Backend

Ingestion + API service. Polls DraftKings for tennis slates, normalizes player
names into a master table, and exposes a frontend-compatible JSON API.

Runs on Railway against a Supabase Postgres. Two services from one repo:
- **api** — FastAPI on uvicorn, serves `/api/slates/*`, `/api/players/*`, `/health`
- **worker** — APScheduler-driven DK poller, runs every 15 min

## Local Development

```bash
cd backend
python -m venv .venv && source .venv/bin/activate
pip install -e .[dev]
cp .env.example .env
# Fill in SUPABASE_URL and SUPABASE_SERVICE_KEY (see below)
```

### One-time DB setup

1. Create a Supabase project at https://supabase.com/dashboard (or use your existing one)
2. Open **SQL Editor**, paste the contents of `migrations/001_initial.sql`, and run
3. Open **Settings → API**, copy:
   - **Project URL** → `SUPABASE_URL`
   - **service_role key** → `SUPABASE_SERVICE_KEY` (this bypasses RLS — backend only, never ship to frontend)

### Run the API locally

```bash
uvicorn app.main:app --reload --port 8000
```

Visit `http://localhost:8000/health` — should return `{"status": "ok", ...}`.

### Run the worker manually

```bash
# One-shot: poll DK once and exit
python -c "import asyncio; from app.workers.slate_watcher import run_slate_watcher_once; print(asyncio.run(run_slate_watcher_once()))"

# Continuous: run the scheduler
python -m app.workers.slate_watcher
```

### Force a refresh via the API

```bash
curl -X POST http://localhost:8000/api/slates/refresh
```

## Deploying to Railway

1. Sign in at https://railway.app
2. **New Project → Deploy from GitHub repo** → select your monorepo
3. Railway auto-detects the `backend/` Dockerfile. Set the **Root Directory**
   to `backend` in service settings.
4. Add env vars (copy from `.env.example`):
   - `SUPABASE_URL`
   - `SUPABASE_SERVICE_KEY`
   - `DISCORD_WEBHOOK_SLATES` (optional)
   - `DISCORD_WEBHOOK_ERRORS` (optional)
   - `SENTRY_DSN` (optional)
   - `CORS_ORIGINS` — comma-separated list including your Vercel URL
   - `ENVIRONMENT=production`
5. Deploy. Railway gives you a URL like `overowned-api.up.railway.app`.
6. **Add a second service** for the worker:
   - In the Railway project dashboard → **+ New → Empty Service**
   - Connect to the same repo, Root Directory = `backend`
   - In **Settings → Deploy → Custom Start Command**:
     `python -m app.workers.slate_watcher`
   - Copy the same env vars across
7. Both services are now running. The worker polls DK every 15 min, the API
   serves whatever the worker has ingested.

## Verifying end-to-end

1. Wait for one poll cycle (or POST `/api/slates/refresh` to force one)
2. Check Discord for the new-slate notification
3. Hit `GET https://<your-api-url>/api/slates/today?sport=tennis`
4. Should return a `FrontendSlate` with `dk_players` and `matches` populated

## API Endpoints

| Endpoint | Purpose |
|---|---|
| `GET /health` | Liveness + DB connectivity + last ingest timestamp |
| `GET /api/slates/today?sport=tennis` | Current active slate (frontend-compatible JSON) |
| `GET /api/slates/{slate_id}` | Specific slate by UUID |
| `GET /api/slates/manifest/tennis` | Archive manifest (list of past slate dates) |
| `POST /api/slates/refresh` | Manually trigger a DK poll |
| `GET /api/players/master?sport=tennis` | Full player master table |
| `GET /api/players/unmatched?sport=tennis` | Unresolved name-matching queue |
| `POST /api/players/unmatched/{id}/resolve` | Resolve a queued name to a canonical player |

## Name-matching Behavior

The normalizer uses a three-tier strategy:

1. **Exact alias match** (100%) — auto-resolves instantly
2. **Fuzzy match ≥88%** — auto-resolves, records the new alias for next time
3. **Fuzzy match 70–87%** — queues in `unmatched_names`, pings Discord, suggests best guess
4. **Fuzzy match <70%** — creates a new player row (first time we're seeing them on tour)

In practice, after the first 1–2 weeks of daily slates your master table
stabilizes and 99% of names auto-resolve. The remaining edge cases (name changes,
weird transliterations, surname collisions) take a single Discord click to fix.

## Observability

- **Every poll cycle** logs to the `ingestion_log` table (source, duration, errors)
- **Every unmatched name** lives in the `unmatched_names` table until resolved
- **Every new slate** fires a Discord embed to `DISCORD_WEBHOOK_SLATES`
- **Every error** fires a Discord embed to `DISCORD_WEBHOOK_ERRORS`
- **Sentry** (if DSN set) captures uncaught exceptions with stack traces

## Adding a New Sport Later

Everything is sport-agnostic at the schema level. To re-enable NBA:

1. Backend: add `NBA` to `DK_SPORTS` env var on Railway
2. Frontend: set `VITE_SHOW_NBA=true` in Vercel
3. Redeploy both

The player master table, slates table, matches table, and API all handle
multiple sports natively.

## Troubleshooting

**403 from DK**
The User-Agent string occasionally gets rate-limited. If you see this, update
the UA in `app/services/dk_client.py` to a more recent Chrome version.

**"No active slate"**
Means the watcher hasn't found a current tennis draft group yet. Check:
- `GET /health` → what's `last_successful_ingest`?
- `SELECT * FROM ingestion_log ORDER BY started_at DESC LIMIT 5` in Supabase
- DK itself — is there actually a tennis slate today?

**Unmatched names piling up**
Open `GET /api/players/unmatched?sport=tennis` and resolve each. An admin UI
is on the roadmap but for now `curl` + the best_guess_id field is enough.
