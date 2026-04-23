# OverOwned

Tennis-first DFS analytics and automation platform.

```
overowned/
├── frontend/        Existing Vercel React app (mostly unchanged)
│   ├── src/lib/     API client + feature flags (NEW)
│   ├── .env.example
│   └── APPJSX_PATCH.md   Surgical instructions for existing App.jsx
└── backend/         NEW — ingestion + API (FastAPI + Supabase, deploys to Railway)
    ├── app/         Python package
    ├── migrations/  SQL to run in Supabase once
    ├── Dockerfile
    ├── railway.json
    └── README.md    Full deploy walkthrough
```

## What this first ship includes

**Slate Watcher** — polls DraftKings every 15 min, detects new tennis slates,
normalizes player names against a master table, writes to Supabase, exposes
a JSON API the React frontend consumes. Posts Discord notifications on new
slates and unmatched names. Completely replaces the manual `convert.py` step.

Once live, your new daily workflow:

1. Cron-watcher pulls DK automatically → slate appears in your DB
2. Discord pings you when a new slate is detected
3. You upload PP lines + your projections via the existing UI
4. Name matching is automatic for 99% of players after the first 1–2 weeks

Your site keeps every pixel of its existing styling — the only change is
where the data comes from.

## Quick start

### Backend
```bash
cd backend
python -m venv .venv && source .venv/bin/activate
pip install -e .[dev]
cp .env.example .env
# Fill in Supabase credentials
uvicorn app.main:app --reload
```

Full walkthrough including Railway deploy: `backend/README.md`

### Frontend
```bash
cd frontend   # (your existing frontend root — these files drop in alongside)
cp .env.example .env.local
# Set VITE_API_URL to your backend URL once it's deployed
```

Then apply the patch in `frontend/APPJSX_PATCH.md` (5 small edits, ~15 lines
in your 7,345-line App.jsx). Deploy to Vercel as usual.

## What's next after this ships

Per our conversation, the planned build order continues:

1. ~~Slate watcher~~ ← we are here
2. ML odds ingestion — DK Sportsbook + Odds API → `match_odds` table
3. Kalshi websocket subscriber — live `%` to win for each match
4. Pickfinder Discord bot — PP lines auto-ingested via existing webhook
5. Signals table — every edge call logged with frozen state for auto-grading
6. Results ingestion — match outcomes auto-graded against signals
7. Contest results ingestion — optimal lineup + realized ownership attribution

Each adds one new service + one new table + one new API route. The foundation
you're deploying today (players master, slates, matches, ingestion log,
unmatched queue, Discord notifier, Sentry, Railway multi-service) supports
all of them without rework.
