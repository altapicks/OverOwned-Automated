# App.jsx Integration — Minimal Patch

App.jsx is 7,345 lines and battle-tested. The integration only requires
three surgical edits. **Copy-paste each block exactly as shown.** No
styles, animations, or engine imports are touched.

---

## Edit 1: Add the import at the top

**Find** (line 17):
```js
import { useAuth } from './lib/auth-context';
```

**Add immediately after**:
```js
import { loadSlate, loadManifest, isSportEnabled } from './lib/api';
```

---

## Edit 2: Replace the slate fetch in `useSlate`

**Find** (around line 346–357, inside the `useSlate` hook):
```js
    // Live = current slate.json (root). Archive = /slates/{sport}/{date}.json
    const liveUrl = sport === 'mma' ? './slate-mma.json'
                  : sport === 'nba' ? './slate-nba.json'
                  : './slate.json';
    const url = isArchive ? `/slates/${sport}/${slateDate}.json` : liveUrl;
    fetch(url)
      .then(r => { if (!r.ok) throw new Error('No slate'); return r.json(); })
      .then(d => finalize(() => { hasLoadedRef.current = true; setData(d); }))
      .catch(e => finalize(() => setError(e.message)));
    return () => { cancelled = true; };
```

**Replace with**:
```js
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
```

---

## Edit 3: Replace the manifest fetch in `useSlateManifest`

**Find** (around line 365–370):
```js
    fetch(`/slates/${sport}/manifest.json`)
      .then(r => r.ok ? r.json() : { slates: [] })
      .then(m => setSlates(m.slates || []))
      .catch(() => setSlates([]));
```

**Replace with**:
```js
    loadManifest(sport).then(slates => setSlates(slates)).catch(() => setSlates([]));
```

---

## Edit 4: Hide NBA/MMA tabs behind the feature flag

**Find** (line 2036 — the MMA button):
```js
        <button onClick={() => onSportChange('mma')} title="MMA" aria-label="MMA" style={{
```

**Wrap the entire `<button>...</button>` block in a conditional**. So the MMA
button block becomes:
```js
        {isSportEnabled('mma') && <button onClick={() => onSportChange('mma')} title="MMA" aria-label="MMA" style={{
          /* ... existing styles ... */
        }}>
          {/* MMA glove SVG — unchanged */}
        </button>}
```

**Do the same for the NBA button** (line 2049). Wrap it in
`{isSportEnabled('nba') && ...}`.

When the env var is set to `VITE_SHOW_MMA=false` (the default) and
`VITE_SHOW_NBA=false`, the buttons simply don't render and tennis is the
only visible tab.

---

## Edit 5 (safety): Redirect to tennis if saved sport is hidden

**Find** (around line 1369, the sport state initializer):
```js
  const [sport, setSportRaw] = useState(() => {
```

**Peek at the existing logic** — it probably reads from localStorage.
Right after the `useState(...)` closing `)`, **add**:
```js
  // If feature-flagged off, force back to tennis
  useEffect(() => {
    if (!isSportEnabled(sport)) setSportRaw('tennis');
  }, [sport]);
```

This ensures a user who visited in MMA mode before you flipped the flag
gets dropped back to tennis automatically.

---

## That's it

5 edits, ~15 lines changed in a 7,345 line file. Your engines, your styles,
your animations, your auth flow — all untouched.

After deploying:
- If `VITE_API_URL` is unset: app behaves exactly as before (static files).
- If `VITE_API_URL` is set but the backend is down: app auto-falls back to
  static files. You'll see a console warning; users see nothing.
- If both set and backend is up: app uses live data from Supabase.
