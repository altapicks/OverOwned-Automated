"""
AccuWeather match-weather service.

Fetches weather forecasts for tennis matches and persists them on the matches
row. Display-only — engine projections are NOT consumed.

Architecture
────────────
1. Lookup venue from `tennis_venues.py` by match.tournament substring match.
2. Resolve venue lat/lon → AccuWeather location_key (one-time per venue,
   cached in `weather_locations` table).
3. Fetch 12-hour hourly forecast for that location_key.
4. Pick the hourly forecast nearest to match.start_time.
5. Write a compact weather dict to `matches.weather` (jsonb column).

API budget: AccuWeather free tier = 50 calls/day. With location_key cached
forever per venue, an unbroken refresh of a 16-match slate consumes ~16 calls
(one forecast per unique location). New venues add 1 call each (geoposition
lookup) until cached. Plenty of headroom for 2-3 refreshes per slate per day.

Schema dependencies (run weather_migration.sql before deploying):
    matches.weather               jsonb (added)
    weather_locations             new table

Env vars required:
    ACCUWEATHER_API_KEY           AccuWeather Core API key (free tier OK)
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx

from app.db import get_client
from app.services.tennis_venues import lookup_venue

log = logging.getLogger(__name__)

ACCU_API_KEY = os.getenv("ACCUWEATHER_API_KEY", "")
ACCU_BASE = "http://dataservice.accuweather.com"

# Forecast freshness: refuse to refresh a match's weather more often than
# this. Prevents accidental burning of API budget on rapid-fire calls.
MIN_REFRESH_MINUTES = 15


# ───────────────────────────────────────────────────────────────────────
# Location key resolution (cached)
# ───────────────────────────────────────────────────────────────────────

def _round_coord(c: float) -> float:
    """Round to 4 decimals (~11m precision) for cache key stability.

    AccuWeather binds queries to nearest weather station. We don't need
    sub-meter precision and rounding gives us stable cache keys regardless
    of upstream lat/lon precision drift.
    """
    return round(float(c), 4)


def _get_cached_location_key(lat: float, lon: float) -> Optional[Dict[str, Any]]:
    """Pull cached AccuWeather location for (lat, lon). None if not cached."""
    db = get_client()
    lat_r, lon_r = _round_coord(lat), _round_coord(lon)
    try:
        rows = (
            db.table("weather_locations")
            .select("*")
            .eq("latitude", lat_r)
            .eq("longitude", lon_r)
            .limit(1)
            .execute()
            .data
            or []
        )
        return rows[0] if rows else None
    except Exception as e:
        log.warning("weather_locations cache lookup failed: %s", e)
        return None


def _persist_location_key(
    lat: float, lon: float, location_key: str,
    localized_name: Optional[str], timezone_name: Optional[str],
) -> None:
    """Insert a new location_key into the cache. Idempotent on (lat, lon)."""
    db = get_client()
    lat_r, lon_r = _round_coord(lat), _round_coord(lon)
    try:
        db.table("weather_locations").insert({
            "latitude": lat_r,
            "longitude": lon_r,
            "location_key": location_key,
            "localized_name": localized_name,
            "timezone": timezone_name,
        }).execute()
    except Exception as e:
        # Most likely a race / unique-violation if two slate fetches hit the
        # same new venue simultaneously. Re-read to confirm we got a row.
        log.info("weather_locations insert raced or duplicate: %s", e)


def _fetch_location_key_via_geoposition(
    lat: float, lon: float, timeout: float = 15.0,
) -> Optional[Tuple[str, str, str]]:
    """Hit AccuWeather geoposition search to resolve lat/lon → location_key.

    Returns (location_key, localized_name, time_zone_name) on success, None
    on any failure (including missing API key, HTTP error, malformed JSON).
    """
    if not ACCU_API_KEY:
        log.error("ACCUWEATHER_API_KEY not set; cannot resolve location.")
        return None
    url = f"{ACCU_BASE}/locations/v1/cities/geoposition/search"
    params = {"apikey": ACCU_API_KEY, "q": f"{lat},{lon}"}
    try:
        with httpx.Client(timeout=timeout) as client:
            r = client.get(url, params=params)
        if r.status_code != 200:
            log.warning(
                "AccuWeather geoposition HTTP %s for %s,%s: %s",
                r.status_code, lat, lon, r.text[:200],
            )
            return None
        body = r.json()
        if not isinstance(body, dict):
            log.warning("AccuWeather geoposition unexpected shape: %s", type(body))
            return None
        location_key = str(body.get("Key") or "")
        if not location_key:
            log.warning("AccuWeather geoposition no Key in body: %s", str(body)[:200])
            return None
        localized = (body.get("LocalizedName") or "")
        tz_info = body.get("TimeZone") or {}
        tz_name = (tz_info.get("Name") or "") if isinstance(tz_info, dict) else ""
        return location_key, localized, tz_name
    except httpx.HTTPError as e:
        log.warning("AccuWeather geoposition HTTP error: %s", e)
        return None
    except Exception as e:
        log.exception("AccuWeather geoposition unexpected error: %s", e)
        return None


def resolve_location_key(lat: float, lon: float) -> Optional[Dict[str, Any]]:
    """Get AccuWeather location_key for (lat, lon). Cache-first, then API.

    Returns dict with keys location_key, localized_name, timezone (any may
    be None on partial data). Returns None if the venue can't be resolved
    against AccuWeather at all.
    """
    cached = _get_cached_location_key(lat, lon)
    if cached:
        return {
            "location_key": cached.get("location_key"),
            "localized_name": cached.get("localized_name"),
            "timezone": cached.get("timezone"),
        }
    fetched = _fetch_location_key_via_geoposition(lat, lon)
    if not fetched:
        return None
    location_key, localized_name, tz_name = fetched
    _persist_location_key(lat, lon, location_key, localized_name, tz_name)
    return {
        "location_key": location_key,
        "localized_name": localized_name,
        "timezone": tz_name,
    }


# ───────────────────────────────────────────────────────────────────────
# Forecast fetch
# ───────────────────────────────────────────────────────────────────────

def _fetch_hourly_12h_forecast(
    location_key: str, timeout: float = 15.0,
) -> Optional[List[Dict[str, Any]]]:
    """Fetch the next 12 hourly forecasts for a location.

    Free tier endpoint. Returns a list of 12 forecast dicts on success, None
    on any failure. Each dict has keys: DateTime, Temperature, RealFeelTemperature,
    Wind.Speed/Direction, RelativeHumidity, PrecipitationProbability, IconPhrase,
    WeatherIcon, etc.
    """
    if not ACCU_API_KEY:
        log.error("ACCUWEATHER_API_KEY not set; cannot fetch forecast.")
        return None
    url = f"{ACCU_BASE}/forecasts/v1/hourly/12hour/{location_key}"
    params = {"apikey": ACCU_API_KEY, "details": "true", "metric": "false"}
    try:
        with httpx.Client(timeout=timeout) as client:
            r = client.get(url, params=params)
        if r.status_code == 401:
            log.error("AccuWeather 401: API key invalid or expired.")
            return None
        if r.status_code == 429 or r.status_code == 503:
            log.warning("AccuWeather rate limit or busy (HTTP %s)", r.status_code)
            return None
        if r.status_code != 200:
            log.warning(
                "AccuWeather forecast HTTP %s for loc=%s: %s",
                r.status_code, location_key, r.text[:200],
            )
            return None
        body = r.json()
        if not isinstance(body, list):
            log.warning("AccuWeather forecast unexpected shape: %s", type(body))
            return None
        return body
    except httpx.HTTPError as e:
        log.warning("AccuWeather forecast HTTP error: %s", e)
        return None
    except Exception as e:
        log.exception("AccuWeather forecast unexpected error: %s", e)
        return None


def _pick_forecast_nearest(
    forecasts: List[Dict[str, Any]], target_iso: Optional[str],
) -> Optional[Dict[str, Any]]:
    """Pick the hourly forecast whose DateTime is closest to target_iso.

    Falls back to the first forecast if target_iso is missing/unparseable
    (which is reasonable: we'd then be showing "current" weather).
    """
    if not forecasts:
        return None
    if not target_iso:
        return forecasts[0]
    try:
        target = datetime.fromisoformat(str(target_iso).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return forecasts[0]
    best = None
    best_delta = float("inf")
    for f in forecasts:
        dt_str = f.get("DateTime")
        if not dt_str:
            continue
        try:
            f_dt = datetime.fromisoformat(str(dt_str).replace("Z", "+00:00"))
        except (TypeError, ValueError):
            continue
        delta = abs((f_dt - target).total_seconds())
        if delta < best_delta:
            best_delta = delta
            best = f
    # If every forecast had unparseable timestamps, fall back to first.
    return best or forecasts[0]


def _compact_forecast(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Reduce AccuWeather's verbose hourly forecast object to a compact dict.

    Frontend reads this directly; keep field names short and stable. Units
    are imperial (°F, mph) because we requested metric=false.
    """
    temp = (raw.get("Temperature") or {}).get("Value")
    feel = (raw.get("RealFeelTemperature") or {}).get("Value")
    wind = raw.get("Wind") or {}
    wind_speed = ((wind.get("Speed") or {}).get("Value")) if isinstance(wind, dict) else None
    wind_dir = ((wind.get("Direction") or {}).get("Localized")) if isinstance(wind, dict) else None

    return {
        "datetime":           raw.get("DateTime"),
        "temperature_f":      temp,
        "feels_like_f":       feel,
        "wind_speed_mph":     wind_speed,
        "wind_direction":     wind_dir,
        "humidity_pct":       raw.get("RelativeHumidity"),
        "precipitation_pct":  raw.get("PrecipitationProbability"),
        "rain_pct":           raw.get("RainProbability"),
        "condition":          raw.get("IconPhrase"),
        "icon_id":            raw.get("WeatherIcon"),
        "is_daylight":        raw.get("IsDaylight"),
        "uv_index":           raw.get("UVIndex"),
    }


# ───────────────────────────────────────────────────────────────────────
# Per-match orchestration
# ───────────────────────────────────────────────────────────────────────

def fetch_weather_for_match(
    match_row: Dict[str, Any], force: bool = False,
) -> Optional[Dict[str, Any]]:
    """Resolve venue → location_key → forecast → persist on matches.weather.

    `match_row` should contain at least: id, tournament, start_time, weather.
    `force=True` bypasses the MIN_REFRESH_MINUTES freshness check.

    Returns the persisted weather dict on success, None if the match couldn't
    be resolved (unknown venue, API failure, missing data) — in which case
    matches.weather is left unchanged.
    """
    match_id = match_row.get("id")
    tournament = match_row.get("tournament")
    start_time = match_row.get("start_time")

    venue = lookup_venue(tournament)
    if not venue:
        log.info(
            "weather: no venue match for tournament=%r match=%s — skip",
            tournament, match_id,
        )
        return None

    # Indoor venues skip the API call entirely. Frontend renders "Indoor".
    if venue.get("is_indoor"):
        out = {
            "fetched_at":      datetime.now(timezone.utc).isoformat(),
            "tournament":      tournament,
            "venue_name":      venue.get("name"),
            "venue_timezone":  venue.get("tz"),
            "is_indoor":       True,
            "forecast":        None,
            "source":          "indoor_skip",
        }
        _persist_match_weather(match_id, out)
        return out

    # Freshness gate. matches.weather may already be recent enough that we
    # shouldn't re-fetch (saves API budget). Bypassed by force=True.
    if not force:
        existing = match_row.get("weather")
        if isinstance(existing, dict) and existing.get("fetched_at"):
            try:
                last = datetime.fromisoformat(
                    str(existing["fetched_at"]).replace("Z", "+00:00")
                )
                age_min = (datetime.now(timezone.utc) - last).total_seconds() / 60.0
                if age_min < MIN_REFRESH_MINUTES:
                    return existing
            except (TypeError, ValueError):
                pass  # fall through to fetch

    loc = resolve_location_key(venue["lat"], venue["lon"])
    if not loc or not loc.get("location_key"):
        log.warning(
            "weather: location_key resolution failed for match=%s venue=%s",
            match_id, venue.get("name"),
        )
        return None

    forecasts = _fetch_hourly_12h_forecast(loc["location_key"])
    if not forecasts:
        log.warning(
            "weather: forecast fetch failed for match=%s loc_key=%s",
            match_id, loc["location_key"],
        )
        return None

    chosen = _pick_forecast_nearest(forecasts, start_time)
    if not chosen:
        log.warning("weather: no usable forecast hour for match=%s", match_id)
        return None

    out = {
        "fetched_at":      datetime.now(timezone.utc).isoformat(),
        "tournament":      tournament,
        "venue_name":      venue.get("name"),
        "venue_timezone":  venue.get("tz") or loc.get("timezone"),
        "venue_lat":       venue["lat"],
        "venue_lon":       venue["lon"],
        "location_key":    loc["location_key"],
        "is_indoor":       False,
        "forecast":        _compact_forecast(chosen),
        "source":          "accuweather_hourly_12h",
    }
    _persist_match_weather(match_id, out)
    return out


def _persist_match_weather(match_id: Any, weather: Dict[str, Any]) -> None:
    """Write weather dict to matches.weather column."""
    if not match_id:
        return
    db = get_client()
    try:
        db.table("matches").update({"weather": weather}).eq("id", match_id).execute()
    except Exception as e:
        log.exception("matches.weather write failed for match=%s: %s", match_id, e)


# ───────────────────────────────────────────────────────────────────────
# Slate-level orchestration
# ───────────────────────────────────────────────────────────────────────

def refresh_weather_for_slate(slate_id: str, force: bool = False) -> Dict[str, Any]:
    """Refresh weather for every match on a slate.

    Returns a summary dict with counts: matches_total, success, skipped_indoor,
    skipped_unknown_venue, skipped_fresh, failed. Used by both the manual
    admin route and any background scheduler hook.
    """
    summary: Dict[str, Any] = {
        "slate_id": slate_id,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "matches_total": 0,
        "success": 0,
        "skipped_indoor": 0,
        "skipped_unknown_venue": 0,
        "skipped_fresh": 0,
        "failed": 0,
        "errors": [],
    }
    db = get_client()
    try:
        rows = (
            db.table("matches")
            .select("id, tournament, start_time, weather")
            .eq("slate_id", slate_id)
            .execute()
            .data
            or []
        )
    except Exception as e:
        summary["errors"].append(f"matches_load_failed: {e}")
        return summary

    summary["matches_total"] = len(rows)

    for m in rows:
        venue = lookup_venue(m.get("tournament"))
        if not venue:
            summary["skipped_unknown_venue"] += 1
            continue

        # Pre-check freshness so the per-match counter is right; reduces a
        # round-trip into fetch_weather_for_match for matches that won't run.
        if not force and isinstance(m.get("weather"), dict):
            try:
                last = datetime.fromisoformat(
                    str(m["weather"].get("fetched_at", "")).replace("Z", "+00:00")
                )
                age_min = (datetime.now(timezone.utc) - last).total_seconds() / 60.0
                if age_min < MIN_REFRESH_MINUTES:
                    summary["skipped_fresh"] += 1
                    continue
            except (TypeError, ValueError):
                pass

        result = fetch_weather_for_match(m, force=force)
        if result is None:
            summary["failed"] += 1
        elif result.get("is_indoor"):
            summary["skipped_indoor"] += 1
            summary["success"] += 1  # still counts as a successful row write
        else:
            summary["success"] += 1

    summary["finished_at"] = datetime.now(timezone.utc).isoformat()
    log.info(
        "weather refresh slate=%s total=%d ok=%d indoor=%d unknown=%d fresh=%d fail=%d",
        slate_id, summary["matches_total"], summary["success"],
        summary["skipped_indoor"], summary["skipped_unknown_venue"],
        summary["skipped_fresh"], summary["failed"],
    )
    return summary
