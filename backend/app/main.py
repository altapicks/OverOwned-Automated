"""FastAPI application entrypoint.

Runs as a single Railway service that does BOTH:

1. Serves HTTP (uvicorn running this app)
2. Runs the scheduled slate + odds watcher in-process via FastAPI lifespan

The watcher uses APScheduler's AsyncIOScheduler, which shares the event loop
with FastAPI. max_instances=1 + coalesce=True on each job prevents overlap.

If you ever split into separate services, set ENABLE_IN_PROCESS_WORKER=false
on the api service and run `python -m app.workers.slate_watcher` as its own
process.
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager

import sentry_sdk
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app import __version__
from app.config import get_settings
from app.routes import admin_slate, health, players, prizepicks, slates, tracker


def _configure_logging(level: str):
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )


logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start the scheduled watcher alongside the HTTP server."""
    settings = get_settings()

    _log_provider_diagnostics(settings)

    if settings.enable_in_process_worker:
        try:
            from app.workers.slate_watcher import scheduled_watcher

            logger.info(
                "Starting in-process slate watcher (sports=%s, types=%s, fallback=%s)",
                settings.sports_list,
                settings.slate_types_list,
                settings.dk_fallback_to_showdown,
            )
            async with scheduled_watcher():
                yield
        except Exception as e:
            logger.exception("In-process watcher failed to start: %s", e)
            yield
    else:
        logger.info("In-process watcher disabled (ENABLE_IN_PROCESS_WORKER=false)")
        yield

    logger.info("Application shutdown complete")


def _log_provider_diagnostics(settings):
    """Report provider config status at boot. Never crashes — purely informational."""
    # Supabase (critical — app won't function without)
    logger.info(
        "Supabase config: url=%s, service_key=%s",
        "OK" if settings.supabase_url else "MISSING",
        "OK" if settings.supabase_service_key else "MISSING",
    )

    # SportsGameOdds (primary tennis odds + PrizePicks props)
    sgo_key = settings.sgo_api_key or ""
    logger.info(
        "SportsGameOdds config: key=%s",
        f"set (len={len(sgo_key)})" if sgo_key else "MISSING — SGO ticks will skip",
    )

    # The Odds API (legacy fallback)
    oa_key = settings.odds_api_key or ""
    logger.info(
        "The Odds API config (legacy): key=%s",
        f"set (len={len(oa_key)})" if oa_key else "not set",
    )

    # Kalshi — eagerly try loading the key so PEM issues surface at boot
    kk = settings.kalshi_key_id or ""
    kpk = settings.kalshi_private_key or ""
    kb = settings.kalshi_api_base or ""
    logger.info(
        "Kalshi config: key_id=%s, private_key=%s, base=%s",
        f"set (prefix={kk[:8]}...)" if kk else "MISSING",
        f"set (len={len(kpk)})" if kpk else "MISSING",
        kb or "MISSING",
    )
    if kk and kpk:
        try:
            from app.services.kalshi import _load_private_key

            key = _load_private_key()
            if key is None:
                logger.warning(
                    "Kalshi private key failed to load at boot. "
                    "Kalshi calls will return 401 until this is fixed."
                )
        except Exception as e:
            logger.warning("Kalshi eager-load raised: %s", e)

    # Discord webhooks (optional)
    logger.info(
        "Discord webhooks: slates=%s, errors=%s",
        "set" if settings.discord_webhook_slates else "not set",
        "set" if settings.discord_webhook_errors else "not set",
    )


def create_app() -> FastAPI:
    settings = get_settings()
    _configure_logging(settings.log_level)

    if settings.sentry_dsn:
        sentry_sdk.init(
            dsn=settings.sentry_dsn,
            environment=settings.environment,
            traces_sample_rate=0.1 if settings.environment == "production" else 0,
            release=__version__,
        )

    app = FastAPI(
        title="OverOwned API",
        version=__version__,
        description="Ingestion and slate API for OverOwned DFS.",
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins_list,
        allow_credentials=True,
        allow_methods=["GET", "POST", "OPTIONS"],
        allow_headers=["*"],
    )

    app.include_router(health.router)
    app.include_router(slates.router)
    app.include_router(players.router)
    app.include_router(prizepicks.router)
    app.include_router(tracker.router)
    app.include_router(admin_slate.router)
    return app


app = create_app()
