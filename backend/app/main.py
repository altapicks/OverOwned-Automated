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
from app.routes import health, players, prizepicks, slates


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

    return app


app = create_app()
