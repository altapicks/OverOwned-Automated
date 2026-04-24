"""FastAPI application entrypoint.

In production on Railway, two services share this codebase:
  - `api`    → runs uvicorn on this app (this module)
  - `worker` → runs app.workers.slate_watcher

Both read Supabase credentials from the same env vars.
"""
from __future__ import annotations

import logging

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
