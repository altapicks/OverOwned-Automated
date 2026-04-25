"""Application settings loaded from environment variables."""
from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Supabase
    supabase_url: str
    supabase_service_key: str

    # DK polling
    dk_poll_interval_minutes: int = 15
    dk_sports: str = "TEN"

    # In-process worker (default ON — single-service Railway deployments)
    # Set to false if running a separate Railway worker service.
    enable_in_process_worker: bool = True

    # Slate filtering
    dk_slate_types: str = "classic"  # comma list: classic,showdown,other
    dk_fallback_to_showdown: bool = True  # when no Classic found, ingest Showdown instead

    # Discord
    discord_webhook_slates: str = ""
    discord_webhook_errors: str = ""

    # The Odds API (legacy fallback — superseded by SGO)
    odds_api_key: str = ""

    # SportsGameOdds API (primary tennis odds + PrizePicks lines)
    sgo_api_key: str = ""

    # Kalshi
    kalshi_key_id: str = ""
    kalshi_private_key: str = ""
    kalshi_api_base: str = "https://trading-api.kalshi.com/trade-api/v2"

    # Sentry
    sentry_dsn: str = ""

    # API
    api_port: int = 8000
    cors_origins: str = "http://localhost:5173"

    # Misc
    log_level: str = "INFO"
    environment: Literal["development", "production"] = "development"

    @property
    def sports_list(self) -> list[str]:
        return [s.strip().upper() for s in self.dk_sports.split(",") if s.strip()]

    @property
    def slate_types_list(self) -> list[str]:
        return [t.strip().lower() for t in self.dk_slate_types.split(",") if t.strip()]

    @property
    def cors_origins_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
