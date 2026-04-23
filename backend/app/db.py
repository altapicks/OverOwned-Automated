"""Supabase client. We use the service_role key on the backend so RLS is bypassed
for ingestion jobs. The frontend uses the anon key + RLS as usual."""
from functools import lru_cache

from supabase import Client, create_client

from app.config import get_settings


@lru_cache(maxsize=1)
def get_client() -> Client:
    s = get_settings()
    return create_client(s.supabase_url, s.supabase_service_key)
