"""Compatibility shim for legacy main.py imports.

The actual admin debug routes live in admin_debug.py. This module exists
so that any older copy of main.py — or any other consumer that does
`from app.routes import admin_debug_extra` — resolves cleanly at import
time instead of crashing app startup with an ImportError.

The exposed `router` is empty: if anything tries to mount it via
`app.include_router(admin_debug_extra.router)`, that's a no-op (no routes
get added). No conflicts with admin_debug's actual routes.

Safe to delete once main.py no longer references admin_debug_extra.
"""
from fastapi import APIRouter

router = APIRouter()
