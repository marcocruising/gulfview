from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
from supabase import Client, create_client


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _get_url() -> str | None:
    return os.environ.get("SUPABASE_URL")


def _get_server_secret_key() -> str | None:
    """Key for server-side clients that bypass RLS (pullers, loaders, optional personal Streamlit).

    Matches .env.example names, in order of preference.
    Legacy SUPABASE_KEY is accepted last for older setups.
    """
    return (
        os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
        or os.environ.get("SUPABASE_SECRET_KEY")
        or os.environ.get("SUPABASE_KEY")
    )


def _get_anon_or_publishable_key() -> str | None:
    """Public / anon JWT for browser-safe, RLS-respecting reads (preferred for deployed Streamlit)."""
    return os.environ.get("SUPABASE_ANON_PUBLIC_KEY") or os.environ.get(
        "SUPABASE_PUBLISHABLE_KEY"
    )


def get_client() -> Client:
    """Supabase client with write access (service role or secret key). Used by pullers and loaders."""
    load_dotenv(_project_root() / ".env")
    url = _get_url()
    key = _get_server_secret_key()
    if not url or not key:
        raise RuntimeError(
            "Set SUPABASE_URL and a server key in .env. "
            "Use SUPABASE_SERVICE_ROLE_KEY or SUPABASE_SECRET_KEY "
            "(see .env.example). Legacy SUPABASE_KEY is also accepted."
        )
    return create_client(url, key)


def get_read_client() -> Client:
    """Supabase client using anon / publishable key (RLS applies). Use when the app must not hold the service role."""
    load_dotenv(_project_root() / ".env")
    url = _get_url()
    key = _get_anon_or_publishable_key()
    if not url or not key:
        raise RuntimeError(
            "Set SUPABASE_URL and SUPABASE_ANON_PUBLIC_KEY or SUPABASE_PUBLISHABLE_KEY in .env "
            "(see .env.example)."
        )
    return create_client(url, key)
