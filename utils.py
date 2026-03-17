"""
Utility functions for the Email Warmup System.
"""

import re
import random
import time


def mask_password(password: str) -> str:
    """Mask a password for display, showing only first 2 and last 2 characters."""
    if not password or len(password) <= 4:
        return "••••"
    return password[:2] + "•" * (len(password) - 4) + password[-2:]


def validate_email(email: str) -> bool:
    """Basic email format validation."""
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, email))


def get_delay_seconds(delay_minutes: float, random_delay: bool) -> float:
    """
    Calculate delay in seconds.
    If random_delay is True, returns a random value between 50% and 150% of base delay.
    """
    base = delay_minutes * 60
    if random_delay and base > 0:
        return random.uniform(base * 0.5, base * 1.5)
    return base


def format_timestamp(ts: str) -> str:
    """Format a timestamp string for display."""
    if not ts:
        return "—"
    return ts


def status_color(status: str) -> str:
    """Return a color emoji for status display."""
    if status == "Sent":
        return "🟢"
    elif status == "Failed":
        return "🔴"
    return "⚪"
