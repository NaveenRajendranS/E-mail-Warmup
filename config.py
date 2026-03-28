"""
Configuration constants and defaults for the Email Warmup System.
"""

# SMTP Configuration
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587

# IMAP Configuration (for reply checking)
IMAP_SERVER = "imap.gmail.com"
IMAP_PORT = 993

# Default Settings
DEFAULT_SETTINGS = {
    "gemini_api_key": "",
    "batch_size": 5,
    "delay_minutes": 0.5,
    "daily_limit": 20,
    "random_delay": True,
    "tone": "Casual",
    "rounds_per_day": 5,
    "gap_minutes": 60,
}

# Tone options for email generation
TONE_OPTIONS = ["Casual", "Friendly", "Internal office"]

# Database file
DB_PATH = "warmup.db"

# Log statuses
STATUS_SENT = "Sent"
STATUS_FAILED = "Failed"
