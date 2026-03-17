"""
Configuration constants and defaults for the Email Warmup System.
"""

# SMTP Configuration
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587

# Default Settings
DEFAULT_SETTINGS = {
    "gemini_api_key": "",
    "batch_size": 5,
    "delay_minutes": 2,
    "daily_limit": 20,
    "random_delay": True,
    "tone": "Casual",
}

# Tone options for email generation
TONE_OPTIONS = ["Casual", "Friendly", "Internal office"]

# Database file
DB_PATH = "warmup.db"

# Log statuses
STATUS_SENT = "Sent"
STATUS_FAILED = "Failed"
