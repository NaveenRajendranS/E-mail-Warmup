"""
SQLite database layer for the Email Warmup System.
Handles CRUD for senders, receivers, settings, and logs.
"""

import sqlite3
import os
from datetime import datetime, date
from config import DB_PATH, DEFAULT_SETTINGS


def get_connection():
    """Get a SQLite connection with row factory."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Initialize database tables if they don't exist."""
    conn = get_connection()
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS senders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE NOT NULL,
            app_password TEXT NOT NULL,
            active INTEGER DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS receivers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sender_email TEXT NOT NULL,
            receiver_email TEXT NOT NULL,
            receiver_name TEXT,
            subject TEXT,
            status TEXT NOT NULL,
            error TEXT,
            timestamp TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)

    # Insert default settings if not present
    for key, value in DEFAULT_SETTINGS.items():
        c.execute(
            "INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)",
            (key, str(value)),
        )

    conn.commit()
    conn.close()


# ── Sender CRUD ──────────────────────────────────────────────

def get_all_senders():
    conn = get_connection()
    rows = conn.execute("SELECT * FROM senders ORDER BY id").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_active_senders():
    conn = get_connection()
    rows = conn.execute("SELECT * FROM senders WHERE active = 1 ORDER BY id").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def add_sender(email, app_password):
    conn = get_connection()
    try:
        conn.execute(
            "INSERT INTO senders (email, app_password) VALUES (?, ?)",
            (email, app_password),
        )
        conn.commit()
        return True, "Sender added successfully."
    except sqlite3.IntegrityError:
        return False, "Sender email already exists."
    finally:
        conn.close()


def update_sender(sender_id, email, app_password, active):
    conn = get_connection()
    try:
        conn.execute(
            "UPDATE senders SET email = ?, app_password = ?, active = ? WHERE id = ?",
            (email, app_password, int(active), sender_id),
        )
        conn.commit()
        return True, "Sender updated."
    except sqlite3.IntegrityError:
        return False, "Another sender with that email already exists."
    finally:
        conn.close()


def delete_sender(sender_id):
    conn = get_connection()
    conn.execute("DELETE FROM senders WHERE id = ?", (sender_id,))
    conn.commit()
    conn.close()


def toggle_sender(sender_id, active):
    conn = get_connection()
    conn.execute("UPDATE senders SET active = ? WHERE id = ?", (int(active), sender_id))
    conn.commit()
    conn.close()


# ── Receiver CRUD ────────────────────────────────────────────

def get_all_receivers():
    conn = get_connection()
    rows = conn.execute("SELECT * FROM receivers ORDER BY id").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def add_receiver(name, email):
    conn = get_connection()
    try:
        conn.execute(
            "INSERT INTO receivers (name, email) VALUES (?, ?)",
            (name, email),
        )
        conn.commit()
        return True, "Receiver added successfully."
    except sqlite3.IntegrityError:
        return False, "Receiver email already exists."
    finally:
        conn.close()


def update_receiver(receiver_id, name, email):
    conn = get_connection()
    try:
        conn.execute(
            "UPDATE receivers SET name = ?, email = ? WHERE id = ?",
            (name, email, receiver_id),
        )
        conn.commit()
        return True, "Receiver updated."
    except sqlite3.IntegrityError:
        return False, "Another receiver with that email already exists."
    finally:
        conn.close()


def delete_receiver(receiver_id):
    conn = get_connection()
    conn.execute("DELETE FROM receivers WHERE id = ?", (receiver_id,))
    conn.commit()
    conn.close()


# ── Settings ─────────────────────────────────────────────────

def get_settings():
    conn = get_connection()
    rows = conn.execute("SELECT * FROM settings").fetchall()
    conn.close()
    settings = {}
    for r in rows:
        key = r["key"]
        val = r["value"]
        # Convert types based on defaults
        default = DEFAULT_SETTINGS.get(key)
        if isinstance(default, bool):
            settings[key] = val.lower() in ("true", "1", "yes")
        elif isinstance(default, int):
            try:
                settings[key] = int(val)
            except ValueError:
                settings[key] = default
        else:
            settings[key] = val
    return settings


def save_settings(settings_dict):
    conn = get_connection()
    for key, value in settings_dict.items():
        conn.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
            (key, str(value)),
        )
    conn.commit()
    conn.close()


# ── Logs ─────────────────────────────────────────────────────

def add_log(sender_email, receiver_email, receiver_name, subject, status, error=None):
    conn = get_connection()
    conn.execute(
        """INSERT INTO logs (sender_email, receiver_email, receiver_name, subject, status, error, timestamp)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (sender_email, receiver_email, receiver_name, subject, status, error,
         datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
    )
    conn.commit()
    conn.close()


def get_logs(sender_filter=None, date_filter=None, status_filter=None, limit=200):
    conn = get_connection()
    query = "SELECT * FROM logs WHERE 1=1"
    params = []

    if sender_filter and sender_filter != "All":
        query += " AND sender_email = ?"
        params.append(sender_filter)

    if date_filter:
        query += " AND DATE(timestamp) = ?"
        params.append(str(date_filter))

    if status_filter and status_filter != "All":
        query += " AND status = ?"
        params.append(status_filter)

    query += " ORDER BY id DESC LIMIT ?"
    params.append(limit)

    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_today_sent_count(sender_email=None):
    conn = get_connection()
    today = date.today().strftime("%Y-%m-%d")
    if sender_email:
        row = conn.execute(
            "SELECT COUNT(*) as cnt FROM logs WHERE DATE(timestamp) = ? AND sender_email = ? AND status = 'Sent'",
            (today, sender_email),
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT COUNT(*) as cnt FROM logs WHERE DATE(timestamp) = ? AND status = 'Sent'",
            (today,),
        ).fetchone()
    conn.close()
    return row["cnt"] if row else 0


def get_today_failed_count():
    conn = get_connection()
    today = date.today().strftime("%Y-%m-%d")
    row = conn.execute(
        "SELECT COUNT(*) as cnt FROM logs WHERE DATE(timestamp) = ? AND status = 'Failed'",
        (today,),
    ).fetchone()
    conn.close()
    return row["cnt"] if row else 0


def get_last_run_time():
    conn = get_connection()
    row = conn.execute(
        "SELECT timestamp FROM logs ORDER BY id DESC LIMIT 1"
    ).fetchone()
    conn.close()
    return row["timestamp"] if row else "Never"


def clear_logs():
    conn = get_connection()
    conn.execute("DELETE FROM logs")
    conn.commit()
    conn.close()
