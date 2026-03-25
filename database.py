"""
PostgreSQL (Supabase) database layer for the Email Warmup System.
Handles CRUD for senders, receivers, settings, and logs.
"""

import os
import psycopg2
import psycopg2.extras
from datetime import datetime, date, timezone, timedelta
from config import DEFAULT_SETTINGS

# IST timezone (UTC+5:30)
IST = timezone(timedelta(hours=5, minutes=30))


def get_database_url():
    """Get the database URL from Streamlit secrets or environment."""
    # Try Streamlit secrets first
    try:
        import streamlit as st
        url = st.secrets.get("DATABASE_URL", "")
        if url:
            return url
    except Exception:
        pass
    # Fallback to environment variable
    return os.environ.get("DATABASE_URL", "")


def _create_connection():
    """Create a new PostgreSQL connection."""
    url = get_database_url()
    if not url:
        raise RuntimeError("DATABASE_URL not configured. Add it to Streamlit secrets or environment.")
    conn = psycopg2.connect(url)
    conn.autocommit = False
    return conn


# Cache a single connection per app session
_cached_conn = None


def get_connection():
    """Get a reusable PostgreSQL connection (auto-reconnects if stale)."""
    global _cached_conn
    try:
        if _cached_conn is None or _cached_conn.closed:
            _cached_conn = _create_connection()
        else:
            # Test if connection is still alive
            _cached_conn.cursor().execute("SELECT 1")
            _cached_conn.rollback()  # discard the test query
    except Exception:
        try:
            _cached_conn.close()
        except Exception:
            pass
        _cached_conn = _create_connection()
    return _cached_conn


def init_db():
    """Initialize database tables if they don't exist."""
    conn = get_connection()
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS senders (
            id SERIAL PRIMARY KEY,
            email TEXT UNIQUE NOT NULL,
            app_password TEXT NOT NULL,
            active INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS receivers (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS logs (
            id SERIAL PRIMARY KEY,
            sender_email TEXT NOT NULL,
            receiver_email TEXT NOT NULL,
            receiver_name TEXT,
            subject TEXT,
            status TEXT NOT NULL,
            error TEXT,
            timestamp TEXT DEFAULT TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS')
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS sender_receiver_map (
            id SERIAL PRIMARY KEY,
            sender_id INTEGER NOT NULL REFERENCES senders(id) ON DELETE CASCADE,
            receiver_id INTEGER NOT NULL REFERENCES receivers(id) ON DELETE CASCADE,
            UNIQUE(sender_id, receiver_id)
        )
    """)

    # Insert default settings if not present
    for key, value in DEFAULT_SETTINGS.items():
        c.execute(
            "INSERT INTO settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO NOTHING",
            (key, str(value)),
        )

    conn.commit()

    # Always seed senders and receivers (ON CONFLICT DO NOTHING makes this safe)
    seed_senders()
    seed_receivers()

    # Auto-map only on first run
    conn2 = get_connection()
    c2 = conn2.cursor()
    c2.execute("SELECT value FROM settings WHERE key = 'seeded'")
    seeded = c2.fetchone()

    if not seeded:
        auto_map_senders()
        c2.execute("INSERT INTO settings (key, value) VALUES ('seeded', 'true') ON CONFLICT (key) DO NOTHING")
        conn2.commit()



# â”€â”€ Seed Senders â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

# (email, app_password, active)
SEED_SENDERS = [
    ("admin@reimaginehome.app", "CHANGE_ME", 0),
    ("sopia@reimaginehome.app", "CHANGE_ME", 0),
    ("alex@reimaginehome.app", "CHANGE_ME", 0),
    ("jason@reimaginehome.app", "CHANGE_ME", 0),
    ("emily@reimaginehome.app", "CHANGE_ME", 0),
    ("henan@reimaginehome.app", "CHANGE_ME", 0),
    ("desiree@reimaginehome.app", "CHANGE_ME", 0),
    ("tiffany@reimaginehome.app", "CHANGE_ME", 0),
    ("chris@reimaginehome.app", "CHANGE_ME", 0),
    ("melissa@reimaginehome.app", "CHANGE_ME", 0),
    ("admin@reimaginehome.tech", "CHANGE_ME", 0),
    ("valentina@reimaginehome.tech", "CHANGE_ME", 0),
    ("lucas@reimaginehome.tech", "CHANGE_ME", 0),
    ("amelia@reimaginehome.tech", "CHANGE_ME", 0),
    ("ryan@reimaginehome.tech", "CHANGE_ME", 0),
    ("helen@reimaginehome.tech", "CHANGE_ME", 0),
    ("barbara@reimaginehome.tech", "CHANGE_ME", 0),
    ("jade@reimaginehome.tech", "CHANGE_ME", 0),
    ("valencia@reimaginehome.tech", "CHANGE_ME", 0),
    ("luke@reimaginehome.tech", "CHANGE_ME", 0),
    ("admin@reimaginehome.homes", "CHANGE_ME", 0),
    ("olivia@reimaginehome.homes", "CHANGE_ME", 0),
    ("lena@reimaginehome.homes", "CHANGE_ME", 0),
    ("daniel@reimaginehome.homes", "CHANGE_ME", 0),
    ("elena@reimaginehome.homes", "CHANGE_ME", 0),
    ("ben@reimaginehome.homes", "CHANGE_ME", 0),
    ("jennifer@reimaginehome.homes", "CHANGE_ME", 0),
    ("shelly@reimaginehome.homes", "CHANGE_ME", 0),
    ("kimberley@reimaginehome.homes", "CHANGE_ME", 0),
    ("stephan@reimaginehome.homes", "CHANGE_ME", 0),
    # reimaginehome.net senders
    ("akhilesh.majumdar@reimaginehome.net", "lkdf crxq acgq unyr", 1),
    ("m.akhilesh@reimaginehome.net", "dxnv nowf geyn uiny", 1),
    ("gohilshital@reimaginehome.net", "uvss atnu stms hcxe", 1),
    ("akhilesh@reimaginehome.net", "namn fmmv njoi coph", 1),
    ("shitalgohil@reimaginehome.net", "wqja tnku zpgy lulc", 1),
    ("majumdarakhilesh@reimaginehome.net", "imzk jgms qurb xfqx", 1),
    ("akhileshmajumdar@reimaginehome.net", "haqe azpu xzhm njxl", 1),
    ("gohil.shital@reimaginehome.net", "pwbt moii xnut pbbz", 1),
    ("shital.g@reimaginehome.net", "cjhl eurk fxoz pfji", 1),
    ("shital.gohil@reimaginehome.net", "heic dyqo ntky zisk", 1),
    ("shitalg@reimaginehome.net", "vaog dlqw wvmo zanw", 1),
    ("Shital@reimaginehome.net", "opxh uzpi nnlq vovi", 1),
    ("akhilesh.m@reimaginehome.net", "pcql okak crsa zrre", 1),
]


def seed_senders():
    """Pre-load sender emails if they don't already exist."""
    conn = get_connection()
    c = conn.cursor()
    for email, app_password, active in SEED_SENDERS:
        c.execute(
            "INSERT INTO senders (email, app_password, active) VALUES (%s, %s, %s) ON CONFLICT (email) DO NOTHING",
            (email, app_password, active),
        )
    conn.commit()



# â”€â”€ Seed Receivers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

SEED_RECEIVERS = [
    "prithvir.1011@gmail.com",
    "prithvi.r3957@gmail.com",
    "prithvi.gowda21@gmail.com",
    "prithvi.pr1011@gmail.com",
    "prithvi.gowda1999@gmail.com",
    "test.automation.styldod@gmail.com",
    "rohit.a.thorat@gmail.com",
    "rothorat7779@gmail.com",
    "jkomal9797@gmail.com",
    "sladewinter@gmail.com",
    "shital.nid@gmail.com",
    "deepakpandey7100@gmail.com",
    "craftsointeriors@gmail.com",
    "arulpradeep95@gmail.com",
    "arulpradeep05@gmail.com",
    "komalrohan9797@gmail.com",
    "mbc261996@gmail.com",
    "editzpituresque@gmail.com",
    "rewritingtheera@gmail.com",
    "arulpradeepp05@icloud.com",
    "ramyakrishna3024@gmail.com",
    "puttanaik1993@gmail.com",
    "abhishek.rath85@gmail.com",
    "henanmaliyakkal@gmail.com",
    "amalyaaamz@gmail.com",
    "Amalyashaji926@gmail.com",
    "amalyabackup0@gmail.com",
    "wave.crest444@gmail.com",
    "akash.shitole.5595@gmail.com",
    "akash.shitole.5902@gmail.com",
    "akash.shitole.0509@gmail.com",
]


def seed_receivers():
    """Pre-load receiver emails if they don't already exist. Name defaults to email username."""
    conn = get_connection()
    c = conn.cursor()
    for email in SEED_RECEIVERS:
        name = email.split("@")[0].replace(".", " ").title()
        c.execute(
            "INSERT INTO receivers (name, email) VALUES (%s, %s) ON CONFLICT (email) DO NOTHING",
            (name, email),
        )
    conn.commit()



def auto_map_senders():
    """Auto-assign 5 unique receivers to each sender that has no mappings.
    Uses round-robin so each receiver is distributed fairly across senders."""
    conn = get_connection()
    c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    c.execute("SELECT id FROM senders ORDER BY id")
    senders = c.fetchall()
    c.execute("SELECT id FROM receivers ORDER BY id")
    receivers = c.fetchall()

    if not senders or not receivers:

        return

    receiver_ids = [r["id"] for r in receivers]
    num_receivers = len(receiver_ids)
    receivers_per_sender = 5

    offset = 0
    mapped_any = False

    for sender in senders:
        sid = sender["id"]
        # Skip if this sender already has mappings
        c.execute(
            "SELECT COUNT(*) as cnt FROM sender_receiver_map WHERE sender_id = %s", (sid,)
        )
        existing = c.fetchone()["cnt"]
        if existing > 0:
            offset += receivers_per_sender
            continue

        # Pick 5 receivers using round-robin offset
        assigned = []
        for i in range(receivers_per_sender):
            idx = (offset + i) % num_receivers
            assigned.append(receiver_ids[idx])
        offset += receivers_per_sender

        for rid in assigned:
            c.execute(
                "INSERT INTO sender_receiver_map (sender_id, receiver_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (sid, rid),
            )
        mapped_any = True

    if mapped_any:
        conn.commit()



def randomize_all_mappings(receivers_per_sender=5):
    """Clear all mappings and randomly assign receivers to each sender."""
    import random

    conn = get_connection()
    c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    c.execute("SELECT id FROM senders ORDER BY id")
    senders = c.fetchall()
    c.execute("SELECT id FROM receivers ORDER BY id")
    receivers = c.fetchall()

    if not senders or not receivers:

        return

    receiver_ids = [r["id"] for r in receivers]

    # Clear all existing mappings
    c.execute("DELETE FROM sender_receiver_map")

    for sender in senders:
        sid = sender["id"]
        # Shuffle a copy of receiver IDs and pick the first N
        shuffled = receiver_ids.copy()
        random.shuffle(shuffled)
        assigned = shuffled[:receivers_per_sender]

        for rid in assigned:
            c.execute(
                "INSERT INTO sender_receiver_map (sender_id, receiver_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (sid, rid),
            )

    conn.commit()



# â”€â”€ Sender CRUD â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def get_all_senders():
    conn = get_connection()
    c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    c.execute("SELECT * FROM senders ORDER BY id")
    rows = c.fetchall()

    return [dict(r) for r in rows]


def get_active_senders():
    conn = get_connection()
    c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    c.execute("SELECT * FROM senders WHERE active = 1 ORDER BY id")
    rows = c.fetchall()

    return [dict(r) for r in rows]


def add_sender(email, app_password):
    conn = get_connection()
    c = conn.cursor()
    try:
        c.execute(
            "INSERT INTO senders (email, app_password) VALUES (%s, %s)",
            (email, app_password),
        )
        conn.commit()
        return True, "Sender added successfully."
    except psycopg2.IntegrityError:
        conn.rollback()
        return False, "Sender email already exists."


def update_sender(sender_id, email, app_password, active):
    conn = get_connection()
    c = conn.cursor()
    try:
        c.execute(
            "UPDATE senders SET email = %s, app_password = %s, active = %s WHERE id = %s",
            (email, app_password, int(active), sender_id),
        )
        conn.commit()
        return True, "Sender updated."
    except psycopg2.IntegrityError:
        conn.rollback()
        return False, "Another sender with that email already exists."


def delete_sender(sender_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute("DELETE FROM sender_receiver_map WHERE sender_id = %s", (sender_id,))
    c.execute("DELETE FROM senders WHERE id = %s", (sender_id,))
    conn.commit()



def toggle_sender(sender_id, active):
    conn = get_connection()
    c = conn.cursor()
    c.execute("UPDATE senders SET active = %s WHERE id = %s", (int(active), sender_id))
    conn.commit()



# â”€â”€ Receiver CRUD â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def get_all_receivers():
    conn = get_connection()
    c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    c.execute("SELECT * FROM receivers ORDER BY id")
    rows = c.fetchall()

    return [dict(r) for r in rows]


def add_receiver(name, email):
    conn = get_connection()
    c = conn.cursor()
    try:
        c.execute(
            "INSERT INTO receivers (name, email) VALUES (%s, %s)",
            (name, email),
        )
        conn.commit()
        return True, "Receiver added successfully."
    except psycopg2.IntegrityError:
        conn.rollback()
        return False, "Receiver email already exists."


def update_receiver(receiver_id, name, email):
    conn = get_connection()
    c = conn.cursor()
    try:
        c.execute(
            "UPDATE receivers SET name = %s, email = %s WHERE id = %s",
            (name, email, receiver_id),
        )
        conn.commit()
        return True, "Receiver updated."
    except psycopg2.IntegrityError:
        conn.rollback()
        return False, "Another receiver with that email already exists."


def delete_receiver(receiver_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute("DELETE FROM sender_receiver_map WHERE receiver_id = %s", (receiver_id,))
    c.execute("DELETE FROM receivers WHERE id = %s", (receiver_id,))
    conn.commit()



# â”€â”€ Sender-Receiver Mapping â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def get_mapped_receivers(sender_id):
    """Get receivers mapped to a specific sender."""
    conn = get_connection()
    c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    c.execute("""
        SELECT r.* FROM receivers r
        JOIN sender_receiver_map m ON r.id = m.receiver_id
        WHERE m.sender_id = %s
        ORDER BY r.id
    """, (sender_id,))
    rows = c.fetchall()

    return [dict(r) for r in rows]


def get_mapped_receiver_ids(sender_id):
    """Get receiver IDs mapped to a specific sender."""
    conn = get_connection()
    c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    c.execute(
        "SELECT receiver_id FROM sender_receiver_map WHERE sender_id = %s",
        (sender_id,),
    )
    rows = c.fetchall()

    return [r["receiver_id"] for r in rows]


def set_sender_mappings(sender_id, receiver_ids):
    """Replace all mappings for a sender with the given receiver IDs."""
    conn = get_connection()
    c = conn.cursor()
    c.execute("DELETE FROM sender_receiver_map WHERE sender_id = %s", (sender_id,))
    for rid in receiver_ids:
        c.execute(
            "INSERT INTO sender_receiver_map (sender_id, receiver_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (sender_id, rid),
        )
    conn.commit()



def get_all_mappings():
    """Get all sender-receiver mappings grouped by sender."""
    conn = get_connection()
    c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    c.execute("""
        SELECT m.sender_id, s.email as sender_email, m.receiver_id, r.email as receiver_email, r.name as receiver_name
        FROM sender_receiver_map m
        JOIN senders s ON s.id = m.sender_id
        JOIN receivers r ON r.id = m.receiver_id
        ORDER BY m.sender_id, m.receiver_id
    """)
    rows = c.fetchall()

    return [dict(r) for r in rows]


# â”€â”€ Settings â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def get_settings():
    conn = get_connection()
    c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    c.execute("SELECT * FROM settings")
    rows = c.fetchall()

    settings = {}
    for r in rows:
        key = r["key"]
        val = r["value"]
        # Convert types based on defaults
        default = DEFAULT_SETTINGS.get(key)
        if isinstance(default, bool):
            settings[key] = val.lower() in ("true", "1", "yes")
        elif isinstance(default, float):
            try:
                settings[key] = float(val)
            except ValueError:
                settings[key] = default
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
    c = conn.cursor()
    for key, value in settings_dict.items():
        c.execute(
            "INSERT INTO settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
            (key, str(value)),
        )
    conn.commit()



# â”€â”€ Logs â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def add_log(sender_email, receiver_email, receiver_name, subject, status, error=None):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        """INSERT INTO logs (sender_email, receiver_email, receiver_name, subject, status, error, timestamp)
           VALUES (%s, %s, %s, %s, %s, %s, %s)""",
        (sender_email, receiver_email, receiver_name, subject, status, error,
         datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")),
    )
    conn.commit()



def get_logs(sender_filter=None, date_filter=None, status_filter=None, limit=200):
    conn = get_connection()
    c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    query = "SELECT * FROM logs WHERE 1=1"
    params = []

    if sender_filter and sender_filter != "All":
        query += " AND sender_email = %s"
        params.append(sender_filter)

    if date_filter:
        query += " AND LEFT(timestamp, 10) = %s"
        params.append(str(date_filter))

    if status_filter and status_filter != "All":
        query += " AND status = %s"
        params.append(status_filter)

    query += " ORDER BY id DESC LIMIT %s"
    params.append(limit)

    c.execute(query, params)
    rows = c.fetchall()

    return [dict(r) for r in rows]


def get_today_sent_count(sender_email=None):
    conn = get_connection()
    c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    today = date.today().strftime("%Y-%m-%d")
    if sender_email:
        c.execute(
            "SELECT COUNT(*) as cnt FROM logs WHERE LEFT(timestamp, 10) = %s AND sender_email = %s AND status = 'Sent'",
            (today, sender_email),
        )
    else:
        c.execute(
            "SELECT COUNT(*) as cnt FROM logs WHERE LEFT(timestamp, 10) = %s AND status = 'Sent'",
            (today,),
        )
    row = c.fetchone()

    return row["cnt"] if row else 0


def get_today_failed_count():
    conn = get_connection()
    c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    today = date.today().strftime("%Y-%m-%d")
    c.execute(
        "SELECT COUNT(*) as cnt FROM logs WHERE LEFT(timestamp, 10) = %s AND status = 'Failed'",
        (today,),
    )
    row = c.fetchone()

    return row["cnt"] if row else 0


def get_last_run_time():
    conn = get_connection()
    c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    c.execute(
        "SELECT timestamp FROM logs ORDER BY id DESC LIMIT 1"
    )
    row = c.fetchone()

    return row["timestamp"] if row else "Never"


def clear_logs():
    conn = get_connection()
    c = conn.cursor()
    c.execute("DELETE FROM logs")
    conn.commit()

