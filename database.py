"""
SQLite database layer for the Email Warmup System.
Handles CRUD for senders, receivers, settings, and logs.
"""

import sqlite3
import os
from datetime import datetime, date, timezone, timedelta
from config import DB_PATH, DEFAULT_SETTINGS

# IST timezone (UTC+5:30)
IST = timezone(timedelta(hours=5, minutes=30))


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
            name TEXT DEFAULT '',
            email TEXT UNIQUE NOT NULL,
            app_password TEXT NOT NULL,
            active INTEGER DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Migration: add name column if missing (for existing DBs)
    try:
        c.execute("ALTER TABLE senders ADD COLUMN name TEXT DEFAULT ''")
    except Exception:
        pass  # Column already exists

    c.execute("""
        CREATE TABLE IF NOT EXISTS receivers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            active INTEGER DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Migration: add active column to receivers if missing
    try:
        c.execute("ALTER TABLE receivers ADD COLUMN active INTEGER DEFAULT 1")
    except Exception:
        pass

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

    c.execute("""
        CREATE TABLE IF NOT EXISTS sender_receiver_map (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sender_id INTEGER NOT NULL,
            receiver_id INTEGER NOT NULL,
            FOREIGN KEY (sender_id) REFERENCES senders(id) ON DELETE CASCADE,
            FOREIGN KEY (receiver_id) REFERENCES receivers(id) ON DELETE CASCADE,
            UNIQUE(sender_id, receiver_id)
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

    # Always seed senders and receivers (INSERT OR IGNORE makes this safe)
    seed_senders()
    seed_receivers()

    # Auto-map only on first run
    conn2 = get_connection()
    seeded = conn2.execute("SELECT value FROM settings WHERE key = 'seeded'").fetchone()
    if not seeded:
        auto_map_senders()
        conn2.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('seeded', 'true')")
        conn2.commit()
    conn2.close()


# ── Seed Senders ─────────────────────────────────────────────

# (name, email, app_password, active)
SEED_SENDERS = [
    # reimaginehome.shop
    ("Bill Cain", "bill@reimaginehome.shop", "qgbk nmwg ffwa tqwk", 1),
    ("Shital Gohil", "shitalgohil@reimaginehome.shop", "kkgm ajcj hxqw xhbd", 1),
    ("Christina West", "christina@reimaginehome.shop", "ulok lnwy krxl kltc", 1),
    ("Christie Brooks", "christie@reimaginehome.shop", "jgrn weyi odqx ahcc", 1),
    ("Ava Morgan", "ava@reimaginehome.shop", "qljc yymu rgqm ggf", 1),
    ("Noah Bennett", "noah@reimaginehome.shop", "tnar qslz fekv yogp", 1),
    ("Akhilesh Majumdar", "akhileshmajumdar@reimaginehome.shop", "aqik xflb xdes oebv", 1),
    ("Akhilesh Majumdar", "akhileshm@reimaginehome.shop", "kyga cfox sjnu apyu", 1),
    ("Shital Gohil", "shitalg@reimaginehome.shop", "uduh belb eufw buco", 1),
    # reimaginehome.app
    ("Tayne Jacobs", "tayne@reimaginehome.app", "wbuq dqal pmqt fnuc", 1),
    ("Sophia Mitchell", "sopia@reimaginehome.app", "vpsv dlrj dmfd ydag", 1),
    ("Alex Morgan", "alex@reimaginehome.app", "sobi mvoa jxmu afwp", 1),
    ("Jason Kim", "jason@reimaginehome.app", "pkqo kvtf sxwy ncsh", 1),
    ("Emily Harper", "emily@reimaginehome.app", "xsof dcnp anos bntq", 1),
    ("Henan Marakkar", "henan@reimaginehome.app", "cljh cgfw fkdk qczm", 1),
    ("Desiree Bennet", "desiree@reimaginehome.app", "bpgc hvws eaxu mtcp", 1),
    ("Tiffany Forsyth", "tiffany@reimaginehome.app", "ykkv dwob zydq xnot", 1),
    ("Chris Dodge", "chris@reimaginehome.app", "rhss mwjm gmhe snkl", 1),
    ("Melissa Morgan", "melissa@reimaginehome.app", "fsek mhia tiiy xcnp", 1),
    # reimaginehome.tech
    ("Jeroen Lublin", "jeroen@reimaginehome.tech", "sgix bkyz fwnw kacb", 1),
    ("Valentina Russo", "valentina@reimaginehome.tech", "igmq woeh kqvu bwcr", 1),
    ("Lucas Meyer", "lucas@reimaginehome.tech", "wfyu bzvp nqgt ifhe", 1),
    ("Amelia Brooks", "amelia@reimaginehome.tech", "xnzt yuli digp dqvd", 1),
    ("Ryan Cole", "ryan@reimaginehome.tech", "ecxt enlw afnz ljt", 1),
    ("Helen Evans", "helen@reimaginehome.tech", "faah uxsm btxv soxt", 1),
    ("Barbara Fredricksen", "barbara@reimaginehome.tech", "aqvm vfkf yveg vzhu", 1),
    ("Jade Briggs", "jade@reimaginehome.tech", "eodu gpnk kngx pkze", 1),
    ("Valencia Pareira", "valencia@reimaginehome.tech", "jyeu cpxz gttq zqrx", 1),
    ("Luke Walker", "luke@reimaginehome.tech", "uhno lcsz upaq jktk", 1),
    # reimaginehome.homes
    ("Diego Edmondson", "diego@reimaginehome.homes", "bypt sdfi uboj rspe", 1),
    ("Olivia Grant", "olivia@reimaginehome.homes", "xbjj qzhy yzgf oqwc", 1),
    ("Lena Fischer", "lena@reimaginehome.homes", "plyp tinn yjne uhxd", 1),
    ("Daniel Wright", "daniel@reimaginehome.homes", "lvqu qkad lyor svzo", 1),
    ("Elena Martinez", "elena@reimaginehome.homes", "yulw ionu xueq ejzx", 1),
    ("Ben Hogan", "ben@reimaginehome.homes", "oghl gnmf swda wbhf", 1),
    ("Jennifer Wong", "jennifer@reimaginehome.homes", "okao irni tdtt jdht", 1),
    ("Shelly Potter", "shelly@reimaginehome.homes", "kiag crjp xiuy qyly", 1),
    ("Kimberley Patterson", "kimberley@reimaginehome.homes", "sqsq orhn oouh otbt", 1),
    ("Stephan Pitts", "stephan@reimaginehome.homes", "dsod fcpf enps qeve", 1),
    # reimaginehome.net
    ("Akhilesh Majumdar", "akhilesh.majumdar@reimaginehome.net", "lkdf crxq acgq unyr", 1),
    ("Akhilesh Majumdar", "m.akhilesh@reimaginehome.net", "dxnv nowf geyn uiny", 1),
    ("Shital Gohil", "gohilshital@reimaginehome.net", "uvss atnu stms hcxe", 1),
    ("Akhilesh Majumdar", "akhilesh@reimaginehome.net", "namn fmmv njoi coph", 1),
    ("Shital Gohil", "shitalgohil@reimaginehome.net", "wqja tnku zpgy lulc", 1),
    ("Akhilesh Majumdar", "majumdarakhilesh@reimaginehome.net", "imzk jgms qurb xfqx", 1),
    ("Akhilesh Majumdar", "akhileshmajumdar@reimaginehome.net", "haqe azpu xzhm njxl", 1),
    ("Shital Gohil", "gohil.shital@reimaginehome.net", "pwbt moii xnut pbbz", 1),
    ("Shital Gohil", "shital.g@reimaginehome.net", "cjhl eurk fxoz pfji", 1),
    ("Shital Gohil", "shital.gohil@reimaginehome.net", "heic dyqo ntky zisk", 1),
    ("Shital Gohil", "shitalg@reimaginehome.net", "vaog dlqw wvmo zanw", 1),
    ("Shital Gohil", "Shital@reimaginehome.net", "opxh uzpi nnlq vovi", 1),
    ("Akhilesh Majumdar", "akhilesh.m@reimaginehome.net", "pcql okak crsa zrre", 1),
]


def seed_senders():
    """Pre-load sender emails — update name/password if sender already exists."""
    conn = get_connection()
    for name, email, app_password, active in SEED_SENDERS:
        conn.execute(
            """INSERT INTO senders (name, email, app_password, active) VALUES (?, ?, ?, ?)
               ON CONFLICT(email) DO UPDATE SET name=excluded.name, app_password=excluded.app_password""",
            (name, email, app_password, active),
        )
        # Force-update name for any existing rows (belt and suspenders)
        conn.execute(
            "UPDATE senders SET name = ? WHERE email = ? AND (name IS NULL OR name = '')",
            (name, email),
        )
    conn.commit()
    conn.close()


# Build a fast email → name lookup from the seed list (guaranteed correct)
SENDER_NAME_LOOKUP = {email.lower(): name for name, email, _, _ in SEED_SENDERS}


def get_sender_display_name(email):
    """Get the display name for a sender email, always from the hardcoded seed list."""
    return SENDER_NAME_LOOKUP.get(email.lower(), email.split("@")[0].replace(".", " ").title())


# ── Seed Receivers ───────────────────────────────────────────

# (name, email)
SEED_RECEIVERS = [
    ("Prithvi", "prithvi.r3957@gmail.com"),
    ("Prithvi", "prithvi.gowda21@gmail.com"),
    ("Prithvi", "prithvi.pr1011@gmail.com"),
    ("Prithvi", "prithvi.gowda1999@gmail.com"),
    ("Rohit", "rohit.a.thorat@gmail.com"),
    ("Rohit", "rothorat7779@gmail.com"),
    ("Slade Winter", "sladewinter@gmail.com"),
    ("Deepak", "deepakpandey7100@gmail.com"),
    ("Arul", "arulpradeep95@gmail.com"),
    ("Arul", "arulpradeep05@gmail.com"),
    ("Manju", "mbc261996@gmail.com"),
    ("Manju", "editzpituresque@gmail.com"),
    ("Theera", "rewritingtheera@gmail.com"),
    ("Arul", "arulpradeepp05@icloud.com"),
    ("Ramya", "ramyakrishna3024@gmail.com"),
    ("Abhishek Rath", "abhishek.rath85@gmail.com"),
    ("Henan", "henanmaliyakkal@gmail.com"),
    ("Amalya", "amalyaaamz@gmail.com"),
    ("Amalya", "Amalyashaji926@gmail.com"),
    ("Amalya", "amalyabackup0@gmail.com"),
    ("Wave", "wave.crest444@gmail.com"),
    ("Akash", "akash.shitole.5595@gmail.com"),
    ("Akash", "akash.shitole.5902@gmail.com"),
    ("Akash", "akash.shitole.0509@gmail.com"),
    ("Geethu", "geetu.chaurasiya@styldod.com"),
    ("Prithvi", "prithvi@styldod.com"),
    ("Samardip", "samardip.mandal@styldod.com"),
    ("Akash", "akash.shitole@styldod.com"),
    ("Rohit", "rohit.panda@styldod.com"),
    ("Manas", "manas.samal@styldod.com"),
    ("Siddhanta", "siddhanta.gupta@styldod.com"),
    ("Vishal", "vishal.yadav@styldod.com"),
    ("Deepak", "deepak.pandey@styldod.com"),
    ("Swetha", "shweta.shaw@styldod.com"),
    ("Rohit", "rohit.thorat@styldod.com"),
    ("Zeeshan", "zeeshan.noor@styldod.com"),
    ("Yuvraj", "yuvraj.garg@styldod.com"),
    ("Manju", "manjunath.bc@styldod.com"),
    ("Arul", "arul.p@styldod.com"),
    ("Abhishek Rath", "abhishek.rath@styldod.com"),
    ("Henan", "hannan@styldod.com"),
    ("Kiran", "kiran@styldod.com"),
    ("Komal", "komal@styldod.com"),
    ("Ruturaj", "ruturaj@styldod.com"),
]


def seed_receivers():
    """Pre-load receiver emails if they don't already exist."""
    conn = get_connection()
    for name, email in SEED_RECEIVERS:
        conn.execute(
            "INSERT OR IGNORE INTO receivers (name, email) VALUES (?, ?)",
            (name, email),
        )
    conn.commit()
    conn.close()


def auto_map_senders():
    """Auto-assign 5 unique receivers to each sender that has no mappings."""
    conn = get_connection()
    c = conn.cursor()

    senders = c.execute("SELECT id FROM senders ORDER BY id").fetchall()
    receivers = c.execute("SELECT id FROM receivers ORDER BY id").fetchall()

    if not senders or not receivers:
        conn.close()
        return

    receiver_ids = [r["id"] for r in receivers]
    num_receivers = len(receiver_ids)
    receivers_per_sender = 5

    offset = 0
    mapped_any = False

    for sender in senders:
        sid = sender["id"]
        existing = c.execute(
            "SELECT COUNT(*) FROM sender_receiver_map WHERE sender_id = ?", (sid,)
        ).fetchone()[0]
        if existing > 0:
            offset += receivers_per_sender
            continue

        assigned = []
        for i in range(receivers_per_sender):
            idx = (offset + i) % num_receivers
            assigned.append(receiver_ids[idx])
        offset += receivers_per_sender

        for rid in assigned:
            c.execute(
                "INSERT OR IGNORE INTO sender_receiver_map (sender_id, receiver_id) VALUES (?, ?)",
                (sid, rid),
            )
        mapped_any = True

    if mapped_any:
        conn.commit()
    conn.close()


def randomize_all_mappings(receivers_per_sender=5):
    """Clear all mappings and randomly assign receivers to each sender."""
    import random

    conn = get_connection()
    c = conn.cursor()

    senders = c.execute("SELECT id FROM senders ORDER BY id").fetchall()
    receivers = c.execute("SELECT id FROM receivers ORDER BY id").fetchall()

    if not senders or not receivers:
        conn.close()
        return

    receiver_ids = [r["id"] for r in receivers]
    c.execute("DELETE FROM sender_receiver_map")

    for sender in senders:
        sid = sender["id"]
        shuffled = receiver_ids.copy()
        random.shuffle(shuffled)
        assigned = shuffled[:receivers_per_sender]

        for rid in assigned:
            c.execute(
                "INSERT OR IGNORE INTO sender_receiver_map (sender_id, receiver_id) VALUES (?, ?)",
                (sid, rid),
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
    conn.execute("DELETE FROM sender_receiver_map WHERE sender_id = ?", (sender_id,))
    conn.execute("DELETE FROM senders WHERE id = ?", (sender_id,))
    conn.commit()
    conn.close()


def toggle_sender(sender_id, active):
    conn = get_connection()
    conn.execute("UPDATE senders SET active = ? WHERE id = ?", (int(active), sender_id))
    conn.commit()
    conn.close()


def activate_all_senders():
    conn = get_connection()
    conn.execute("UPDATE senders SET active = 1")
    conn.commit()
    conn.close()


def deactivate_all_senders():
    conn = get_connection()
    conn.execute("UPDATE senders SET active = 0")
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
    conn.execute("DELETE FROM sender_receiver_map WHERE receiver_id = ?", (receiver_id,))
    conn.execute("DELETE FROM receivers WHERE id = ?", (receiver_id,))
    conn.commit()
    conn.close()


def toggle_receiver(receiver_id, active):
    conn = get_connection()
    conn.execute("UPDATE receivers SET active = ? WHERE id = ?", (int(active), receiver_id))
    conn.commit()
    conn.close()


def activate_all_receivers():
    conn = get_connection()
    conn.execute("UPDATE receivers SET active = 1")
    conn.commit()
    conn.close()


def deactivate_all_receivers():
    conn = get_connection()
    conn.execute("UPDATE receivers SET active = 0")
    conn.commit()
    conn.close()


def get_active_receivers():
    conn = get_connection()
    rows = conn.execute("SELECT * FROM receivers WHERE active = 1 ORDER BY id").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_recent_receiver_emails(sender_email, days=3):
    """Get receiver emails that this sender has emailed in the last N days."""
    conn = get_connection()
    rows = conn.execute(
        """SELECT DISTINCT receiver_email FROM logs
           WHERE sender_email = ? AND status = 'Sent'
           AND timestamp >= datetime('now', ?)""",
        (sender_email, f"-{days} days"),
    ).fetchall()
    conn.close()
    return {row["receiver_email"] for row in rows}


def pick_receivers_for_sender(sender_email, all_active_receivers, count=5, cooldown_days=3):
    """
    Pick receivers for a sender, prioritizing those NOT recently emailed.
    If not enough fresh receivers, allow reuse of recent ones.
    """
    import random
    recent = get_recent_receiver_emails(sender_email, days=cooldown_days)

    fresh = [r for r in all_active_receivers if r["email"] not in recent]
    stale = [r for r in all_active_receivers if r["email"] in recent]

    random.shuffle(fresh)
    random.shuffle(stale)

    picked = fresh[:count]
    if len(picked) < count:
        picked += stale[:count - len(picked)]

    return picked

def get_all_mapped_receiver_ids_bulk():
    """Fetch ALL sender→receiver mappings in one query. Returns {sender_id: [receiver_ids]}."""
    conn = get_connection()
    rows = conn.execute("SELECT sender_id, receiver_id FROM sender_receiver_map ORDER BY sender_id, receiver_id").fetchall()
    conn.close()
    result = {}
    for r in rows:
        sid = r["sender_id"]
        if sid not in result:
            result[sid] = []
        result[sid].append(r["receiver_id"])
    return result


def get_mapped_receivers(sender_id):
    """Get receivers mapped to a specific sender."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT r.* FROM receivers r
        JOIN sender_receiver_map m ON r.id = m.receiver_id
        WHERE m.sender_id = ?
        ORDER BY r.id
    """, (sender_id,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_mapped_receiver_ids(sender_id):
    """Get receiver IDs mapped to a specific sender."""
    conn = get_connection()
    rows = conn.execute(
        "SELECT receiver_id FROM sender_receiver_map WHERE sender_id = ?",
        (sender_id,),
    ).fetchall()
    conn.close()
    return [r["receiver_id"] for r in rows]


def set_sender_mappings(sender_id, receiver_ids):
    """Replace all mappings for a sender with the given receiver IDs."""
    conn = get_connection()
    conn.execute("DELETE FROM sender_receiver_map WHERE sender_id = ?", (sender_id,))
    for rid in receiver_ids:
        conn.execute(
            "INSERT OR IGNORE INTO sender_receiver_map (sender_id, receiver_id) VALUES (?, ?)",
            (sender_id, rid),
        )
    conn.commit()
    conn.close()


def get_all_mappings():
    """Get all sender-receiver mappings grouped by sender."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT m.sender_id, s.email as sender_email, m.receiver_id, r.email as receiver_email, r.name as receiver_name
        FROM sender_receiver_map m
        JOIN senders s ON s.id = m.sender_id
        JOIN receivers r ON r.id = m.receiver_id
        ORDER BY m.sender_id, m.receiver_id
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ── Settings ─────────────────────────────────────────────────

def get_settings():
    conn = get_connection()
    rows = conn.execute("SELECT * FROM settings").fetchall()
    conn.close()
    settings = {}
    for r in rows:
        key = r["key"]
        val = r["value"]
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
         datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")),
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
