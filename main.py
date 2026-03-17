"""
AI Email Warmup System — Streamlit Application
Multi-sender dashboard for warming up email accounts with AI-generated content.
"""

import streamlit as st
import time
import threading
from datetime import datetime, date

import database as db
from config import TONE_OPTIONS, STATUS_SENT, STATUS_FAILED, DEFAULT_SETTINGS
from ai_generator import generate_email
from send_email import send_email
from utils import mask_password, validate_email, get_delay_seconds, status_color, get_avatar_color

# ── Page Config ──────────────────────────────────────────────

st.set_page_config(
    page_title="Email Warmup System",
    page_icon="🔥",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Initialize Database ─────────────────────────────────────

db.init_db()

# ── Session State Defaults ───────────────────────────────────

if "warmup_running" not in st.session_state:
    st.session_state.warmup_running = False
if "warmup_log" not in st.session_state:
    st.session_state.warmup_log = []

# ── Card HTML Helper ─────────────────────────────────────────

def render_profile_card(letter, label, sublabel, color, status_text=None, extra_html=""):
    """Render a dark profile card with circular avatar."""
    status_badge = ""
    if status_text:
        badge_color = "#10b981" if status_text == "Active" else "#6b7280"
        status_badge = f'<div style="position:absolute;top:10px;left:10px;background:{badge_color};color:#fff;padding:2px 10px;border-radius:12px;font-size:0.7rem;font-weight:600;">● {status_text}</div>'

    html = f"""<div style="background:linear-gradient(145deg,#1a1a2e 0%,#16213e 100%);border:1px solid rgba(255,255,255,0.08);border-radius:16px;padding:24px 16px 20px;text-align:center;position:relative;min-height:200px;box-shadow:0 4px 20px rgba(0,0,0,0.3);">
{status_badge}
<div style="width:72px;height:72px;background:{color};border-radius:50%;display:flex;align-items:center;justify-content:center;margin:8px auto 14px;font-size:1.8rem;font-weight:700;color:white;box-shadow:0 4px 15px {color}66;">{letter}</div>
<div style="color:#fff;font-weight:600;font-size:0.95rem;margin-bottom:2px;word-break:break-all;">{label}</div>
<div style="color:#8b8ba7;font-size:0.78rem;word-break:break-all;">{sublabel}</div>
{extra_html}
</div>"""
    return html


# ── Custom CSS ───────────────────────────────────────────────

st.markdown("""
<style>
    /* Global */
    .block-container { padding-top: 1.5rem; }

    /* Metric cards */
    div[data-testid="stMetric"] {
        background: linear-gradient(135deg, #1e1e2e 0%, #2d2d44 100%);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 12px;
        padding: 1rem 1.2rem;
        box-shadow: 0 4px 20px rgba(0,0,0,0.3);
    }
    div[data-testid="stMetric"] label {
        color: #a0a0c0 !important;
        font-size: 0.85rem !important;
        font-weight: 500 !important;
    }
    div[data-testid="stMetric"] [data-testid="stMetricValue"] {
        color: #ffffff !important;
        font-size: 1.8rem !important;
        font-weight: 700 !important;
    }

    /* Sidebar */
    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0f0f1a 0%, #1a1a2e 100%);
        border-right: 1px solid rgba(255,255,255,0.06);
    }
    section[data-testid="stSidebar"] .stRadio label {
        font-size: 1rem !important;
    }

    /* Tables */
    .stDataFrame { border-radius: 8px; overflow: hidden; }

    /* Buttons */
    .stButton > button {
        border-radius: 8px;
        font-weight: 600;
        transition: all 0.2s ease;
    }
    .stButton > button:hover {
        transform: translateY(-1px);
        box-shadow: 0 4px 15px rgba(0,0,0,0.3);
    }

    /* Headers */
    h1 { letter-spacing: -0.5px; }
    h2, h3 { color: #e0e0ff !important; }

    /* Dividers */
    hr { border-color: rgba(255,255,255,0.08) !important; }

    /* Mapped count badge */
    .mapped-badge {
        display: inline-block;
        background: #4F46E5;
        color: white;
        padding: 2px 8px;
        border-radius: 10px;
        font-size: 0.72rem;
        font-weight: 600;
        margin-top: 8px;
    }
</style>
""", unsafe_allow_html=True)

# ── Sidebar Navigation ──────────────────────────────────────

with st.sidebar:
    st.markdown("## 🔥 Email Warmup")
    st.markdown("---")
    page = st.radio(
        "Navigation",
        ["📊 Dashboard", "📤 Senders", "📥 Receivers", "🔗 Mapping", "⚙️ Settings", "🚀 Run Controls", "📋 Logs"],
        label_visibility="collapsed",
    )
    st.markdown("---")
    if st.session_state.warmup_running:
        st.markdown("🟢 **Warmup Running**")
    else:
        st.markdown("⚪ **Warmup Idle**")


# ══════════════════════════════════════════════════════════════
# 📊 DASHBOARD
# ══════════════════════════════════════════════════════════════

if page == "📊 Dashboard":
    st.markdown("# 📊 Dashboard")
    st.markdown("Real-time overview of your email warmup engine.")
    st.markdown("---")

    senders = db.get_all_senders()
    receivers = db.get_all_receivers()
    sent_today = db.get_today_sent_count()
    failed_today = db.get_today_failed_count()
    last_run = db.get_last_run_time()

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Senders", len(senders))
    c2.metric("Total Receivers", len(receivers))
    c3.metric("Sent Today", sent_today)
    c4.metric("Failed Today", failed_today)
    c5.metric("Last Run", last_run if last_run != "Never" else "—")

    st.markdown("---")

    # Recent activity
    st.markdown("### Recent Activity")
    recent_logs = db.get_logs(limit=10)
    if recent_logs:
        for log in recent_logs:
            icon = status_color(log["status"])
            st.markdown(
                f"{icon} **{log['sender_email']}** → {log['receiver_email']}  "
                f"| {log['status']} | {log['timestamp']}"
            )
    else:
        st.info("No emails sent yet. Go to **Run Controls** to start your first warmup.")


# ══════════════════════════════════════════════════════════════
# 📤 SENDERS — Card Grid UI
# ══════════════════════════════════════════════════════════════

elif page == "📤 Senders":
    st.markdown("# 📤 Sender Management")
    st.markdown("Manage your Gmail sender accounts for warmup.")
    st.markdown("---")

    # Add Sender
    with st.expander("➕ Add New Sender", expanded=False):
        with st.form("add_sender_form", clear_on_submit=True):
            col1, col2 = st.columns(2)
            new_email = col1.text_input("Sender Email", placeholder="sender@gmail.com")
            new_password = col2.text_input("App Password", type="password", placeholder="xxxx xxxx xxxx xxxx")
            submitted = st.form_submit_button("Add Sender", use_container_width=True)
            if submitted:
                if not new_email or not new_password:
                    st.error("Both email and app password are required.")
                elif not validate_email(new_email):
                    st.error("Invalid email format.")
                else:
                    ok, msg = db.add_sender(new_email.strip(), new_password.strip())
                    if ok:
                        st.success(msg)
                        st.rerun()
                    else:
                        st.error(msg)

    # Sender Card Grid
    senders = db.get_all_senders()
    if senders:
        CARDS_PER_ROW = 4
        for i in range(0, len(senders), CARDS_PER_ROW):
            cols = st.columns(CARDS_PER_ROW)
            for j, col in enumerate(cols):
                idx = i + j
                if idx >= len(senders):
                    break
                sender = senders[idx]
                email = sender["email"]
                letter = email[0].upper()
                color = get_avatar_color(email)
                status_text = "Active" if sender["active"] else "Inactive"
                name_part = email.split("@")[0].replace(".", " ").title()

                with col:
                    st.markdown(
                        render_profile_card(letter, name_part, email, color, status_text),
                        unsafe_allow_html=True,
                    )

                    # Action buttons row
                    b1, b2, b3 = st.columns(3)
                    if sender["active"]:
                        if b1.button("Deactivate", key=f"deact_{sender['id']}", type="secondary"):
                            db.toggle_sender(sender["id"], False)
                            st.rerun()
                    else:
                        if b1.button("Activate", key=f"act_{sender['id']}", type="primary"):
                            db.toggle_sender(sender["id"], True)
                            st.rerun()

                    if b2.button("✏️", key=f"edit_s_{sender['id']}"):
                        st.session_state[f"editing_sender_{sender['id']}"] = True

                    if b3.button("🗑️", key=f"del_s_{sender['id']}"):
                        db.delete_sender(sender["id"])
                        st.rerun()

                    # Edit form (expands below the card)
                    if st.session_state.get(f"editing_sender_{sender['id']}"):
                        with st.form(f"edit_sender_form_{sender['id']}"):
                            edit_email = st.text_input("Email", value=sender["email"], key=f"ee_{sender['id']}")
                            edit_pass = st.text_input("App Password", type="password", value=sender["app_password"], key=f"ep_{sender['id']}")
                            edit_active = st.checkbox("Active", value=bool(sender["active"]), key=f"ea_{sender['id']}")
                            sc1, sc2 = st.columns(2)
                            if sc1.form_submit_button("Save"):
                                ok, msg = db.update_sender(sender["id"], edit_email.strip(), edit_pass.strip(), edit_active)
                                if ok:
                                    st.session_state[f"editing_sender_{sender['id']}"] = False
                                    st.success(msg)
                                    st.rerun()
                                else:
                                    st.error(msg)
                            if sc2.form_submit_button("Cancel"):
                                st.session_state[f"editing_sender_{sender['id']}"] = False
                                st.rerun()
    else:
        st.info("No senders added yet. Click **Add New Sender** above.")


# ══════════════════════════════════════════════════════════════
# 📥 RECEIVERS — Card Grid UI
# ══════════════════════════════════════════════════════════════

elif page == "📥 Receivers":
    st.markdown("# 📥 Receiver Management")
    st.markdown("Manage the list of receiver email addresses.")
    st.markdown("---")

    # Add Receiver
    with st.expander("➕ Add New Receiver", expanded=False):
        with st.form("add_receiver_form", clear_on_submit=True):
            col1, col2 = st.columns(2)
            new_name = col1.text_input("Receiver Name", placeholder="John")
            new_email = col2.text_input("Receiver Email", placeholder="john@example.com")
            submitted = st.form_submit_button("Add Receiver", use_container_width=True)
            if submitted:
                if not new_name or not new_email:
                    st.error("Both name and email are required.")
                elif not validate_email(new_email):
                    st.error("Invalid email format.")
                else:
                    ok, msg = db.add_receiver(new_name.strip(), new_email.strip())
                    if ok:
                        st.success(msg)
                        st.rerun()
                    else:
                        st.error(msg)

    # Receiver Card Grid
    receivers = db.get_all_receivers()
    if receivers:
        CARDS_PER_ROW = 4
        for i in range(0, len(receivers), CARDS_PER_ROW):
            cols = st.columns(CARDS_PER_ROW)
            for j, col in enumerate(cols):
                idx = i + j
                if idx >= len(receivers):
                    break
                recv = receivers[idx]
                letter = recv["name"][0].upper() if recv["name"] else "?"
                color = get_avatar_color(recv["email"])

                with col:
                    st.markdown(
                        render_profile_card(letter, recv["name"], recv["email"], color),
                        unsafe_allow_html=True,
                    )

                    # Action buttons
                    b1, b2 = st.columns(2)
                    if b1.button("✏️ Edit", key=f"edit_r_{recv['id']}"):
                        st.session_state[f"editing_receiver_{recv['id']}"] = True
                    if b2.button("🗑️ Del", key=f"del_r_{recv['id']}"):
                        db.delete_receiver(recv["id"])
                        st.rerun()

                    # Edit form
                    if st.session_state.get(f"editing_receiver_{recv['id']}"):
                        with st.form(f"edit_receiver_form_{recv['id']}"):
                            edit_name = st.text_input("Name", value=recv["name"], key=f"en_{recv['id']}")
                            edit_email = st.text_input("Email", value=recv["email"], key=f"ere_{recv['id']}")
                            sc1, sc2 = st.columns(2)
                            if sc1.form_submit_button("Save"):
                                ok, msg = db.update_receiver(recv["id"], edit_name.strip(), edit_email.strip())
                                if ok:
                                    st.session_state[f"editing_receiver_{recv['id']}"] = False
                                    st.success(msg)
                                    st.rerun()
                                else:
                                    st.error(msg)
                            if sc2.form_submit_button("Cancel"):
                                st.session_state[f"editing_receiver_{recv['id']}"] = False
                                st.rerun()
    else:
        st.info("No receivers added yet. Click **Add New Receiver** above.")


# ══════════════════════════════════════════════════════════════
# 🔗 MAPPING — Card Grid with Receiver Expander
# ══════════════════════════════════════════════════════════════

elif page == "🔗 Mapping":
    st.markdown("# 🔗 Sender → Receiver Mapping")
    st.markdown("Assign up to 5 receivers to each sender. Click a sender card to manage its receivers.")
    st.markdown("---")

    senders = db.get_all_senders()
    receivers = db.get_all_receivers()

    if not senders:
        st.warning("No senders available. Add senders first.")
    elif not receivers:
        st.warning("No receivers available. Add receivers first.")
    else:
        receiver_options = {r["id"]: f"{r['name']} ({r['email']})" for r in receivers}
        receiver_labels_list = list(receiver_options.values())

        CARDS_PER_ROW = 4
        for i in range(0, len(senders), CARDS_PER_ROW):
            cols = st.columns(CARDS_PER_ROW)
            for j, col in enumerate(cols):
                idx = i + j
                if idx >= len(senders):
                    break
                sender = senders[idx]
                email = sender["email"]
                letter = email[0].upper()
                color = get_avatar_color(email)
                name_part = email.split("@")[0].replace(".", " ").title()
                mapped_count = len(db.get_mapped_receiver_ids(sender["id"]))

                badge_html = ""
                if mapped_count > 0:
                    badge_html = f'<div class="mapped-badge">{mapped_count} receiver(s)</div>'
                else:
                    badge_html = '<div style="color:#f59e0b;font-size:0.75rem;margin-top:8px;">⚠️ Not mapped</div>'

                with col:
                    st.markdown(
                        render_profile_card(letter, name_part, email, color, extra_html=badge_html),
                        unsafe_allow_html=True,
                    )

            # Expander row for managing mappings (after each row of cards)
            for j in range(min(CARDS_PER_ROW, len(senders) - i)):
                sender = senders[i + j]
                current_mapped = db.get_mapped_receiver_ids(sender["id"])

                with st.expander(f"▶ Manage receivers for **{sender['email']}**", expanded=False):
                    default_labels = []
                    for rid in current_mapped:
                        if rid in receiver_options:
                            default_labels.append(receiver_options[rid])

                    selected_labels = st.multiselect(
                        "Select receivers (max 5)",
                        options=receiver_labels_list,
                        default=default_labels,
                        key=f"map_{sender['id']}",
                        max_selections=5,
                    )

                    label_to_id = {v: k for k, v in receiver_options.items()}
                    selected_ids = [label_to_id[lbl] for lbl in selected_labels if lbl in label_to_id]

                    if st.button("💾 Save Mapping", key=f"save_map_{sender['id']}", use_container_width=True):
                        db.set_sender_mappings(sender["id"], selected_ids)
                        st.success(f"Mapping saved — {len(selected_ids)} receiver(s)")
                        st.rerun()

            st.markdown("---")


# ══════════════════════════════════════════════════════════════
# ⚙️ SETTINGS
# ══════════════════════════════════════════════════════════════

elif page == "⚙️ Settings":
    st.markdown("# ⚙️ Settings")
    st.markdown("Configure your warmup engine parameters.")
    st.markdown("---")

    settings = db.get_settings()

    with st.form("settings_form"):
        # API Key — prefer Streamlit secrets, allow manual override
        default_api_key = ""
        try:
            default_api_key = st.secrets.get("GEMINI_API_KEY", "")
        except Exception:
            pass
        current_key = settings.get("gemini_api_key", "") or default_api_key

        gemini_key = st.text_input(
            "Gemini API Key",
            value=current_key,
            type="password",
            help="Stored in Streamlit Secrets on cloud. Enter here for local dev.",
        )

        st.markdown("---")
        st.markdown("### Sending Parameters")

        col1, col2 = st.columns(2)
        batch_size = col1.number_input(
            "Emails per batch",
            min_value=1, max_value=50,
            value=settings.get("batch_size", 5),
            help="Number of receivers each sender emails per run.",
        )
        daily_limit = col2.number_input(
            "Daily send limit per sender",
            min_value=1, max_value=200,
            value=settings.get("daily_limit", 20),
            help="Max emails a single sender can send per day.",
        )

        col3, col4 = st.columns(2)
        delay_minutes = col3.number_input(
            "Delay between emails (minutes)",
            min_value=0.0, max_value=30.0, step=0.5,
            value=float(settings.get("delay_minutes", 2)),
        )
        random_delay = col4.checkbox(
            "Enable random delay variation",
            value=settings.get("random_delay", True),
            help="Adds ±50% randomness to the delay to simulate human behavior.",
        )

        tone = st.selectbox(
            "Email tone",
            options=TONE_OPTIONS,
            index=TONE_OPTIONS.index(settings.get("tone", "Casual")),
        )

        st.markdown("---")
        st.markdown("### Scheduler")

        col5, col6 = st.columns(2)
        rounds_per_day = col5.number_input(
            "Rounds per day",
            min_value=1, max_value=20,
            value=settings.get("rounds_per_day", 5),
            help="Number of sending rounds per day. Each round sends to all mapped receivers.",
        )
        gap_minutes = col6.number_input(
            "Gap between rounds (minutes)",
            min_value=10, max_value=180,
            value=settings.get("gap_minutes", 60),
            help="Wait time between each round.",
        )

        st.markdown("---")
        save_btn = st.form_submit_button("💾 Save Settings", use_container_width=True, type="primary")

        if save_btn:
            db.save_settings({
                "gemini_api_key": gemini_key,
                "batch_size": int(batch_size),
                "delay_minutes": delay_minutes,
                "daily_limit": int(daily_limit),
                "random_delay": random_delay,
                "tone": tone,
                "rounds_per_day": int(rounds_per_day),
                "gap_minutes": int(gap_minutes),
            })
            st.success("Settings saved successfully!")
            st.rerun()


# ══════════════════════════════════════════════════════════════
# 🚀 RUN CONTROLS
# ══════════════════════════════════════════════════════════════

elif page == "🚀 Run Controls":
    st.markdown("# 🚀 Run Controls")
    st.markdown("Auto-scheduler: sends emails in rounds with gaps between each round.")
    st.markdown("---")

    settings = db.get_settings()
    senders = db.get_active_senders()
    receivers = db.get_all_receivers()

    # Preflight checks
    api_key = settings.get("gemini_api_key", "")
    if not api_key:
        try:
            api_key = st.secrets.get("GEMINI_API_KEY", "")
        except Exception:
            pass

    # Check if any sender has mapped receivers
    any_mapped = False
    for s in senders:
        if db.get_mapped_receivers(s["id"]):
            any_mapped = True
            break

    ready = True
    if not api_key:
        st.warning("⚠️ Gemini API key not configured. Go to **Settings**.")
        ready = False
    if not senders:
        st.warning("⚠️ No active senders. Go to **Senders** to add and activate accounts.")
        ready = False
    if not any_mapped:
        st.warning("⚠️ No sender-receiver mappings configured. Go to **🔗 Mapping** to assign receivers.")
        ready = False

    # Status display
    st.markdown("### Current Status")
    rounds_per_day = settings.get("rounds_per_day", 5)
    gap_minutes = settings.get("gap_minutes", 60)

    status_cols = st.columns(5)
    status_cols[0].metric("Active Senders", len(senders))
    status_cols[1].metric("Receivers", len(receivers))
    status_cols[2].metric("Rounds/Day", rounds_per_day)
    status_cols[3].metric("Gap", f"{gap_minutes} min")
    status_cols[4].metric("Daily Limit", settings.get("daily_limit", 20))

    st.markdown("---")

    # Control Buttons
    btn_cols = st.columns(3)

    with btn_cols[0]:
        start_clicked = st.button(
            "▶️ Start Auto Warmup",
            disabled=not ready or st.session_state.warmup_running,
            use_container_width=True,
            type="primary",
        )

    with btn_cols[1]:
        stop_clicked = st.button(
            "⏹️ Stop",
            disabled=not st.session_state.warmup_running,
            use_container_width=True,
            type="secondary",
        )

    with btn_cols[2]:
        reset_clicked = st.button(
            "🔁 Reset Logs",
            use_container_width=True,
            type="secondary",
        )

    if reset_clicked:
        db.clear_logs()
        st.session_state.warmup_log = []
        st.success("All logs cleared.")
        st.rerun()

    if stop_clicked:
        st.session_state.warmup_running = False
        st.info("Warmup stopped.")
        st.rerun()

    # ── Auto-Scheduler Execution ─────────────────────────────

    if start_clicked and ready:
        st.session_state.warmup_running = True
        st.session_state.warmup_log = []

        delay_min = settings.get("delay_minutes", 2)
        random_delay_flag = settings.get("random_delay", True)
        daily_limit = settings.get("daily_limit", 20)
        tone = settings.get("tone", "Casual")

        round_header = st.empty()
        progress_bar = st.progress(0)
        status_text = st.empty()
        countdown_area = st.empty()
        log_area = st.container()

        for current_round in range(1, rounds_per_day + 1):
            if not st.session_state.warmup_running:
                break

            round_header.markdown(f"### 🔄 Round {current_round} of {rounds_per_day}")

            # Re-fetch active senders each round (in case user changed them)
            active_senders = db.get_active_senders()

            # Calculate total ops for this round
            total_ops = 0
            for s in active_senders:
                mapped = db.get_mapped_receivers(s["id"])
                total_ops += len(mapped)
            total_ops = max(total_ops, 1)
            completed = 0

            for sender in active_senders:
                if not st.session_state.warmup_running:
                    break

                # Get mapped receivers for this sender
                mapped_receivers = db.get_mapped_receivers(sender["id"])
                if not mapped_receivers:
                    with log_area:
                        st.info(f"⏭️ {sender['email']} — no receivers mapped. Skipping.")
                    continue

                # Check daily limit
                today_count = db.get_today_sent_count(sender["email"])
                if today_count >= daily_limit:
                    with log_area:
                        st.warning(f"⏭️ {sender['email']} — daily limit reached ({today_count}/{daily_limit}). Skipping.")
                    completed += len(mapped_receivers)
                    progress_bar.progress(min(completed / total_ops, 1.0))
                    continue

                for recv in mapped_receivers:
                    if not st.session_state.warmup_running:
                        break

                    # Re-check daily limit per email
                    today_count = db.get_today_sent_count(sender["email"])
                    if today_count >= daily_limit:
                        with log_area:
                            st.warning(f"⏭️ {sender['email']} — daily limit reached mid-round.")
                        break

                    status_text.markdown(
                        f"**Round {current_round}** | Sending: {sender['email']} → {recv['email']}..."
                    )

                    # Generate AI email
                    result = generate_email(recv["name"], tone, api_key)

                    if "error" in result:
                        db.add_log(sender["email"], recv["email"], recv["name"], "", STATUS_FAILED, result["error"])
                        with log_area:
                            st.error(f"🔴 AI error for {recv['email']}: {result['error']}")
                        completed += 1
                        progress_bar.progress(min(completed / total_ops, 1.0))
                        continue

                    # Send email
                    send_result = send_email(
                        sender["email"], sender["app_password"],
                        recv["email"], result["subject"], result["body"],
                    )

                    db.add_log(
                        sender["email"], recv["email"], recv["name"],
                        result["subject"], send_result["status"],
                        send_result.get("error"),
                    )

                    with log_area:
                        if send_result["status"] == STATUS_SENT:
                            st.success(f"🟢 R{current_round} | {sender['email']} → {recv['email']} | \"{result['subject']}\"")
                        else:
                            st.error(f"🔴 R{current_round} | {sender['email']} → {recv['email']} | {send_result.get('error', 'Unknown')}")

                    completed += 1
                    progress_bar.progress(min(completed / total_ops, 1.0))

                    # Random delay between individual emails
                    if st.session_state.warmup_running:
                        delay_sec = get_delay_seconds(delay_min, random_delay_flag)
                        if delay_sec > 0:
                            status_text.markdown(f"⏳ Waiting **{delay_sec:.0f}s** before next email...")
                            time.sleep(delay_sec)

            # Round complete
            progress_bar.progress(1.0)

            # Gap between rounds (countdown timer)
            if current_round < rounds_per_day and st.session_state.warmup_running:
                gap_seconds = gap_minutes * 60
                # Add ±10% randomness to gap
                import random as rnd
                gap_seconds = int(gap_seconds * rnd.uniform(0.9, 1.1))

                with log_area:
                    st.info(f"✅ Round {current_round} complete! Next round in ~{gap_seconds // 60} minutes.")

                # Countdown timer
                for remaining in range(gap_seconds, 0, -1):
                    if not st.session_state.warmup_running:
                        break
                    mins, secs = divmod(remaining, 60)
                    countdown_area.markdown(
                        f"⏱️ **Next round in {mins:02d}:{secs:02d}** "
                        f"(Round {current_round + 1}/{rounds_per_day})"
                    )
                    time.sleep(1)

                countdown_area.empty()

        # All rounds done
        st.session_state.warmup_running = False
        progress_bar.progress(1.0)
        round_header.empty()
        status_text.markdown(f"✅ **All {rounds_per_day} rounds complete for today!**")
        st.balloons()


# ══════════════════════════════════════════════════════════════
# 📋 LOGS
# ══════════════════════════════════════════════════════════════

elif page == "📋 Logs":
    st.markdown("# 📋 Email Logs")
    st.markdown("View and filter all email sending activity.")
    st.markdown("---")

    # Filters
    filter_cols = st.columns(3)

    senders = db.get_all_senders()
    sender_options = ["All"] + [s["email"] for s in senders]
    sender_filter = filter_cols[0].selectbox("Filter by Sender", sender_options)

    status_filter = filter_cols[1].selectbox("Filter by Status", ["All", "Sent", "Failed"])

    date_filter = filter_cols[2].date_input("Filter by Date", value=None)

    # Fetch logs
    logs = db.get_logs(
        sender_filter=sender_filter,
        date_filter=date_filter if date_filter else None,
        status_filter=status_filter,
    )

    st.markdown(f"**Showing {len(logs)} log entries**")
    st.markdown("---")

    if logs:
        # Build display table
        import pandas as pd

        df = pd.DataFrame(logs)
        display_cols = ["sender_email", "receiver_email", "subject", "status", "timestamp", "error"]
        available_cols = [c for c in display_cols if c in df.columns]
        df = df[available_cols]

        # Rename columns for display
        df.columns = [c.replace("_", " ").title() for c in df.columns]

        # Style the dataframe
        def highlight_status(val):
            if val == "Sent":
                return "background-color: #0d7a3e; color: white; border-radius: 4px; padding: 2px 8px;"
            elif val == "Failed":
                return "background-color: #c0392b; color: white; border-radius: 4px; padding: 2px 8px;"
            return ""

        styled = df.style.applymap(highlight_status, subset=["Status"] if "Status" in df.columns else [])
        st.dataframe(styled, use_container_width=True, hide_index=True)
    else:
        st.info("No logs found matching your filters.")
