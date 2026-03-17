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
from utils import mask_password, validate_email, get_delay_seconds, status_color

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

    /* Status badges */
    .status-sent {
        background: #0d7a3e; color: #fff;
        padding: 2px 10px; border-radius: 20px;
        font-size: 0.82rem; font-weight: 600;
    }
    .status-failed {
        background: #c0392b; color: #fff;
        padding: 2px 10px; border-radius: 20px;
        font-size: 0.82rem; font-weight: 600;
    }

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
</style>
""", unsafe_allow_html=True)

# ── Sidebar Navigation ──────────────────────────────────────

with st.sidebar:
    st.markdown("## 🔥 Email Warmup")
    st.markdown("---")
    page = st.radio(
        "Navigation",
        ["📊 Dashboard", "📤 Senders", "📥 Receivers", "⚙️ Settings", "🚀 Run Controls", "📋 Logs"],
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
# 📤 SENDERS
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

    # Sender Table
    senders = db.get_all_senders()
    if senders:
        for sender in senders:
            with st.container():
                cols = st.columns([3, 3, 1.5, 1, 1, 1])
                cols[0].markdown(f"**{sender['email']}**")
                cols[1].code(mask_password(sender["app_password"]), language=None)
                status = "🟢 Active" if sender["active"] else "🔴 Inactive"
                cols[2].markdown(status)

                # Toggle active
                if sender["active"]:
                    if cols[3].button("Deactivate", key=f"deact_{sender['id']}", type="secondary"):
                        db.toggle_sender(sender["id"], False)
                        st.rerun()
                else:
                    if cols[3].button("Activate", key=f"act_{sender['id']}", type="primary"):
                        db.toggle_sender(sender["id"], True)
                        st.rerun()

                # Edit
                if cols[4].button("✏️", key=f"edit_s_{sender['id']}"):
                    st.session_state[f"editing_sender_{sender['id']}"] = True

                # Delete
                if cols[5].button("🗑️", key=f"del_s_{sender['id']}"):
                    db.delete_sender(sender["id"])
                    st.rerun()

                # Edit form
                if st.session_state.get(f"editing_sender_{sender['id']}"):
                    with st.form(f"edit_sender_form_{sender['id']}"):
                        ec1, ec2 = st.columns(2)
                        edit_email = ec1.text_input("Email", value=sender["email"])
                        edit_pass = ec2.text_input("App Password", type="password", value=sender["app_password"])
                        edit_active = st.checkbox("Active", value=bool(sender["active"]))
                        sc1, sc2 = st.columns(2)
                        if sc1.form_submit_button("Save", use_container_width=True):
                            ok, msg = db.update_sender(sender["id"], edit_email.strip(), edit_pass.strip(), edit_active)
                            if ok:
                                st.session_state[f"editing_sender_{sender['id']}"] = False
                                st.success(msg)
                                st.rerun()
                            else:
                                st.error(msg)
                        if sc2.form_submit_button("Cancel", use_container_width=True):
                            st.session_state[f"editing_sender_{sender['id']}"] = False
                            st.rerun()

                st.markdown("---")
    else:
        st.info("No senders added yet. Click **Add New Sender** above.")


# ══════════════════════════════════════════════════════════════
# 📥 RECEIVERS
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

    # Receiver Table
    receivers = db.get_all_receivers()
    if receivers:
        for recv in receivers:
            with st.container():
                cols = st.columns([3, 4, 1, 1])
                cols[0].markdown(f"**{recv['name']}**")
                cols[1].markdown(recv["email"])

                # Edit
                if cols[2].button("✏️", key=f"edit_r_{recv['id']}"):
                    st.session_state[f"editing_receiver_{recv['id']}"] = True

                # Delete
                if cols[3].button("🗑️", key=f"del_r_{recv['id']}"):
                    db.delete_receiver(recv["id"])
                    st.rerun()

                # Edit form
                if st.session_state.get(f"editing_receiver_{recv['id']}"):
                    with st.form(f"edit_receiver_form_{recv['id']}"):
                        ec1, ec2 = st.columns(2)
                        edit_name = ec1.text_input("Name", value=recv["name"])
                        edit_email = ec2.text_input("Email", value=recv["email"])
                        sc1, sc2 = st.columns(2)
                        if sc1.form_submit_button("Save", use_container_width=True):
                            ok, msg = db.update_receiver(recv["id"], edit_name.strip(), edit_email.strip())
                            if ok:
                                st.session_state[f"editing_receiver_{recv['id']}"] = False
                                st.success(msg)
                                st.rerun()
                            else:
                                st.error(msg)
                        if sc2.form_submit_button("Cancel", use_container_width=True):
                            st.session_state[f"editing_receiver_{recv['id']}"] = False
                            st.rerun()

                st.markdown("---")
    else:
        st.info("No receivers added yet. Click **Add New Receiver** above.")


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
        save_btn = st.form_submit_button("💾 Save Settings", use_container_width=True, type="primary")

        if save_btn:
            db.save_settings({
                "gemini_api_key": gemini_key,
                "batch_size": int(batch_size),
                "delay_minutes": delay_minutes,
                "daily_limit": int(daily_limit),
                "random_delay": random_delay,
                "tone": tone,
            })
            st.success("Settings saved successfully!")
            st.rerun()


# ══════════════════════════════════════════════════════════════
# 🚀 RUN CONTROLS
# ══════════════════════════════════════════════════════════════

elif page == "🚀 Run Controls":
    st.markdown("# 🚀 Run Controls")
    st.markdown("Start, stop, or reset the email warmup engine.")
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

    ready = True
    if not api_key:
        st.warning("⚠️ Gemini API key not configured. Go to **Settings**.")
        ready = False
    if not senders:
        st.warning("⚠️ No active senders. Go to **Senders** to add and activate accounts.")
        ready = False
    if not receivers:
        st.warning("⚠️ No receivers. Go to **Receivers** to add email addresses.")
        ready = False

    # Status display
    st.markdown("### Current Status")
    status_cols = st.columns(4)
    status_cols[0].metric("Active Senders", len(senders))
    status_cols[1].metric("Receivers", len(receivers))
    status_cols[2].metric("Batch Size", settings.get("batch_size", 5))
    status_cols[3].metric("Daily Limit", settings.get("daily_limit", 20))

    st.markdown("---")

    # Control Buttons
    btn_cols = st.columns(3)

    with btn_cols[0]:
        start_clicked = st.button(
            "▶️ Start Warmup",
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

    # ── Warmup Execution ─────────────────────────────────────

    if start_clicked and ready:
        st.session_state.warmup_running = True
        st.session_state.warmup_log = []

        batch_size = settings.get("batch_size", 5)
        delay_min = settings.get("delay_minutes", 2)
        random_delay = settings.get("random_delay", True)
        daily_limit = settings.get("daily_limit", 20)
        tone = settings.get("tone", "Casual")

        progress_bar = st.progress(0)
        status_text = st.empty()
        log_area = st.container()

        total_ops = len(senders) * min(batch_size, len(receivers))
        completed = 0

        for sender in senders:
            if not st.session_state.warmup_running:
                break

            # Check daily limit
            today_count = db.get_today_sent_count(sender["email"])
            if today_count >= daily_limit:
                with log_area:
                    st.warning(f"⏭️ {sender['email']} — daily limit reached ({today_count}/{daily_limit}). Skipping.")
                continue

            remaining_limit = daily_limit - today_count
            batch_receivers = receivers[:min(batch_size, remaining_limit)]

            for recv in batch_receivers:
                if not st.session_state.warmup_running:
                    break

                status_text.markdown(f"**Sending:** {sender['email']} → {recv['email']}...")

                # Generate email
                result = generate_email(recv["name"], tone, api_key)

                if "error" in result:
                    db.add_log(sender["email"], recv["email"], recv["name"], "", STATUS_FAILED, result["error"])
                    with log_area:
                        st.error(f"🔴 AI generation failed for {recv['email']}: {result['error']}")
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
                        st.success(f"🟢 {sender['email']} → {recv['email']} | \"{result['subject']}\"")
                    else:
                        st.error(f"🔴 {sender['email']} → {recv['email']} | {send_result.get('error', 'Unknown error')}")

                completed += 1
                progress_bar.progress(min(completed / total_ops, 1.0))

                # Delay between emails
                if st.session_state.warmup_running:
                    delay_sec = get_delay_seconds(delay_min, random_delay)
                    if delay_sec > 0:
                        status_text.markdown(f"⏳ Waiting **{delay_sec:.0f}s** before next email...")
                        time.sleep(delay_sec)

        # Done
        st.session_state.warmup_running = False
        progress_bar.progress(1.0)
        status_text.markdown("✅ **Warmup run complete!**")
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
