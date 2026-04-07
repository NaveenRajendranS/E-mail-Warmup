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
from ai_generator import generate_email, generate_reply
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
        ["📊 Dashboard", "📤 Senders", "📥 Receivers", "🔗 Mapping", "⚙️ Settings", "🚀 Run Controls", "📋 Logs", "📈 Analytics"],
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

    # Summary metrics
    all_senders = db.get_all_senders()
    total_senders = len(all_senders)
    active_senders = sum(1 for s in all_senders if s["active"])
    inactive_senders = total_senders - active_senders
    mc1, mc2, mc3 = st.columns(3)
    mc1.metric("Total Senders", total_senders)
    mc2.metric("Active", active_senders)
    mc3.metric("Inactive", inactive_senders)

    # Global bulk actions
    btn_col1, btn_col2, btn_col3 = st.columns([1, 1, 4])
    with btn_col1:
        if st.button("✅ Activate All", use_container_width=True, type="primary"):
            db.activate_all_senders()
            st.success("All senders activated!")
            st.rerun()
    with btn_col2:
        if st.button("⛔ Deactivate All", use_container_width=True):
            db.deactivate_all_senders()
            st.success("All senders deactivated!")
            st.rerun()

    st.markdown("---")

    # ── Domain-wise Toggle Panel ──────────────────────────────
    sender_domains = db.get_sender_domains()

    if sender_domains:
        st.markdown("### 🌐 Domain Controls")
        st.markdown("Quickly activate or deactivate all senders for a specific domain.")

        # Build domain stats
        domain_stats = {}
        for s in all_senders:
            d = s["email"].split("@")[1].lower()
            if d not in domain_stats:
                domain_stats[d] = {"total": 0, "active": 0}
            domain_stats[d]["total"] += 1
            if s["active"]:
                domain_stats[d]["active"] += 1

        # Domain toggle grid (3 per row)
        DOMAIN_COLS = 3
        for i in range(0, len(sender_domains), DOMAIN_COLS):
            cols = st.columns(DOMAIN_COLS)
            for j, col in enumerate(cols):
                idx = i + j
                if idx >= len(sender_domains):
                    break
                domain = sender_domains[idx]
                stats = domain_stats.get(domain, {"total": 0, "active": 0})
                all_active = stats["active"] == stats["total"]
                none_active = stats["active"] == 0

                with col:
                    # Domain info card
                    bg_color = "#10b981" if all_active else ("#f59e0b" if stats["active"] > 0 else "#6b7280")
                    st.markdown(f"""<div style="background:linear-gradient(145deg,#1a1a2e 0%,#16213e 100%);border:1px solid rgba(255,255,255,0.08);border-radius:12px;padding:14px 16px;margin-bottom:8px;box-shadow:0 2px 10px rgba(0,0,0,0.2);">
                        <div style="display:flex;align-items:center;justify-content:space-between;">
                            <div>
                                <div style="color:#fff;font-weight:600;font-size:0.95rem;">🌐 {domain}</div>
                                <div style="color:#8b8ba7;font-size:0.78rem;margin-top:2px;">{stats['active']}/{stats['total']} active</div>
                            </div>
                            <div style="width:12px;height:12px;border-radius:50%;background:{bg_color};box-shadow:0 0 8px {bg_color}88;"></div>
                        </div>
                    </div>""", unsafe_allow_html=True)

                    dc1, dc2 = st.columns(2)
                    with dc1:
                        if st.button("✅ Activate", key=f"act_domain_s_{domain}", use_container_width=True,
                                     type="primary", disabled=all_active):
                            db.activate_senders_by_domain(domain)
                            st.success(f"All {domain} senders activated!")
                            st.rerun()
                    with dc2:
                        if st.button("⛔ Deactivate", key=f"deact_domain_s_{domain}", use_container_width=True,
                                     disabled=none_active):
                            db.deactivate_senders_by_domain(domain)
                            st.success(f"All {domain} senders deactivated!")
                            st.rerun()

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

    # ── Sender Cards grouped by Domain ────────────────────────
    for domain in sender_domains:
        domain_senders = [s for s in all_senders if s["email"].split("@")[1].lower() == domain]
        domain_active = sum(1 for s in domain_senders if s["active"])

        status_dot = "🟢" if domain_active == len(domain_senders) else ("🟡" if domain_active > 0 else "🔴")
        st.markdown(f"### {status_dot} {domain}  `({domain_active}/{len(domain_senders)} active)`")

        CARDS_PER_ROW = 4
        for i in range(0, len(domain_senders), CARDS_PER_ROW):
            cols = st.columns(CARDS_PER_ROW)
            for j, col in enumerate(cols):
                idx = i + j
                if idx >= len(domain_senders):
                    break
                sender = domain_senders[idx]
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

        st.markdown("---")

    if not all_senders:
        st.info("No senders added yet. Click **Add New Sender** above.")


# ══════════════════════════════════════════════════════════════
# 📥 RECEIVERS — Card Grid UI
# ══════════════════════════════════════════════════════════════

elif page == "📥 Receivers":
    st.markdown("# 📥 Receiver Management")
    st.markdown("Manage the list of receiver email addresses.")

    # Summary metrics
    all_receivers = db.get_all_receivers()
    total_receivers = len(all_receivers)
    active_receivers = sum(1 for r in all_receivers if r.get("active", 1))
    inactive_receivers = total_receivers - active_receivers
    mc1, mc2, mc3 = st.columns(3)
    mc1.metric("Total Receivers", total_receivers)
    mc2.metric("Active", active_receivers)
    mc3.metric("Inactive", inactive_receivers)

    # Global bulk actions
    btn_col1, btn_col2, btn_col3 = st.columns([1, 1, 4])
    with btn_col1:
        if st.button("✅ Activate All", use_container_width=True, type="primary", key="activate_all_recv"):
            db.activate_all_receivers()
            st.success("All receivers activated!")
            st.rerun()
    with btn_col2:
        if st.button("⛔ Deactivate All", use_container_width=True, key="deactivate_all_recv"):
            db.deactivate_all_receivers()
            st.success("All receivers deactivated!")
            st.rerun()

    st.markdown("---")

    # ── Domain-wise Toggle Panel ──────────────────────────────
    receiver_domains = db.get_receiver_domains()

    if receiver_domains:
        st.markdown("### 🌐 Domain Controls")
        st.markdown("Quickly activate or deactivate all receivers for a specific domain.")

        # Build domain stats
        domain_stats = {}
        for r in all_receivers:
            d = r["email"].split("@")[1].lower()
            if d not in domain_stats:
                domain_stats[d] = {"total": 0, "active": 0}
            domain_stats[d]["total"] += 1
            if r.get("active", 1):
                domain_stats[d]["active"] += 1

        # Domain toggle grid (3 per row)
        DOMAIN_COLS = 3
        for i in range(0, len(receiver_domains), DOMAIN_COLS):
            cols = st.columns(DOMAIN_COLS)
            for j, col in enumerate(cols):
                idx = i + j
                if idx >= len(receiver_domains):
                    break
                domain = receiver_domains[idx]
                stats = domain_stats.get(domain, {"total": 0, "active": 0})
                all_active = stats["active"] == stats["total"]
                none_active = stats["active"] == 0

                with col:
                    # Domain info card
                    bg_color = "#10b981" if all_active else ("#f59e0b" if stats["active"] > 0 else "#6b7280")
                    st.markdown(f"""<div style="background:linear-gradient(145deg,#1a1a2e 0%,#16213e 100%);border:1px solid rgba(255,255,255,0.08);border-radius:12px;padding:14px 16px;margin-bottom:8px;box-shadow:0 2px 10px rgba(0,0,0,0.2);">
                        <div style="display:flex;align-items:center;justify-content:space-between;">
                            <div>
                                <div style="color:#fff;font-weight:600;font-size:0.95rem;">🌐 {domain}</div>
                                <div style="color:#8b8ba7;font-size:0.78rem;margin-top:2px;">{stats['active']}/{stats['total']} active</div>
                            </div>
                            <div style="width:12px;height:12px;border-radius:50%;background:{bg_color};box-shadow:0 0 8px {bg_color}88;"></div>
                        </div>
                    </div>""", unsafe_allow_html=True)

                    dc1, dc2 = st.columns(2)
                    with dc1:
                        if st.button("✅ Activate", key=f"act_domain_r_{domain}", use_container_width=True,
                                     type="primary", disabled=all_active):
                            db.activate_receivers_by_domain(domain)
                            st.success(f"All {domain} receivers activated!")
                            st.rerun()
                    with dc2:
                        if st.button("⛔ Deactivate", key=f"deact_domain_r_{domain}", use_container_width=True,
                                     disabled=none_active):
                            db.deactivate_receivers_by_domain(domain)
                            st.success(f"All {domain} receivers deactivated!")
                            st.rerun()

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

    # ── Receiver Cards grouped by Domain ──────────────────────
    for domain in receiver_domains:
        domain_receivers = [r for r in all_receivers if r["email"].split("@")[1].lower() == domain]
        domain_active = sum(1 for r in domain_receivers if r.get("active", 1))

        status_dot = "🟢" if domain_active == len(domain_receivers) else ("🟡" if domain_active > 0 else "🔴")
        st.markdown(f"### {status_dot} {domain}  `({domain_active}/{len(domain_receivers)} active)`")

        CARDS_PER_ROW = 4
        for i in range(0, len(domain_receivers), CARDS_PER_ROW):
            cols = st.columns(CARDS_PER_ROW)
            for j, col in enumerate(cols):
                idx = i + j
                if idx >= len(domain_receivers):
                    break
                recv = domain_receivers[idx]
                letter = recv["name"][0].upper() if recv["name"] else "?"
                color = get_avatar_color(recv["email"])

                with col:
                    is_active = recv.get("active", 1)
                    status_badge = ""
                    if not is_active:
                        status_badge = '<div style="color:#ef4444;font-size:0.75rem;margin-top:4px;">⛔ Inactive</div>'

                    st.markdown(
                        render_profile_card(letter, recv["name"], recv["email"], color, extra_html=status_badge),
                        unsafe_allow_html=True,
                    )

                    # Action buttons
                    b1, b2, b3 = st.columns(3)
                    if b1.button("✏️ Edit", key=f"edit_r_{recv['id']}"):
                        st.session_state[f"editing_receiver_{recv['id']}"] = True
                    if b2.button("🗑️ Del", key=f"del_r_{recv['id']}"):
                        db.delete_receiver(recv["id"])
                        st.rerun()
                    toggle_label = "⛔" if is_active else "✅"
                    if b3.button(toggle_label, key=f"toggle_r_{recv['id']}"):
                        db.toggle_receiver(recv["id"], not is_active)
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

        st.markdown("---")

    if not all_receivers:
        st.info("No receivers added yet. Click **Add New Receiver** above.")


# ══════════════════════════════════════════════════════════════
# 🔗 MAPPING — Card Grid with Receiver Expander
# ══════════════════════════════════════════════════════════════

elif page == "🔗 Mapping":
    st.markdown("# 🔗 Sender → Receiver Mapping")
    st.markdown("Assign receivers to each sender. Click a sender card to manage its receivers.")

    # Controls row: receivers-per-sender + shuffle button
    ctrl_col1, ctrl_col2, ctrl_col3 = st.columns([2, 1, 3])
    with ctrl_col1:
        receivers_per_sender = st.number_input(
            "Receivers per sender",
            min_value=1, max_value=50,
            value=5,
            help="Number of receivers to assign to each sender when shuffling.",
            key="receivers_per_sender",
        )
    with ctrl_col2:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("🔀 Shuffle All Mappings", use_container_width=True, type="primary"):
            db.randomize_all_mappings(receivers_per_sender=int(receivers_per_sender))
            st.success(f"✅ All mappings shuffled — {int(receivers_per_sender)} receiver(s) per sender!")
            st.rerun()

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
        max_sel = int(receivers_per_sender)

        # Fetch ALL mappings in one query
        all_mappings = db.get_all_mapped_receiver_ids_bulk()

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
                mapped_count = len(all_mappings.get(sender["id"], []))

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
                current_mapped = all_mappings.get(sender["id"], [])

                with st.expander(f"▶ Manage receivers for **{sender['email']}**", expanded=False):
                    default_labels = []
                    for rid in current_mapped:
                        if rid in receiver_options:
                            default_labels.append(receiver_options[rid])

                    # Force-set default if widget hasn't been manually changed
                    widget_key = f"map_{sender['id']}"
                    if widget_key not in st.session_state:
                        st.session_state[widget_key] = default_labels

                    selected_labels = st.multiselect(
                        f"Select receivers (max {max_sel})",
                        options=receiver_labels_list,
                        key=widget_key,
                        max_selections=max_sel,
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
    receivers = db.get_active_receivers()

    # Preflight checks
    api_key = settings.get("gemini_api_key", "")
    if not api_key:
        try:
            api_key = st.secrets.get("GEMINI_API_KEY", "")
        except Exception:
            pass

    # Check if there are active receivers
    active_recv = db.get_active_receivers()

    ready = True
    if not api_key:
        st.warning("⚠️ Gemini API key not configured. Go to **Settings**.")
        ready = False
    if not senders:
        st.warning("⚠️ No active senders. Go to **Senders** to add and activate accounts.")
        ready = False
    if not active_recv:
        st.warning("⚠️ No active receivers. Go to **📥 Receivers** to activate some.")
        ready = False

    # Status display
    st.markdown("### Current Status")
    rounds_per_day = settings.get("rounds_per_day", 5)
    gap_minutes = settings.get("gap_minutes", 60)

    status_cols = st.columns(5)
    status_cols[0].metric("Active Senders", len(senders))
    status_cols[1].metric("Active Receivers", len(receivers))
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
            active_senders_dict = {s["email"].lower(): s for s in active_senders}

            # Calculate total ops for this round
            # Smart selection: each sender gets 5 receivers they haven't emailed recently
            all_active_receivers = db.get_active_receivers()

            sender_receiver_map = {}
            for s in active_senders:
                picked = db.pick_receivers_for_sender(
                    s["email"], all_active_receivers, count=5, cooldown_days=3
                )
                sender_receiver_map[s["id"]] = picked

            total_ops = sum(len(v) for v in sender_receiver_map.values())
            total_ops = max(total_ops, 1)
            completed = 0

            # ── Parallel sender execution ──────────────────
            from concurrent.futures import ThreadPoolExecutor, as_completed
            import threading

            # Thread-safe counters and stop signal
            lock = threading.Lock()
            completed = [0]  # mutable for closure
            stop_event = threading.Event()  # thread-safe stop signal

            def send_for_sender(sender, mapped_receivers):
                """Process all mapped receivers for one sender (runs in thread)."""
                results = []

                # Check daily limit
                today_count = db.get_today_sent_count(sender["email"])
                if today_count >= daily_limit:
                    results.append(("warning", f"⏭️ {sender['email']} — daily limit reached ({today_count}/{daily_limit}). Skipping."))
                    with lock:
                        completed[0] += len(mapped_receivers)
                    return results

                for recv in mapped_receivers:
                    if stop_event.is_set():
                        break

                    # Re-check daily limit
                    today_count = db.get_today_sent_count(sender["email"])
                    if today_count >= daily_limit:
                        results.append(("warning", f"⏭️ {sender['email']} — daily limit reached mid-round."))
                        break

                    # Generate AI email
                    result = generate_email(recv["name"], tone, api_key)

                    if "error" in result:
                        db.add_log(sender["email"], recv["email"], recv["name"], "", STATUS_FAILED, result["error"])
                        results.append(("error", f"🔴 AI error for {sender['email']} → {recv['email']}: {result['error']}"))
                        with lock:
                            completed[0] += 1
                        continue

                    # Send email
                    send_result = send_email(
                        sender["email"], sender["app_password"],
                        recv["email"], result["subject"], result["body"],
                        sender_name=db.get_sender_display_name(sender["email"]),
                    )

                    db.add_log(
                        sender["email"], recv["email"], recv["name"],
                        result["subject"], send_result["status"],
                        send_result.get("error"),
                    )

                    if send_result["status"] == STATUS_SENT:
                        db.increment_sent_count(sender["email"])
                        results.append(("success", f"🟢 R{current_round} | {sender['email']} → {recv['email']} | \"{result['subject']}\""))

                        # Background reply feature
                        recv_email_lower = recv["email"].lower()
                        if recv_email_lower in active_senders_dict:
                            recv_cred = active_senders_dict[recv_email_lower]
                            reply_to_msg_id = send_result.get("message_id")
                            
                            def background_reply(sender_email, sender_name, receiver_cred, original_subject, msg_id):
                                import random
                                time.sleep(random.randint(60, 180))
                                reply_content = generate_reply(sender_name.split()[0], tone, api_key)
                                if "error" in reply_content: return
                                
                                reply_subject = original_subject if str(original_subject).lower().startswith("re:") else f"Re: {original_subject}"
                                reply_result = send_email(
                                    receiver_cred["email"], receiver_cred["app_password"],
                                    sender_email, reply_subject, reply_content["body"],
                                    sender_name=db.get_sender_display_name(receiver_cred["email"]),
                                    reply_to_msg_id=msg_id
                                )
                                
                                if reply_result["status"] == STATUS_SENT:
                                    db.increment_sent_count(receiver_cred["email"])
                                    db.add_log(receiver_cred["email"], sender_email, sender_name,
                                               reply_subject, STATUS_SENT, None)
                                else:
                                    db.add_log(receiver_cred["email"], sender_email, sender_name,
                                               reply_subject, STATUS_FAILED, reply_result.get("error"))
                                               
                            threading.Thread(
                                target=background_reply,
                                args=(sender["email"], db.get_sender_display_name(sender["email"]), recv_cred, result["subject"], reply_to_msg_id)
                            ).start()

                    else:
                        results.append(("error", f"🔴 R{current_round} | {sender['email']} → {recv['email']} | {send_result.get('error', 'Unknown')}"))

                    with lock:
                        completed[0] += 1

                    # Delay between receivers within a sender (configurable in Settings)
                    delay_sec = get_delay_seconds(delay_min, random_delay_flag)
                    if delay_sec > 0:
                        time.sleep(delay_sec)

                return results

            # Build task list from shuffled distribution
            sender_tasks = []
            for sender in active_senders:
                assigned_receivers = sender_receiver_map[sender["id"]]
                if not assigned_receivers:
                    with log_area:
                        st.info(f"⏭️ {sender['email']} — no receivers assigned this round. Skipping.")
                    continue
                sender_tasks.append((sender, assigned_receivers))

            status_text.markdown(
                f"**Round {current_round}** | Sending from **{len(sender_tasks)} senders** in parallel..."
            )

            # Launch all senders in parallel
            with ThreadPoolExecutor(max_workers=min(len(sender_tasks), 15)) as executor:
                futures = {
                    executor.submit(send_for_sender, sender, mapped): sender
                    for sender, mapped in sender_tasks
                }

                for future in as_completed(futures):
                    sender = futures[future]
                    try:
                        results = future.result()
                        for msg_type, msg in results:
                            with log_area:
                                if msg_type == "success":
                                    st.success(msg)
                                elif msg_type == "error":
                                    st.error(msg)
                                elif msg_type == "warning":
                                    st.warning(msg)
                    except Exception as e:
                        with log_area:
                            st.error(f"🔴 Thread error for {sender['email']}: {e}")

                    # Update progress
                    progress_bar.progress(min(completed[0] / total_ops, 1.0))

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
    st.markdown("View, filter, and download all email sending activity.")

    # Auto-refresh control
    refresh_col1, refresh_col2 = st.columns([4, 1])
    with refresh_col2:
        auto_refresh = st.selectbox(
            "Auto Refresh",
            options=["Off", "1 min", "2 min", "3 min"],
            index=0,
            key="log_auto_refresh",
        )

    refresh_seconds = {"Off": None, "1 min": 60, "2 min": 120, "3 min": 180}
    interval = refresh_seconds.get(auto_refresh)
    if interval:
        import streamlit.components.v1 as components
        components.html(
            f"""
            <script>
                setTimeout(function() {{
                    window.parent.document.querySelectorAll('button[kind="secondary"]').forEach(function(btn) {{}});
                    window.parent.location.reload();
                }}, {interval * 1000});
            </script>
            """,
            height=0,
        )

    st.markdown("---")

    import pandas as pd
    from io import BytesIO

    # Filters
    filter_cols = st.columns(3)

    senders = db.get_all_senders()
    sender_options = ["All"] + [s["email"] for s in senders]
    sender_filter = filter_cols[0].selectbox("Filter by Sender", sender_options)

    status_filter = filter_cols[1].selectbox("Filter by Status", ["All", "Sent", "Failed"])

    date_filter = filter_cols[2].date_input("Filter by Date", value=None)

    # Fetch logs (limit raised to 5000 to keep history)
    logs = db.get_logs(
        sender_filter=sender_filter,
        date_filter=date_filter if date_filter else None,
        status_filter=status_filter,
        limit=5000,
    )

    # Summary metrics
    total = len(logs)
    sent_count = sum(1 for l in logs if l.get("status") == "Sent")
    failed_count = sum(1 for l in logs if l.get("status") == "Failed")

    metric_cols = st.columns(3)
    metric_cols[0].metric("Total Logs", total)
    metric_cols[1].metric("✅ Sent", sent_count)
    metric_cols[2].metric("❌ Failed", failed_count)

    st.markdown("---")

    if logs:
        df = pd.DataFrame(logs)

        # CSV download columns
        csv_cols = ["sender_email", "receiver_email", "subject", "status", "timestamp"]
        csv_available = [c for c in csv_cols if c in df.columns]
        csv_df = df[csv_available].copy()
        csv_df.columns = ["Sender Email", "Receiver Email", "Subject", "Status", "Timestamp"][:len(csv_available)]

        # Generate CSV
        csv_buffer = BytesIO()
        csv_df.to_csv(csv_buffer, index=False)
        csv_data = csv_buffer.getvalue()

        # Date label for filename (DD-MonthName-YYYY)
        from datetime import datetime as _dt
        if date_filter:
            fname = f"warmup_logs_{date_filter.strftime('%d-%B-%Y')}.csv"
        else:
            fname = f"warmup_logs_{_dt.now().strftime('%d-%B-%Y')}.csv"

        dl_cols = st.columns([3, 1])
        dl_cols[0].markdown(f"**Showing {total} log entries**")
        dl_cols[1].download_button(
            label="📥 Download CSV",
            data=csv_data,
            file_name=fname,
            mime="text/csv",
            use_container_width=True,
        )

        st.markdown("---")

        # Display table
        display_cols = ["sender_email", "receiver_email", "subject", "status", "timestamp", "error"]
        available_cols = [c for c in display_cols if c in df.columns]
        display_df = df[available_cols].copy()
        display_df.columns = [c.replace("_", " ").title() for c in display_df.columns]

        def highlight_status(val):
            if val == "Sent":
                return "background-color: #0d7a3e; color: white; border-radius: 4px; padding: 2px 8px;"
            elif val == "Failed":
                return "background-color: #c0392b; color: white; border-radius: 4px; padding: 2px 8px;"
            return ""

        styled = display_df.style.map(highlight_status, subset=["Status"] if "Status" in display_df.columns else [])
        st.dataframe(styled, use_container_width=True, hide_index=True)

        # Group by date summary
        if "timestamp" in df.columns:
            st.markdown("---")
            st.markdown("### 📊 Logs by Date")
            df["date"] = df["timestamp"].str[:10]
            date_summary = df.groupby("date").agg(
                Total=("status", "count"),
                Sent=("status", lambda x: (x == "Sent").sum()),
                Failed=("status", lambda x: (x == "Failed").sum()),
            ).reset_index()
            date_summary.columns = ["Date", "Total", "Sent", "Failed"]
            date_summary = date_summary.sort_values("Date", ascending=False)
            st.dataframe(date_summary, use_container_width=True, hide_index=True)
    else:
        st.info("No logs found matching your filters.")

# ══════════════════════════════════════════════════════════════
# 📈 ANALYTICS — Live Gmail Mailbox Stats via IMAP
# ══════════════════════════════════════════════════════════════

elif page == "📈 Analytics":
    st.markdown("# 📈 Email Analytics")
    st.markdown("Live mailbox stats pulled directly from Gmail via IMAP.")
    st.markdown("---")

    import pandas as pd
    from reply_checker import get_mailbox_stats

    all_senders = db.get_all_senders()

    if not all_senders:
        st.warning("No sender accounts configured. Go to **📤 Senders** to add accounts.")
    else:
        # ── Date Filter ───────────────────────────────────
        st.markdown("### Filter Options")
        analytics_date_filter = st.date_input(
            "Filter stats by Date", 
            value=None, 
            help="If set to a specific date (e.g., today), only emails sent, received, and replied on this date will be counted."
        )
        st.markdown("---")

        # ── Sender Selection ──────────────────────────────
        st.markdown("### Select Senders to Check")

        sel_col1, sel_col2, sel_col3 = st.columns([1, 1, 4])
        with sel_col1:
            if st.button("✅ Select All", key="analytics_select_all", use_container_width=True):
                for s in all_senders:
                    st.session_state[f"chk_{s['id']}"] = True
                st.rerun()
        with sel_col2:
            if st.button("⛔ Clear All", key="analytics_clear_all", use_container_width=True):
                for s in all_senders:
                    st.session_state[f"chk_{s['id']}"] = False
                st.rerun()

        # Checkbox grid for sender selection
        COLS = 4
        selected_senders = []
        for i in range(0, len(all_senders), COLS):
            cols = st.columns(COLS)
            for j, col in enumerate(cols):
                idx = i + j
                if idx >= len(all_senders):
                    break
                sender = all_senders[idx]
                with col:
                    checked = st.checkbox(
                        sender["email"],
                        value=st.session_state.get(f"chk_{sender['id']}", False),
                        key=f"chk_{sender['id']}",
                    )
                    if checked:
                        selected_senders.append(sender)

        st.markdown("---")

        # ── Fetch Stats Button ────────────────────────────
        fetch_clicked = st.button(
            "📊 Fetch Mailbox Stats",
            use_container_width=True,
            type="primary",
            disabled=len(selected_senders) == 0,
            help="Connect to Gmail via IMAP and read Sent/Inbox/Reply counts.",
        )

        if fetch_clicked and selected_senders:
            progress = st.progress(0)
            status_text = st.empty()
            results = []

            for i, sender in enumerate(selected_senders):
                status_text.markdown(f"📡 Checking **{sender['email']}**...")
                stats = get_mailbox_stats(sender["email"], sender["app_password"], date_filter=analytics_date_filter)
                stats["sender_email"] = sender["email"]
                results.append(stats)
                progress.progress((i + 1) / len(selected_senders))

            progress.empty()
            status_text.empty()

            # Store results in session state so they persist
            st.session_state["analytics_results"] = results
            st.rerun()

        # ── Display Results ───────────────────────────────
        results = st.session_state.get("analytics_results", [])

        if results:
            # Filter out errors
            success = [r for r in results if not r.get("error")]
            errors = [r for r in results if r.get("error")]

            if success:
                total_sent = sum(r["total_sent"] for r in success)
                total_inbox = sum(r["total_inbox"] for r in success)
                total_replied = sum(r["total_replied"] for r in success)

                # Summary cards
                m1, m2, m3, m4 = st.columns(4)
                m1.metric("📤 Total Sent", f"{total_sent:,}")
                m2.metric("📥 Total Inbox", f"{total_inbox:,}")
                m3.metric("💬 Total Replies", f"{total_replied:,}")
                m4.metric("👥 Accounts Checked", len(success))

                st.markdown("---")

                # Per-sender table
                st.markdown("### Per-Sender Breakdown")
                df = pd.DataFrame(success)
                df = df[["sender_email", "total_sent", "total_inbox", "total_replied"]]
                df.columns = ["Sender Email", "Total Sent", "Total Inbox", "Total Replied"]
                df = df.sort_values("Total Sent", ascending=False)

                st.dataframe(df, use_container_width=True, hide_index=True)

                # CSV Download
                from io import BytesIO
                csv_buffer = BytesIO()
                df.to_csv(csv_buffer, index=False)
                st.download_button(
                    label="📥 Download CSV",
                    data=csv_buffer.getvalue(),
                    file_name=f"mailbox_stats_{datetime.now().strftime('%d-%B-%Y')}.csv",
                    mime="text/csv",
                )

            # Show errors if any
            if errors:
                st.markdown("---")
                st.markdown("### ⚠️ Failed Connections")
                for err in errors:
                    st.error(f"**{err['sender_email']}**: {err['error']}")
        else:
            st.info("Select senders above and click **📊 Fetch Mailbox Stats** to check their Gmail accounts.")

