"""
Gmail IMAP Mail Checker — Read actual mailbox stats directly from Gmail.
Checks Sent folder count, Inbox count, and reply detection.
Uses the same App Passwords stored for each sender account.
"""

import imaplib
import email
from email.header import decode_header
from config import IMAP_SERVER, IMAP_PORT


def _decode_header_value(raw):
    """Decode an email header value safely."""
    if not raw:
        return ""
    parts = decode_header(raw)
    decoded = []
    for part, charset in parts:
        if isinstance(part, bytes):
            decoded.append(part.decode(charset or "utf-8", errors="replace"))
        else:
            decoded.append(str(part))
    return "".join(decoded)


def get_mailbox_stats(email_addr: str, app_password: str) -> dict:
    """
    Connect to Gmail via IMAP and get mailbox statistics.

    Returns:
        dict with:
            - total_sent: number of emails in Sent folder
            - total_inbox: number of emails in Inbox
            - total_replied: number of reply emails (Re:) in Inbox
            - error: error message if connection failed
    """
    result = {
        "total_sent": 0,
        "total_inbox": 0,
        "total_replied": 0,
        "error": None,
    }

    try:
        mail = imaplib.IMAP4_SSL(IMAP_SERVER, IMAP_PORT)
        mail.login(email_addr, app_password)

        # ── Count Sent emails ──────────────────────────────
        # Gmail's Sent folder is "[Gmail]/Sent Mail"
        sent_status, _ = mail.select('"[Gmail]/Sent Mail"', readonly=True)
        if sent_status == "OK":
            status, data = mail.search(None, "ALL")
            if status == "OK" and data[0]:
                result["total_sent"] = len(data[0].split())

        # ── Count Inbox emails ─────────────────────────────
        inbox_status, _ = mail.select("INBOX", readonly=True)
        if inbox_status == "OK":
            status, data = mail.search(None, "ALL")
            if status == "OK" and data[0]:
                result["total_inbox"] = len(data[0].split())

            # Count replies (subjects starting with "Re:")
            status, data = mail.search(None, '(SUBJECT "Re:")')
            if status == "OK" and data[0]:
                result["total_replied"] = len(data[0].split())

        mail.logout()

    except imaplib.IMAP4.error as e:
        result["error"] = f"IMAP auth failed: {str(e)}"
    except Exception as e:
        result["error"] = f"Connection error: {str(e)}"

    return result


def get_all_mailbox_stats(senders: list[dict]) -> list[dict]:
    """
    Get mailbox stats for a list of senders.

    Args:
        senders: list of dicts with 'email' and 'app_password' keys.

    Returns:
        list of dicts with email + stats for each sender.
    """
    all_stats = []
    for sender in senders:
        stats = get_mailbox_stats(sender["email"], sender["app_password"])
        stats["sender_email"] = sender["email"]
        all_stats.append(stats)
    return all_stats
