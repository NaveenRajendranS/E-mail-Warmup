"""
SMTP email sender for Gmail using App Passwords.
"""

import smtplib
from email.message import EmailMessage
from email.utils import formataddr
from config import SMTP_SERVER, SMTP_PORT


def send_email(sender_email: str, app_password: str,
               receiver_email: str, subject: str, body: str,
               sender_name: str = "") -> dict:
    """
    Send an email via Gmail SMTP with TLS.

    Args:
        sender_email: Gmail address of the sender.
        app_password: Gmail App Password for the sender.
        receiver_email: Recipient email address.
        subject: Email subject line.
        body: Email body text.
        sender_name: Display name for the sender (e.g. "Alex Morgan").

    Returns:
        dict with 'status' ('Sent' or 'Failed') and optionally 'error'.
    """
    try:
        # Auto-generate name from email if not provided
        if not sender_name:
            sender_name = sender_email.split("@")[0].replace(".", " ").title()

        msg = EmailMessage()
        msg["From"] = formataddr((sender_name, sender_email))
        msg["To"] = receiver_email
        msg["Subject"] = subject

        # Append signature
        full_body = body.rstrip() + "\n\nRegards,\n" + sender_name

        msg.set_content(full_body)

        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=30) as server:
            server.starttls()
            server.login(sender_email, app_password)
            server.send_message(msg)

        return {"status": "Sent"}

    except smtplib.SMTPAuthenticationError:
        return {"status": "Failed", "error": "SMTP authentication failed. Check email/app password."}
    except smtplib.SMTPRecipientsRefused:
        return {"status": "Failed", "error": f"Recipient refused: {receiver_email}"}
    except smtplib.SMTPException as e:
        return {"status": "Failed", "error": f"SMTP error: {str(e)}"}
    except Exception as e:
        return {"status": "Failed", "error": f"Unexpected error: {str(e)}"}
