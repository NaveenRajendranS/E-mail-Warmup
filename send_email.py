"""
SMTP email sender for Gmail using App Passwords.
"""

import smtplib
from email.message import EmailMessage
from config import SMTP_SERVER, SMTP_PORT


def send_email(sender_email: str, app_password: str,
               receiver_email: str, subject: str, body: str) -> dict:
    """
    Send an email via Gmail SMTP with TLS.

    Args:
        sender_email: Gmail address of the sender.
        app_password: Gmail App Password for the sender.
        receiver_email: Recipient email address.
        subject: Email subject line.
        body: Email body text.

    Returns:
        dict with 'status' ('Sent' or 'Failed') and optionally 'error'.
    """
    try:
        msg = EmailMessage()
        msg["From"] = sender_email
        msg["To"] = receiver_email
        msg["Subject"] = subject
        msg.set_content(body)

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
