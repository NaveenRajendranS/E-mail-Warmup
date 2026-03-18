"""
AI email content generator using Gemini API.
Generates short, natural, casual office emails for warmup.
"""

import google.generativeai as genai


def generate_email(receiver_name: str, tone: str, api_key: str) -> dict:
    """
    Generate a short, casual email using Gemini API.

    Args:
        receiver_name: First name of the receiver.
        tone: Email tone — Casual, Friendly, or Internal office.
        api_key: Gemini API key.

    Returns:
        dict with 'subject' and 'body' keys, or 'error' key on failure.
    """
    try:
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel("gemini-2.5-flash")

        # Pick ONE random topic so the AI writes only a single email
        import random
        topics = [
            "letting them know you uploaded a file to Drive",
            "telling them you made a small change to a document",
            "asking if they saw your previous message",
            "saying you're stepping out for a bit",
            "informing you're back online and jump in call once available",
            "mentioning you left a comment on a doc that need changes",
            "asking them to check a quick update on the latest task",
            "saying you fixed a minor issue which don't need a new CR",
            "notifying that the pending task is ready for review",
            "saying you'll take a look and get back once done",
            "asking if they are available for a quick call to discuss about the bug",
            "confirming you received their message in teams / slack",
            "saying you're working on it now and need few more hours",
            "telling them you'll share shortly and all changes are updated",
            "asking if anything is pending from your side",
            "mentioning you pushed a small update on the ticket",
            "saying you've aligned with another teammate on the approach",
            "asking them to verify something quickly and to push the changes",
            "informing that the task is almost done and ready for deployment",
            "asking if timelines are still the same or any adjustments has to be done",
            "saying you added notes for clarity at the end of the ticket",
            "telling them you'll update by EOD without fail",
            "asking if they need help with anything to settle up",
            "mentioning a small delay in the business trip",
            "saying you'll circle back later once you are back from the trip",
            "informing you tested and it looks fine",
            "asking them to recheck once since it is not working from our side",
            "saying you've cleaned up the file with latest updates",
            "mentioning you created a new version for the file",
            "informing about a minor fix",
            "asking if you can close the task by EOD",
            "saying everything looks good from your side and no further changes needed",
            "asking if anything else is needed from your side",
            "mentioning you scheduled something for the task",
            "confirming meeting time for the weekly report discussion",
            "telling them you joined the call with the new client",
            "asking if they are joining the call with the customer support",
            "saying you'll resend something",
            "informing about a quick correction",
            "asking them to approve quickly",
            "saying you updated as discussed",
            "mentioning you flagged something important",
            "asking for a quick confirmation",
            "informing about a quick sync needed",
            "saying you'll pick this up next",
            "mentioning you are reviewing it now",
            "telling them it's done from your side",
            "asking them to take a final look",
            "saying you'll follow up tomorrow",
            "mentioning you dropped a note earlier",
        ]
        chosen_topic = random.choice(topics)

        prompt = f"""Write ONE short casual {tone.lower()} internal office email to {receiver_name}.

Topic: {chosen_topic}

Rules:
- Write ONLY ONE email, not multiple
- Subject line: short (5-7 words), natural, no "Re:" or "Fwd:"
- Body: exactly 2-3 short lines
- Sound completely natural and human
- Use {receiver_name}'s name naturally (like "Hey {receiver_name}" or "Hi {receiver_name}")
- Do NOT use any placeholders like [Name], [Your Name], [Topic]
- Do NOT include a sign-off or signature
- Do NOT use marketing or promotional language
- Make it sound like a real quick message between coworkers

Return ONLY in this exact format (nothing else):
SUBJECT: <subject line>
BODY: <email body>"""

        response = model.generate_content(prompt)
        text = response.text.strip()

        # Parse the response — strip any markdown formatting
        import re
        subject = ""
        body_lines = []
        in_body = False

        for line in text.split("\n"):
            line = line.strip()
            # Remove markdown bold/italic wrappers
            clean = re.sub(r'[*_`]', '', line).strip()

            if re.match(r'^subject\s*:', clean, re.IGNORECASE):
                subject = re.sub(r'^subject\s*:\s*', '', clean, flags=re.IGNORECASE).strip()
                in_body = False
            elif re.match(r'^body\s*:', clean, re.IGNORECASE):
                first_body = re.sub(r'^body\s*:\s*', '', clean, flags=re.IGNORECASE).strip()
                if first_body:
                    body_lines.append(first_body)
                in_body = True
            elif in_body and clean:
                body_lines.append(clean)

        body = "\n".join(body_lines)

        # Extra safety: strip "Subject:" if it leaked into the subject value
        subject = re.sub(r'^subject\s*:\s*', '', subject, flags=re.IGNORECASE).strip()

        if not subject or not body:
            # Fallback: use the whole response as body
            subject = f"Quick note for {receiver_name}"
            body = text[:200] if text else f"Hey {receiver_name}, hope you're doing well!"

        return {"subject": subject, "body": body}

    except Exception as e:
        return {"error": str(e)}
