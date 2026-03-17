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

        prompt = f"""Write a very casual {tone.lower()} internal office email to {receiver_name}.

Rules:
- Subject line: short (3-6 words), natural, no "Re:" or "Fwd:"
- Body: exactly 1-2 lines, very short
- Sound completely natural and human
- Use {receiver_name}'s name naturally (like "Hey {receiver_name}" or "Hi {receiver_name}")
- Do NOT use any placeholders like [Name], [Your Name], [Topic]
- Do NOT include a sign-off or signature
- Do NOT use marketing or promotional language
- Make it sound like a real quick message between coworkers
- Vary the topic: could be about coffee, a quick question, a meeting reminder, saying thanks, sharing something, etc.

Return ONLY in this exact format:
SUBJECT: <subject line>
BODY: <email body>"""

        response = model.generate_content(prompt)
        text = response.text.strip()

        # Parse the response
        subject = ""
        body = ""

        for line in text.split("\n"):
            line = line.strip()
            if line.upper().startswith("SUBJECT:"):
                subject = line[len("SUBJECT:"):].strip()
            elif line.upper().startswith("BODY:"):
                body = line[len("BODY:"):].strip()

        if not subject or not body:
            # Fallback: use the whole response as body
            subject = f"Quick note for {receiver_name}"
            body = text[:200] if text else f"Hey {receiver_name}, hope you're doing well!"

        return {"subject": subject, "body": body}

    except Exception as e:
        return {"error": str(e)}
