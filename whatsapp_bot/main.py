# ============================================================
# main.py
# FastAPI server — receives Twilio webhooks
# Deployed via Cloudflare Tunnel (no port forwarding needed)
# ============================================================

import os
import asyncio
import tempfile
from fastapi import FastAPI, Request, Form, Response
from fastapi.responses import PlainTextResponse
from twilio.rest import Client as TwilioClient
from twilio.twiml.messaging_response import MessagingResponse
from dotenv import load_dotenv

from handler import handle_message

load_dotenv()

# Add this BEFORE app = FastAPI(...)

import os
from dotenv import load_dotenv
load_dotenv()

def validate_env():
    required = {
        "DATABRICKS_TOKEN":    os.getenv("DATABRICKS_TOKEN"),
        "DATABRICKS_WAREHOUSE_ID": os.getenv("DATABRICKS_WAREHOUSE_ID"),
        "TWILIO_ACCOUNT_SID":  os.getenv("TWILIO_ACCOUNT_SID"),
        "TWILIO_AUTH_TOKEN":   os.getenv("TWILIO_AUTH_TOKEN"),
        "SARVAM_API_KEY":      os.getenv("SARVAM_API_KEY"),
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        raise EnvironmentError(
            f"❌ Missing env vars: {missing}\n"
            f"Check your .env file."
        )
    print("✅ All environment variables present")

validate_env()   # Crashes immediately if .env is wrong

app = FastAPI(title="Sarkari-Mitra WhatsApp Bot")

TWILIO_SID   = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_TOKEN = os.getenv("TWILIO_AUTH_TOKEN")
TWILIO_FROM  = os.getenv("TWILIO_WHATSAPP_NUMBER",
                          "whatsapp:+15072644422")

_twilio = TwilioClient(TWILIO_SID, TWILIO_TOKEN)


@app.get("/")
async def health():
    return {"status": "🇮🇳 Sarkari-Mitra is running"}


@app.post("/webhook")
async def webhook(
    request: Request,
    From:               str = Form(...),
    Body:               str = Form(default=""),
    NumMedia:           int = Form(default=0),
    MediaUrl0:          str = Form(default=None),
    MediaContentType0:  str = Form(default=None),
):
    phone = From.replace("whatsapp:", "").strip()

    # Detect voice message
    is_voice = (
        NumMedia > 0 and
        MediaContentType0 and
        "audio" in MediaContentType0.lower()
    )

    # Replace the try/except in webhook() with this:

    try:
        if is_voice:
            result = await asyncio.wait_for(
                handle_message(phone=phone, audio_url=MediaUrl0),
                timeout=25    # Twilio webhook times out at 30s
            )
        else:
            result = await asyncio.wait_for(
                handle_message(phone=phone, message=Body),
                timeout=25
            )
        response_text  = result["text"]
        response_audio = result.get("audio")

    except asyncio.TimeoutError:
        response_text  = (
            "Thoda time lag raha hai. 🙏\n"
            "Dobara try karein ya helpline: 1800-11-0001"
        )
        response_audio = None

    except Exception as e:
        print(f"❌ Unhandled error for {phone}: {type(e).__name__}: {e}")
        response_text  = (
            "Technical problem aayi. 🙏\n"
            "Thodi der baad try karein."
        )
        response_audio = None

    # ── Send voice response ───────────────────────────────
    if response_audio:
        # Upload to Twilio and send
        _send_voice(From, response_audio, caption=response_text[:200])
        return Response(
            content='<?xml version="1.0"?><Response></Response>',
            media_type="application/xml"
        )

    # ── Send text response ────────────────────────────────
    chunks = _split(response_text)

    if len(chunks) == 1:
        twiml = MessagingResponse()
        twiml.message(chunks[0])
        return Response(content=str(twiml), media_type="application/xml")

    # Multiple chunks — send all but last via API
    for chunk in chunks[:-1]:
        _twilio.messages.create(from_=TWILIO_FROM, to=From, body=chunk)
        await asyncio.sleep(0.5)   # Avoid rate limit

    twiml = MessagingResponse()
    twiml.message(chunks[-1])
    return Response(content=str(twiml), media_type="application/xml")


def _split(text: str, limit: int = 1500) -> list[str]:
    """Split at newlines to stay under WhatsApp limit."""
    if len(text) <= limit:
        return [text]
    chunks, current = [], ""
    for line in text.split("\n"):
        if len(current) + len(line) + 1 > limit:
            if current:
                chunks.append(current.strip())
            current = line
        else:
            current += ("\n" + line if current else line)
    if current:
        chunks.append(current.strip())
    return chunks or [text[:limit]]


def _send_voice(to: str, audio: bytes, caption: str = ""):
    """Send voice note. Falls back to text if upload fails."""
    try:
        with tempfile.NamedTemporaryFile(suffix=".mp3", delete=False) as f:
            f.write(audio)
            # For production: upload to public URL (S3/Cloudflare R2)
            # For hackathon: send caption as text fallback
        _twilio.messages.create(from_=TWILIO_FROM, to=to, body=caption)
    except Exception as e:
        print(f"⚠️ Voice send failed: {e}")
        _twilio.messages.create(from_=TWILIO_FROM, to=to, body=caption)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)