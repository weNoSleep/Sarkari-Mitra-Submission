# ============================================================
# sarvam_client.py
# Exactly mirrors your 10_llm_client Sarvam section
# Adds STT and TTS for WhatsApp voice messages
# ============================================================

import os
import base64
import httpx
from sarvamai import SarvamAI
from dotenv import load_dotenv

load_dotenv()

_sarvam = SarvamAI(api_subscription_key=os.getenv("SARVAM_API_KEY"))

# ── Exact copy from your 10_llm_client ───────────────────
LANGUAGE_CODES = {
    "hi": "hi-IN", "te": "te-IN", "ta": "ta-IN",
    "kn": "kn-IN", "ml": "ml-IN", "mr": "mr-IN",
    "gu": "gu-IN", "pa": "pa-IN", "bn": "bn-IN",
    "or": "or-IN", "en": "en-IN",
}

SCRIPT_RANGES = {
    "hi": ('\u0900', '\u097F'),
    "te": ('\u0C00', '\u0C7F'),
    "ta": ('\u0B80', '\u0BFF'),
    "kn": ('\u0C80', '\u0CFF'),
    "ml": ('\u0D00', '\u0D7F'),
    "gu": ('\u0A80', '\u0AFF'),
    "bn": ('\u0980', '\u09FF'),
    "pa": ('\u0A00', '\u0A7F'),
    "or": ('\u0B00', '\u0B7F'),
}


def detect_language(text: str) -> str:
    """Exact copy from your 10_llm_client."""
    scores = {}
    for lang, (start, end) in SCRIPT_RANGES.items():
        scores[lang] = sum(1 for c in text if start <= c <= end)
    best_lang = max(scores, key=scores.get)
    if scores[best_lang] > 2:
        return best_lang
    return "en"


def translate_to_english(text: str) -> tuple[str, str]:
    """Exact copy from your 10_llm_client."""
    source_lang = detect_language(text)
    if source_lang in ("en", "hi"):
        return text, source_lang
    source_bcp47 = LANGUAGE_CODES.get(source_lang, "hi-IN")
    try:
        response = _sarvam.text.translate(
            input=text,
            source_language_code=source_bcp47,
            target_language_code="en-IN",
            speaker_gender="Male"
        )
        return response.translated_text, source_lang
    except Exception as e:
        print(f"⚠️ Input translation failed: {e}")
        return text, source_lang


def translate_response(text: str, target_lang: str) -> str:
    """Exact copy from your 10_llm_client."""
    if target_lang in ("en", "hi"):
        return text
    target_bcp47 = LANGUAGE_CODES.get(target_lang, "hi-IN")
    try:
        response = _sarvam.text.translate(
            input=text,
            source_language_code="en-IN",
            target_language_code=target_bcp47,
            speaker_gender="Male"
        )
        return response.translated_text
    except Exception as e:
        print(f"⚠️ Response translation failed: {e}")
        return text


# ── STT — WhatsApp voice note → text ─────────────────────
async def speech_to_text(
    audio_url: str,
    language_hint: str = "hi-IN"
) -> str:
    """
    Download WhatsApp audio from Twilio and transcribe via Sarvam.
    Returns empty string on failure — caller handles gracefully.
    """
    try:
        # Download audio with Twilio credentials
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                audio_url,
                auth=(
                    os.getenv("TWILIO_ACCOUNT_SID"),
                    os.getenv("TWILIO_AUTH_TOKEN")
                ),
                timeout=30
            )
            if resp.status_code != 200:
                print(f"⚠️ Audio download failed: {resp.status_code}")
                return ""
            audio_b64 = base64.b64encode(resp.content).decode()

        # Sarvam STT
        result = _sarvam.speech.transcribe(
            audio=audio_b64,
            language_code=language_hint,
            model="saarika:v2"
        )
        return result.transcript or ""

    except Exception as e:
        print(f"⚠️ STT failed: {e}")
        return ""


# ── TTS — text → audio bytes ──────────────────────────────
async def text_to_speech(
    text: str,
    language: str = "hi-IN"
) -> bytes | None:
    """
    Convert text to speech for voice responses.
    Only called for voice message replies.
    Strips markdown before sending.
    """
    import re
    clean = re.sub(r'[*_#\[\]`]', '', text)[:500]

    try:
        response = _sarvam.tts.convert(
            text=clean,
            target_language_code=language,
            speaker="arvind",
            model="bulbul:v1"
        )
        if hasattr(response, 'audios') and response.audios:
            return base64.b64decode(response.audios[0])
    except Exception as e:
        print(f"⚠️ TTS failed: {e}")
    return None