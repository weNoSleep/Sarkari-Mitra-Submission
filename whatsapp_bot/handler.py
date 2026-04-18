# ============================================================
# handler.py
# Core message logic — thin orchestrator
# All intelligence delegated to databricks_client.py
# ============================================================

import asyncio
from memory import (
    get_session, save_session, add_to_history,
    clear_session, save_profile_longterm, load_profile_longterm
)
from sarvam_client import (
    translate_to_english, translate_response,
    speech_to_text, text_to_speech
)
from databricks_client import (
    EMPTY_PROFILE, ALL_INTENTS,
    INTENT_DISCOVERY, INTENT_DETAIL,
    INTENT_FOLLOWUP, INTENT_GREETING,
    INTENT_OFFTOPIC, INTENT_UNCLEAR,
    route_and_extract, sql_match,
    generate_action_plan, answer_followup,
    fetch_scheme_details
)

# ── Canned responses (WhatsApp formatted) ─────────────────

WELCOME_NEW = """🙏 *Namaste! Main Sarkari-Mitra hoon!*

Main aapko sarkari yojanaon ke baare mein guide karta hoon.

Bas apni situation batayein:
_"Main MP ka kisan hoon, BPL card hai, 2 bachche hain"_

Main bataunga:
✅ Aap kaunsi schemes ke liye eligible hain
📋 Kaunse documents pehle banwaiye
🗓️ Kaunsi scheme pehle apply karein

Hindi, English, ya apni bhasha mein likhein! 😊

Type *help* kabhi bhi madad ke liye."""

WELCOME_BACK = """🙏 *Wapas aye! Aapko yaad hai mujhe.*

Aapki profile: *{state}*{extra}

Kya naya poochna hai? Ya *reset* type karein nayi shuruwat ke liye."""

OFF_TOPIC = """Maafi chahta hoon! 🙏

Main sirf *sarkari yojanaon* ke baare mein help kar sakta hoon.

Koi government scheme ke baare mein poochna ho toh zaroor batayein! 😊"""

UNCLEAR = """Thoda aur samjha dijiye? 🙏

Jaise:
- _"Main 40 saal ka hoon, MP se, BPL card hai"_
- _"PM Ujjwala ke documents kya chahiye?"_
- _"Kisan ke liye koi subsidy?"_"""

RESET_DONE = "✅ Aapki profile clear kar di. Nayi shuruwat karein! 😊"

VOICE_FAIL = ("Aapki awaaz sun nahi paya. 🙏\n"
              "Kripya text mein likhein.")

CLARIFY_QUESTIONS = {
    "state":          "Aap kaunse *राज्य (state)* mein rehte hain?",
    "occupation":     "Aap kya kaam karte hain? (Jaise: kisan, student, mazdoor)",
    "has_bpl_card":   "Kya aapke paas *BPL card* hai? (Haan / Nahi)",
    "income_annual":  "Aapki *saalana aay* (annual income) kitni hai approximately?",
    "category":       "Aap kis *category* se hain? SC / ST / OBC / General",
    "age":            "Aapki *umar (age)* kya hai?",
}


async def handle_message(
    phone:     str,
    message:   str   = None,
    audio_url: str   = None,
) -> dict:
    """
    Main entry point. Returns:
    {"text": str, "audio": bytes|None, "send_voice": bool}
    """
    session    = get_session(phone)
    send_voice = False
    audio_out  = None

    # ── Handle voice message ──────────────────────────────
    if audio_url:
        transcribed = await speech_to_text(audio_url)
        if not transcribed:
            return {"text": VOICE_FAIL, "audio": None}
        message    = transcribed
        send_voice = True   # Reply with voice to voice messages

    if not message or not message.strip():
        return {"text": UNCLEAR, "audio": None}

    # ── Handle commands ───────────────────────────────────
    cmd = message.strip().lower()
    if cmd in ("reset", "clear", "nayi shuruwat", "/reset"):
        clear_session(phone)
        return {"text": RESET_DONE, "audio": None}
    if cmd in ("help", "madad", "/help", "?"):
        return {"text": WELCOME_NEW, "audio": None}

    # ── First-time user: load long-term profile ───────────
    is_first_turn = session.get("turn", 0) == 0
    if is_first_turn:
        saved = await load_profile_longterm(phone)
        if saved:
            session["profile"] = saved
            state    = saved.get("state", "")
            category = saved.get("category", "")
            extra    = f", {category}" if category else ""
            welcome  = WELCOME_BACK.format(state=state, extra=extra)
            session  = add_to_history(session, "assistant", welcome)
            session["turn"] = 1
            save_session(phone, session)
            return {"text": welcome, "audio": None}
        else:
            # Brand new user
            session = add_to_history(session, "assistant", WELCOME_NEW)
            session["turn"] = 1
            save_session(phone, session)
            return {"text": WELCOME_NEW, "audio": None}

    # ── Translate input to English ────────────────────────
    translated, source_lang = translate_to_english(message)
    session["source_lang"]  = source_lang

    # Log user message
    session = add_to_history(session, "user", message)

    # ── Route + Profile extraction (1 LLM call) ───────────
    result = await route_and_extract(
        user_input=translated,
        existing_profile=session.get("profile", {}),
        conversation_history=session.get("history", [])
    )
    intent           = result["intent"]
    session["profile"] = result["profile"]
    session["turn"]    = session.get("turn", 0) + 1

    # ── Generate response ─────────────────────────────────
    response = ""

    if intent == INTENT_GREETING:
        if session.get("schemes"):
            response = (
                "Namaste! 😊\n\n"
                "Aap pehle wali schemes ke baare mein aur jaanna chahte hain?\n"
                "Ya naya sawaal poochna hai?"
            )
        else:
            response = WELCOME_NEW

    elif intent == INTENT_OFFTOPIC:
        response = OFF_TOPIC

    elif intent == INTENT_UNCLEAR:
        response = UNCLEAR

    elif intent in (INTENT_DETAIL, INTENT_FOLLOWUP):
        # Follow-up question about a scheme
        schemes = session.get("schemes", [])
        if schemes:
            active = session.get("active_scheme") or schemes[0]
            scheme_name = active.get("scheme_name", "this scheme")
            scheme_id   = active.get("scheme_id", "")
            response = await answer_followup(
                question=translated,
                scheme_id=scheme_id,
                scheme_name=scheme_name
            )
        else:
            # No schemes in context — do discovery
            intent = INTENT_DISCOVERY
            # Fall through to discovery below

    if intent == INTENT_DISCOVERY or (
        intent in (INTENT_DETAIL, INTENT_FOLLOWUP)
        and not session.get("schemes")
    ):
        profile = session["profile"]

        # Slot-filling — ask clarifying question first
        clarify_field = profile.get("clarify_field")
        if clarify_field and clarify_field in CLARIFY_QUESTIONS:
            # Only ask clarifying question in first 3 turns
            if session.get("turn", 0) <= 4 and not profile.get("state"):
                response = CLARIFY_QUESTIONS[clarify_field]
            else:
                # Proceed with what we have
                pass

        if not response:
            matched = await sql_match(profile, limit=10)

            if matched:
                session["schemes"] = [
                    {
                        "scheme_id":   s.get("scheme_id", s.get("slug", "")),
                        "scheme_name": s.get("scheme_name", "")
                    }
                    for s in matched
                ]
                session["active_scheme"] = session["schemes"][0]

                response = await generate_action_plan(
                    profile=profile,
                    schemes=matched,
                    user_input=translated
                )

                # Save profile to long-term Delta storage
                asyncio.create_task(
                    save_profile_longterm(phone, profile)
                )
            else:
                response = (
                    "Aapki profile ke liye abhi koi scheme nahi mili. 🙏\n\n"
                    "Kripya batayein:\n"
                    "• Aap kaunse *state* mein hain?\n"
                    "• *Category* kya hai? (SC/ST/OBC/General)\n"
                    "• Kya *BPL card* hai?"
                )

    # ── Translate response back ───────────────────────────
    if source_lang not in ("en", "hi"):
        response = translate_response(response, source_lang)

    # ── Generate voice response if needed ────────────────
    if send_voice and response:
        lang_code = {
            "hi": "hi-IN", "te": "te-IN", "ta": "ta-IN",
            "kn": "kn-IN", "ml": "ml-IN", "en": "en-IN"
        }.get(source_lang, "hi-IN")
        audio_out = await text_to_speech(response, lang_code)

    # ── Update session ────────────────────────────────────
    session = add_to_history(session, "assistant", response)
    save_session(phone, session)

    return {
        "text":  response,
        "audio": audio_out
    }