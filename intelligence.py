# COMMAND ----------
import os
from dotenv import load_dotenv

load_dotenv()

DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN")
SARVAM_API_KEY   = os.environ.get("SARVAM_API_KEY")

DATABRICKS_BASE_URL = "https://7474653884759133.ai-gateway.cloud.databricks.com/mlflow/v1"
DATABRICKS_MODEL    = "llama-endpoint"

assert DATABRICKS_TOKEN, "❌ DATABRICKS_TOKEN not found"
assert SARVAM_API_KEY,   "❌ SARVAM_API_KEY not found"

print("✅ Environment loaded")

# COMMAND ----------
import json
import time
from openai import OpenAI
from typing import Generator, Optional

_llm = OpenAI(
    api_key=DATABRICKS_TOKEN,
    base_url=DATABRICKS_BASE_URL
)

MAX_TOKENS   = 1024
TEMPERATURE  = 0.2
MAX_RETRIES  = 3
RETRY_DELAY  = 2


def call_llm(
    system_prompt: str,
    user_message: str,
    max_tokens: int = MAX_TOKENS,
    temperature: float = TEMPERATURE,
    json_mode: bool = False
) -> str:
    if json_mode:
        system_prompt += (
            "\n\nCRITICAL: Your response must be a single valid JSON object. "
            "No explanation. No markdown. No backticks. Just the JSON."
        )
    last_error = None
    for attempt in range(MAX_RETRIES):
        try:
            response = _llm.chat.completions.create(
                model=DATABRICKS_MODEL,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user",   "content": user_message}
                ],
                max_tokens=max_tokens,
                temperature=temperature
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            last_error = e
            wait = RETRY_DELAY * (attempt + 1)
            print(f"⚠️ LLM attempt {attempt+1} failed: {e}. Retrying in {wait}s...")
            time.sleep(wait)
    raise RuntimeError(f"❌ LLM failed after {MAX_RETRIES} attempts: {last_error}")


def call_llm_stream(
    system_prompt: str,
    user_message: str,
    max_tokens: int = MAX_TOKENS,
    temperature: float = TEMPERATURE
) -> Generator[str, None, None]:
    stream = _llm.chat.completions.create(
        model=DATABRICKS_MODEL,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": user_message}
        ],
        max_tokens=max_tokens,
        temperature=temperature,
        stream=True
    )
    for chunk in stream:
        delta = chunk.choices[0].delta.content
        if delta:
            yield delta


def extract_json(text: str) -> Optional[dict]:
    text = text.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        inner = lines[1:-1] if lines[-1].strip() == "```" else lines[1:]
        text = "\n".join(inner).strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    import re
    match = re.search(r'\{.*\}', text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass
    print(f"⚠️ extract_json failed on: {text[:200]}")
    return None


print("✅ LLM client loaded")

# COMMAND ----------
from sarvamai import SarvamAI

_sarvam = SarvamAI(api_subscription_key=SARVAM_API_KEY)

LANGUAGE_CODES = {
    "hi": "hi-IN",
    "te": "te-IN",
    "ta": "ta-IN",
    "kn": "kn-IN",
    "ml": "ml-IN",
    "mr": "mr-IN",
    "gu": "gu-IN",
    "pa": "pa-IN",
    "bn": "bn-IN",
    "or": "or-IN",
    "en": "en-IN",
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

LANGUAGE_NAMES = {
    "en": "English",
    "hi": "Hindi",
    "te": "Telugu",
    "ta": "Tamil",
    "kn": "Kannada",
    "ml": "Malayalam",
    "mr": "Marathi",
    "gu": "Gujarati",
    "bn": "Bengali",
    "pa": "Punjabi",
    "or": "Odia",
}


def detect_language(text: str) -> str:
    scores = {}
    for lang, (start, end) in SCRIPT_RANGES.items():
        scores[lang] = sum(1 for c in text if start <= c <= end)
    best_lang = max(scores, key=scores.get)
    print(f"🔍 Script scores: {scores}")
    if scores[best_lang] > 0:
        return best_lang
    return "en"


def translate_to_english(text: str) -> tuple:
    """
    Detects language and translates to English if needed.
    - English → pass through, return ("en")
    - Hindi → pass through, return ("hi") — Llama handles Hindi natively
    - All other Indic scripts → translate via Sarvam, return lang code
    """
    source_lang = detect_language(text)
    print(f"🌐 Detected language: {source_lang}")

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
        print(f"✅ Sarvam translated {source_lang}→en: {response.translated_text[:60]}")
        return response.translated_text, source_lang
    except Exception as e:
        print(f"⚠️ Sarvam input translation failed ({source_lang}→en): {e}")
        return text, source_lang


def translate_response(text: str, target_lang: str) -> str:
    """
    Translates English response back to user's language via Sarvam.
    English and Hindi pass through — handled natively by Llama.
    """
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
        print(f"✅ Sarvam translated en→{target_lang}")
        return response.translated_text
    except Exception as e:
        print(f"⚠️ Sarvam response translation failed (en→{target_lang}): {e}")
        return text


print("✅ Sarvam translation client loaded")

# COMMAND ----------
INTENT_DISCOVERY = "SCHEME_DISCOVERY"
INTENT_DETAIL    = "SCHEME_DETAIL"
INTENT_FOLLOWUP  = "APPLICATION_HELP"
INTENT_GREETING  = "GREETING"
INTENT_OFFTOPIC  = "OFF_TOPIC"
INTENT_UNCLEAR   = "UNCLEAR"

ALL_INTENTS = [
    INTENT_DISCOVERY, INTENT_DETAIL,
    INTENT_FOLLOWUP, INTENT_GREETING,
    INTENT_OFFTOPIC, INTENT_UNCLEAR
]

EMPTY_PROFILE = {
    "state":          None,
    "age":            None,
    "gender":         None,
    "income_annual":  None,
    "category":       None,
    "occupation":     None,
    "has_bpl_card":   None,
    "specific_need":  None,
    "clarify_field":  None,
}

print("✅ Constants loaded")

# COMMAND ----------
ROUTER_PROFILE_SYSTEM = """You are an AI assistant helping Indian citizens find government welfare schemes.

Analyze the user message and return a JSON object with intent and profile.

INTENT must be exactly one of:
- SCHEME_DISCOVERY   → user wants to find schemes
- SCHEME_DETAIL      → user asks about a specific named scheme
- APPLICATION_HELP   → user asks how to apply or what documents are needed
- GREETING           → hello, hi, namaste, thanks
- OFF_TOPIC          → not related to government schemes
- UNCLEAR            → cannot determine

IMPORTANT: If the user asks to change language — treat this as UNCLEAR.

PROFILE fields (null if not mentioned):
- state: Indian state name in English
- age: integer
- gender: "male" / "female" / "other" / null
- income_annual: integer INR/year (multiply monthly × 12)
- category: "SC" / "ST" / "OBC" / "General" / null
- occupation: "farmer" / "student" / "self_employed" / "laborer" / "unemployed" / "other" / null
- has_bpl_card: true / false / null
- specific_need: "housing" / "education" / "health" / "agriculture" / "employment" / "women" / "disability" / "other" / null
- clarify_field: most important missing field, priority: state > occupation > has_bpl_card > income_annual > category > age

RULES:
- "BPL card hai" → has_bpl_card: true
- "kisan hoon" / "farmer" → occupation: "farmer"
- "MP se hoon" → state: "Madhya Pradesh"
- Monthly income → multiply by 12
- GREETING and OFF_TOPIC → all profile fields null

Return ONLY valid JSON:
{
  "intent": "SCHEME_DISCOVERY",
  "profile": {
    "state": null,
    "age": null,
    "gender": null,
    "income_annual": null,
    "category": null,
    "occupation": null,
    "has_bpl_card": null,
    "specific_need": null,
    "clarify_field": "state"
  }
}"""


def build_router_user_message(
    user_input: str,
    existing_profile: dict = None,
    conversation_history: list = None
) -> str:
    parts = []
    if existing_profile:
        known = {k: v for k, v in existing_profile.items() if v is not None}
        if known:
            parts.append(f"ALREADY KNOWN ABOUT USER: {json.dumps(known, ensure_ascii=False)}")
    if conversation_history and len(conversation_history) > 0:
        recent = conversation_history[-2:]
        history_text = "\n".join([
            f"{turn['role'].upper()}: {turn['content']}"
            for turn in recent
        ])
        parts.append(f"RECENT CONVERSATION:\n{history_text}")
    parts.append(f"NEW USER MESSAGE: {user_input}")
    return "\n\n".join(parts)


def route_and_extract(
    user_input: str,
    existing_profile: dict = None,
    conversation_history: list = None
) -> dict:
    if existing_profile is None:
        existing_profile = EMPTY_PROFILE.copy()

    user_message = build_router_user_message(
        user_input, existing_profile, conversation_history
    )

    try:
        raw = call_llm(
            system_prompt=ROUTER_PROFILE_SYSTEM,
            user_message=user_message,
            max_tokens=512,
            temperature=0.1,
            json_mode=True
        )
        parsed = extract_json(raw)

        if parsed is None:
            return {"intent": INTENT_UNCLEAR, "profile": existing_profile, "raw": raw, "error": "JSON parse failed"}

        intent = parsed.get("intent", INTENT_UNCLEAR)
        if intent not in ALL_INTENTS:
            intent = INTENT_UNCLEAR

        new_profile_data = parsed.get("profile", {})
        merged_profile = existing_profile.copy()
        for field, value in new_profile_data.items():
            if value is not None:
                merged_profile[field] = value

        return {"intent": intent, "profile": merged_profile, "raw": raw, "error": None}

    except Exception as e:
        return {"intent": INTENT_UNCLEAR, "profile": existing_profile, "raw": None, "error": str(e)}


def get_clarifying_question(profile: dict, source_lang: str = "en") -> str:
    clarify_field = profile.get("clarify_field")

    questions_en = {
        "state":         "Which state are you from?",
        "occupation":    "What is your occupation? (farmer, student, laborer, self-employed)",
        "has_bpl_card":  "Do you have a BPL card?",
        "income_annual": "What is your approximate annual income in INR?",
        "category":      "What is your caste category? (SC, ST, OBC, or General)",
        "age":           "What is your age?",
    }

    questions_hi = {
        "state":         "आप किस राज्य से हैं?",
        "occupation":    "आप क्या काम करते हैं? (किसान, छात्र, मजदूर, स्वरोजगार)",
        "has_bpl_card":  "क्या आपके पास BPL कार्ड है?",
        "income_annual": "आपकी सालाना आय लगभग कितनी है?",
        "category":      "आप किस जाति वर्ग से हैं? (SC, ST, OBC, या General)",
        "age":           "आपकी उम्र क्या है?",
    }

    questions = questions_hi if source_lang == "hi" else questions_en

    if clarify_field and clarify_field in questions:
        return questions[clarify_field]
    if not profile.get("state"):
        return questions["state"]
    return None


LANGUAGE_SWITCH_MAP = {
    "english": "en",
    "hindi":   "hi",
    "telugu":  "te",
    "tamil":   "ta",
    "kannada": "kn",
    "malayalam": "ml",
    "marathi": "mr",
    "gujarati": "gu",
    "bengali": "bn",
    "punjabi": "pa",
    "odia":    "or",
}


def detect_language_switch(message: str) -> str:
    msg_lower = message.lower()
    for lang_name, lang_code in LANGUAGE_SWITCH_MAP.items():
        if lang_name in msg_lower:
            return lang_code
    return None


print("✅ Router loaded")

# COMMAND ----------
import numpy as np
from sentence_transformers import SentenceTransformer
import faiss

EMBEDDING_MODEL_NAME = "paraphrase-multilingual-MiniLM-L12-v2"
_embedder = SentenceTransformer(EMBEDDING_MODEL_NAME)

FAISS_INDEX_PATH  = "/Volumes/sarkarimitracatalog/sarkaridatabase/sarkari_files/faiss_index.bin"
CHUNK_ID_MAP_PATH = "/Volumes/sarkarimitracatalog/sarkaridatabase/sarkari_files/chunk_id_map.json"

_faiss_index = faiss.read_index(FAISS_INDEX_PATH)

with open(CHUNK_ID_MAP_PATH, "r") as f:
    _chunk_id_map = json.load(f)

print(f"✅ FAISS index loaded — {_faiss_index.ntotal} vectors")

# COMMAND ----------
def build_semantic_query(profile: dict, user_input: str) -> str:
    parts = [user_input]
    if profile.get("occupation"):    parts.append(f"{profile['occupation']} scheme")
    if profile.get("specific_need"): parts.append(f"{profile['specific_need']} benefit")
    if profile.get("state"):         parts.append(f"{profile['state']} government scheme")
    if profile.get("has_bpl_card"):  parts.append("BPL below poverty line scheme")
    if profile.get("category"):      parts.append(f"{profile['category']} category scheme")
    if profile.get("gender") == "female": parts.append("women scheme")
    if profile.get("age"):           parts.append(f"age {profile['age']}")
    return " ".join(parts)


def faiss_match(
    profile: dict,
    user_input: str,
    top_k: int = 30,
    section_filter: str = None
) -> list:
    query     = build_semantic_query(profile, user_input)
    query_vec = _embedder.encode([query], normalize_embeddings=True).astype("float32")
    search_k  = top_k * 3 if section_filter else top_k
    scores, indices = _faiss_index.search(query_vec, search_k)

    chunk_ids_list  = _chunk_id_map['chunk_ids']
    chunk_meta_list = _chunk_id_map['chunk_meta']

    results      = []
    seen_schemes = set()

    for score, idx in zip(scores[0], indices[0]):
        if idx == -1 or idx >= len(chunk_ids_list):
            continue
        chunk_id  = chunk_ids_list[idx]
        meta      = chunk_meta_list[idx]
        scheme_id = meta.get('slug', chunk_id.split("_")[0])
        section   = meta.get('source_column', 'details')

        if section_filter and section != section_filter:
            continue
        if section_filter is None and scheme_id in seen_schemes:
            continue
        seen_schemes.add(scheme_id)

        results.append({
            "scheme_id": scheme_id,
            "chunk_id":  chunk_id,
            "section":   section,
            "score":     float(score),
            "source":    "faiss"
        })
        if len(results) >= top_k:
            break

    return results


def fetch_scheme_details(scheme_ids: list) -> dict:
    if not scheme_ids:
        return {}
    ids_str = ", ".join([f"'{sid}'" for sid in scheme_ids])
    query = f"""
        SELECT slug AS scheme_id, scheme_name, schemeCategory,
               level, benefits, eligibility,
               application, documents, applicable_states
        FROM sarkarimitracatalog.sarkaridatabase.schemes_structured
        WHERE slug IN ({ids_str})
    """
    try:
        df = spark.sql(query)
        result = {}
        for row in df.collect():
            result[row["scheme_id"]] = row.asDict()
        return result
    except Exception as e:
        print(f"⚠️ fetch_scheme_details failed: {e}")
        return {}


def hybrid_match(profile: dict, user_input: str, top_n: int = 10) -> list:
    faiss_results = faiss_match(profile, user_input, top_k=50)
    if not faiss_results:
        return []

    ranked_ids = [r["scheme_id"] for r in faiss_results[:top_n]]
    details    = fetch_scheme_details(ranked_ids)

    final = []
    for r in faiss_results[:top_n]:
        sid    = r["scheme_id"]
        detail = details.get(sid, {})
        final.append({
            "scheme_id":   sid,
            "scheme_name": detail.get("scheme_name", sid),
            "category":    detail.get("schemeCategory", ""),
            "level":       detail.get("level", ""),
            "benefits":    detail.get("benefits", ""),
            "eligibility": detail.get("eligibility", ""),
            "application": detail.get("application", ""),
            "documents":   detail.get("documents", ""),
            "score":       round(r["score"], 3),
            "sources":     ["faiss"]
        })

    return final


print("✅ Matcher loaded")

# COMMAND ----------
ACTION_PLAN_SYSTEM_EN = """You are Sarkari Mitra, a trusted advisor helping Indian citizens navigate government schemes.

Create a PRACTICAL ACTION PLAN in ENGLISH ONLY.

FORMAT:
**Schemes Found For You**

**Apply This Week (Quick Wins):**
1. [Scheme Name] — [benefit amount if known]
   → Why first: [reason]
   → Documents needed: [specific docs]

**Apply This Month:**
2. [Scheme Name] — [benefit]
   → Documents needed: [docs]

**Common Documents (submit once for all schemes):**
- Aadhaar card
- [other common docs]

**Helplines:** PM Kisan: 155261 | General: 1800-11-0001

RULES:
- Respond in ENGLISH ONLY
- Always mention specific benefit amounts (₹6000/year etc.) where known
- List common documents ONCE at the bottom
- Maximum 5 schemes
- Never invent scheme names or amounts not in the data
- Always name at least one scheme explicitly"""

ACTION_PLAN_SYSTEM_HI = """You are Sarkari Mitra, a trusted advisor helping Indian citizens navigate government schemes.

Create a PRACTICAL ACTION PLAN in HINDI ONLY.

FORMAT:
**आपके लिए योजनाएं**

**तुरंत करें (इस हफ्ते आवेदन करें):**
1. [योजना का नाम] — [लाभ राशि]
   → पहले क्यों: [कारण]
   → दस्तावेज़: [इस योजना के लिए जरूरी दस्तावेज़]

**बाद में करें (इस महीने):**
2. [योजना का नाम] — [लाभ]
   → दस्तावेज़: [दस्तावेज़]

**सभी योजनाओं के लिए जरूरी दस्तावेज़ (एक बार जमा करें):**
- आधार कार्ड
- [अन्य दस्तावेज़]

**हेल्पलाइन:** PM Kisan: 155261 | सामान्य: 1800-11-0001

RULES:
- HINDI ONLY में जवाब दें
- लाभ राशि जरूर बताएं जहां पता हो (जैसे ₹6000/साल)
- दस्तावेज़ एक बार ही लिखें
- अधिकतम 5 योजनाएं
- योजना के नाम या राशि न बनाएं जो डेटा में न हो
- कम से कम एक योजना का नाम जरूर लिखें"""


def build_action_plan_prompt(profile: dict, schemes: list, user_input: str) -> str:
    profile_parts = []
    if profile.get("state"):         profile_parts.append(f"State: {profile['state']}")
    if profile.get("age"):           profile_parts.append(f"Age: {profile['age']}")
    if profile.get("gender"):        profile_parts.append(f"Gender: {profile['gender']}")
    if profile.get("occupation"):    profile_parts.append(f"Occupation: {profile['occupation']}")
    if profile.get("category"):      profile_parts.append(f"Category: {profile['category']}")
    if profile.get("income_annual"): profile_parts.append(f"Annual Income: ₹{profile['income_annual']}")
    if profile.get("has_bpl_card"):  profile_parts.append("Has BPL card: Yes")
    if profile.get("specific_need"): profile_parts.append(f"Looking for: {profile['specific_need']}")

    profile_str = "\n".join(profile_parts) if profile_parts else "No profile info"

    top_schemes = schemes[:5]
    schemes_parts = []
    for i, s in enumerate(top_schemes, 1):
        schemes_parts.append(f"""
SCHEME {i}: {s.get('scheme_name', 'Unknown')}
  Category: {s.get('category', 'N/A')}
  Level: {s.get('level', 'N/A')}
  Benefits: {s.get('benefits', 'N/A')[:300]}
  Eligibility: {s.get('eligibility', 'N/A')[:200]}
  Documents: {s.get('documents', 'N/A')[:300]}
  Score: {s.get('score', 0)}
""")

    return f"""CITIZEN PROFILE:
{profile_str}

USER QUESTION: {user_input}

MATCHED SCHEMES:
{"".join(schemes_parts)}

Generate the action plan now. You MUST name the schemes from the list above."""


def generate_action_plan(
    profile: dict,
    schemes: list,
    user_input: str,
    source_lang: str = "en"
) -> str:
    if not schemes:
        if source_lang == "hi":
            return "माफ़ कीजिए, कोई योजना नहीं मिली। कृपया अपना राज्य, काम और आय बताएं।"
        return "Sorry, no schemes found. Please share your state, occupation, and income."

    prompt = build_action_plan_prompt(profile, schemes, user_input)
    system = ACTION_PLAN_SYSTEM_HI if source_lang == "hi" else ACTION_PLAN_SYSTEM_EN

    return call_llm(
        system_prompt=system,
        user_message=prompt,
        max_tokens=1024,
        temperature=0.3
    )


print("✅ Action plan generator loaded")

# COMMAND ----------
FOLLOWUP_SYSTEM_EN = """You are Sarkari Mitra, helping an Indian citizen with a specific question about a government scheme.

Answer ONLY based on the provided context. Do not add information not in the context.
Respond in ENGLISH ONLY.
If context does not contain the answer, say so honestly.

Format:
- Use bullet points for documents or steps
- Max 150 words
- Include amounts or deadlines if mentioned
- End with relevant helpline number"""

FOLLOWUP_SYSTEM_HI = """You are Sarkari Mitra, helping an Indian citizen with a specific question about a government scheme.

Answer ONLY based on the provided context. Do not add information not in the context.
HINDI ONLY में जवाब दें।
अगर context में जवाब नहीं है तो सच बताएं।

Format:
- दस्तावेज़ या steps के लिए bullet points इस्तेमाल करें
- अधिकतम 150 शब्द
- राशि या deadline का जिक्र करें अगर context में हो
- अंत में helpline number जरूर दें"""

SECTION_KEYWORDS = {
    "documents": [
        "document", "दस्तावेज़", "kagaz", "certificate", "proof",
        "id", "aadhaar", "aadhar", "photo", "kyc", "required", "chahiye"
    ],
    "application": [
        "apply", "application", "कैसे", "kaise", "process", "form",
        "register", "portal", "website", "office", "submit", "where", "steps"
    ],
    "benefits": [
        "benefit", "fayda", "kitna", "how much", "amount", "paisa",
        "money", "rupee", "₹", "milega", "milegi"
    ],
    "eligibility": [
        "eligible", "qualify", "yogya", "kaun", "who", "criteria",
        "condition", "requirement", "age", "income"
    ]
}


def detect_section(question: str) -> str:
    question_lower = question.lower()
    section_scores = {}
    for section, keywords in SECTION_KEYWORDS.items():
        section_scores[section] = sum(1 for kw in keywords if kw in question_lower)
    best = max(section_scores, key=section_scores.get)
    return best if section_scores[best] > 0 else None


def fetch_chunks_for_scheme(scheme_id: str, section: str = None, limit: int = 5) -> list:
    section_filter = f"AND source_column = '{section}'" if section else ""
    query = f"""
        SELECT chunk_id, source_column as section, chunk_text
        FROM sarkarimitracatalog.sarkaridatabase.scheme_chunks
        WHERE slug = '{scheme_id}'
        {section_filter}
        LIMIT {limit}
    """
    try:
        df = spark.sql(query)
        return [{"chunk_id": r["chunk_id"], "section": r["section"], "text": r["chunk_text"]}
                for r in df.collect()]
    except Exception as e:
        print(f"⚠️ fetch_chunks_for_scheme failed: {e}")
        return []


def retrieve_context(question: str, scheme_id: str, top_k: int = 4) -> tuple:
    section = detect_section(question)

    faiss_results = faiss_match(
        profile=EMPTY_PROFILE,
        user_input=question,
        top_k=top_k * 3,
        section_filter=section
    )
    scheme_chunks_faiss = [r for r in faiss_results if r["scheme_id"] == scheme_id]

    if scheme_chunks_faiss:
        chunk_ids = [r["chunk_id"] for r in scheme_chunks_faiss[:top_k]]
        ids_str = ", ".join([f"'{cid}'" for cid in chunk_ids])
        try:
            df = spark.sql(f"""
                SELECT chunk_text FROM sarkarimitracatalog.sarkaridatabase.scheme_chunks
                WHERE chunk_id IN ({ids_str})
            """)
            texts = [row["chunk_text"] for row in df.collect()]
            if texts:
                return "\n\n".join(texts), "faiss"
        except Exception as e:
            print(f"⚠️ FAISS chunk fetch failed: {e}")

    sql_chunks = fetch_chunks_for_scheme(scheme_id, section, limit=top_k)
    if sql_chunks:
        return "\n\n".join([c["text"] for c in sql_chunks]), "sql"

    return None, "none"


def answer_followup(
    question: str,
    scheme_id: str,
    scheme_name: str,
    source_lang: str = "en"
) -> str:
    context, source = retrieve_context(question, scheme_id)

    if context is None or source == "none":
        if source_lang == "hi":
            return (
                f"माफ़ कीजिए, '{scheme_name}' के बारे में जानकारी नहीं मिली।\n"
                f"हेल्पलाइन से संपर्क करें:\n"
                f"• सामान्य: 1800-11-0001\n"
                f"• PM Kisan: 155261"
            )
        return (
            f"Sorry, I couldn't find specific information about '{scheme_name}'.\n"
            f"Please contact:\n"
            f"• General Helpline: 1800-11-0001\n"
            f"• PM Kisan: 155261"
        )

    system = FOLLOWUP_SYSTEM_HI if source_lang == "hi" else FOLLOWUP_SYSTEM_EN

    user_message = f"""SCHEME: {scheme_name}
QUESTION: {question}
CONTEXT: {context}

Answer based only on the context above."""

    try:
        return call_llm(
            system_prompt=system,
            user_message=user_message,
            max_tokens=512,
            temperature=0.2
        )
    except Exception as e:
        return "Sorry, something went wrong. Please call 1800-11-0001."


print("✅ Follow-up RAG loaded")
print("✅ Intelligence layer fully loaded")