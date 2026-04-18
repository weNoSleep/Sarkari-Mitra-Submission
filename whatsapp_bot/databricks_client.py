# All Databricks calls (aligned to your notebooks)
# ============================================================
# databricks_client.py
# Thin HTTP client — mirrors your notebook logic exactly
# No reimplementation — calls same endpoints your notebooks use
# ============================================================

import os
import json
import httpx
import time
import re
from dotenv import load_dotenv

load_dotenv()

# ── Config — exact same as your 10_llm_client ────────────
DATABRICKS_TOKEN    = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_HOST     = os.getenv("DATABRICKS_HOST")
DATABRICKS_WH_ID    = os.getenv("DATABRICKS_WAREHOUSE_ID")
LLM_BASE_URL        = os.getenv("LLM_ENDPOINT",
    "https://7474653884759133.ai-gateway.cloud.databricks.com/mlflow/v1")
DATABRICKS_MODEL    = "llama-endpoint"

LLM_HEADERS = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type":  "application/json"
}
SQL_HEADERS = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type":  "application/json"
}

# ── Intent constants — exact same as your 99_constants ────
INTENT_DISCOVERY = "SCHEME_DISCOVERY"
INTENT_DETAIL    = "SCHEME_DETAIL"
INTENT_FOLLOWUP  = "APPLICATION_HELP"
INTENT_GREETING  = "GREETING"
INTENT_OFFTOPIC  = "OFF_TOPIC"
INTENT_UNCLEAR   = "UNCLEAR"
ALL_INTENTS      = [
    INTENT_DISCOVERY, INTENT_DETAIL, INTENT_FOLLOWUP,
    INTENT_GREETING, INTENT_OFFTOPIC, INTENT_UNCLEAR
]

# ── Empty profile — exact same as your 11_router_profile ──
EMPTY_PROFILE = {
    "state":         None,
    "age":           None,
    "gender":        None,
    "income_annual": None,
    "category":      None,
    "occupation":    None,
    "has_bpl_card":  None,
    "specific_need": None,
    "clarify_field": None,
}

# ── JSON extraction — exact copy from your 10_llm_client ──
def extract_json(text: str) -> dict | None:
    text = text.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        inner = lines[1:-1] if lines[-1].strip() == "```" else lines[1:]
        text  = "\n".join(inner).strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    match = re.search(r'\{.*\}', text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass
    return None


# ── LLM call — async version of your call_llm() ──────────
async def call_llm(
    system_prompt: str,
    user_message: str,
    max_tokens: int = 1024,
    temperature: float = 0.2,
    json_mode: bool = False
) -> str:
    if json_mode:
        system_prompt += (
            "\n\nCRITICAL: Your response must be a single valid JSON object. "
            "No explanation. No markdown. No backticks. Just the JSON."
        )

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user",   "content": user_message}
    ]

    for attempt in range(3):
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.post(
                    f"{LLM_BASE_URL}/chat/completions",
                    headers=LLM_HEADERS,
                    json={
                        "model":       DATABRICKS_MODEL,
                        "messages":    messages,
                        "max_tokens":  max_tokens,
                        "temperature": temperature
                    }
                )
                resp.raise_for_status()
                return resp.json()["choices"][0]["message"]["content"].strip()
        except Exception as e:
            if attempt < 2:
                await asyncio.sleep(2 ** attempt)
                continue
            print(f"❌ LLM call failed: {e}")
            return ""

    return ""


DB_WORKSPACE = "https://dbc-9c147f6e-bf96.cloud.databricks.com"

async def run_sql(query: str, timeout: str = "30s") -> list[dict]:
    try:
        async with httpx.AsyncClient(timeout=45) as client:
            resp = await client.post(
                f"{DB_WORKSPACE}/api/2.0/sql/statements",
                headers=SQL_HEADERS,
                json={
                    "statement":    query,
                    "warehouse_id": DATABRICKS_WH_ID,
                    "wait_timeout": timeout,
                    "format":       "JSON_ARRAY"
                }
            )

            if resp.status_code != 200:
                print(f"⚠️ SQL HTTP {resp.status_code}: {resp.text[:300]}")
                return []

            data         = resp.json()
            statement_id = data.get("statement_id")
            status       = data.get("status", {}).get("state", "")

            while status in ("PENDING", "RUNNING"):
                await asyncio.sleep(1)
                poll   = await client.get(
                    f"{DB_WORKSPACE}/api/2.0/sql/statements/{statement_id}",
                    headers=SQL_HEADERS
                )
                data   = poll.json()
                status = data.get("status", {}).get("state", "")

            if status != "SUCCEEDED":
                print(f"⚠️ SQL failed: {data.get('status',{}).get('error',{})}")
                return []

            cols = [
                c["name"] for c in
                data.get("manifest", {})
                    .get("schema", {})
                    .get("columns", [])
            ]
            rows = data.get("result", {}).get("data_array", [])
            return [dict(zip(cols, row)) for row in rows]

    except Exception as e:
        print(f"⚠️ SQL error: {e}")
        return []
        
# ── Router + Profile — mirrors your route_and_extract() ──

# Exact system prompt from your 11_router_profile
ROUTER_PROFILE_SYSTEM = """You are an AI assistant helping Indian citizens find government welfare schemes.

Your job: analyze the user's message and return a JSON object with TWO things:
1. Their INTENT
2. Their PROFILE (what you can extract about them)

INTENT must be exactly one of:
- SCHEME_DISCOVERY   → user wants to find schemes they qualify for
- SCHEME_DETAIL      → user asks about a specific named scheme
- APPLICATION_HELP   → user asks how to apply, what documents needed
- GREETING           → hello, hi, namaste, thanks
- OFF_TOPIC          → not related to government schemes at all
- UNCLEAR            → cannot determine what they want

PROFILE fields (extract only what is mentioned, use null for unknown):
- state:         Indian state name in English (e.g. "Madhya Pradesh")
- age:           integer (years)
- gender:        "male" / "female" / "other" / null
- income_annual: integer in INR per year (convert monthly × 12 if needed)
- category:      "SC" / "ST" / "OBC" / "General" / null
- occupation:    "farmer" / "student" / "self_employed" / "laborer" / "unemployed" / "other" / null
- has_bpl_card:  true / false / null
- specific_need: "housing" / "education" / "health" / "agriculture" / "employment" / "women" / "disability" / "other" / null
- clarify_field: the SINGLE most important missing field to ask about next, or null if enough info

RULES:
- Input may be Hindi, English, Hinglish, or mixed. Understand all.
- "BPL card hai" → has_bpl_card: true
- "kisan hoon" / "farmer" → occupation: "farmer"
- "MP se hoon" → state: "Madhya Pradesh"
- Monthly income mentioned → multiply by 12 for income_annual
- clarify_field priority: state > occupation > has_bpl_card > income_annual > category > age

RETURN FORMAT — only valid JSON, nothing else:
{
  "intent": "SCHEME_DISCOVERY",
  "profile": {
    "state": "Madhya Pradesh",
    "age": null,
    "gender": null,
    "income_annual": null,
    "category": "OBC",
    "occupation": "farmer",
    "has_bpl_card": true,
    "specific_need": null,
    "clarify_field": "income_annual"
  }
}"""


async def route_and_extract(
    user_input: str,
    existing_profile: dict = None,
    conversation_history: list = None
) -> dict:
    """
    Async mirror of your route_and_extract() from 11_router_profile.
    Same prompt, same merging logic, same return structure.
    """
    if existing_profile is None:
        existing_profile = EMPTY_PROFILE.copy()

    # Build user message — same as build_router_user_message()
    parts = []
    known = {k: v for k, v in existing_profile.items() if v is not None}
    if known:
        parts.append(
            f"ALREADY KNOWN ABOUT USER: {json.dumps(known, ensure_ascii=False)}"
        )
    if conversation_history:
        recent = conversation_history[-2:]
        history_text = "\n".join([
            f"{t['role'].upper()}: {t['content']}"
            for t in recent
        ])
        parts.append(f"RECENT CONVERSATION:\n{history_text}")
    parts.append(f"NEW USER MESSAGE: {user_input}")
    user_message = "\n\n".join(parts)

    raw = await call_llm(
        system_prompt=ROUTER_PROFILE_SYSTEM,
        user_message=user_message,
        max_tokens=512,
        temperature=0.1,
        json_mode=True
    )

    parsed = extract_json(raw)
    if parsed is None:
        return {
            "intent":  INTENT_UNCLEAR,
            "profile": existing_profile,
            "error":   "JSON parse failed"
        }

    intent = parsed.get("intent", INTENT_UNCLEAR)
    if intent not in ALL_INTENTS:
        intent = INTENT_UNCLEAR

    # Merge — same logic as your route_and_extract()
    new_profile_data = parsed.get("profile", {})
    merged           = existing_profile.copy()
    for field, value in new_profile_data.items():
        if value is not None:
            merged[field] = value

    return {
        "intent":  intent,
        "profile": merged,
        "error":   None
    }


# ── SQL Eligibility Match — mirrors your sql_match() ─────

STATE_ABBREV = {
    "Madhya Pradesh": "MP",
    "Uttar Pradesh":  "UP",
    "Tamil Nadu":     "TN",
    "Karnataka":      "KARNATAKA",
}


def build_sql_filter(profile: dict) -> str:
    """Exact copy of build_sql_filter() from your 12_eligibility_matcher."""
    conditions = ["1=1"]

    if profile.get("state"):
        state  = profile["state"].replace("'", "''")
        abbrev = STATE_ABBREV.get(profile["state"], "")
        abbrev_clause = (
            f"OR array_contains(from_json(applicable_states, 'array<string>'), '{abbrev}')"
            if abbrev else ""
        )
        conditions.append(f"""(
            applicable_states IS NULL
            OR array_contains(from_json(applicable_states, 'array<string>'), '{state}')
            {abbrev_clause}
            OR array_contains(from_json(applicable_states, 'array<string>'), 'All States')
            OR array_contains(from_json(applicable_states, 'array<string>'), 'All states')
            OR array_contains(from_json(applicable_states, 'array<string>'), 'all')
            OR array_contains(from_json(applicable_states, 'array<string>'), 'any')
            OR array_contains(from_json(applicable_states, 'array<string>'), 'any state')
            OR array_contains(from_json(applicable_states, 'array<string>'), 'India')
        )""")

    if profile.get("age"):
        age = int(profile["age"])
        conditions.append(
            f"(age_min IS NULL OR age_min <= {age})"
        )
        conditions.append(
            f"(age_max IS NULL OR age_max >= {age})"
        )

    if profile.get("income_annual"):
        income = int(profile["income_annual"])
        conditions.append(
            f"(income_limit_inr IS NULL OR income_limit_inr >= {income})"
        )

    if profile.get("gender"):
        gender = profile["gender"].lower()
        conditions.append(f"""(
            gender_eligibility IS NULL
            OR gender_eligibility = 'all'
            OR LOWER(gender_eligibility) = '{gender}'
        )""")

    if profile.get("category"):
        cat = profile["category"].replace("'", "''")
        conditions.append(f"""(
            category_eligibility IS NULL
            OR array_contains(from_json(category_eligibility, 'array<string>'), '{cat}')
            OR array_contains(from_json(category_eligibility, 'array<string>'), 'any')
            OR array_contains(from_json(category_eligibility, 'array<string>'), 'All')
        )""")

    if profile.get("occupation"):
        occ = profile["occupation"].replace("'", "''")
        conditions.append(f"""(
            occupation_types IS NULL
            OR array_contains(from_json(occupation_types, 'array<string>'), '{occ}')
            OR array_contains(from_json(occupation_types, 'array<string>'), 'any')
        )""")

    return " AND ".join(conditions)


async def sql_match(profile: dict, limit: int = 15) -> list[dict]:
    """
    Async version of sql_match() from your 12_eligibility_matcher.
    Returns same structure: list of scheme dicts.
    """
    where_clause = build_sql_filter(profile)
    query = f"""
        SELECT
            slug AS scheme_id,
            scheme_name,
            schemeCategory,
            level,
            applicable_states,
            benefits,
            eligibility,
            documents,
            application
        FROM sarkarimitracatalog.sarkaridatabase.schemes_structured
        WHERE {where_clause}
        LIMIT {limit}
    """
    rows = await run_sql(query)
    return [
        {**row, "score": 2.0, "source": "sql"}
        for row in rows
    ]


# ── Fetch scheme details — mirrors fetch_scheme_details() ─
async def fetch_scheme_details(scheme_ids: list[str]) -> list[dict]:
    if not scheme_ids:
        return []
    ids_str = ", ".join(f"'{sid}'" for sid in scheme_ids)
    query = f"""
        SELECT
            slug AS scheme_id, scheme_name, schemeCategory,
            level, benefits, eligibility,
            application, documents, applicable_states
        FROM sarkarimitracatalog.sarkaridatabase.schemes_structured
        WHERE slug IN ({ids_str})
    """
    return await run_sql(query)


# ── Fetch chunks for RAG — mirrors fetch_chunks_for_scheme()
async def fetch_chunks(
    scheme_id: str,
    section: str = None,
    limit: int = 5
) -> list[dict]:
    """
    Async mirror of fetch_chunks_for_scheme() from 14_followup_rag.
    Uses source_column (not 'section') — matches your actual table.
    """
    section_filter = (
        f"AND source_column = '{section}'" if section else ""
    )
    query = f"""
        SELECT chunk_id, source_column AS section, chunk_text
        FROM sarkarimitracatalog.sarkaridatabase.scheme_chunks
        WHERE slug = '{scheme_id}'
        {section_filter}
        LIMIT {limit}
    """
    return await run_sql(query)


# ── Action plan — mirrors your ACTION_PLAN_SYSTEM prompt ──
ACTION_PLAN_SYSTEM = """You are Sarkari Mitra, a trusted advisor helping Indian citizens navigate government schemes.

You have matched schemes for this citizen. Your job is to create a PRACTICAL ACTION PLAN.

FORMAT YOUR RESPONSE EXACTLY LIKE THIS (in Hindi):

*आपके लिए योजनाएं* 🎉

*तुरंत करें (इस हफ्ते):*
1. [Scheme Name] — [1 line benefit]
   → दस्तावेज़: [documents for this scheme]

*बाद में करें (1 महीने में):*
2. [Scheme Name] — [benefit]

*सभी योजनाओं के लिए एक बार जमा करें:*
- [Common doc 1]
- [Common doc 2]

*हेल्पलाइन:* PM Kisan: 155261 | सामान्य: 1800-11-0001

RULES:
- Simple Hindi. WhatsApp formatting (*bold*, not markdown #).
- Maximum 5 schemes. Document consolidation is critical.
- Be specific about amounts (₹6000/year etc).
- Never fabricate scheme names or amounts."""


async def generate_action_plan(
    profile: dict,
    schemes: list[dict],
    user_input: str
) -> str:
    """
    Async mirror of generate_action_plan() from 13_action_plan.
    Same prompt structure, WhatsApp-safe formatting.
    """
    if not schemes:
        return (
            "माफ़ कीजिए, आपकी जानकारी के आधार पर कोई योजना नहीं मिली। 🙏\n\n"
            "कृपया अपना *राज्य*, *आय*, और *श्रेणी* बताएं।"
        )

    # Build prompt — same as build_action_plan_prompt()
    profile_parts = []
    if profile.get("state"):          profile_parts.append(f"State: {profile['state']}")
    if profile.get("age"):            profile_parts.append(f"Age: {profile['age']}")
    if profile.get("gender"):         profile_parts.append(f"Gender: {profile['gender']}")
    if profile.get("occupation"):     profile_parts.append(f"Occupation: {profile['occupation']}")
    if profile.get("category"):       profile_parts.append(f"Category: {profile['category']}")
    if profile.get("income_annual"):  profile_parts.append(f"Annual Income: ₹{profile['income_annual']}")
    if profile.get("has_bpl_card"):   profile_parts.append("Has BPL card: Yes")
    if profile.get("specific_need"):  profile_parts.append(f"Looking for: {profile['specific_need']}")

    schemes_parts = []
    for i, s in enumerate(schemes[:5], 1):
        schemes_parts.append(f"""
SCHEME {i}: {s.get('scheme_name', 'Unknown')}
  Category: {s.get('schemeCategory', s.get('category', 'N/A'))}
  Level: {s.get('level', 'N/A')}
  Benefits: {str(s.get('benefits', 'N/A'))[:300]}
  Documents: {str(s.get('documents', 'N/A'))[:200]}""")

    user_message = f"""CITIZEN PROFILE:
{chr(10).join(profile_parts)}

USER'S QUESTION: {user_input}

MATCHED SCHEMES:
{''.join(schemes_parts)}

Generate the WhatsApp-formatted action plan now."""

    return await call_llm(
        system_prompt=ACTION_PLAN_SYSTEM,
        user_message=user_message,
        max_tokens=800,
        temperature=0.3
    )


# ── Follow-up RAG — mirrors your answer_followup() ───────

FOLLOWUP_SYSTEM = """You are Sarkari Mitra, helping an Indian citizen with a specific question about a government scheme.

Answer ONLY based on the provided context. Never fabricate.
Write in simple Hindi mixed with English where helpful.
WhatsApp format: *bold* for important terms, bullet points with -.
Keep answers under 150 words.
If context doesn't contain the answer, say so and give helpline."""

SECTION_KEYWORDS = {
    "documents":   ["document", "दस्तावेज़", "kagaz", "certificate",
                    "proof", "id", "aadhaar", "photo", "required", "chahiye"],
    "application": ["apply", "application", "कैसे", "kaise", "process",
                    "form", "register", "portal", "website", "kahan", "steps"],
    "benefits":    ["benefit", "fayda", "kitna", "how much", "amount",
                    "paisa", "money", "rupee", "milega"],
    "eligibility": ["eligible", "qualify", "yogya", "kaun", "who",
                    "criteria", "condition", "age", "income"],
}


def detect_section(question: str) -> str | None:
    """Exact copy from your 14_followup_rag."""
    lower = question.lower()
    section_scores = {}
    for section, keywords in SECTION_KEYWORDS.items():
        section_scores[section] = sum(
            1 for kw in keywords if kw in lower
        )
    best = max(section_scores, key=section_scores.get)
    return best if section_scores[best] > 0 else None


async def answer_followup(
    question: str,
    scheme_id: str,
    scheme_name: str
) -> str:
    """
    Async mirror of answer_followup() from 14_followup_rag.
    Uses Delta SQL chunks — no FAISS on bot side.
    """
    section = detect_section(question)
    chunks  = await fetch_chunks(scheme_id, section, limit=4)

    if not chunks:
        # Try without section filter
        chunks = await fetch_chunks(scheme_id, None, limit=4)

    if not chunks:
        # Honest decline — same text as your notebook
        return (
            f"माफ़ कीजिए, मुझे '{scheme_name}' के बारे में "
            f"यह जानकारी नहीं मिली। 🙏\n\n"
            f"हेल्पलाइन:\n"
            f"• सामान्य: 1800-11-0001\n"
            f"• PM Kisan: 155261\n"
            f"• Jan Dhan: 1800-11-0001"
        )

    context = "\n\n".join([c["chunk_text"] for c in chunks])

    user_message = f"""SCHEME: {scheme_name}

QUESTION: {question}

CONTEXT FROM SCHEME DATABASE:
{context}

Answer the question based only on the above context."""

    return await call_llm(
        system_prompt=FOLLOWUP_SYSTEM,
        user_message=user_message,
        max_tokens=400,
        temperature=0.2
    )


import asyncio
print("✅ databricks_client.py loaded")