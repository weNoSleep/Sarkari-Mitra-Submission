import os
import json
import time
import asyncio
import httpx
import redis
from dotenv import load_dotenv

load_dotenv()

DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_WH_ID = os.getenv("DATABRICKS_WAREHOUSE_ID")
DB_WORKSPACE     = "https://dbc-9c147f6e-bf96.cloud.databricks.com"

SESSION_TTL = 3600

# ── Redis setup ───────────────────────────────────────────
try:
    _redis = redis.from_url(
        os.getenv("REDIS_URL", "redis://localhost:6379"),
        decode_responses=True,
        socket_connect_timeout=3
    )
    _redis.ping()
    USE_REDIS = True
    print("✅ Redis connected")
except Exception:
    USE_REDIS = False
    _mem: dict = {}
    print("⚠️  Redis unavailable — using in-memory fallback")


def _session_key(phone: str) -> str:
    return f"sm:session:{phone}"


def _new_session(phone: str) -> dict:
    """Always returns a complete session with ALL keys."""
    return {
        "phone":         phone,
        "profile":       {},
        "schemes":       [],
        "active_scheme": None,
        "history":       [],
        "source_lang":   "en",
        "turn":          0,
        "created_at":    int(time.time()),
    }


def _ensure_keys(session: dict, phone: str) -> dict:
    """Add any missing keys to an existing session."""
    defaults = _new_session(phone)
    for key, val in defaults.items():
        if key not in session:
            session[key] = val
    return session


def get_session(phone: str) -> dict:
    key = _session_key(phone)

    if USE_REDIS:
        raw = _redis.get(key)
        if raw:
            try:
                session = json.loads(raw)
                return _ensure_keys(session, phone)
            except Exception:
                pass
    else:
        stored = _mem.get(key)
        if stored:
            return _ensure_keys(stored.copy(), phone)

    return _new_session(phone)


def save_session(phone: str, session: dict):
    session = _ensure_keys(session, phone)
    key     = _session_key(phone)
    data    = json.dumps(session, ensure_ascii=False)
    if USE_REDIS:
        _redis.setex(key, SESSION_TTL, data)
    else:
        _mem[key] = session.copy()


def add_to_history(session: dict, role: str, content: str) -> dict:
    if "history" not in session:
        session["history"] = []
    session["history"].append({
        "role":    role,
        "content": content[:500],
        "ts":      int(time.time())
    })
    if len(session["history"]) > 10:
        session["history"] = session["history"][-10:]
    return session


def clear_session(phone: str):
    key = _session_key(phone)
    if USE_REDIS:
        _redis.delete(key)
    else:
        _mem.pop(key, None)


# ── SQL helpers ───────────────────────────────────────────
SQL_HEADERS = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type":  "application/json"
}


async def _run_sql_simple(query: str) -> dict:
    try:
        async with httpx.AsyncClient(timeout=25) as client:
            resp = await client.post(
                f"{DB_WORKSPACE}/api/2.0/sql/statements",
                headers=SQL_HEADERS,
                json={
                    "statement":    query,
                    "warehouse_id": DATABRICKS_WH_ID,
                    "wait_timeout": "20s",
                    "format":       "JSON_ARRAY"
                }
            )
            if resp.status_code != 200:
                print(f"⚠️ Memory SQL HTTP {resp.status_code}: {resp.text[:300]}")
                return {}
            return resp.json()
    except Exception as e:
        print(f"⚠️ Memory SQL failed: {e}")
        return {}


# ── Long-term profile persistence ─────────────────────────
async def save_profile_longterm(phone: str, profile: dict):
    safe_phone = phone.replace("+", "").replace(" ", "")
    state    = (profile.get("state") or "").replace("'", "''")
    category = (profile.get("category") or "").replace("'", "''")
    occ      = (profile.get("occupation") or "").replace("'", "''")
    age      = profile.get("age") or "NULL"
    income   = profile.get("income_annual") or "NULL"
    bpl      = str(bool(profile.get("has_bpl_card", False))).upper()
    ts       = int(time.time())

    create_sql = """
        CREATE TABLE IF NOT EXISTS
        sarkarimitracatalog.sarkaridatabase.user_profiles (
            phone         STRING,
            state         STRING,
            age           INT,
            category      STRING,
            occupation    STRING,
            income_annual BIGINT,
            has_bpl_card  BOOLEAN,
            last_updated  BIGINT
        ) USING DELTA
    """
    await _run_sql_simple(create_sql)

    merge_sql = f"""
        MERGE INTO sarkarimitracatalog.sarkaridatabase.user_profiles t
        USING (SELECT
            '{safe_phone}' AS phone,
            '{state}'      AS state,
            {age}          AS age,
            '{category}'   AS category,
            '{occ}'        AS occupation,
            {income}       AS income_annual,
            {bpl}          AS has_bpl_card,
            {ts}           AS last_updated
        ) s ON t.phone = s.phone
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """
    await _run_sql_simple(merge_sql)


async def load_profile_longterm(phone: str) -> dict | None:
    safe_phone = phone.replace("+", "").replace(" ", "")
    query = f"""
        SELECT state, age, category, occupation,
               income_annual, has_bpl_card
        FROM sarkarimitracatalog.sarkaridatabase.user_profiles
        WHERE phone = '{safe_phone}'
        LIMIT 1
    """
    data = await _run_sql_simple(query)
    rows = data.get("result", {}).get("data_array", [])
    if not rows:
        return None
    cols    = ["state","age","category","occupation",
               "income_annual","has_bpl_card"]
    profile = dict(zip(cols, rows[0]))
    return {k: v for k, v in profile.items() if v is not None}