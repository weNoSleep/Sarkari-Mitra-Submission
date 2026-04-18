# File: test_layers.py
# Run: python test_layers.py

import asyncio
from databricks_client import (
    call_llm, route_and_extract,
    sql_match, EMPTY_PROFILE
)
from sarvam_client import detect_language, translate_to_english
from memory import get_session, save_session

async def test_all():
    print("=" * 50)
    
    # Test 1: LLM reachable
    result = await call_llm(
        system_prompt="Reply with exactly: OK",
        user_message="test"
    )
    assert "OK" in result.upper(), f"LLM failed: {result}"
    print("✅ Test 1: LLM reachable")

    # Test 2: Router works
    result = await route_and_extract(
        "Main MP ka kisan hoon BPL card hai",
        EMPTY_PROFILE.copy()
    )
    assert result["intent"] == "SCHEME_DISCOVERY"
    assert result["profile"]["state"] == "Madhya Pradesh"
    print(f"✅ Test 2: Router works — {result['intent']}")

    # Test 3: SQL match returns schemes
    profile = {**EMPTY_PROFILE,
               "state": "Madhya Pradesh",
               "occupation": "farmer"}
    schemes = await sql_match(profile, limit=5)
    assert len(schemes) > 0, "SQL returned 0 schemes"
    print(f"✅ Test 3: SQL match — {len(schemes)} schemes")
    print(f"   First: {schemes[0]['scheme_name']}")

    # Test 4: Language detection
    lang = detect_language("मैं MP से हूं")
    assert lang == "hi"
    print("✅ Test 4: Language detection works")

    # Test 5: Translation
    text, src = translate_to_english("நான் தமிழ்நாட்டில் இருந்து")
    assert src == "ta"
    print(f"✅ Test 5: Translation — ta→en: {text[:40]}")

    # Test 6: Redis/memory
    save_session("+911234567890", {"test": True})
    s = get_session("+911234567890")
    assert s.get("test") == True
    print("✅ Test 6: Memory works")

    # Test 7: Full flow end-to-end
    from handler import handle_message
    result = await handle_message(
        phone="+911234567890_test",
        message="Main MP ka kisan hoon, BPL card hai"
    )
    assert result["text"], "Empty response"
    assert len(result["text"]) > 50
    print(f"✅ Test 7: Full flow — {len(result['text'])} chars")
    print(f"   Preview: {result['text'][:100]}...")

    print("=" * 50)
    print("ALL TESTS PASSED — safe to run main.py")

asyncio.run(test_all())