# 🇮🇳 Sarkari-Mitra

**Actionable, conversational, multilingual guide to Indian government schemes — built on Databricks.**

> _"myScheme tells you what exists. Sarkari-Mitra tells you what to do next — in your language, in the right order, with your documents."_

---

## What It Does

Sarkari-Mitra is a bilingual AI advisor that helps Indian citizens navigate 3,400+ central and state government welfare schemes. Users describe their situation in Hindi, English, Hinglish, or any regional language — the system extracts their profile, matches eligible schemes through hybrid SQL + semantic search on Databricks, and returns a **prioritized action plan** with document consolidation and helpline numbers. Available through both a **Gradio web app** and a **WhatsApp bot**.

---

### Quick Start (Local Execution)

To run the project locally, open your terminal (Command Prompt or PowerShell) and run the following combined command. Ensure you have Python and Git installed.

```cmd
git clone https://github.com/weNoSleep/Sarkari-Mitra-Submission && ^
cd sarkari_mitra && ^
pip install -r requirements.txt && ^
jupyter nbconvert --to notebook --execute data_pipeline/00_installs.ipynb --inplace && ^
jupyter nbconvert --to notebook --execute data_pipeline/01_clean_validate.ipynb --inplace && ^
jupyter nbconvert --to notebook --execute data_pipeline/02_chunk_text.ipynb --inplace && ^
jupyter nbconvert --to notebook --execute data_pipeline/03_embed.ipynb --inplace && ^
jupyter nbconvert --to notebook --execute data_pipeline/04_enrich_overlap.ipynb --inplace && ^
jupyter nbconvert --to notebook --execute data_pipeline/05_enrich_cluster.ipynb --inplace && ^
jupyter nbconvert --to notebook --execute data_pipeline/06_priority_rules.ipynb --inplace && ^
jupyter nbconvert --to notebook --execute 00_setup.ipynb --inplace && ^
python intelligence.py && ^
jupyter nbconvert --to notebook --execute gradio_app.ipynb --inplace


---

## Pipeline Breakdown

### 1. Data Pipeline (one-time ingestion)

```
Raw CSV (3,400 schemes)
    │
    ├── 00_ingest_csv         → raw_data Delta table
    ├── 01_structure_csv      → extract structured fields (age, income, state arrays)
    ├── 02_chunk_text         → split into ~40k chunks by section
    ├── 03_embed_faiss        → paraphrase-multilingual-MiniLM embeddings
    ├── 04_enrich_overlap     → document frequency analysis (Spark)
    └── 05_priority_rules     → KMeans clusters for recommendation priority
```

### 2. Query Pipeline (runtime, every user message)

| Step | Component | Latency | What It Does |
|------|-----------|---------|--------------|
| 1 | Sarvam translate input | ~1s | Detect language via Unicode script; translate non-Hindi/English to English |
| 2 | Router + Profile | ~3-4s | One Llama 3.3 call — returns intent + merged profile JSON |
| 3a | SQL + FAISS match | ~1-2s | Hybrid retrieval: hard filters + semantic top-K, deduplicated to top 10 schemes |
| 3b | RAG follow-up | ~2s | Section-aware chunk retrieval (documents / application / benefits / eligibility) |
| 4 | LLM generation | ~5s | Streaming Hindi action plan OR grounded follow-up answer |
| 5 | Sarvam translate output | ~1-2s | English/Hindi → user's original language |
| 6 | MLflow log (async) | 0s | Query metadata + unanswered tracker, non-blocking |

**Total end-to-end: ~8-12 seconds**, with streaming making first-token appear in ~5s.




### 3. WhatsApp Pipeline (additional layer)

```

### Architecture

<img width="737" height="1004" alt="image" src="https://github.com/user-attachments/assets/00f3d9fc-13a9-4a00-be89-03fa45fb8cf8" />



WhatsApp Message
    ↓
Twilio Webhook
    ↓
FastAPI /webhook (Cloudflare tunnel)
    ↓
Redis session (short-term memory, 1hr TTL)
    ↓
Databricks long-term profile (Delta user_profiles table)
    ↓
[Same intelligence pipeline as above]
    ↓
Voice response option via Sarvam TTS
    ↓
Twilio → WhatsApp
```

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| **Compute** | Databricks Free Edition (serverless CPU) |
| **Storage** | Delta Lake + Unity Catalog |
| **Processing** | Apache Spark / PySpark |
| **LLM** | Llama 3.3 70B via Databricks AI Gateway |
| **Embeddings** | paraphrase-multilingual-MiniLM-L12-v2 |
| **Vector Search** | FAISS on DBFS Volume |
| **Translation/STT/TTS** | Sarvam AI (IndicTrans2-based) |
| **ML Ops** | MLflow on Databricks |
| **Web UI** | Gradio (Databricks App) |
| **WhatsApp** | FastAPI + Twilio + Cloudflare Tunnel |
| **Session cache** | Redis (with in-memory fallback) |

---

## How To Run

### Prerequisites

- Databricks workspace with Free Edition
- Unity Catalog enabled
- SQL Warehouse (note the warehouse ID)
- Databricks Personal Access Token
- Sarvam AI API key ([sarvam.ai](https://sarvam.ai))
- For WhatsApp: Twilio account + WhatsApp Sandbox

### Part A — Databricks Backend Setup

**1. Clone repo to Databricks workspace**
```bash
git clone https://github.com/<your-username>/sarkari-mitra.git
# Import notebooks into /Workspace/Users/you@email.com/sarkari-mitra/
```

**2. Create Unity Catalog objects**
```sql
CREATE CATALOG IF NOT EXISTS sarkarimitracatalog;
CREATE SCHEMA IF NOT EXISTS sarkarimitracatalog.sarkaridatabase;
CREATE VOLUME IF NOT EXISTS sarkarimitracatalog.sarkaridatabase.sarkari_files;
```

**3. Upload scheme dataset**

Place `schemes.csv` in the Volume:
```
/Volumes/sarkarimitracatalog/sarkaridatabase/sarkari_files/schemes.csv
```

**4. Run data pipeline notebooks in order**
```
pipeline/00_ingest_csv
pipeline/01_structure_csv
pipeline/02_chunk_text
pipeline/03_embed_faiss
pipeline/04_enrich_overlap        (optional)
pipeline/05_priority_rules         (optional)
```

**5. Set environment secrets**

In Databricks Secrets (or notebook env vars):
```
DATABRICKS_TOKEN=<your-token>
SARVAM_API_KEY=<your-key>
```

**6. Run intelligence notebooks (loads functions into scope)**
```
intelligence/10_llm_client        (LLM + Sarvam wrappers)
intelligence/11_router_profile    (intent + profile extraction)
intelligence/12_eligibility_matcher (SQL + FAISS hybrid)
intelligence/13_action_plan       (streaming Hindi plan)
intelligence/14_followup_rag      (section-aware RAG)
```

**7. Launch Gradio app**
```
app/20_gradio_app
```
Click **"Open in new tab"** to get the shareable URL.

### Part B — WhatsApp Bot Setup

**1. Install dependencies**
```bash
cd whatsapp_bot/
pip install -r requirements.txt
```

**2. Configure `.env`**
```properties
# Twilio
TWILIO_ACCOUNT_SID=ACxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
TWILIO_AUTH_TOKEN=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
TWILIO_WHATSAPP_NUMBER=whatsapp:+14155238886

# Databricks
DATABRICKS_HOST=https://dbc-xxxxxxxx-xxxx.cloud.databricks.com
DATABRICKS_TOKEN=dapixxxxxxxxxxxxxxxxxxxxxx
DATABRICKS_WAREHOUSE_ID=xxxxxxxxxxxxxxxx
LLM_ENDPOINT=https://xxxxxxxxxxxxxxxx.ai-gateway.cloud.databricks.com/mlflow/v1

# Sarvam
SARVAM_API_KEY=sk_xxxxxxxxxxxxxxxxxxxxxxxx

# Redis (optional — auto-fallback to in-memory)
REDIS_URL=redis://localhost:6379
```

**3. Validate setup**
```bash
python test_layers.py
```
All 7 tests must print ✅ before proceeding.

**4. Start FastAPI (Terminal 1)**
```bash
uvicorn main:app --host 0.0.0.0 --port 8000
```

**5. Start Cloudflare tunnel (Terminal 2)**
```bash
cloudflared tunnel --url http://localhost:8000
```
Copy the `https://xxx.trycloudflare.com` URL shown.

**6. Configure Twilio webhook**
- Go to [Twilio Console → Messaging → WhatsApp Sandbox Settings](https://console.twilio.com)
- Set **"When a message comes in"** to: `https://xxx.trycloudflare.com/webhook`
- Method: HTTP POST
- Save

**7. Join the sandbox**
- From WhatsApp, send `join <your-sandbox-word>` to `+1 415 523 8886`
- (Find your sandbox word in the Twilio Console)

---

## Demo Steps

### Demo 1 — Gradio Web App

1. Open the Gradio app URL
2. Select persona: **Sita Bai (MP BPL farmer widow)**
3. Type: `मैं MP से हूं, 45 साल की हूं, BPL card है, पति नहीं है`
4. Watch:
   - Profile sidebar populates in real-time
   - Action plan streams in Hindi
   - Top 5 schemes ranked by urgency
   - Document consolidation section
   - Helpline numbers at bottom
5. Follow up: `इनके लिए क्या documents चाहिए?`
6. Confirm: RAG retrieves only documents section, no hallucination

### Demo 2 — Multilingual Input

1. Select persona: **Venkat (Telugu senior AP)**
2. Type in Telugu: `నేను 62 ఏళ్ల వ్యక్తిని, ఆంధ్రప్రదేశ్ నుండి`
3. Confirm: Sarvam translates Telugu → English → processes → response in Telugu

### Demo 3 — WhatsApp Bot

1. Open WhatsApp, send to your sandbox number:
   ```
   Main MP ka kisan hoon, BPL card hai, 45 saal ka hoon
   ```
2. Receive action plan within ~12s
3. Follow up: `PM Kisan ke documents kya chahiye?`
4. Receive grounded answer from scheme_chunks table
5. Try voice: Send a voice note, receive voice reply

### Demo 4 — Show Databricks Usage

Open notebook `intelligence/12_eligibility_matcher`, run:
```python
hybrid_match(
    {**EMPTY_PROFILE, "state": "Madhya Pradesh", "occupation": "farmer"},
    "government schemes for farmer BPL card",
    top_n=10
)
```
Show judges:
- Spark SQL filtering on `schemes_structured` Delta table
- FAISS semantic search
- Merged results

---

## Project Structure

```
sarkari-mitra/
├── pipeline/
│   ├── 00_ingest_csv.py
│   ├── 01_structure_csv.py
│   ├── 02_chunk_text.py
│   ├── 03_embed_faiss.py
│   ├── 04_enrich_overlap.py
│   └── 05_priority_rules.py
├── intelligence/
│   ├── 10_llm_client.py         # Llama + Sarvam wrappers
│   ├── 11_router_profile.py     # Intent + profile (1 LLM call)
│   ├── 12_eligibility_matcher.py # SQL + FAISS hybrid
│   ├── 13_action_plan.py        # Streaming Hindi plan
│   └── 14_followup_rag.py       # Section-aware RAG
├── app/
│   └── 20_gradio_app.py         # Main web UI
├── whatsapp_bot/
│   ├── main.py                  # FastAPI webhook server
│   ├── handler.py               # Message orchestration
│   ├── databricks_client.py     # Async HTTP client for Databricks
│   ├── memory.py                # Redis + Delta long-term
│   ├── sarvam_client.py         # STT/TTS/translation
│   ├── test_layers.py           # 7-layer validation
│   └── requirements.txt
├── benchmarks/
│   ├── 30_profile_accuracy.py
│   ├── 31_eligibility_f1.py
│   ├── 32_rag_quality.py
│   └── 33_latency.py
├── config/
│   └── 99_constants.py          # Table names, model IDs, intents
├── docs/
│   └── architecture.png         # Full-resolution diagram
└── README.md
```

---

## Innovation Highlights

1. **Single-call router+profile** — combined intent classification and profile extraction into one Llama call, cutting latency by ~4 seconds vs. two sequential calls.
2. **Section-aware RAG** — user asking "documents क्या चाहिए?" retrieves only chunks from the `documents` section, not eligibility or benefits, sharply reducing hallucination.
3. **Action plan vs. scheme list** — most competitors return scheme names; Sarkari-Mitra consolidates shared documents once, sequences schemes by approval speed, and tells the user *what to do first*.
4. **Document consolidation** — reduces 15 documents listed across 5 schemes into 5 unique documents to fetch once.
5. **Graceful degradation** — three-layer RAG fallback (FAISS → Delta SQL → honest decline with helpline) ensures no hallucinated answers.
6. **Multilingual without fine-tuning** — Sarvam handles translation on the edges, Llama handles the Hindi/English core, keeping the pipeline simple.
7. **Unified backend, two frontends** — Gradio and WhatsApp share the exact same Databricks backend code, no duplication.

---

## Databricks Components Used

| Component | How |
|-----------|-----|
| **Delta Lake** | `schemes_structured`, `scheme_chunks`, `user_profiles` tables with Unity Catalog |
| **Unity Catalog** | Three-level namespace `sarkarimitracatalog.sarkaridatabase.*` + Volumes for FAISS index |
| **Apache Spark** | Eligibility SQL with `from_json` array filters, document overlap aggregation, cluster enrichment |
| **Spark MLlib** | KMeans clustering for scheme priority rules |
| **Databricks AI Gateway** | Llama 3.3 70B for all generation |
| **FAISS on DBFS Volume** | 40k chunk semantic search index |
| **MLflow** | Query logs, unanswered question tracking, experiment metadata |
| **SQL Warehouse** | External access from WhatsApp bot via Statement API |
| **Databricks Apps** | Gradio app deployment |

---

## Benchmarks

| Metric | Value |
|--------|-------|
| Profile extraction accuracy | 92% on 50-case held-out set |
| Eligibility match F1 | 0.84 |
| RAG hallucination rate | <3% (section-aware retrieval) |
| Median query latency | 8.2s (first token 5.1s via streaming) |
| Languages supported | 11 (via Sarvam) |
| Schemes covered | 3,400 (all of myScheme.gov.in) |

---

## License

MIT — see `LICENSE` file.

## Team

Built for the Databricks Free Edition Hackathon, April 2026.

## Helplines

- **General:** 1800-11-0001
- **PM Kisan:** 155261
- **Jan Dhan:** 1800-11-0001
