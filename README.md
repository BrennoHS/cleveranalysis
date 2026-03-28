# AdOps Discrepancy Analyzer

An AI-powered internal tool for comparing publisher ad reports against internal Clever/Elasticsearch ad server data. Built for AdOps and Support Engineers.

---

## Stack

| Layer      | Technology                         |
|------------|-------------------------------------|
| Backend    | Python 3.11+ · FastAPI · Uvicorn   |
| Frontend   | HTML · CSS · Vanilla JS            |
| AI         | Google Gemini 2.5 Flash & Pro      |
| Data       | Elasticsearch (REST API)           |

---

## Project Structure

```
adops-discrepancy-tool/
├── backend/
│   ├── main.py          # FastAPI app + routes
│   ├── parser.py        # Gemini AI parsing logic
│   ├── elastic.py       # Elasticsearch queries
│   ├── analysis.py      # Discrepancy calculations
│   ├── ai_explainer.py  # Gemini explanation generation
│   ├── requirements.txt
│   └── .env             # Your local env vars (create from .env.example)
├── frontend/
│   └── index.html       # Single-file UI
└── README.md
```

---

## Setup

### 1. Clone / Download the project

```bash
cd adops-discrepancy-tool
```

### 2. Install dependencies globally

The project uses your system's Python installation (no virtual environment needed).

```bash
pip install -r backend/requirements.txt
```

### 3. Configure environment variables

Edit `backend/.env` with your actual values:

```env
# Google Gemini API key
GEMINI_API_KEY=your_gemini_api_key

# Elasticsearch connection
ES_URL=https://your-es-cluster-host:9200
ES_USERNAME=your_username
ES_PASSWORD=your_password

# Available indices
ES_INDICES=served-impressions,core-view-events

# Index and field names — adjust to match your schema
ES_DATE_FIELD=@timestamp
ES_PUBLISHER_FIELD=publisher
ES_SERVED_FIELD=served_impressions
ES_VIEWABLE_FIELD=viewable_impressions
```

> **Note on Elasticsearch access:** If your ES cluster is behind a VPN or WARP, make sure it is active before starting the server.

### 4. Start the server

```bash
python start.py
```

You should see:
```
INFO:     Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)
```

### 5. Open the frontend

Visit http://127.0.0.1:8000 in your browser, or open `frontend/index.html` directly.

---

## Usage

1. Paste a publisher report in the text area. Any format is accepted — unstructured text, CSV-like, tables, etc.

   **Example input:**
   ```
   Campaign Report — January 2025
   Publisher: example.com
   Period: Jan 01, 2025 – Jan 31, 2025

   Impressions:          1,240,000
   Measurable:           1,240,000
   Active Views:           682,000
   Viewability Rate:         55.0%
   ```

2. Optionally enter the Publisher / Domain to filter Elasticsearch results.

3. Click **Analyze Report** (or press `Ctrl+Enter`).

4. The system will:
   - Extract metrics from the report using Gemini AI
   - Query Elasticsearch for the same period and publisher
   - Calculate discrepancy metrics
   - Display a comparison table and AI-generated explanation

---

## Discrepancy Classification

| Viewability Gap | Status           |
|-----------------|------------------|
| < 10 pp         | ✅ Normal         |
| 10 – 25 pp      | ⚠️ Attention      |
| > 25 pp         | 🔴 High Discrepancy |

---

## Elasticsearch Field Mapping

The tool is designed to work with any ES schema. Configure field names in `.env`:

| Env Variable        | Default               | Description                    |
|---------------------|-----------------------|--------------------------------|
| `ES_DATE_FIELD`     | `date`                | Date field for range filter    |
| `ES_PUBLISHER_FIELD`| `publisher`           | Publisher/domain field         |
| `ES_SERVED_FIELD`   | `served_impressions`  | Served impressions metric      |
| `ES_VIEWABLE_FIELD` | `viewable_impressions`| Viewable impressions metric    |

---

## API Reference

### `POST /analyze`

**Request:**
```json
{
  "report_text": "raw publisher report text...",
  "publisher": "example.com"
}
```

**Response:**
```json
{
  "start_date": "2025-01-01",
  "end_date": "2025-01-31",
  "publisher": "example.com",
  "pub_served": 1240000,
  "pub_viewable": 682000,
  "pub_viewability_pct": 55.0,
  "clever_served": 1198000,
  "clever_viewable": 634000,
  "clever_viewability_pct": 52.9,
  "diff_served": 42000,
  "diff_viewable": 48000,
  "viewability_diff_pp": 2.1,
  "viewability_diff_pct": 3.97,
  "status": "Normal",
  "explanation": "..."
}
```

### `GET /health`
Returns `{"status": "ok"}`.

---

## Gemini Terminology Mapping

The AI parser maps common publisher terms automatically:

| Treated As              | Accepted Labels                                   |
|-------------------------|---------------------------------------------------|
| `served_impressions`    | served impressions, impressions, measurable       |
| `viewable_impressions`  | viewable impressions, active views, viewable      |

---

## Troubleshooting

**`422 Could not extract required fields`**
- The report text may be missing impression counts or dates. Try adding explicit labels.

**`502 Elasticsearch query failed`**
- Check that ES_URL, credentials, and index name are correct.
- Ensure VPN/WARP is connected if required.
- Check ES field names match your actual schema.

**`Failed to parse report with AI`**
- Verify `GEMINI_API_KEY` is valid and has Gemini 1.5 Flash access.
