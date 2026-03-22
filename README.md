# Global Procurement Index Dashboard

A web-based dashboard for tracking procurement-relevant price indices across packaging, labour, logistics, and API/chemicals categories.

**Version 3.0** — Pharmaceutical Sector, Global Direct Procurement
**Stack:** Python 3.11 + FastAPI · SQLite · Vanilla JS + Chart.js · Hosted on Railway.app

---

## Prerequisites

- Python 3.11+
- pip

---

## Local Development

### 1. Clone the repository
```bash
git clone https://github.com/YOUR_ORG/procurement-index-dashboard.git
cd procurement-index-dashboard
```

### 2. Create a virtual environment and install dependencies
```bash
python -m venv venv
# Windows:
venv\Scripts\activate
# macOS/Linux:
source venv/bin/activate

pip install -r requirements.txt
```

### 3. Set environment variables (optional for local dev)
```bash
# Windows PowerShell
$env:DATABASE_PATH = "./data/indices.db"

# macOS/Linux
export DATABASE_PATH=./data/indices.db
```

### 4. Generate sample BLS Excel file
```bash
python sample_data/generate_bls_sample.py
```

### 5. Start the server
```bash
uvicorn backend.main:app --reload --port 8000
```

### 6. Open the dashboard
Navigate to: **http://localhost:8000**

Interactive API docs: **http://localhost:8000/api/docs**

---

## Railway.app Deployment (One-Time Setup)

1. Create a free account at [railway.app](https://railway.app)
2. Push this project to a GitHub repository
3. In Railway: **New Project → Deploy from GitHub repo** → select the repository
4. Railway auto-detects the `Procfile` and starts building (~2 minutes)
5. Go to **Variables tab** → add:
   - `DATABASE_PATH` = `/data/indices.db`
   - `LOG_LEVEL` = `INFO`
6. Go to **Volumes tab** → add a Volume mounted at `/data`
   *(This persists the SQLite database across deployments)*
7. Click the generated domain link (e.g. `yourapp.railway.app`) → dashboard loads
8. Go to **Upload Data** page → upload your first CSV → data appears on dashboard

> ✅ The Railway domain is permanent and HTTPS by default. No additional DNS configuration needed.

---

## Monthly Upload Routine (~10 minutes)

Follow these steps once per month to update all index data:

1. **Eurostat indices** (PPI, HICP, LCI):
   - Go to [ec.europa.eu/eurostat/databrowser](https://ec.europa.eu/eurostat/databrowser)
   - Find the relevant HICP/PPI dataset
   - Click **Export → CSV**
   - Open the dashboard → **Upload Data** → drag & drop the file → **Confirm Import**

2. **BLS indices** (US PPI, CPI, Intermediate):
   - Go to [data.bls.gov/cgi-bin/surveymost](https://data.bls.gov/cgi-bin/surveymost)
   - Select the relevant series → click **Retrieve data** → download Excel
   - Upload via dashboard → **Upload Data** → drag & drop → **Confirm Import**

3. **Paid indices** (NBSK/Fastmarkets, EUWID, ICIS, BOPET, LDPE):
   - Export from the supplier portal or copy values from the data email
   - Open `sample_data/sample_standard.csv` as a template
   - Fill in: `index_id`, `period` (YYYY-MM format), `value`
   - Save as CSV and upload via dashboard

4. **Verify the dashboard**:
   - Open **Dashboard** → check the KPI bar shows the latest upload date
   - Confirm MoM % and YoY % values have updated

5. **Executive Review**:
   - Open **Executive Review** → verify segment cards and charts are current
   - Click **Download PDF** to generate the management report

> ⚠️ Period format must be `YYYY-MM` (e.g. `2024-03`) — not `March 2024` or `03/2024`
> ⚠️ Values must be plain numbers — no `%` sign, no thousand separators

---

## File Structure

```
procurement-index-dashboard/
├── backend/
│   ├── main.py            # FastAPI entry point
│   ├── database.py        # SQLite schema + 35-index seed
│   ├── models.py          # Pydantic schemas
│   └── routers/
│       ├── indices.py     # GET /api/indices, /api/indices/{id}
│       ├── timeseries.py  # GET /api/timeseries/{id}
│       ├── calculator.py  # POST /api/calculate
│       ├── upload.py      # POST /api/upload + /api/upload/confirm
│       ├── export.py      # GET /api/export/dashboard|calculator
│       └── review.py      # GET /api/review/summary
├── frontend/
│   ├── index.html               # Dashboard
│   ├── calculator.html          # % Change calculator
│   ├── upload.html              # CSV/Excel upload
│   ├── executive-review.html    # Executive Review
│   ├── css/
│   │   ├── style.css
│   │   └── print.css
│   └── js/
│       ├── api.js         # Shared fetch wrapper
│       ├── dashboard.js
│       ├── calculator.js
│       ├── upload.js
│       └── review.js
├── data/                  # Railway Volume mount point
│   └── indices.db         # SQLite (auto-created on first run)
├── sample_data/
│   ├── sample_standard.csv
│   ├── sample_eurostat.csv
│   ├── sample_bls.xlsx    # Run generate_bls_sample.py first
│   └── generate_bls_sample.py
├── Procfile
├── requirements.txt
└── README.md
```

---

## API Reference

All endpoints available at `/api/docs` (Swagger UI) when the server is running.

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/indices` | All active indices with latest value, MoM %, YoY % |
| GET | `/api/indices/{id}` | Single index + last 36 months |
| GET | `/api/timeseries/{id}` | Full time-series with ?from=&to= filters |
| POST | `/api/calculate` | % change between two periods |
| POST | `/api/upload` | Preview file (no commit) |
| POST | `/api/upload/confirm` | Commit pending upload |
| GET | `/api/export/dashboard` | Export filtered dashboard (xlsx/csv) |
| GET | `/api/export/calculator` | Export calculator result (xlsx/csv) |
| GET | `/api/review/summary` | Executive review JSON payload |
| GET | `/api/health` | Health check |

---

## Index Catalogue

35 indices pre-seeded across 5 segments:

| Segment | Badge | Count | Paid |
|---------|-------|-------|------|
| Secondary packaging | Blue | 7 | 5 |
| EMN | Green | 7 | 0 |
| Logistics | Orange | 3 | 0 |
| Primary pkg & MD | Purple | 14 | 2 |
| API & chemicals | Teal | 4 | 0 |

> 28 of 35 indices are freely downloadable. 7 require paid subscriptions (Fastmarkets, EUWID, ICIS, Wood McKenzie) and must be entered via manual CSV upload.
