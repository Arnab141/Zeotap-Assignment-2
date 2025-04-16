# Bidirectional ClickHouse ↔ Flat File Ingestion Tool

## Features
- ClickHouse → Flat File (CSV)
- Flat File (CSV) → ClickHouse
- JWT-based ClickHouse authentication
- Column selection and schema discovery

## Setup

### Backend
```bash
cd backend
pip install -r requirements.txt
uvicorn main:app --reload
```

### Frontend
```bash
cd frontend
npm install
npm start
```

## Testing
- Load example data into ClickHouse (`uk_price_paid`, `ontime`)
- Verify ingestion count
- Try file upload for flat file to ClickHouse

## AI Usage
Prompts used: *TBD by user*

---

Let me know what features you’d like added next (e.g., progress bar, multi-table join, data preview, etc.).
