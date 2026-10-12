# Real-Time Financial Data Pipeline with Tiger Cloud

This project streams live crypto prices from Twelve Data into a TimescaleDB hypertable on Tiger Cloud.

Official guide: [Ingest real-time financial data | Tiger Data Docs](https://www.tigerdata.com/docs/learn/tutorials/ingest-real-time-financial-data)

## What it does

- Connects to the Twelve Data websocket API
- Subscribes to symbols such as `BTC/USD` and `ETH/USD`
- Batches incoming price updates before inserting into Tiger Cloud
- Stores data in a hypertable for quick querying and analysis

## Requirements

- Tiger Cloud service
- Python 3
- Twelve Data account and API key

## Project structure

```text
.
├── websocket_test.py   # main ingestion script
├── requirements.txt    # Python dependencies
├── .env                # local secrets (do not commit)
├── .env.example        # sample environment variables
├── .gitignore          # ignores secrets and virtualenv files
├── env/                # local virtual environment
├── README.md
└── .git
```

## Setup

### 1. Create a virtual environment

```bash
python -m venv env

# macOS / Linux
source env/bin/activate

# Windows PowerShell
env\Scripts\Activate.ps1
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure environment variables

Create a `.env` file in the project root and add:

```env
TD_CLIENT_API_KEY=your_twelve_data_api_key
DB_NAME=tsdb
DB_HOST=your_tiger_cloud_host
DB_USER=tsdbadmin
DB_PASSWORD=your_password
DB_PORT=5432
```

### 4. Create the table

Run this in Tiger Cloud SQL editor:

```sql
CREATE TABLE IF NOT EXISTS crypto_ticks (
    "time" TIMESTAMPTZ,
    symbol TEXT,
    price DOUBLE PRECISION,
    day_volume NUMERIC
) WITH (
    tsdb.hypertable,
    tsdb.segmentby = 'symbol',
    tsdb.orderby = 'time DESC'
);
```

## Run the pipeline

```bash
python websocket_test.py
```

The script connects to Twelve Data, subscribes to the configured symbols, and writes price updates into `crypto_ticks`.

## Notes

- Keep `.env` local and do not upload it to GitHub.
- Use crypto pairs like `BTC/USD` and `ETH/USD` for testing.
- Press `Ctrl + C` to stop the stream and flush the final batch.

## Reference

[Ingest real-time financial data | Tiger Data Docs](https://www.tigerdata.com/docs/learn/tutorials/ingest-real-time-financial-data)
