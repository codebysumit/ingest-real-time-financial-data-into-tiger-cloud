# Real-Time Financial Data Pipeline with Tiger Cloud

A small Python pipeline that listens to live crypto prices from **Twelve Data** over a websocket and saves them into a **TimescaleDB hypertable** on **Tiger Cloud**.

This project follows the official Tiger Data tutorial: [Ingest real-time financial data](https://www.tigerdata.com/docs/learn/tutorials/ingest-real-time-financial-data). It was built for the MLH Global Hack Week: Hacktoberfest challenge.

## What It Does

1. Connects to the Twelve Data websocket server.
2. Subscribes to symbols like `BTC/USD` and `ETH/USD`.
3. Collects each price event in memory as a batch.
4. When the batch is full, inserts all rows into Tiger Cloud in one transaction.
5. Lets you query the data, including OHLCV candles, with plain SQL.

## How the Data Flows

```
Twelve Data websocket  ->  Python script (batch in memory)  ->  Tiger Cloud (crypto_ticks hypertable)
```

Batching matters. Inserting many rows in one transaction is faster than inserting row by row.

## Requirements

1. A **Tiger Cloud** service (the free option works).
2. **Python 3** installed.
3. A **Twelve Data** account and API key. The free tier is enough.

## Project Structure

```
.
├── websocket_test.py  # the pipeline script
├── requirements.txt   # Python packages
├── .env               # your secrets (never upload this)
├── .env.example       # fake values to show the format
├── .gitignore         # keeps .env out of GitHub
├── env/               # local virtual environment
└── README.md
```

## Setup

### 1. Create a virtual environment

```bash
python -m venv env

# Mac or Linux
source env/bin/activate

# Windows PowerShell
env\Scripts\Activate.ps1
```

### 2. Install the packages

This project includes a `requirements.txt` file. Install it with:

```bash
pip install -r requirements.txt
```

The dependencies are:

```txt
twelvedata
websocket-client
psycopg2-binary
python-dotenv
```

### 3. Create the .env file

Make a file named `.env` in the same folder as `websocket_test.py`. Use your own values from Tiger Console and Twelve Data.

```env
TD_CLIENT_API_KEY=your_twelve_data_api_key
DB_NAME=tsdb
DB_HOST=your_service_host
DB_USER=tsdbadmin
DB_PASSWORD=your_password
DB_PORT=5432
```

Rules for this file:

1. No spaces around the equals sign.
2. No angle brackets. Those are only placeholders in docs.
3. Python does not read `.env` by itself. The code uses `python-dotenv` to load it.

### 4. Protect your secrets

Make a file called `.gitignore` and add:

```
.env
env/
__pycache__/
```

Share a `.env.example` file with fake values instead of your real `.env`.

### 5. Create the tables

Open the **SQL editor** in Tiger Console and run:

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

CREATE TABLE IF NOT EXISTS crypto_assets (
    symbol TEXT UNIQUE,
    "name" TEXT
);
```

What these two tables are:

| Table | Type | Purpose |
|-------|------|---------|
| `crypto_ticks` | Hypertable | Stores the live price data, split by time into chunks |
| `crypto_assets` | Normal PostgreSQL table | Stores the symbol and name of each asset |

Notes on the settings:

1. `tsdb.hypertable` turns the table into a hypertable. The first timestamp column is used as the time partition.
2. `segmentby = 'symbol'` and `orderby = 'time DESC'` make queries fast once older data moves to the columnstore.

## The Code

Save this as `websocket_test.py`:

```python
import os
import time
from datetime import datetime, timezone

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values
from twelvedata import TDClient

load_dotenv()  # reads the .env file

API_KEY = os.getenv("TD_CLIENT_API_KEY")
DB_NAME = os.getenv("DB_NAME")
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT")


class WebsocketPipeline:
    # name of the hypertable
    DB_TABLE = "crypto_ticks"

    # columns in the hypertable, in the correct order
    DB_COLUMNS = ["time", "symbol", "price", "day_volume"]

    # how many rows to collect before one insert
    MAX_BATCH_SIZE = 100

    def __init__(self, conn):
        """Stream data from the Twelve Data websocket into the database.

        Args:
            conn: psycopg2 connection object
        """
        self.conn = conn
        self.current_batch = []
        self.insert_counter = 0

    def _insert_values(self, data):
        if self.conn is not None:
            cursor = self.conn.cursor()
            sql = f"""
            INSERT INTO {self.DB_TABLE} ({','.join(self.DB_COLUMNS)})
            VALUES %s;"""
            execute_values(cursor, sql, data)
            self.conn.commit()

    def _on_event(self, event):
        """Runs every time a new record arrives from the server.

        Args:
            event (dict): data record
        """
        # skip status and heartbeat messages
        if event.get("event") != "price":
            print(event)
            return

        timestamp = datetime.fromtimestamp(event["timestamp"], tz=timezone.utc)
        data = (timestamp, event["symbol"], event["price"], event.get("day_volume"))

        # add the new record to the batch
        self.current_batch.append(data)
        print(f"Current batch size: {len(self.current_batch)}")

        # insert when the batch is full, then empty it
        if len(self.current_batch) >= self.MAX_BATCH_SIZE:
            self._insert_values(self.current_batch)
            self.insert_counter += 1
            print(f"Batch insert #{self.insert_counter}")
            self.current_batch = []

    def start(self, symbols):
        """Connect to the websocket server and start streaming.

        Args:
            symbols (list): stock or crypto symbols
        """
        td = TDClient(apikey=API_KEY)
        ws = td.websocket(on_event=self._on_event)
        ws.subscribe(symbols)
        ws.connect()
        try:
            while True:
                ws.heartbeat()
                time.sleep(10)
        except KeyboardInterrupt:
            # save the leftover rows when you press Ctrl + C
            if self.current_batch:
                self._insert_values(self.current_batch)
                print("Saved the last batch")


conn = psycopg2.connect(
    database=DB_NAME,
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASSWORD,
    port=DB_PORT,
    sslmode="require",
)

symbols = ["BTC/USD", "ETH/USD"]
websocket = WebsocketPipeline(conn)
websocket.start(symbols=symbols)
```

### Changes From the Tutorial Code

The code is close to the tutorial. These small changes make it safer and easier to test:

1. Keys and database details come from a `.env` file, not from the code.
2. `TDClient(apikey=API_KEY)` passes the variable. Writing it with quotes would send the plain text API_KEY and cause a 401 error.
3. `sslmode="require"` gives a secure connection to Tiger Cloud.
4. `event.get("event")` avoids a crash on messages that have no event key.
5. Timestamps are timezone aware (UTC).
6. Pressing `Ctrl + C` saves the last unfinished batch.
7. The symbol list has only crypto pairs, because crypto trades all day. Stocks like `MSFT` and `AAPL` only send data during market hours, and free plans limit how many symbols you can stream.

### Batch Size Tip

`MAX_BATCH_SIZE = 100` means nothing reaches the database until 100 prices arrive. For a quick test, change it to `10`. You can try 100, 1000, or more later and see what fits.

## Run It

```bash
python websocket_test.py
```

## Expected Output

First you get a status message from the server. Then price events come in and the batch counter goes up. When the batch is full, you see a batch insert line.

```
{'event': 'subscribe-status', 'status': 'ok', ...}
Current batch size: 1
Current batch size: 2
...
Current batch size: 100
Batch insert #1
```

Let it run for a few minutes, then press `Ctrl + C`.

## Check Your Data

Run these in the Tiger Console SQL editor:

```sql
SELECT COUNT(*) FROM crypto_ticks;
```

```sql
SELECT * FROM crypto_ticks ORDER BY time DESC LIMIT 10;
```

Confirm that the table is a hypertable:

```sql
SELECT hypertable_name FROM timescaledb_information.hypertables;
```

## Optional: OHLCV Candles With a Continuous Aggregate

**OHLCV** means Open, High, Low, Close, and Volume. Candlestick charts use these values for each time period. A **continuous aggregate** keeps these summaries ready, so queries stay fast.

Create a daily candle view:

```sql
CREATE MATERIALIZED VIEW one_day_candle
WITH (timescaledb.continuous) AS
    SELECT
        time_bucket('1 day', time) AS bucket,
        symbol,
        FIRST(price, time) AS "open",
        MAX(price) AS high,
        MIN(price) AS low,
        LAST(price, time) AS "close",
        LAST(day_volume, time) AS day_volume
    FROM crypto_ticks
    GROUP BY bucket, symbol;
```

Add a refresh policy:

```sql
SELECT add_continuous_aggregate_policy('one_day_candle',
    start_offset => INTERVAL '3 days',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day');
```

Query the Bitcoin candles for the last 14 days:

```sql
SELECT * FROM one_day_candle
WHERE symbol = 'BTC/USD' AND bucket >= NOW() - INTERVAL '14 days'
ORDER BY bucket;
```

If you only collected data for a few minutes, you will see one row per day. For a quick look at minute candles straight from the raw table, run:

```sql
SELECT
    time_bucket('1 minute', time) AS bucket,
    symbol,
    FIRST(price, time) AS "open",
    MAX(price) AS high,
    MIN(price) AS low,
    LAST(price, time) AS "close"
FROM crypto_ticks
GROUP BY bucket, symbol
ORDER BY bucket DESC
LIMIT 10;
```

## Optional: Visualize With Grafana

1. In Grafana, go to Connections, then Data sources, then add **PostgreSQL**.
2. Fill in the host (as `host:port`), database, user, and password from Tiger Console.
3. Set **TLS/SSL Mode** to `require` and turn on the **TimescaleDB** option.
4. Create a dashboard, add a **Candlestick** visualization, and paste the `one_day_candle` query.

## Troubleshooting

| Problem | Likely cause and fix |
|---------|----------------------|
| `401 apikey parameter is incorrect` | The key is wrong or the code uses the text API_KEY with quotes. Use `apikey=API_KEY` and check `.env`. |
| `Handshake status 200 OK` | Check that you use a proper API key from Twelve Data. |
| `.env` values are empty | Make sure the file is named exactly `.env` (not `.env.txt`), sits next to `websocket_test.py`, and `load_dotenv()` runs first. |
| `relation crypto_ticks already exists` | The table is already there. Use `CREATE TABLE IF NOT EXISTS`, or drop it and create it again. |
| `password authentication failed` | Wrong password. Reset it in Tiger Console and update `.env`. |
| Connection timed out | Check the host and port. Some college networks block the port. Try a mobile hotspot. |
| No rows in the table | The batch is not full yet. Lower `MAX_BATCH_SIZE` to 10, or stop with `Ctrl + C` to save the last batch. |
| No data for MSFT or AAPL | Stocks only stream during market hours. Test with crypto pairs. |

## Security Notes

1. Never upload `.env`, your API key, or your database password to GitHub.
2. Hide the connection string and password in screenshots.
3. If a secret was shared by mistake, reset the password in Tiger Console and create a new API key in Twelve Data.

## Built With

1. [Tiger Cloud](https://www.tigerdata.com) (TimescaleDB on PostgreSQL)
2. [Twelve Data](https://twelvedata.com) websocket API and Python library
3. Python, psycopg2, and python-dotenv

## Credits

Based on the Tiger Data tutorial: [Ingest real-time financial data](https://www.tigerdata.com/docs/learn/tutorials/ingest-real-time-financial-data).