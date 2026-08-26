# Real_Psy_29

PSY29 live intraday market-data collector.

## Scope

- 29 NSE equity stocks
- DhanHQ as the market-data source
- Live quote stream through DhanHQ WebSocket
- Intraday 1-minute backfill used to reconstruct 5m, 15m and 1h candles
- Previous-day high, low and close
- VWAP, EMA9 and EMA20
- 09:15-09:30 opening range
- Basic 5-minute swing/structure inputs
- Timestamp, trading date, session status and source status
- In-memory session recording only
- No Postgres
- No permanent historical storage

## Runtime lifecycle

The collector operates only during the NSE cash session. GitHub Actions sends periodic health requests during the session to wake a Render Free web service if it has gone idle. When the session ends, collection stops and the in-memory dataset is disposable.

## API

- `GET /health` — liveness
- `GET /` — service status
- `GET /data` — all 29 stocks
- `GET /data/{SYMBOL}` — one stock

## Secrets

The Render service uses the existing `PSY29-DHAN` environment group:

- `DHAN_CLIENT_ID`
- `DHAN_PIN`
- `DHAN_TOTP_SECRET`

No credentials belong in this repository.
