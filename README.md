# Order Management System

High-throughput order management system in Go that simulates a stock brokerage backend. Inspired by how platforms like Zerodha and Groww handle order placement, matching, and execution.

## What it does

- Accepts buy/sell orders via REST API
- Validates them through a risk engine (margin, price band, freeze quantity)
- Matches orders in-memory using price-time priority
- Persists state in Redis for crash recovery and Postgres for permanent record
- Pushes live price ticks and order updates over WebSocket
- Supports GTT (stop-loss / take-profit) orders
- Tracks real-time P&L per client

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | /orders | Place an order |
| GET | /orders/{id} | Get order status |
| DELETE | /orders/{id} | Cancel an order |
| GET | /clients/{id}/orders | List client orders |
| GET | /accounts/{id} | Account balance & margin |
| GET | /quote/{symbol} | Live quote / LTP |
| GET | /depth/{symbol} | Market depth (top 5 levels) |
| GET | /trades | Recent trades |
| POST | /gtt | Place GTT stop-loss order |
| GET | /gtt/{clientID} | List client GTTs |
| DELETE | /gtt/{id} | Cancel GTT |
| GET | /positions/{clientID} | Live positions & P&L |
| GET | /positions/{clientID}/summary | Day P&L summary |
| GET | /reports/trades/{clientID} | Trade history (Postgres) |
| GET | /reports/volume | Top symbols by volume |
| POST | /admin/eod-settlement | Run EOD settlement |
| GET | /health | Health check |
| WS | /ws?client_id=X | WebSocket real-time push |

## Stack

Go · Redis · PostgreSQL · WebSocket · Docker

## Run it

```bash
docker compose up --build
```

Server runs on http://localhost:8080. Redis at 6379, Postgres at 5432.

## Test it

```bash
# Place an order
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"client_id":"CLIENT001","symbol":"RELIANCE","side":"BUY","order_type":"LIMIT","product_type":"MIS","quantity":100,"price":2500}'

# Run unit tests
go test ./tests/unit/... -v

# Python load tester
python3 tests/test_oms.py --mode integration
```

## Architecture notes

The matching engine runs in-memory because Redis round-trips would be the bottleneck during peak load. Order state is kept in Redis so the in-memory book can be rebuilt after a crash. Margin reservation uses a Redis Lua script for atomic check-and-deduct, preventing double-spend across concurrent orders.


## Known Limitations

This is a learning project. Things missing from a production system:

- No authentication (any client_id accepted)
- MIS orders not auto squared-off at 3:20 PM
- FIX protocol not implemented (exchange is simulated)
- No Prometheus metrics or distributed tracing
- Async Postgres write has a small durability window

## Status

This is a learning project. Production gaps include: authentication, FIX protocol integration, MIS auto square-off at 3:20 PM, and observability (Prometheus, distributed tracing).