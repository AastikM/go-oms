-- =============================================================================
-- OMS Database Schema
-- =============================================================================
-- Run this once to set up the database:
--   psql -U oms_user -d oms_db -f migrations/001_schema.sql
--
-- DESIGN DECISIONS:
--
-- 1. orders table: stores every order ever placed — including rejected ones.
--    This is your audit trail. SEBI requires brokers to keep order records
--    for 5 years. Never delete from this table.
--
-- 2. trades table: every matched trade. One order can produce multiple trades
--    (partial fills). Foreign keys reference orders for data integrity.
--
-- 3. positions table: net position per client per symbol per day.
--    Recomputed after every trade. This drives the P&L screen.
--
-- 4. eod_settlements table: end-of-day settlement snapshot.
--    After market close (3:30 PM), positions are squared off (MIS),
--    and a settlement record is written. This is what Zerodha's
--    "Console" uses for tax reports and P&L statements.
--
-- 5. We use NUMERIC(18,4) for prices — NOT float.
--    Float has rounding errors (0.1 + 0.2 != 0.3 in IEEE 754).
--    For money, you always use fixed-point decimals.
-- =============================================================================

-- Enable uuid extension for generating UUIDs
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- CLIENTS
CREATE TABLE IF NOT EXISTS clients (
    client_id       VARCHAR(50)     PRIMARY KEY,
    name            VARCHAR(200)    NOT NULL,
    email           VARCHAR(200),
    balance         NUMERIC(18,4)   NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    is_active       BOOLEAN         NOT NULL DEFAULT TRUE
);

-- ORDERS
-- Full history of every order, every state transition
CREATE TABLE IF NOT EXISTS orders (
    order_id            VARCHAR(100)    PRIMARY KEY,
    client_id           VARCHAR(50)     NOT NULL REFERENCES clients(client_id),
    symbol              VARCHAR(20)     NOT NULL,
    exchange            VARCHAR(10)     NOT NULL,  -- NSE, BSE, MCX

    -- What kind of order
    side                VARCHAR(4)      NOT NULL,  -- BUY, SELL
    order_type          VARCHAR(10)     NOT NULL,  -- MARKET, LIMIT, SL, SL-M
    product_type        VARCHAR(10)     NOT NULL,  -- CNC, MIS, NRML
    validity            VARCHAR(10)     NOT NULL,  -- DAY, IOC, GTT

    -- Quantities
    quantity            BIGINT          NOT NULL,
    filled_qty          BIGINT          NOT NULL DEFAULT 0,
    remaining_qty       BIGINT          NOT NULL,

    -- Prices (NUMERIC not FLOAT — critical for money)
    price               NUMERIC(18,4)   NOT NULL DEFAULT 0,
    trigger_price       NUMERIC(18,4)   NOT NULL DEFAULT 0,
    average_price       NUMERIC(18,4)   NOT NULL DEFAULT 0,

    -- Status lifecycle
    status              VARCHAR(20)     NOT NULL DEFAULT 'PENDING',
    status_msg          TEXT,

    -- Audit
    placed_at           TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    exchange_order_id   VARCHAR(100),   -- the ID the exchange gave back
    tag                 VARCHAR(50),    -- user-defined label

    -- Derived: order value at time of placement (for reporting)
    order_value         NUMERIC(18,4)   GENERATED ALWAYS AS (price * quantity) STORED,

    CONSTRAINT chk_quantity     CHECK (quantity > 0),
    CONSTRAINT chk_filled       CHECK (filled_qty >= 0 AND filled_qty <= quantity),
    CONSTRAINT chk_side         CHECK (side IN ('BUY', 'SELL')),
    CONSTRAINT chk_order_type   CHECK (order_type IN ('MARKET', 'LIMIT', 'SL', 'SL-M')),
    CONSTRAINT chk_product_type CHECK (product_type IN ('CNC', 'MIS', 'NRML')),
    CONSTRAINT chk_status       CHECK (status IN (
        'PENDING', 'OPEN', 'PARTIALLY_FILLED', 'FILLED', 'CANCELLED', 'REJECTED', 'MODIFIED'
    ))
);

-- Indexes for the most common queries
-- "Get all orders for a client" — used on every page load
CREATE INDEX IF NOT EXISTS idx_orders_client_id
    ON orders(client_id, placed_at DESC);

-- "Get all open orders for a symbol" — used by matching engine recovery
CREATE INDEX IF NOT EXISTS idx_orders_symbol_status
    ON orders(symbol, status)
    WHERE status IN ('OPEN', 'PARTIALLY_FILLED');

-- "Get today's orders" — common for intraday dashboards
CREATE INDEX IF NOT EXISTS idx_orders_placed_at
    ON orders(placed_at DESC);

-- TRADES
-- Every execution — one order can produce multiple trades (partial fills)
CREATE TABLE IF NOT EXISTS trades (
    trade_id        VARCHAR(100)    PRIMARY KEY,
    buy_order_id    VARCHAR(100)    NOT NULL REFERENCES orders(order_id),
    sell_order_id   VARCHAR(100)    NOT NULL REFERENCES orders(order_id),
    buyer_id        VARCHAR(50)     NOT NULL REFERENCES clients(client_id),
    seller_id       VARCHAR(50)     NOT NULL REFERENCES clients(client_id),

    symbol          VARCHAR(20)     NOT NULL,
    exchange        VARCHAR(10)     NOT NULL,
    quantity        BIGINT          NOT NULL,
    price           NUMERIC(18,4)   NOT NULL,

    -- Derived: total value of this trade
    trade_value     NUMERIC(18,4)   GENERATED ALWAYS AS (price * quantity) STORED,

    executed_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_trade_qty    CHECK (quantity > 0),
    CONSTRAINT chk_trade_price  CHECK (price > 0)
);

CREATE INDEX IF NOT EXISTS idx_trades_symbol_time
    ON trades(symbol, executed_at DESC);

CREATE INDEX IF NOT EXISTS idx_trades_buyer
    ON trades(buyer_id, executed_at DESC);

CREATE INDEX IF NOT EXISTS idx_trades_seller
    ON trades(seller_id, executed_at DESC);

-- POSITIONS
-- Net position per client per symbol — updated after every trade
CREATE TABLE IF NOT EXISTS positions (
    id              BIGSERIAL       PRIMARY KEY,
    client_id       VARCHAR(50)     NOT NULL REFERENCES clients(client_id),
    symbol          VARCHAR(20)     NOT NULL,
    exchange        VARCHAR(10)     NOT NULL,
    product_type    VARCHAR(10)     NOT NULL,
    trade_date      DATE            NOT NULL DEFAULT CURRENT_DATE,

    -- Quantities
    quantity        BIGINT          NOT NULL DEFAULT 0,  -- net (positive=long, negative=short)
    buy_qty         BIGINT          NOT NULL DEFAULT 0,
    sell_qty        BIGINT          NOT NULL DEFAULT 0,

    -- Average prices
    buy_avg         NUMERIC(18,4)   NOT NULL DEFAULT 0,
    sell_avg        NUMERIC(18,4)   NOT NULL DEFAULT 0,

    -- P&L
    realized_pnl    NUMERIC(18,4)   NOT NULL DEFAULT 0,
    unrealized_pnl  NUMERIC(18,4)   NOT NULL DEFAULT 0,
    last_price      NUMERIC(18,4)   NOT NULL DEFAULT 0,

    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    -- One position row per client per symbol per product per day
    UNIQUE (client_id, symbol, product_type, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_positions_client_date
    ON positions(client_id, trade_date DESC);

-- EOD SETTLEMENTS
-- End-of-day settlement snapshot — written after market close (3:30 PM)
-- This is the source of truth for tax reports, P&L statements, brokerage bills
CREATE TABLE IF NOT EXISTS eod_settlements (
    id              BIGSERIAL       PRIMARY KEY,
    client_id       VARCHAR(50)     NOT NULL REFERENCES clients(client_id),
    settlement_date DATE            NOT NULL DEFAULT CURRENT_DATE,

    -- Summary for the day
    total_buy_value     NUMERIC(18,4)   NOT NULL DEFAULT 0,
    total_sell_value    NUMERIC(18,4)   NOT NULL DEFAULT 0,
    total_trades        INT             NOT NULL DEFAULT 0,

    -- P&L
    realized_pnl        NUMERIC(18,4)   NOT NULL DEFAULT 0,
    brokerage           NUMERIC(18,4)   NOT NULL DEFAULT 0,  -- ₹20 per order cap
    stt                 NUMERIC(18,4)   NOT NULL DEFAULT 0,  -- Securities Transaction Tax
    exchange_charges    NUMERIC(18,4)   NOT NULL DEFAULT 0,
    gst                 NUMERIC(18,4)   NOT NULL DEFAULT 0,
    net_pnl             NUMERIC(18,4)   GENERATED ALWAYS AS
                            (realized_pnl - brokerage - stt - exchange_charges - gst) STORED,

    -- Account balance after settlement
    opening_balance     NUMERIC(18,4)   NOT NULL DEFAULT 0,
    closing_balance     NUMERIC(18,4)   NOT NULL DEFAULT 0,

    settled_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    UNIQUE (client_id, settlement_date)
);

-- MARKET_DATA_LOG
-- Historical price snapshots — useful for backtesting and audit
-- Partitioned by month for query performance on large datasets
CREATE TABLE IF NOT EXISTS market_data_log (
    id          BIGSERIAL       PRIMARY KEY,
    symbol      VARCHAR(20)     NOT NULL,
    exchange    VARCHAR(10)     NOT NULL,
    ltp         NUMERIC(18,4)   NOT NULL,
    open_price  NUMERIC(18,4),
    high_price  NUMERIC(18,4),
    low_price   NUMERIC(18,4),
    volume      BIGINT,
    logged_at   TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- Partial index: only index recent data (last 30 days) for fast queries
CREATE INDEX IF NOT EXISTS idx_market_data_symbol_time
    ON market_data_log(symbol, logged_at DESC)
    ;

-- USEFUL VIEWS — pre-built queries for common reporting needs

-- Today's P&L per client
CREATE OR REPLACE VIEW v_today_pnl AS
SELECT
    c.client_id,
    c.name,
    SUM(t.trade_value) FILTER (WHERE o.side = 'BUY')  AS total_buy,
    SUM(t.trade_value) FILTER (WHERE o.side = 'SELL') AS total_sell,
    COUNT(DISTINCT t.trade_id)                          AS trade_count,
    COUNT(DISTINCT t.symbol)                            AS symbols_traded
FROM clients c
LEFT JOIN trades t  ON t.buyer_id = c.client_id OR t.seller_id = c.client_id
LEFT JOIN orders o  ON o.order_id = t.buy_order_id OR o.order_id = t.sell_order_id
WHERE t.executed_at::DATE = CURRENT_DATE
GROUP BY c.client_id, c.name;

-- Order fill rate per symbol (how liquid is the book?)
CREATE OR REPLACE VIEW v_symbol_fill_rate AS
SELECT
    symbol,
    COUNT(*) FILTER (WHERE status = 'FILLED')           AS filled,
    COUNT(*) FILTER (WHERE status = 'CANCELLED')        AS cancelled,
    COUNT(*) FILTER (WHERE status = 'REJECTED')         AS rejected,
    COUNT(*) FILTER (WHERE status IN ('OPEN', 'PARTIALLY_FILLED')) AS still_open,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE status = 'FILLED') / NULLIF(COUNT(*), 0),
        2
    ) AS fill_rate_pct
FROM orders
WHERE placed_at::DATE = CURRENT_DATE
GROUP BY symbol
ORDER BY filled DESC;
