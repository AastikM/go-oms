package db

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq"

	"github.com/AastikM/go-oms/internal/models"
)

type DB struct {
	pool    *sql.DB
	writeCh chan writeJob
	stopCh  chan struct{}
}

type writeJob struct {
	kind string
	data interface{}
}

type Config struct {
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
	SSLMode  string
}

func (c Config) DSN() string {
	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		c.Host, c.Port, c.User, c.Password, c.DBName, c.SSLMode,
	)
}

func NewDB(cfg Config) (*DB, error) {
	pool, err := sql.Open("postgres", cfg.DSN())
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}

	pool.SetMaxOpenConns(25)
	pool.SetMaxIdleConns(10)
	pool.SetConnMaxLifetime(5 * time.Minute)

	if err := pool.Ping(); err != nil {
		return nil, fmt.Errorf("postgres unreachable: %w", err)
	}

	d := &DB{
		pool:    pool,
		writeCh: make(chan writeJob, 10000),
		stopCh:  make(chan struct{}),
	}

	go d.asyncWriter()

	log.Printf("[DB] Connected to Postgres at %s:%d/%s", cfg.Host, cfg.Port, cfg.DBName)
	return d, nil
}

func (d *DB) asyncWriter() {
	// Batch writes every 50ms for efficiency
	// Instead of 1 insert per trade, we accumulate and do bulk insert
	ticker := time.NewTicker(50 * time.Millisecond)
	var pending []writeJob

	flush := func() {
		if len(pending) == 0 {
			return
		}
		for _, job := range pending {
			if err := d.executeWrite(job); err != nil {
				log.Printf("[DB] Write error (%s): %v", job.kind, err)
			}
		}
		pending = pending[:0]
	}

	for {
		select {
		case job := <-d.writeCh:
			pending = append(pending, job)
			if len(pending) >= 100 {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-d.stopCh:
			flush()
			return
		}
	}
}

func (d *DB) executeWrite(job writeJob) error {
	switch job.kind {
	case "order":
		return d.insertOrder(job.data.(*models.Order))
	case "order_update":
		return d.updateOrder(job.data.(*models.Order))
	case "trade":
		return d.insertTrade(job.data.(*models.Trade))
	case "position":
		return d.upsertPosition(job.data.(*models.Position))
	case "market_data":
		return d.insertMarketData(job.data.(*MarketDataPoint))
	}
	return fmt.Errorf("unknown job kind: %s", job.kind)
}

func (d *DB) SaveOrderAsync(order *models.Order) {
	select {
	case d.writeCh <- writeJob{kind: "order", data: order}:
	default:
		log.Printf("[DB] WARNING: write channel full, dropping order %s", order.OrderID)
	}
}

func (d *DB) UpdateOrderAsync(order *models.Order) {
	select {
	case d.writeCh <- writeJob{kind: "order_update", data: order}:
	default:
		log.Printf("[DB] WARNING: write channel full, dropping order update %s", order.OrderID)
	}
}

func (d *DB) SaveTradeAsync(trade *models.Trade) {
	select {
	case d.writeCh <- writeJob{kind: "trade", data: trade}:
	default:
		log.Printf("[DB] WARNING: write channel full, dropping trade %s", trade.TradeID)
	}
}

func (d *DB) SavePositionAsync(pos *models.Position) {
	select {
	case d.writeCh <- writeJob{kind: "position", data: pos}:
	default:
		log.Printf("[DB] WARNING: write channel full, dropping position update")
	}
}

// LogMarketDataAsync logs a price snapshot for historical data.
func (d *DB) LogMarketDataAsync(point *MarketDataPoint) {
	select {
	case d.writeCh <- writeJob{kind: "market_data", data: point}:
	default:
		log.Printf("[DB] WARNING: write channel full, dropping market data point for %s", point.Symbol)
	}
}

func (d *DB) RegisterClient(clientID, name, email string, balance float64) error {
	_, err := d.pool.Exec(`
		INSERT INTO clients (client_id, name, email, balance)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (client_id) DO UPDATE
			SET name = EXCLUDED.name,
			    balance = EXCLUDED.balance,
			    updated_at = NOW()
	`, clientID, name, email, balance)
	return err
}

func (d *DB) insertOrder(o *models.Order) error {
	_, err := d.pool.Exec(`
		INSERT INTO orders (
			order_id, client_id, symbol, exchange,
			side, order_type, product_type, validity,
			quantity, filled_qty, remaining_qty,
			price, trigger_price, average_price,
			status, status_msg, placed_at, updated_at, tag
		) VALUES (
			$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19
		) ON CONFLICT (order_id) DO NOTHING
	`,
		o.OrderID, o.ClientID, o.Symbol, string(o.Exchange),
		string(o.Side), string(o.OrderType), string(o.ProductType), string(o.Validity),
		o.Quantity, o.FilledQty, o.RemainingQty,
		o.Price, o.TriggerPrice, o.AveragePrice,
		string(o.Status), o.StatusMsg, o.Timestamp, o.UpdatedAt, o.Tag,
	)
	return err
}

func (d *DB) updateOrder(o *models.Order) error {
	_, err := d.pool.Exec(`
		UPDATE orders SET
			status        = $2,
			status_msg    = $3,
			filled_qty    = $4,
			remaining_qty = $5,
			average_price = $6,
			updated_at    = $7
		WHERE order_id = $1
	`,
		o.OrderID, string(o.Status), o.StatusMsg,
		o.FilledQty, o.RemainingQty, o.AveragePrice, o.UpdatedAt,
	)
	return err
}

func (d *DB) insertTrade(t *models.Trade) error {
	_, err := d.pool.Exec(`
		INSERT INTO trades (
			trade_id, buy_order_id, sell_order_id,
			buyer_id, seller_id, symbol, exchange,
			quantity, price, executed_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		ON CONFLICT (trade_id) DO NOTHING
	`,
		t.TradeID, t.BuyOrderID, t.SellOrderID,
		t.BuyerID, t.SellerID, t.Symbol, string(t.Exchange),
		t.Quantity, t.Price, t.Timestamp,
	)
	return err
}

func (d *DB) upsertPosition(p *models.Position) error {
	_, err := d.pool.Exec(`
		INSERT INTO positions (
			client_id, symbol, exchange, product_type, trade_date,
			quantity, buy_qty, sell_qty, buy_avg, sell_avg,
			realized_pnl, unrealized_pnl, last_price, updated_at
		) VALUES ($1,$2,$3,$4,CURRENT_DATE,$5,$6,$7,$8,$9,$10,$11,$12,NOW())
		ON CONFLICT (client_id, symbol, product_type, trade_date)
		DO UPDATE SET
			quantity       = EXCLUDED.quantity,
			buy_qty        = EXCLUDED.buy_qty,
			sell_qty       = EXCLUDED.sell_qty,
			buy_avg        = EXCLUDED.buy_avg,
			sell_avg       = EXCLUDED.sell_avg,
			realized_pnl   = EXCLUDED.realized_pnl,
			unrealized_pnl = EXCLUDED.unrealized_pnl,
			last_price     = EXCLUDED.last_price,
			updated_at     = NOW()
	`,
		p.ClientID, p.Symbol, string(p.Exchange), string(p.ProductType),
		p.Quantity, p.BuyQty, p.SellQty, p.BuyAvg, p.SellAvg,
		p.RealizedPnL, p.UnrealizedPnL, p.LastPrice,
	)
	return err
}

type MarketDataPoint struct {
	Symbol   string
	Exchange string
	LTP      float64
	Open     float64
	High     float64
	Low      float64
	Volume   int64
	LoggedAt time.Time
}

func (d *DB) insertMarketData(m *MarketDataPoint) error {
	_, err := d.pool.Exec(`
		INSERT INTO market_data_log (symbol, exchange, ltp, open_price, high_price, low_price, volume, logged_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
	`, m.Symbol, m.Exchange, m.LTP, m.Open, m.High, m.Low, m.Volume, m.LoggedAt)
	return err
}

type TradeHistoryRow struct {
	TradeID    string
	Symbol     string
	Side       string
	Quantity   int64
	Price      float64
	TradeValue float64
	ExecutedAt time.Time
}

func (d *DB) GetTradeHistory(clientID string, from, to time.Time, limit int) ([]TradeHistoryRow, error) {
	rows, err := d.pool.Query(`
		SELECT
			t.trade_id,
			t.symbol,
			CASE WHEN t.buyer_id = $1 THEN 'BUY' ELSE 'SELL' END AS side,
			t.quantity,
			t.price,
			t.trade_value,
			t.executed_at
		FROM trades t
		WHERE (t.buyer_id = $1 OR t.seller_id = $1)
		  AND t.executed_at BETWEEN $2 AND $3
		ORDER BY t.executed_at DESC
		LIMIT $4
	`, clientID, from, to, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []TradeHistoryRow
	for rows.Next() {
		var r TradeHistoryRow
		if err := rows.Scan(&r.TradeID, &r.Symbol, &r.Side,
			&r.Quantity, &r.Price, &r.TradeValue, &r.ExecutedAt); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

type DaySummary struct {
	Date           time.Time
	TotalBuyValue  float64
	TotalSellValue float64
	TradeCount     int
	RealizedPnL    float64
}

func (d *DB) GetDaySummary(clientID string, date time.Time) (*DaySummary, error) {
	row := d.pool.QueryRow(`
		SELECT
			$2::date                                                        AS date,
			COALESCE(SUM(t.trade_value) FILTER (WHERE o.side = 'BUY'), 0)  AS buy_val,
			COALESCE(SUM(t.trade_value) FILTER (WHERE o.side = 'SELL'), 0) AS sell_val,
			COUNT(DISTINCT t.trade_id)                                      AS trades
		FROM trades t
		JOIN orders o ON o.order_id = t.buy_order_id OR o.order_id = t.sell_order_id
		WHERE (t.buyer_id = $1 OR t.seller_id = $1)
		  AND t.executed_at::DATE = $2::DATE
	`, clientID, date)

	var s DaySummary
	if err := row.Scan(&s.Date, &s.TotalBuyValue, &s.TotalSellValue, &s.TradeCount); err != nil {
		if err == sql.ErrNoRows {
			return &DaySummary{Date: date}, nil
		}
		return nil, err
	}
	return &s, nil
}

type SymbolVolume struct {
	Symbol     string
	TotalQty   int64
	TotalValue float64
	TradeCount int
}

func (d *DB) GetTopSymbolsByVolume(limit int) ([]SymbolVolume, error) {
	rows, err := d.pool.Query(`
		SELECT
			symbol,
			SUM(quantity)    AS total_qty,
			SUM(trade_value) AS total_value,
			COUNT(*)         AS trade_count
		FROM trades
		WHERE executed_at::DATE = CURRENT_DATE
		GROUP BY symbol
		ORDER BY total_value DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []SymbolVolume
	for rows.Next() {
		var v SymbolVolume
		rows.Scan(&v.Symbol, &v.TotalQty, &v.TotalValue, &v.TradeCount)
		result = append(result, v)
	}
	return result, rows.Err()
}

func (d *DB) RunEODSettlement(date time.Time) (int, error) {
	log.Printf("[EOD] Running settlement for %s", date.Format("2006-01-02"))

	// 1. Calculate brokerage per client
	rows, err := d.pool.Query(`
		SELECT
			o.client_id,
			COUNT(DISTINCT o.order_id) FILTER (
				WHERE o.status = 'FILLED' AND o.product_type != 'CNC'
			) AS chargeable_orders,
			COALESCE(SUM(t.trade_value) FILTER (WHERE o.side = 'BUY'),  0) AS buy_val,
			COALESCE(SUM(t.trade_value) FILTER (WHERE o.side = 'SELL'), 0) AS sell_val,
			COALESCE(SUM(t.trade_value), 0) AS total_turnover
		FROM orders o
		LEFT JOIN trades t ON t.buy_order_id = o.order_id OR t.sell_order_id = o.order_id
		WHERE o.placed_at::DATE = $1
		GROUP BY o.client_id
	`, date)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	settled := 0
	for rows.Next() {
		var clientID string
		var chargeableOrders int
		var buyVal, sellVal, turnover float64

		if err := rows.Scan(&clientID, &chargeableOrders, &buyVal, &sellVal, &turnover); err != nil {
			continue
		}

		// Brokerage: ₹20 per order, max ₹20
		brokerage := float64(chargeableOrders) * 20.0

		// STT: 0.025% of sell-side turnover (intraday equity)
		stt := sellVal * 0.00025

		// Exchange charges: 0.00325% of total turnover (NSE)
		exchangeCharges := turnover * 0.0000325

		gst := (brokerage + exchangeCharges) * 0.18

		var openingBalance float64
		d.pool.QueryRow(`SELECT balance FROM clients WHERE client_id = $1`,
			clientID).Scan(&openingBalance)

		totalCharges := brokerage + stt + exchangeCharges + gst

		_, err := d.pool.Exec(`
			INSERT INTO eod_settlements (
				client_id, settlement_date,
				total_buy_value, total_sell_value, total_trades,
				brokerage, stt, exchange_charges, gst,
				opening_balance, closing_balance
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
			ON CONFLICT (client_id, settlement_date) DO UPDATE SET
				total_buy_value  = EXCLUDED.total_buy_value,
				total_sell_value = EXCLUDED.total_sell_value,
				brokerage        = EXCLUDED.brokerage,
				settled_at       = NOW()
		`,
			clientID, date,
			buyVal, sellVal, chargeableOrders,
			brokerage, stt, exchangeCharges, gst,
			openingBalance, openingBalance-totalCharges,
		)
		if err != nil {
			log.Printf("[EOD] Settlement error for %s: %v", clientID, err)
			continue
		}
		settled++
		log.Printf("[EOD] Settled %s: brokerage=₹%.2f STT=₹%.2f total_charges=₹%.2f",
			clientID, brokerage, stt, totalCharges)
	}

	log.Printf("[EOD] Settlement complete: %d clients processed", settled)
	return settled, nil
}

func (d *DB) Close() {
	close(d.stopCh)
	time.Sleep(100 * time.Millisecond)
	d.pool.Close()
}
