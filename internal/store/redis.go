package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/AastikM/go-oms/internal/models"
)

const (
	orderTTL = 24 * time.Hour
	tradeTTL = 7 * 24 * time.Hour
	quoteTTL = 5 * time.Minute // quotes are stale if older than 5 min
)

// RedisStore is the shared state layer backed by Redis.
// Multiple Go processes connecting to the same Redis instance
// will all see the same order state, balances, and prices.
type RedisStore struct {
	client *redis.Client
	ctx    context.Context
}

// NewRedisStore creates a store connected to Redis.
func NewRedisStore(addr string) (*RedisStore, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:         addr, // e.g. localhost:6379
		DB:           0,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  1 * time.Second,
		WriteTimeout: 1 * time.Second,
		PoolSize:     20, // connection pool — important for concurrent order flow
	})

	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis connection failed: %w", err)
	}

	return &RedisStore{client: rdb, ctx: ctx}, nil
}

// SaveOrder persists an order to Redis.
// Uses a Hash (HSET) — one field per order attribute.
// Also adds the order ID to relevant index Sets.
func (s *RedisStore) SaveOrder(order *models.Order) error {
	key := fmt.Sprintf("order:%s", order.OrderID)

	// Marshal the full order as JSON — simpler than individual HSET fields
	// for this use case. For ultra-high-frequency, you'd use individual fields.
	data, err := json.Marshal(order)
	if err != nil {
		return fmt.Errorf("marshal order: %w", err)
	}

	pipe := s.client.Pipeline()

	// Store the order itself
	pipe.Set(s.ctx, key, data, orderTTL)

	// Index: client's orders
	pipe.SAdd(s.ctx, fmt.Sprintf("orders:client:%s", order.ClientID), order.OrderID)
	pipe.Expire(s.ctx, fmt.Sprintf("orders:client:%s", order.ClientID), orderTTL)

	// Index: orders by symbol
	pipe.SAdd(s.ctx, fmt.Sprintf("orders:symbol:%s", order.Symbol), order.OrderID)

	// Index: orders by status
	pipe.SAdd(s.ctx, fmt.Sprintf("orders:status:%s", order.Status), order.OrderID)

	_, err = pipe.Exec(s.ctx)
	return err
}

// UpdateOrderStatus updates just the status of an order in Redis.
// Called every time an order transitions state (OPEN → FILLED, etc.)
func (s *RedisStore) UpdateOrderStatus(orderID string, status models.OrderStatus, msg string) error {
	// Get existing order
	order, err := s.GetOrder(orderID)
	if err != nil {
		return err
	}

	// Remove from old status index
	s.client.SRem(s.ctx,
		fmt.Sprintf("orders:status:%s", order.Status),
		orderID)

	// Update status
	order.Status = status
	order.StatusMsg = msg
	order.UpdatedAt = time.Now()

	// Save back and add to new status index
	if err := s.SaveOrder(order); err != nil {
		return err
	}

	return nil
}

// GetOrder retrieves a single order by ID.
func (s *RedisStore) GetOrder(orderID string) (*models.Order, error) {
	key := fmt.Sprintf("order:%s", orderID)
	data, err := s.client.Get(s.ctx, key).Bytes()
	if err == redis.Nil {
		return nil, fmt.Errorf("order %s not found", orderID)
	}
	if err != nil {
		return nil, fmt.Errorf("redis get: %w", err)
	}

	var order models.Order
	if err := json.Unmarshal(data, &order); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}
	return &order, nil
}

// GetOrdersByClient returns all orders for a given client.
// Useful for "My Orders" screen in a trading app.
func (s *RedisStore) GetOrdersByClient(clientID string) ([]*models.Order, error) {
	ids, err := s.client.SMembers(s.ctx,
		fmt.Sprintf("orders:client:%s", clientID)).Result()
	if err != nil {
		return nil, err
	}
	return s.batchGetOrders(ids)
}

// GetOrdersByStatus returns all orders with a given status.
// Useful for "get all open orders" to re-populate the order book on restart.
func (s *RedisStore) GetOrdersByStatus(status models.OrderStatus) ([]*models.Order, error) {
	ids, err := s.client.SMembers(s.ctx,
		fmt.Sprintf("orders:status:%s", status)).Result()
	if err != nil {
		return nil, err
	}
	return s.batchGetOrders(ids)
}

// batchGetOrders fetches multiple orders using a pipeline (single round trip).
// Without pipeline: N orders = N round trips to Redis.
// With pipeline: N orders = 1 round trip. Huge difference at scale.
func (s *RedisStore) batchGetOrders(ids []string) ([]*models.Order, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	pipe := s.client.Pipeline()
	cmds := make([]*redis.StringCmd, len(ids))
	for i, id := range ids {
		cmds[i] = pipe.Get(s.ctx, fmt.Sprintf("order:%s", id))
	}
	pipe.Exec(s.ctx)

	orders := make([]*models.Order, 0, len(ids))
	for _, cmd := range cmds {
		data, err := cmd.Bytes()
		if err != nil {
			continue // order may have expired
		}
		var o models.Order
		if err := json.Unmarshal(data, &o); err == nil {
			orders = append(orders, &o)
		}
	}
	return orders, nil
}

// InitAccount sets up a new client's account in Redis.
func (s *RedisStore) InitAccount(clientID string, balance float64) error {
	pipe := s.client.Pipeline()
	pipe.Set(s.ctx, fmt.Sprintf("account:%s:balance", clientID),
		strconv.FormatFloat(balance, 'f', 2, 64), 0)
	pipe.Set(s.ctx, fmt.Sprintf("account:%s:used", clientID), "0", 0)
	_, err := pipe.Exec(s.ctx)
	return err
}

// GetBalance returns a client's current free and used margin.
func (s *RedisStore) GetBalance(clientID string) (balance, used float64, err error) {
	pipe := s.client.Pipeline()
	balCmd := pipe.Get(s.ctx, fmt.Sprintf("account:%s:balance", clientID))
	usedCmd := pipe.Get(s.ctx, fmt.Sprintf("account:%s:used", clientID))
	pipe.Exec(s.ctx)

	balStr, err := balCmd.Result()
	if err != nil {
		return 0, 0, fmt.Errorf("client %s not found", clientID)
	}
	usedStr, _ := usedCmd.Result()

	balance, _ = strconv.ParseFloat(balStr, 64)
	used, _ = strconv.ParseFloat(usedStr, 64)
	return balance, used, nil
}

func (s *RedisStore) BlockMargin(clientID string, amount float64) error {
	balKey := fmt.Sprintf("account:%s:balance", clientID)
	usedKey := fmt.Sprintf("account:%s:used", clientID)

	// Use a Lua script for atomic check-and-increment
	// Lua scripts in Redis execute atomically — no other command runs between lines
	script := redis.NewScript(`
		local balance = tonumber(redis.call('GET', KEYS[1]))
		local used    = tonumber(redis.call('GET', KEYS[2]))
		local amount  = tonumber(ARGV[1])

		if balance == nil then
			return redis.error_reply('client not found')
		end

		local free = balance - used
		if free < amount then
			return redis.error_reply('insufficient margin: need ' .. amount .. ' have ' .. free)
		end

		redis.call('INCRBYFLOAT', KEYS[2], amount)
		return 'OK'
	`)

	err := script.Run(s.ctx, s.client,
		[]string{balKey, usedKey},
		strconv.FormatFloat(amount, 'f', 2, 64),
	).Err()

	return err
}

// ReleaseMargin frees up margin when an order is filled or cancelled.
func (s *RedisStore) ReleaseMargin(clientID string, amount float64) error {
	usedKey := fmt.Sprintf("account:%s:used", clientID)

	// Atomically decrease used margin, floor at 0
	script := redis.NewScript(`
		local used   = tonumber(redis.call('GET', KEYS[1])) or 0
		local amount = tonumber(ARGV[1])
		local newUsed = math.max(0, used - amount)
		redis.call('SET', KEYS[1], newUsed)
		return 'OK'
	`)

	return script.Run(s.ctx, s.client, []string{usedKey},
		strconv.FormatFloat(amount, 'f', 2, 64)).Err()
}

// SaveTrade persists an executed trade.
func (s *RedisStore) SaveTrade(trade *models.Trade) error {
	data, err := json.Marshal(trade)
	if err != nil {
		return err
	}

	pipe := s.client.Pipeline()
	pipe.Set(s.ctx, fmt.Sprintf("trade:%s", trade.TradeID), data, tradeTTL)
	// Prepend to global trades list (newest first)
	pipe.LPush(s.ctx, "trades:all", trade.TradeID)
	pipe.LPush(s.ctx, fmt.Sprintf("trades:symbol:%s", trade.Symbol), trade.TradeID)
	pipe.LTrim(s.ctx, "trades:all", 0, 9999) // keep last 10,000 trades
	_, err = pipe.Exec(s.ctx)
	return err
}

// GetRecentTrades returns the N most recent trades.
func (s *RedisStore) GetRecentTrades(limit int64) ([]*models.Trade, error) {
	ids, err := s.client.LRange(s.ctx, "trades:all", 0, limit-1).Result()
	if err != nil {
		return nil, err
	}

	pipe := s.client.Pipeline()
	cmds := make([]*redis.StringCmd, len(ids))
	for i, id := range ids {
		cmds[i] = pipe.Get(s.ctx, fmt.Sprintf("trade:%s", id))
	}
	pipe.Exec(s.ctx)

	trades := make([]*models.Trade, 0, len(ids))
	for _, cmd := range cmds {
		data, err := cmd.Bytes()
		if err != nil {
			continue
		}
		var t models.Trade
		if err := json.Unmarshal(data, &t); err == nil {
			trades = append(trades, &t)
		}
	}
	return trades, nil
}

// SetLTP stores the last traded price for a symbol.
// All processes reading from Redis see the same price.
func (s *RedisStore) SetLTP(symbol string, price float64) error {
	return s.client.Set(s.ctx,
		fmt.Sprintf("ltp:%s", symbol),
		strconv.FormatFloat(price, 'f', 2, 64),
		quoteTTL,
	).Err()
}

// GetLTP retrieves the last traded price for a symbol.
// Returns 0 if not found (market not yet open, or symbol not tracked).
func (s *RedisStore) GetLTP(symbol string) float64 {
	val, err := s.client.Get(s.ctx, fmt.Sprintf("ltp:%s", symbol)).Result()
	if err != nil {
		return 0
	}
	f, _ := strconv.ParseFloat(val, 64)
	return f
}

// SetQuote stores the full market quote for a symbol.
func (s *RedisStore) SetQuote(symbol string, quote interface{}) error {
	data, err := json.Marshal(quote)
	if err != nil {
		return err
	}
	return s.client.Set(s.ctx, fmt.Sprintf("quote:%s", symbol), data, quoteTTL).Err()
}

// GetQuote retrieves the full quote for a symbol as raw JSON bytes.
func (s *RedisStore) GetQuoteRaw(symbol string) ([]byte, error) {
	return s.client.Get(s.ctx, fmt.Sprintf("quote:%s", symbol)).Bytes()
}

// GetOpenOrders returns all orders that should be in the order book.
// Called at startup to re-populate the in-memory book after a restart.

func (s *RedisStore) GetOpenOrders() ([]*models.Order, error) {
	open, err := s.GetOrdersByStatus(models.StatusOpen)
	if err != nil {
		return nil, err
	}
	partial, err := s.GetOrdersByStatus(models.StatusPartiallyFilled)
	if err != nil {
		return nil, err
	}
	return append(open, partial...), nil
}

// Stats returns a summary of what's in Redis right now.
func (s *RedisStore) Stats() map[string]interface{} {
	openIDs, _ := s.client.SMembers(s.ctx, "orders:status:OPEN").Result()
	filledIDs, _ := s.client.SMembers(s.ctx, "orders:status:FILLED").Result()
	rejectedIDs, _ := s.client.SMembers(s.ctx, "orders:status:REJECTED").Result()
	tradeCount, _ := s.client.LLen(s.ctx, "trades:all").Result()

	return map[string]interface{}{
		"open_orders":     len(openIDs),
		"filled_orders":   len(filledIDs),
		"rejected_orders": len(rejectedIDs),
		"total_trades":    tradeCount,
	}
}

// Flush clears all OMS data (USE ONLY IN TESTS).
func (s *RedisStore) Flush() error {
	return s.client.FlushDB(s.ctx).Err()
}

func (s *RedisStore) AddGTT(gttID, symbol, condition string, triggerPrice float64, data interface{}) error {
	jsonBytes, err := json.Marshal(data)
	if err != nil {
		return err
	}

	// Which sorted set: triggers firing when LTP goes below, or above?
	zkey := fmt.Sprintf("gtt:%s:%s", condition, symbol) // e.g. "gtt:LTP_BELOW:RELIANCE"

	pipe := s.client.Pipeline()
	// Store in sorted set: score = trigger price, member = gttID
	pipe.ZAdd(s.ctx, zkey, redis.Z{Score: triggerPrice, Member: gttID})
	// Store full data separately
	pipe.Set(s.ctx, fmt.Sprintf("gtt:data:%s", gttID), jsonBytes, 24*time.Hour)
	_, err = pipe.Exec(s.ctx)
	return err
}

// RemoveGTT removes a GTT from its sorted set (on cancel/trigger/expire).
func (s *RedisStore) RemoveGTT(gttID, symbol, condition string) error {
	zkey := fmt.Sprintf("gtt:%s:%s", condition, symbol)
	pipe := s.client.Pipeline()
	pipe.ZRem(s.ctx, zkey, gttID)
	pipe.Del(s.ctx, fmt.Sprintf("gtt:data:%s", gttID))
	_, err := pipe.Exec(s.ctx)
	return err
}

// We only get IDs that actually triggered, not all GTTs. At 100,000 GTTs with 5 firing: we do 1 Redis call,
// not 100,000 comparisons.
func (s *RedisStore) GetTriggeredGTTs(symbol string, ltp float64) ([]string, error) {
	var triggered []string

	// Check LTP_BELOW: all GTTs where trigger_price >= LTP (stop-loss hit)
	belowKey := fmt.Sprintf("gtt:LTP_BELOW:%s", symbol)
	// Score range: -inf to ltp (any GTT with trigger <= current LTP fires)
	belowIDs, err := s.client.ZRangeByScore(s.ctx, belowKey, &redis.ZRangeBy{
		Min: "-inf",
		Max: fmt.Sprintf("%f", ltp),
	}).Result()
	if err == nil {
		triggered = append(triggered, belowIDs...)
	}

	// Check LTP_ABOVE: all GTTs where trigger_price <= LTP (breakout hit)
	aboveKey := fmt.Sprintf("gtt:LTP_ABOVE:%s", symbol)
	// Score range: ltp to +inf (any GTT with trigger >= current LTP fires)
	aboveIDs, err := s.client.ZRangeByScore(s.ctx, aboveKey, &redis.ZRangeBy{
		Min: fmt.Sprintf("%f", ltp),
		Max: "+inf",
	}).Result()
	if err == nil {
		triggered = append(triggered, aboveIDs...)
	}

	return triggered, nil
}

// GetGTTData retrieves the full GTT object by ID.
func (s *RedisStore) GetGTTData(gttID string) ([]byte, error) {
	return s.client.Get(s.ctx, fmt.Sprintf("gtt:data:%s", gttID)).Bytes()
}

// GTTCount returns how many active GTTs exist for a symbol and condition.
// Useful for monitoring dashboards.
func (s *RedisStore) GTTCount(symbol, condition string) int64 {
	zkey := fmt.Sprintf("gtt:%s:%s", condition, symbol)
	count, _ := s.client.ZCard(s.ctx, zkey).Result()
	return count
}

// windowMs = window size in milliseconds, maxRequests = limit within window.
func (s *RedisStore) CheckRateLimit(clientID string, windowMs, maxRequests int64) (bool, error) {
	key := fmt.Sprintf("ratelimit:%s", clientID)
	now := time.Now().UnixMilli()
	// windowStart handled in Lua script

	// Use a pipeline + Lua script for atomic check-and-add
	script := redis.NewScript(`
		local key      = KEYS[1]
		local now      = tonumber(ARGV[1])
		local window   = tonumber(ARGV[2])
		local maxReqs  = tonumber(ARGV[3])
		local reqID    = ARGV[4]

		-- Remove entries older than the window
		redis.call('ZREMRANGEBYSCORE', key, '-inf', now - window)

		-- Count requests in current window
		local count = redis.call('ZCARD', key)
		if count >= maxReqs then
			return 0  -- rate limited
		end

		-- Add this request with timestamp as score
		redis.call('ZADD', key, now, reqID)
		redis.call('PEXPIRE', key, window)  -- auto-cleanup
		return 1  -- allowed
	`)

	reqID := fmt.Sprintf("%d", now) // unique enough within the window
	result, err := script.Run(s.ctx, s.client,
		[]string{key},
		now, windowMs, maxRequests, reqID,
	).Int()

	return result == 1, err
}
