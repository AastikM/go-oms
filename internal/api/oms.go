package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/AastikM/go-oms/internal/db"
	"github.com/AastikM/go-oms/internal/gtt"
	"github.com/AastikM/go-oms/internal/marketdata"
	"github.com/AastikM/go-oms/internal/matching"
	"github.com/AastikM/go-oms/internal/models"
	"github.com/AastikM/go-oms/internal/orderbook"
	"github.com/AastikM/go-oms/internal/position"
	"github.com/AastikM/go-oms/internal/risk"
	"github.com/AastikM/go-oms/internal/store"
	"github.com/AastikM/go-oms/internal/ws"
)

type OMS struct {
	mu sync.RWMutex

	redis      *store.RedisStore
	postgres   *db.DB
	wsHub      *ws.Hub
	riskEngine *risk.Engine
	gttManager *gtt.Manager
	posMgr     *position.Manager
	marketFeed *marketdata.Feed
	orderBooks map[string]*orderbook.OrderBook
	engines    map[string]*matching.Engine
	tradeCh    chan *models.Trade
}

// NewOMS creates a new OMS instance backed by Redis and Postgres.
func NewOMS(redisAddr string, pgCfg db.Config, simMode bool) (*OMS, error) {
	redisStore, err := store.NewRedisStore(redisAddr)
	if err != nil {
		return nil, fmt.Errorf("redis connection failed: %w", err)
	}

	pgDB, err := db.NewDB(pgCfg)
	if err != nil {
		// Postgres failure is non fatal we can run without it
		// (Redis still works for live trading, just no persistence)
		log.Printf("[OMS] WARNING: Postgres unavailable: %v", err)
		log.Printf("[OMS] Running without persistent storage")
		pgDB = nil
	}
	log.Printf("[OMS] Connected to Redis at %s", redisAddr)

	posMgr := position.NewManager()

	oms := &OMS{
		redis:      redisStore,
		postgres:   pgDB,
		wsHub:      ws.NewHub(),
		riskEngine: risk.NewEngine(),
		posMgr:     posMgr,
		marketFeed: marketdata.NewFeed(simMode),
		orderBooks: make(map[string]*orderbook.OrderBook),
		engines:    make(map[string]*matching.Engine),
		tradeCh:    make(chan *models.Trade, 50000),
	}

	// GTT manager uses the OMS itself as the order placer
	oms.gttManager = gtt.NewManager(oms, oms.redis)

	// Start WebSocket hub
	go oms.wsHub.Run()

	oms.marketFeed.Subscribe(func(q marketdata.Quote) {
		oms.redis.SetLTP(q.Symbol, q.LTP)
		oms.redis.SetQuote(q.Symbol, q)
		oms.wsHub.BroadcastPriceTick(q.Symbol, q)
		oms.posMgr.UpdateLTP(q.Symbol, q.LTP)
	})

	// Start position event consumer (pushes updates to WebSocket)
	go oms.consumePositionEvents()

	// Start GTT watcher
	ctx := context.Background()
	go oms.gttManager.Start(ctx)
	go oms.consumeGTTEvents()

	go oms.consumeTrades()
	return oms, nil
}

// RegisterClient creates a new client account in Redis.
func (o *OMS) RegisterClient(clientID string, balance float64) error {
	if err := o.redis.InitAccount(clientID, balance); err != nil {
		return err
	}
	if o.postgres != nil {
		o.postgres.RegisterClient(clientID, clientID, "", balance)
	}
	log.Printf("[OMS] Registered client %s with balance ₹%.2f", clientID, balance)
	return nil
}

// AddSymbol initializes an order book + matching engine for a symbol.
func (o *OMS) AddSymbol(symbol string, exchange models.Exchange) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if _, exists := o.orderBooks[symbol]; exists {
		return
	}

	book := orderbook.NewOrderBook(symbol, exchange)
	engine := matching.NewEngine(book)
	engine.Start()

	go func() {
		for trade := range engine.TradeCh {
			o.tradeCh <- trade
		}
	}()

	o.orderBooks[symbol] = book
	o.engines[symbol] = engine
	log.Printf("[OMS] Added symbol %s on %s", symbol, exchange)
}

func (o *OMS) StartMarketData(symbols []string, interval time.Duration) {
	o.marketFeed.StartPolling(symbols, interval)
}

// RecoverFromRedis reloads open/partial orders from Redis into the in-memory
// order books on startup after a crash or restart.
func (o *OMS) RecoverFromRedis() error {
	orders, err := o.redis.GetOpenOrders()
	if err != nil {
		return fmt.Errorf("recovery failed: %w", err)
	}
	if len(orders) == 0 {
		log.Printf("[OMS] Recovery: no open orders found")
		return nil
	}
	log.Printf("[OMS] Recovery: reloading %d open orders...", len(orders))
	recovered := 0
	for _, order := range orders {
		book, exists := o.orderBooks[order.Symbol]
		if !exists {
			continue
		}
		if err := book.Add(order); err == nil {
			recovered++
		}
	}
	log.Printf("[OMS] Recovery: %d/%d orders restored", recovered, len(orders))
	return nil
}

// PlaceOrder is the main order entry point.
func (o *OMS) PlaceOrder(order *models.Order) error {
	ltp := o.redis.GetLTP(order.Symbol)
	if ltp == 0 {
		ltp = o.marketFeed.GetLTP(order.Symbol)
	}
	if ltp == 0 && order.OrderType == models.Market {
		return fmt.Errorf("no market data available for %s", order.Symbol)
	}

	if err := o.riskEngine.ValidateBasics(order); err != nil {
		order.UpdateStatus(models.StatusRejected, err.Error())
		_ = o.redis.SaveOrder(order)
		return fmt.Errorf("order rejected: %w", err)
	}

	if order.OrderType == models.Limit {
		if err := o.riskEngine.CheckPriceBand(order, ltp); err != nil {
			order.UpdateStatus(models.StatusRejected, err.Error())
			_ = o.redis.SaveOrder(order)
			return fmt.Errorf("order rejected: %w", err)
		}
	}

	if err := o.riskEngine.CheckFreezeQty(order); err != nil {
		order.UpdateStatus(models.StatusRejected, err.Error())
		_ = o.redis.SaveOrder(order)
		return fmt.Errorf("order rejected: %w", err)
	}

	effectivePrice := order.Price
	if order.OrderType == models.Market {
		effectivePrice = ltp
	}
	marginRates := map[models.ProductType]float64{
		models.CNC: 1.00, models.MIS: 0.20, models.NRML: 0.15,
	}
	marginRequired := effectivePrice * float64(order.Quantity) * marginRates[order.ProductType]

	// ATOMIC margin block — prevents double-spend across concurrent orders
	if err := o.redis.BlockMargin(order.ClientID, marginRequired); err != nil {
		order.UpdateStatus(models.StatusRejected, err.Error())
		_ = o.redis.SaveOrder(order)
		return fmt.Errorf("order rejected: %w", err)
	}

	order.RemainingQty = order.Quantity
	order.UpdateStatus(models.StatusOpen, "Order accepted")

	if err := o.redis.SaveOrder(order); err != nil {
		_ = o.redis.ReleaseMargin(order.ClientID, marginRequired)
		return fmt.Errorf("failed to persist order: %w", err)
	}

	o.mu.RLock()
	book, exists := o.orderBooks[order.Symbol]
	o.mu.RUnlock()

	if !exists {
		_ = o.redis.ReleaseMargin(order.ClientID, marginRequired)
		return fmt.Errorf("symbol %s not supported", order.Symbol)
	}

	if err := book.Add(order); err != nil {
		_ = o.redis.ReleaseMargin(order.ClientID, marginRequired)
		return fmt.Errorf("failed to add to book: %w", err)
	}

	// Async write to Postgres — does NOT block order placement
	if o.postgres != nil {
		o.postgres.SaveOrderAsync(order)
	}

	// Push order placed event to the client's WebSocket connection
	o.wsHub.BroadcastOrderUpdate(order.ClientID, map[string]interface{}{
		"event":    "order_placed",
		"order_id": order.OrderID,
		"symbol":   order.Symbol,
		"side":     order.Side,
		"status":   order.Status,
		"quantity": order.Quantity,
		"price":    order.Price,
	})

	log.Printf("[OMS] Order placed: %s | %s %s %d@%.2f | client:%s",
		order.OrderID, order.Side, order.Symbol, order.Quantity, order.Price, order.ClientID)

	return nil
}

// CancelOrder cancels an open order and releases its margin.
func (o *OMS) CancelOrder(orderID string) error {
	order, err := o.redis.GetOrder(orderID)
	if err != nil {
		return err
	}

	o.mu.RLock()
	book, exists := o.orderBooks[order.Symbol]
	o.mu.RUnlock()
	if !exists {
		return fmt.Errorf("symbol %s not found", order.Symbol)
	}

	if err := book.Cancel(orderID); err != nil {
		return err
	}

	marginRates := map[models.ProductType]float64{
		models.CNC: 1.00, models.MIS: 0.20, models.NRML: 0.15,
	}
	ltp := o.redis.GetLTP(order.Symbol)
	price := order.Price
	if order.OrderType == models.Market {
		price = ltp
	}
	marginUsed := price * float64(order.RemainingQty) * marginRates[order.ProductType]
	_ = o.redis.ReleaseMargin(order.ClientID, marginUsed)

	return o.redis.UpdateOrderStatus(orderID, models.StatusCancelled, "Cancelled by user")
}

func (o *OMS) GetOrder(orderID string) (*models.Order, error) {
	return o.redis.GetOrder(orderID)
}

func (o *OMS) GetClientOrders(clientID string) ([]*models.Order, error) {
	return o.redis.GetOrdersByClient(clientID)
}

func (o *OMS) consumeTrades() {
	for trade := range o.tradeCh {
		// 1. Write to Redis
		if err := o.redis.SaveTrade(trade); err != nil {
			log.Printf("[OMS] Redis trade save failed %s: %v", trade.TradeID, err)
		}
		// 2. Async write to Postgres
		if o.postgres != nil {
			o.postgres.SaveTradeAsync(trade)
		}

		// 3. Update positions- need both orders to know product type
		// Fetch orders from Redis for product type info
		buyOrder, _ := o.redis.GetOrder(trade.BuyOrderID)
		sellOrder, _ := o.redis.GetOrder(trade.SellOrderID)
		if buyOrder != nil && sellOrder != nil {
			o.posMgr.ApplyTrade(trade, buyOrder, sellOrder)
		}

		// 4. Push trade alert to all "trades:*" WebSocket subscribers
		o.wsHub.BroadcastTrade(map[string]interface{}{
			"trade_id":  trade.TradeID,
			"symbol":    trade.Symbol,
			"quantity":  trade.Quantity,
			"price":     trade.Price,
			"buyer_id":  trade.BuyerID,
			"seller_id": trade.SellerID,
		})
		// 5. Push fill notification to the specific buyer and seller
		fillPayload := func(side string) map[string]interface{} {
			return map[string]interface{}{
				"event":    "order_filled",
				"trade_id": trade.TradeID,
				"symbol":   trade.Symbol,
				"side":     side,
				"quantity": trade.Quantity,
				"price":    trade.Price,
			}
		}
		o.wsHub.BroadcastOrderUpdate(trade.BuyerID, fillPayload("BUY"))
		o.wsHub.BroadcastOrderUpdate(trade.SellerID, fillPayload("SELL"))

		log.Printf("[OMS] Trade: %s | %s %d@%.2f | buyer:%s seller:%s",
			trade.TradeID, trade.Symbol, trade.Quantity, trade.Price,
			trade.BuyerID, trade.SellerID)
	}
}

// consumePositionEvents listens for P&L updates and pushes them over WebSocket.
func (o *OMS) consumePositionEvents() {
	for event := range o.posMgr.Events() {
		pos := event.Position
		o.wsHub.Broadcast(ws.Message{
			Type:  ws.MsgType("position_update"),
			Topic: "positions:" + pos.ClientID,
			Payload: map[string]interface{}{
				"symbol":         pos.Symbol,
				"quantity":       pos.Quantity,
				"buy_avg":        pos.BuyAvg,
				"last_price":     pos.LastPrice,
				"realized_pnl":   pos.RealizedPnL,
				"unrealized_pnl": pos.UnrealizedPnL,
				"net_pnl":        pos.NetPnL(),
				"trigger":        event.Trigger,
			},
		})

		if event.Trigger == "trade" && o.postgres != nil {
			o.postgres.SavePositionAsync(pos.ToModel())
		}
	}
}

// consumeGTTEvents listens for GTT state changes and notifies clients.
func (o *OMS) consumeGTTEvents() {
	for event := range o.gttManager.Events() {
		g := event.GTT
		log.Printf("[GTT] Event: %s → %s (id:%s)", g.Symbol, event.Event, g.ID)
		o.wsHub.BroadcastOrderUpdate(g.ClientID, map[string]interface{}{
			"event":         "gtt_" + event.Event,
			"gtt_id":        g.ID,
			"symbol":        g.Symbol,
			"trigger_price": g.TriggerPrice,
			"condition":     g.Condition,
			"fired_order":   g.FiredOrderID,
		})
	}
}

func (o *OMS) Router() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /orders", o.handlePlaceOrder)
	mux.HandleFunc("GET /orders/{id}", o.handleGetOrder)
	mux.HandleFunc("DELETE /orders/{id}", o.handleCancelOrder)
	mux.HandleFunc("GET /clients/{id}/orders", o.handleClientOrders)
	mux.HandleFunc("GET /depth/{symbol}", o.handleMarketDepth)
	mux.HandleFunc("GET /quote/{symbol}", o.handleGetQuote)
	mux.HandleFunc("GET /trades", o.handleGetTrades)
	mux.HandleFunc("GET /accounts/{id}", o.handleGetAccount)
	mux.HandleFunc("GET /ws", o.handleWebSocket)
	mux.HandleFunc("GET /ws/stats", o.handleWSStats)
	// GTT endpoints
	mux.HandleFunc("POST /gtt", o.handlePlaceGTT)
	mux.HandleFunc("GET /gtt/{clientID}", o.handleGetGTTs)
	mux.HandleFunc("DELETE /gtt/{id}", o.handleCancelGTT)
	// Position / P&L endpoints
	mux.HandleFunc("GET /positions/{clientID}", o.handleGetPositions)
	mux.HandleFunc("GET /positions/{clientID}/summary", o.handlePnLSummary)
	mux.HandleFunc("GET /health", o.handleHealth)
	// Reporting endpoints (Postgres backed)
	mux.HandleFunc("GET /reports/trades/{clientID}", o.handleTradeHistory)
	mux.HandleFunc("GET /reports/volume", o.handleTopVolume)
	mux.HandleFunc("POST /admin/eod-settlement", o.handleEODSettlement)
	return mux
}

type PlaceOrderRequest struct {
	ClientID     string               `json:"client_id"`
	Symbol       string               `json:"symbol"`
	Exchange     models.Exchange      `json:"exchange"`
	Side         models.OrderSide     `json:"side"`
	OrderType    models.OrderType     `json:"order_type"`
	ProductType  models.ProductType   `json:"product_type"`
	Validity     models.OrderValidity `json:"validity"`
	Quantity     int64                `json:"quantity"`
	Price        float64              `json:"price"`
	TriggerPrice float64              `json:"trigger_price"`
}

func (o *OMS) handlePlaceOrder(w http.ResponseWriter, r *http.Request) {
	var req PlaceOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body: "+err.Error())
		return
	}
	order := models.NewOrder(req.ClientID, req.Symbol)
	order.Exchange = req.Exchange
	order.Side = req.Side
	order.OrderType = req.OrderType
	order.ProductType = req.ProductType
	order.Validity = req.Validity
	order.Quantity = req.Quantity
	order.Price = req.Price
	order.TriggerPrice = req.TriggerPrice
	if order.Exchange == "" {
		order.Exchange = models.NSE
	}
	if order.ProductType == "" {
		order.ProductType = models.MIS
	}
	if order.Validity == "" {
		order.Validity = models.DAY
	}
	if err := o.PlaceOrder(order); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, map[string]interface{}{
		"order_id": order.OrderID, "status": order.Status,
	})
}

func (o *OMS) handleGetOrder(w http.ResponseWriter, r *http.Request) {
	order, err := o.GetOrder(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, order)
}

func (o *OMS) handleCancelOrder(w http.ResponseWriter, r *http.Request) {
	if err := o.CancelOrder(r.PathValue("id")); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"message": "cancelled"})
}

func (o *OMS) handleClientOrders(w http.ResponseWriter, r *http.Request) {
	orders, err := o.GetClientOrders(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"orders": orders, "count": len(orders)})
}

func (o *OMS) handleMarketDepth(w http.ResponseWriter, r *http.Request) {
	symbol := r.PathValue("symbol")
	o.mu.RLock()
	book, exists := o.orderBooks[symbol]
	o.mu.RUnlock()
	if !exists {
		writeError(w, http.StatusNotFound, "symbol not found")
		return
	}
	bids, asks := book.MarketDepth(5)
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"symbol": symbol, "bids": bids, "asks": asks,
	})
}

func (o *OMS) handleGetQuote(w http.ResponseWriter, r *http.Request) {
	data, err := o.redis.GetQuoteRaw(r.PathValue("symbol"))
	if err != nil {
		writeError(w, http.StatusNotFound, "no quote available")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func (o *OMS) handleGetTrades(w http.ResponseWriter, r *http.Request) {
	trades, err := o.redis.GetRecentTrades(100)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"trades": trades, "count": len(trades)})
}

func (o *OMS) handleGetAccount(w http.ResponseWriter, r *http.Request) {
	clientID := r.PathValue("id")
	balance, used, err := o.redis.GetBalance(clientID)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"client_id": clientID, "balance": balance, "used": used, "free": balance - used,
	})
}

func (o *OMS) handleHealth(w http.ResponseWriter, r *http.Request) {
	stats := o.redis.Stats()
	stats["status"] = "ok"
	stats["timestamp"] = time.Now()
	writeJSON(w, http.StatusOK, stats)
}

func (o *OMS) handleTradeHistory(w http.ResponseWriter, r *http.Request) {
	if o.postgres == nil {
		writeError(w, http.StatusServiceUnavailable, "Postgres not configured")
		return
	}
	clientID := r.PathValue("clientID")
	from := time.Now().Truncate(24 * time.Hour)
	to := time.Now()
	rows, err := o.postgres.GetTradeHistory(clientID, from, to, 100)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"trades": rows, "count": len(rows)})
}

func (o *OMS) handleTopVolume(w http.ResponseWriter, r *http.Request) {
	if o.postgres == nil {
		writeError(w, http.StatusServiceUnavailable, "Postgres not configured")
		return
	}
	rows, err := o.postgres.GetTopSymbolsByVolume(10)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"symbols": rows})
}

func (o *OMS) handleEODSettlement(w http.ResponseWriter, r *http.Request) {
	if o.postgres == nil {
		writeError(w, http.StatusServiceUnavailable, "Postgres not configured")
		return
	}
	count, err := o.postgres.RunEODSettlement(time.Now())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"message":         "EOD settlement complete",
		"clients_settled": count,
		"settlement_date": time.Now().Format("2006-01-02"),
	})
}

type PlaceGTTRequest struct {
	ClientID     string               `json:"client_id"`
	Symbol       string               `json:"symbol"`
	Exchange     models.Exchange      `json:"exchange"`
	Condition    gtt.TriggerCondition `json:"condition"` // LTP_ABOVE or LTP_BELOW
	TriggerPrice float64              `json:"trigger_price"`
	Side         models.OrderSide     `json:"side"`
	OrderType    models.OrderType     `json:"order_type"`
	ProductType  models.ProductType   `json:"product_type"`
	Quantity     int64                `json:"quantity"`
	LimitPrice   float64              `json:"limit_price,omitempty"`
	PairedGTTID  string               `json:"paired_gtt_id,omitempty"`
}

func (o *OMS) handlePlaceGTT(w http.ResponseWriter, r *http.Request) {
	var req PlaceGTTRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body: "+err.Error())
		return
	}

	g := &gtt.GTTOrder{
		ClientID:     req.ClientID,
		Symbol:       req.Symbol,
		Exchange:     req.Exchange,
		Condition:    req.Condition,
		TriggerPrice: req.TriggerPrice,
		Side:         req.Side,
		OrderType:    req.OrderType,
		ProductType:  req.ProductType,
		Quantity:     req.Quantity,
		LimitPrice:   req.LimitPrice,
		PairedGTTID:  req.PairedGTTID,
	}
	if g.Exchange == "" {
		g.Exchange = models.NSE
	}
	if g.ProductType == "" {
		g.ProductType = models.MIS
	}

	if err := o.gttManager.Add(g); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, map[string]interface{}{
		"gtt_id": g.ID,
		"status": g.Status,
		"message": fmt.Sprintf("GTT created: fires %s when %s %s ₹%.2f",
			g.Side, g.Symbol, g.Condition, g.TriggerPrice),
	})
}

func (o *OMS) handleGetGTTs(w http.ResponseWriter, r *http.Request) {
	clientID := r.PathValue("clientID")
	orders := o.gttManager.GetByClient(clientID)
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"gtts":  orders,
		"count": len(orders),
	})
}

func (o *OMS) handleCancelGTT(w http.ResponseWriter, r *http.Request) {
	if err := o.gttManager.Cancel(r.PathValue("id")); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"message": "GTT cancelled"})
}

func (o *OMS) handleGetPositions(w http.ResponseWriter, r *http.Request) {
	clientID := r.PathValue("clientID")
	positions := o.posMgr.GetAllPositions(clientID)

	type PositionResponse struct {
		Symbol        string  `json:"symbol"`
		ProductType   string  `json:"product_type"`
		Quantity      int64   `json:"quantity"`
		BuyAvg        float64 `json:"buy_avg"`
		LastPrice     float64 `json:"last_price"`
		RealizedPnL   float64 `json:"realized_pnl"`
		UnrealizedPnL float64 `json:"unrealized_pnl"`
		NetPnL        float64 `json:"net_pnl"`
	}

	resp := make([]PositionResponse, 0, len(positions))
	for _, pos := range positions {
		ltp := o.redis.GetLTP(pos.Symbol)
		if ltp > 0 {
			pos.LastPrice = ltp
		}
		resp = append(resp, PositionResponse{
			Symbol:        pos.Symbol,
			ProductType:   string(pos.ProductType),
			Quantity:      pos.Quantity,
			BuyAvg:        pos.BuyAvg,
			LastPrice:     pos.LastPrice,
			RealizedPnL:   pos.RealizedPnL,
			UnrealizedPnL: pos.UnrealizedPnL,
			NetPnL:        pos.NetPnL(),
		})
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"positions": resp,
		"count":     len(resp),
	})
}

func (o *OMS) handlePnLSummary(w http.ResponseWriter, r *http.Request) {
	clientID := r.PathValue("clientID")
	summary := o.posMgr.GetDaySummary(clientID)
	writeJSON(w, http.StatusOK, summary)
}

func (o *OMS) GetLTP(symbol string) float64 {
	ltp := o.redis.GetLTP(symbol)
	if ltp == 0 {
		ltp = o.marketFeed.GetLTP(symbol)
	}
	return ltp
}

func (o *OMS) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	o.wsHub.ServeWS(w, r)
}

func (o *OMS) handleWSStats(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"connected_clients": o.wsHub.ConnectedClients(),
	})
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
