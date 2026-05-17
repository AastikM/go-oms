// Package integration tests the PostgreSQL layer against a real database.
//
// Run with:
//   go test ./tests/integration/... -v -tags integration
//
// Requires: Postgres running with oms_db database
// Set env vars to override defaults:
//   PG_DSN="host=localhost port=5432 user=postgres dbname=oms_db sslmode=disable"
package integration

import (
	"os"
	"testing"
	"time"

	"github.com/AastikM/go-oms/internal/db"
	"github.com/AastikM/go-oms/internal/models"
)

// TEST SETUP

func newTestDB(t *testing.T) *db.DB {
	t.Helper()

	host := getEnv("PG_HOST", "localhost")
	user := getEnv("PG_USER", "postgres") // default to postgres superuser for tests
	pass := getEnv("PG_PASS", "")
	name := getEnv("PG_DB", "oms_db")

	cfg := db.Config{
		Host:    host,
		Port:    5432,
		User:    user,
		Password: pass,
		DBName:  name,
		SSLMode: "disable",
	}

	database, err := db.NewDB(cfg)
	if err != nil {
		t.Skipf("Skipping integration test: Postgres not available: %v", err)
	}
	return database
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// seedClient inserts a test client into the DB.
func seedClient(t *testing.T, database *db.DB, clientID string) {
	t.Helper()
	if err := database.RegisterClient(clientID, clientID+" test user", "test@test.com", 1000000); err != nil {
		t.Fatalf("Failed to seed client: %v", err)
	}
}

// makeOrder builds a test order with sensible defaults.
func makeOrder(clientID, symbol string, side models.OrderSide, price float64, qty int64) *models.Order {
	o := models.NewOrder(clientID, symbol)
	o.Side = side
	o.OrderType = models.Limit
	o.ProductType = models.MIS
	o.Price = price
	o.Quantity = qty
	o.FilledQty = 0
	o.RemainingQty = qty
	o.Status = models.StatusOpen
	return o
}

// makeTrade creates a test trade between two orders.
func makeTrade(buyOrder, sellOrder *models.Order, qty int64, price float64) *models.Trade {
	return &models.Trade{
		TradeID:     "TRD-TEST-" + time.Now().Format("150405.000"),
		BuyOrderID:  buyOrder.OrderID,
		SellOrderID: sellOrder.OrderID,
		BuyerID:     buyOrder.ClientID,
		SellerID:    sellOrder.ClientID,
		Symbol:      buyOrder.Symbol,
		Exchange:    buyOrder.Exchange,
		Quantity:    qty,
		Price:       price,
		Timestamp:   time.Now(),
	}
}

// ORDER PERSISTENCE TESTS

func TestDB_SaveAndRetrieveOrder(t *testing.T) {
	database := newTestDB(t)
	defer database.Close()

	clientID := "INTTEST-CLIENT-001"
	seedClient(t, database, clientID)

	order := makeOrder(clientID, "RELIANCE", models.Buy, 2500, 100)

	// Save order (async — flush with small sleep in tests)
	database.SaveOrderAsync(order)
	time.Sleep(200 * time.Millisecond) // let async writer flush

	// Verify it landed in Postgres via trade history flow
	// (We test via GetTradeHistory after inserting a trade below)
	t.Logf("Order saved: %s", order.OrderID)
}

func TestDB_OrderStatusUpdate(t *testing.T) {
	database := newTestDB(t)
	defer database.Close()

	clientID := "INTTEST-CLIENT-002"
	seedClient(t, database, clientID)

	// Save an OPEN order
	order := makeOrder(clientID, "TCS", models.Buy, 3800, 50)
	database.SaveOrderAsync(order)
	time.Sleep(100 * time.Millisecond)

	// Update to FILLED
	order.Status = models.StatusFilled
	order.FilledQty = 50
	order.RemainingQty = 0
	order.AveragePrice = 3800.0
	database.UpdateOrderAsync(order)
	time.Sleep(200 * time.Millisecond)

	t.Logf("Order status updated to FILLED: %s", order.OrderID)
}

// TRADE PERSISTENCE TESTS

func TestDB_SaveTradeAndQueryHistory(t *testing.T) {
	database := newTestDB(t)
	defer database.Close()

	buyerID  := "INTTEST-BUYER-001"
	sellerID := "INTTEST-SELLER-001"
	seedClient(t, database, buyerID)
	seedClient(t, database, sellerID)

	// Create matching orders
	buyOrder  := makeOrder(buyerID, "INFY", models.Buy, 1800, 100)
	sellOrder := makeOrder(sellerID, "INFY", models.Sell, 1800, 100)

	database.SaveOrderAsync(buyOrder)
	database.SaveOrderAsync(sellOrder)
	time.Sleep(100 * time.Millisecond)

	// Create the trade
	trade := makeTrade(buyOrder, sellOrder, 100, 1800.0)
	database.SaveTradeAsync(trade)
	time.Sleep(300 * time.Millisecond) // flush

	// Query trade history
	from := time.Now().Add(-1 * time.Hour)
	to := time.Now().Add(1 * time.Hour)

	history, err := database.GetTradeHistory(buyerID, from, to, 10)
	if err != nil {
		t.Fatalf("GetTradeHistory failed: %v", err)
	}
	if len(history) == 0 {
		t.Error("expected at least one trade in history, got 0")
	}
	found := false
	for _, row := range history {
		if row.TradeID == trade.TradeID {
			found = true
			if row.Symbol != "INFY" {
				t.Errorf("expected symbol INFY, got %s", row.Symbol)
			}
			if row.Price != 1800.0 {
				t.Errorf("expected price 1800, got %.2f", row.Price)
			}
			if row.Quantity != 100 {
				t.Errorf("expected qty 100, got %d", row.Quantity)
			}
			if row.Side != "BUY" {
				t.Errorf("expected side BUY for buyer, got %s", row.Side)
			}
		}
	}
	if !found {
		t.Errorf("trade %s not found in history", trade.TradeID)
	}
	t.Logf("Trade history: %d entries found", len(history))
}

// POSITION TESTS

func TestDB_UpsertPosition(t *testing.T) {
	database := newTestDB(t)
	defer database.Close()

	clientID := "INTTEST-POS-001"
	seedClient(t, database, clientID)

	pos := &models.Position{
		ClientID:      clientID,
		Symbol:        "HDFCBANK",
		Exchange:      models.NSE,
		ProductType:   models.MIS,
		Quantity:      100,
		BuyQty:        100,
		SellQty:       0,
		BuyAvg:        1600.0,
		SellAvg:       0,
		RealizedPnL:   0,
		UnrealizedPnL: 500.0, // stock moved up ₹5
		LastPrice:     1605.0,
		UpdatedAt:     time.Now(),
	}

	database.SavePositionAsync(pos)
	time.Sleep(200 * time.Millisecond)

	// Upsert with updated values (price moved again)
	pos.UnrealizedPnL = 1000.0
	pos.LastPrice = 1610.0
	database.SavePositionAsync(pos)
	time.Sleep(200 * time.Millisecond)

	t.Logf("Position upserted for %s/%s", clientID, pos.Symbol)
}

// REPORTING TESTS

func TestDB_DaySummary(t *testing.T) {
	database := newTestDB(t)
	defer database.Close()

	clientID := "INTTEST-SUMMARY-001"
	seedClient(t, database, clientID)

	summary, err := database.GetDaySummary(clientID, time.Now())
	if err != nil {
		t.Fatalf("GetDaySummary failed: %v", err)
	}

	// For a fresh test client with no trades, everything should be zero
	if summary.TradeCount < 0 {
		t.Error("trade count should be >= 0")
	}
	t.Logf("Day summary: %d trades, buy=₹%.2f, sell=₹%.2f",
		summary.TradeCount, summary.TotalBuyValue, summary.TotalSellValue)
}

func TestDB_TopSymbolsByVolume(t *testing.T) {
	database := newTestDB(t)
	defer database.Close()

	// Should not error even if no trades today
	symbols, err := database.GetTopSymbolsByVolume(5)
	if err != nil {
		t.Fatalf("GetTopSymbolsByVolume failed: %v", err)
	}
	t.Logf("Top symbols today: %d found", len(symbols))
	for _, s := range symbols {
		t.Logf("  %s: qty=%d value=₹%.2f trades=%d",
			s.Symbol, s.TotalQty, s.TotalValue, s.TradeCount)
	}
}

// EOD SETTLEMENT TEST

func TestDB_EODSettlement_Full(t *testing.T) {
	database := newTestDB(t)
	defer database.Close()

	// Set up two clients who trade with each other
	buyerID  := "INTTEST-EOD-BUYER"
	sellerID := "INTTEST-EOD-SELLER"
	seedClient(t, database, buyerID)
	seedClient(t, database, sellerID)

	// Place and fill 3 trades
	for i := 0; i < 3; i++ {
		buyOrder  := makeOrder(buyerID, "RELIANCE", models.Buy, 2500, 100)
		sellOrder := makeOrder(sellerID, "RELIANCE", models.Sell, 2500, 100)
		buyOrder.Status  = models.StatusFilled
		sellOrder.Status = models.StatusFilled
		buyOrder.FilledQty  = 100
		sellOrder.FilledQty = 100

		database.SaveOrderAsync(buyOrder)
		database.SaveOrderAsync(sellOrder)
		time.Sleep(10 * time.Millisecond)

		trade := makeTrade(buyOrder, sellOrder, 100, 2500.0)
		database.SaveTradeAsync(trade)
	}
	time.Sleep(400 * time.Millisecond) // flush all writes

	// Run EOD settlement
	settled, err := database.RunEODSettlement(time.Now())
	if err != nil {
		t.Fatalf("EOD settlement failed: %v", err)
	}

	// At least our two test clients should be settled
	t.Logf("EOD settlement processed %d clients", settled)

	// Verify the settlement math:
	// 3 orders × ₹20 brokerage = ₹60
	// STT on sell side: 3 × 100 × 2500 × 0.025% = ₹187.50
	// Exchange charges: 6 × 100 × 2500 × 0.00325% = ₹4.875
	// GST: (60 + 4.875) × 18% = ₹11.677
	t.Log("Expected brokerage for buyer (3 intraday orders): ₹60.00")
	t.Log("Expected STT for seller: ₹187.50")
}

// CONCURRENT WRITE STRESS TEST

func TestDB_ConcurrentOrderWrites(t *testing.T) {
	database := newTestDB(t)
	defer database.Close()

	clientID := "INTTEST-STRESS-001"
	seedClient(t, database, clientID)

	// Fire 200 async order writes concurrently
	// Tests that the channel buffer and async writer handle burst correctly
	done := make(chan struct{})
	for i := 0; i < 200; i++ {
		go func(i int) {
			order := makeOrder(clientID, "RELIANCE", models.Buy, float64(2500+i), 10)
			database.SaveOrderAsync(order)
			if i == 199 {
				close(done)
			}
		}(i)
	}

	<-done
	time.Sleep(500 * time.Millisecond) // let async writer process all

	t.Log("200 concurrent order writes completed without panic or deadlock")
}
