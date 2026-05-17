package unit

import (
	"fmt"
	"testing"
	"time"

	"github.com/AastikM/go-oms/internal/matching"
	"github.com/AastikM/go-oms/internal/models"
	"github.com/AastikM/go-oms/internal/orderbook"
	"github.com/AastikM/go-oms/internal/risk"
)

// HELPERS

func makeLimitOrder(clientID, symbol string, side models.OrderSide, price float64, qty int64) *models.Order {
	o := models.NewOrder(clientID, symbol)
	o.Side = side
	o.OrderType = models.Limit
	o.Price = price
	o.Quantity = qty
	o.RemainingQty = qty
	o.ProductType = models.MIS
	return o
}

func makeMarketOrder(clientID, symbol string, side models.OrderSide, qty int64) *models.Order {
	o := models.NewOrder(clientID, symbol)
	o.Side = side
	o.OrderType = models.Market
	o.Quantity = qty
	o.RemainingQty = qty
	o.ProductType = models.MIS
	// Small sleep to ensure unique nanosecond timestamps
	time.Sleep(1 * time.Nanosecond)
	return o
}

// ORDER MODEL TESTS

func TestOrderFill_Partial(t *testing.T) {
	order := makeLimitOrder("C1", "RELIANCE", models.Buy, 2500, 100)
	order.Fill(50, 2500)

	if order.FilledQty != 50 {
		t.Errorf("expected FilledQty=50, got %d", order.FilledQty)
	}
	if order.RemainingQty != 50 {
		t.Errorf("expected RemainingQty=50, got %d", order.RemainingQty)
	}
	if order.Status != models.StatusPartiallyFilled {
		t.Errorf("expected status PARTIALLY_FILLED, got %s", order.Status)
	}
}

func TestOrderFill_Full(t *testing.T) {
	order := makeLimitOrder("C1", "RELIANCE", models.Buy, 2500, 100)
	order.Fill(100, 2500)

	if order.FilledQty != 100 {
		t.Errorf("expected FilledQty=100, got %d", order.FilledQty)
	}
	if order.RemainingQty != 0 {
		t.Errorf("expected RemainingQty=0, got %d", order.RemainingQty)
	}
	if order.Status != models.StatusFilled {
		t.Errorf("expected status FILLED, got %s", order.Status)
	}
}

func TestOrderFill_AveragePrice(t *testing.T) {
	// Test weighted average price calculation
	// Fill 100 @ 2500, then 100 @ 2510 → avg should be 2505
	order := makeLimitOrder("C1", "RELIANCE", models.Buy, 2510, 200)
	order.Fill(100, 2500)
	order.Fill(100, 2510)

	expectedAvg := 2505.0
	if order.AveragePrice != expectedAvg {
		t.Errorf("expected average price %.2f, got %.2f", expectedAvg, order.AveragePrice)
	}
}

func TestOrderValue(t *testing.T) {
	order := makeLimitOrder("C1", "TCS", models.Buy, 3800, 10)
	expected := 3800.0 * 10
	if order.OrderValue(3800) != expected {
		t.Errorf("expected order value %.2f, got %.2f", expected, order.OrderValue(3800))
	}

	// Market order should use LTP
	mktOrder := makeMarketOrder("C1", "TCS", models.Buy, 10)
	ltp := 3850.0
	expectedMkt := ltp * 10
	if mktOrder.OrderValue(ltp) != expectedMkt {
		t.Errorf("expected market order value %.2f, got %.2f", expectedMkt, mktOrder.OrderValue(ltp))
	}
}

// ORDER BOOK TESTS

func TestOrderBook_AddBid_SortedDescending(t *testing.T) {
	book := orderbook.NewOrderBook("RELIANCE", models.NSE)

	// Add bids at different prices — should sort highest → lowest
	prices := []float64{2500, 2510, 2495, 2505}
	for _, p := range prices {
		order := makeLimitOrder("C1", "RELIANCE", models.Buy, p, 100)
		book.Add(order)
	}

	bids, _ := book.MarketDepth(10)

	// Verify descending order
	for i := 1; i < len(bids); i++ {
		if bids[i].Price > bids[i-1].Price {
			t.Errorf("bids not in descending order: bids[%d]=%.2f > bids[%d]=%.2f",
				i, bids[i].Price, i-1, bids[i-1].Price)
		}
	}

	// Best bid should be 2510
	if bids[0].Price != 2510 {
		t.Errorf("expected best bid 2510, got %.2f", bids[0].Price)
	}
}

func TestOrderBook_AddAsk_SortedAscending(t *testing.T) {
	book := orderbook.NewOrderBook("RELIANCE", models.NSE)

	prices := []float64{2510, 2520, 2505, 2515}
	for _, p := range prices {
		order := makeLimitOrder("C1", "RELIANCE", models.Sell, p, 100)
		book.Add(order)
	}

	_, asks := book.MarketDepth(10)

	// Verify ascending order
	for i := 1; i < len(asks); i++ {
		if asks[i].Price < asks[i-1].Price {
			t.Errorf("asks not in ascending order")
		}
	}

	// Best ask should be 2505
	if asks[0].Price != 2505 {
		t.Errorf("expected best ask 2505, got %.2f", asks[0].Price)
	}
}

func TestOrderBook_Cancel(t *testing.T) {
	book := orderbook.NewOrderBook("RELIANCE", models.NSE)
	order := makeLimitOrder("C1", "RELIANCE", models.Buy, 2500, 100)
	book.Add(order)

	// Drain the event channel so cancel can proceed
	go func() { <-book.EventCh }()

	err := book.Cancel(order.OrderID)
	if err != nil {
		t.Fatalf("cancel failed: %v", err)
	}
	if order.Status != models.StatusCancelled {
		t.Errorf("expected CANCELLED, got %s", order.Status)
	}
}

func TestOrderBook_Spread(t *testing.T) {
	book := orderbook.NewOrderBook("RELIANCE", models.NSE)

	// Add buy at 2500, sell at 2505 → spread = 5
	buy := makeLimitOrder("C1", "RELIANCE", models.Buy, 2500, 100)
	sell := makeLimitOrder("C2", "RELIANCE", models.Sell, 2505, 100)
	book.Add(buy)
	book.Add(sell)

	spread := book.Spread()
	if spread != 5.0 {
		t.Errorf("expected spread 5.0, got %.2f", spread)
	}
}

// MATCHING ENGINE TESTS

func setupMatchingTest(symbol string) (*orderbook.OrderBook, *matching.Engine) {
	book := orderbook.NewOrderBook(symbol, models.NSE)
	engine := matching.NewEngine(book)
	engine.Start()
	return book, engine
}

func TestMatching_LimitBuyMatchesLimitSell(t *testing.T) {
	book, engine := setupMatchingTest("RELIANCE")
	defer engine.Stop()

	// Place a sell order first (resting)
	sell := makeLimitOrder("SELLER", "RELIANCE", models.Sell, 2500, 100)
	book.Add(sell)
	time.Sleep(10 * time.Millisecond) // let matching engine process

	// Place a buy order at same price → should match
	buy := makeLimitOrder("BUYER", "RELIANCE", models.Buy, 2500, 100)
	book.Add(buy)

	// Wait for match
	select {
	case trade := <-engine.TradeCh:
		if trade.Quantity != 100 {
			t.Errorf("expected trade qty 100, got %d", trade.Quantity)
		}
		if trade.Price != 2500 {
			t.Errorf("expected trade price 2500, got %.2f", trade.Price)
		}
		if trade.BuyerID != "BUYER" {
			t.Errorf("expected buyer BUYER, got %s", trade.BuyerID)
		}
		if trade.SellerID != "SELLER" {
			t.Errorf("expected seller SELLER, got %s", trade.SellerID)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timeout: expected a trade but got none")
	}
}

func TestMatching_PartialFill(t *testing.T) {
	book, engine := setupMatchingTest("TCS")
	defer engine.Stop()

	// Sell 100, but buyer only wants 60 → partial fill
	sell := makeLimitOrder("SELLER", "TCS", models.Sell, 3800, 100)
	book.Add(sell)
	time.Sleep(10 * time.Millisecond)

	buy := makeLimitOrder("BUYER", "TCS", models.Buy, 3800, 60)
	book.Add(buy)

	select {
	case trade := <-engine.TradeCh:
		if trade.Quantity != 60 {
			t.Errorf("expected partial fill of 60, got %d", trade.Quantity)
		}
		// Sell order should have 40 remaining
		if sell.RemainingQty != 40 {
			t.Errorf("expected sell remaining=40, got %d", sell.RemainingQty)
		}
		if sell.Status != models.StatusPartiallyFilled {
			t.Errorf("expected sell PARTIALLY_FILLED, got %s", sell.Status)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timeout: no trade")
	}
}

func TestMatching_NoMatchWhenPriceCrossed(t *testing.T) {
	book, engine := setupMatchingTest("INFY")
	defer engine.Stop()

	// Sell at 1810, Buy at 1800 → NO match (spread = 10)
	sell := makeLimitOrder("SELLER", "INFY", models.Sell, 1810, 100)
	book.Add(sell)
	time.Sleep(10 * time.Millisecond)

	buy := makeLimitOrder("BUYER", "INFY", models.Buy, 1800, 100)
	book.Add(buy)

	select {
	case trade := <-engine.TradeCh:
		t.Fatalf("unexpected trade: %+v", trade)
	case <-time.After(200 * time.Millisecond):
		// Correct: no trade should happen
	}
}

// RISK ENGINE TESTS

func TestRisk_MarginCheck_Sufficient(t *testing.T) {
	engine := risk.NewEngine()
	engine.RegisterAccount("C1", 100000) // ₹1 lakh

	// BUY 100 RELIANCE @ 2500 (MIS) → need 2500*100*0.20 = ₹50,000
	order := makeLimitOrder("C1", "RELIANCE", models.Buy, 2500, 100)
	order.ProductType = models.MIS

	err := engine.Validate(order, 2500)
	if err != nil {
		t.Errorf("expected order to pass, got error: %v", err)
	}
}

func TestRisk_MarginCheck_Insufficient(t *testing.T) {
	engine := risk.NewEngine()
	engine.RegisterAccount("C1", 10000) // only ₹10,000

	// BUY 100 RELIANCE @ 2500 (MIS) → need ₹50,000 but only have ₹10,000
	order := makeLimitOrder("C1", "RELIANCE", models.Buy, 2500, 100)
	order.ProductType = models.MIS

	err := engine.Validate(order, 2500)
	if err == nil {
		t.Error("expected rejection for insufficient margin, got nil error")
	}
	t.Logf("Correctly rejected: %v", err)
}

func TestRisk_PriceBandViolation(t *testing.T) {
	engine := risk.NewEngine()
	engine.RegisterAccount("C1", 10000000) // ₹1 crore — plenty of margin

	// LTP = 2500, try to place order at 3100 (+24% above LTP)
	// Should fail: exceeds ±20% price band
	order := makeLimitOrder("C1", "RELIANCE", models.Buy, 3100, 10)
	order.ProductType = models.MIS

	err := engine.Validate(order, 2500)
	if err == nil {
		t.Error("expected price band rejection, got nil")
	}
	t.Logf("Correctly rejected: %v", err)
}

func TestRisk_FreezeQuantity(t *testing.T) {
	engine := risk.NewEngine()
	engine.RegisterAccount("C1", 999999999) // huge balance

	// Order for 600,000 shares — above freeze limit of 500,000
	order := makeLimitOrder("C1", "RELIANCE", models.Buy, 2500, 600000)
	order.ProductType = models.MIS

	err := engine.Validate(order, 2500)
	if err == nil {
		t.Error("expected freeze qty rejection, got nil")
	}
	t.Logf("Correctly rejected: %v", err)
}

func TestRisk_UnknownClient(t *testing.T) {
	engine := risk.NewEngine()
	// Don't register any client

	order := makeLimitOrder("GHOST", "RELIANCE", models.Buy, 2500, 100)
	err := engine.Validate(order, 2500)
	if err == nil {
		t.Error("expected client not found error")
	}
}

// TABLE-DRIVEN TEST - Multiple scenarios in one test

func TestRisk_TableDriven(t *testing.T) {
	cases := []struct {
		name        string
		balance     float64
		price       float64
		qty         int64
		ltp         float64
		product     models.ProductType
		expectError bool
	}{
		{"sufficient MIS margin",      500000, 2500, 100, 2500, models.MIS,  false},
		{"insufficient MIS margin",     10000, 2500, 100, 2500, models.MIS,  true},
		{"sufficient CNC margin",      500000, 500,  100, 500,  models.CNC,  false},
		{"insufficient CNC margin",     40000, 500,  100, 500,  models.CNC,  true},
		{"price above upper band",    1000000, 3100,  10, 2500, models.MIS,  true},
		{"price below lower band",    1000000, 1900,  10, 2500, models.MIS,  true},
		{"price within band",         1000000, 2600,  10, 2500, models.MIS,  false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			engine := risk.NewEngine()
			clientID := fmt.Sprintf("client-%s", tc.name)
			engine.RegisterAccount(clientID, tc.balance)

			order := makeLimitOrder(clientID, "RELIANCE", models.Buy, tc.price, tc.qty)
			order.ProductType = tc.product

			err := engine.Validate(order, tc.ltp)
			if tc.expectError && err == nil {
				t.Errorf("expected error but got nil")
			}
			if !tc.expectError && err != nil {
				t.Errorf("expected no error but got: %v", err)
			}
		})
	}
}
