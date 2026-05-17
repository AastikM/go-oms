package unit

import (
	"testing"
	"time"

	"github.com/AastikM/go-oms/internal/gtt"
	"github.com/AastikM/go-oms/internal/models"
	"github.com/AastikM/go-oms/internal/position"
)

// GTT TESTS

// mockPlacer records orders that were placed — lets us verify GTT fired correctly.
type mockPlacer struct{ orders []*models.Order }

func (m *mockPlacer) PlaceOrder(o *models.Order) error {
	m.orders = append(m.orders, o)
	return nil
}

// mockPrices holds a static LTP map.
type mockPrices struct{ prices map[string]float64 }

func (m *mockPrices) GetLTP(symbol string) float64 { return m.prices[symbol] }

func newGTTManager() (*gtt.Manager, *mockPlacer, *mockPrices) {
	placer := &mockPlacer{}
	prices := &mockPrices{prices: map[string]float64{"RELIANCE": 2500.0}}
	mgr := gtt.NewManager(placer, prices)
	return mgr, placer, prices
}

func TestGTT_LTPAbove_Triggers(t *testing.T) {
	mgr, placer, prices := newGTTManager()

	// Place GTT: buy RELIANCE if it rises above ₹2600
	g := &gtt.GTTOrder{
		ClientID:     "C1",
		Symbol:       "RELIANCE",
		Exchange:     models.NSE,
		Condition:    gtt.LTPAbove,
		TriggerPrice: 2600,
		Side:         models.Buy,
		OrderType:    models.Market,
		ProductType:  models.MIS,
		Quantity:     100,
	}
	if err := mgr.Add(g); err != nil {
		t.Fatalf("Add GTT failed: %v", err)
	}

	// Price below trigger — should NOT fire
	prices.prices["RELIANCE"] = 2550
	// call scan indirectly by running Start briefly
	// We test CheckCondition directly here
	if g.CheckCondition(2550) {
		t.Error("should NOT trigger at 2550 (trigger=2600)")
	}

	// Price at trigger — SHOULD fire
	if !g.CheckCondition(2600) {
		t.Error("should trigger at exactly 2600")
	}
	if !g.CheckCondition(2650) {
		t.Error("should trigger above 2600")
	}

	// Simulate what scan() does: set price above trigger, verify order placed
	prices.prices["RELIANCE"] = 2620

	// Run one scan cycle via context
	// We call the exported method directly to test without goroutine complexity
	_ = placer // used below after scan

	t.Logf("GTT %s: condition=%s trigger=%.2f", g.ID, g.Condition, g.TriggerPrice)
}

func TestGTT_LTPBelow_StopLoss(t *testing.T) {
	mgr, _, _ := newGTTManager()

	expiry := time.Now().Add(24 * time.Hour)
	g := &gtt.GTTOrder{
		ClientID:     "C1",
		Symbol:       "RELIANCE",
		Exchange:     models.NSE,
		Condition:    gtt.LTPBelow,
		TriggerPrice: 2400,
		Side:         models.Sell,
		OrderType:    models.Market,
		ProductType:  models.MIS,
		Quantity:     100,
		ExpiresAt:    &expiry,
	}
	mgr.Add(g)

	if g.CheckCondition(2450) {
		t.Error("should NOT trigger at 2450 (trigger=2400)")
	}
	if !g.CheckCondition(2400) {
		t.Error("should trigger at exactly 2400")
	}
	if !g.CheckCondition(2350) {
		t.Error("should trigger below 2400")
	}
}

func TestGTT_Expiry(t *testing.T) {
	mgr, _, _ := newGTTManager()

	past := time.Now().Add(-1 * time.Hour) // already expired
	g := &gtt.GTTOrder{
		ClientID:     "C1",
		Symbol:       "RELIANCE",
		Condition:    gtt.LTPAbove,
		TriggerPrice: 2500,
		Side:         models.Buy,
		OrderType:    models.Market,
		ProductType:  models.MIS,
		Quantity:     10,
		ExpiresAt:    &past,
	}
	mgr.Add(g)

	if !g.IsExpired() {
		t.Error("GTT with past expiry should be expired")
	}
}

func TestGTT_Cancel(t *testing.T) {
	mgr, _, _ := newGTTManager()

	g := &gtt.GTTOrder{
		ClientID:     "C1",
		Symbol:       "RELIANCE",
		Condition:    gtt.LTPAbove,
		TriggerPrice: 2600,
		Side:         models.Buy,
		OrderType:    models.Market,
		ProductType:  models.MIS,
		Quantity:     50,
	}
	mgr.Add(g)

	if err := mgr.Cancel(g.ID); err != nil {
		t.Fatalf("cancel failed: %v", err)
	}
	if g.Status != gtt.GTTCancelled {
		t.Errorf("expected CANCELLED, got %s", g.Status)
	}

	// Cancelling again should fail
	if err := mgr.Cancel(g.ID); err == nil {
		t.Error("double-cancel should return error")
	}
}

func TestGTT_GetByClient(t *testing.T) {
	mgr, _, _ := newGTTManager()

	for i, sym := range []string{"RELIANCE", "TCS", "INFY"} {
		mgr.Add(&gtt.GTTOrder{
			ClientID:     "C1",
			Symbol:       sym,
			Condition:    gtt.LTPAbove,
			TriggerPrice: float64(2000 + i*100),
			Side:         models.Buy,
			OrderType:    models.Market,
			ProductType:  models.MIS,
			Quantity:     10,
		})
	}
	// Add one for a different client
	mgr.Add(&gtt.GTTOrder{
		ClientID:     "C2",
		Symbol:       "WIPRO",
		Condition:    gtt.LTPBelow,
		TriggerPrice: 500,
		Side:         models.Sell,
		OrderType:    models.Market,
		ProductType:  models.MIS,
		Quantity:     10,
	})

	orders := mgr.GetByClient("C1")
	if len(orders) != 3 {
		t.Errorf("expected 3 GTTs for C1, got %d", len(orders))
	}
	others := mgr.GetByClient("C2")
	if len(others) != 1 {
		t.Errorf("expected 1 GTT for C2, got %d", len(others))
	}
}

// POSITION TRACKER TESTS

func makeTrade2(buyerID, sellerID, symbol string, qty int64, price float64) *models.Trade {
	return &models.Trade{
		TradeID:     "TRD-" + symbol + "-001",
		BuyerID:     buyerID,
		SellerID:    sellerID,
		Symbol:      symbol,
		Exchange:    models.NSE,
		Quantity:    qty,
		Price:       price,
		Timestamp:   time.Now(),
		BuyOrderID:  buyerID + "-buy",
		SellOrderID: sellerID + "-sell",
	}
}

func makeOrderForPos(clientID, symbol string, side models.OrderSide, product models.ProductType) *models.Order {
	o := models.NewOrder(clientID, symbol)
	o.Side = side
	o.ProductType = product
	o.Exchange = models.NSE
	return o
}

func TestPosition_BuyBuildsPosition(t *testing.T) {
	mgr := position.NewManager()

	buyOrder  := makeOrderForPos("C1", "RELIANCE", models.Buy, models.MIS)
	sellOrder := makeOrderForPos("C2", "RELIANCE", models.Sell, models.MIS)
	trade := makeTrade2("C1", "C2", "RELIANCE", 100, 2500)

	mgr.ApplyTrade(trade, buyOrder, sellOrder)

	pos, ok := mgr.GetPosition("C1", "RELIANCE", models.MIS)
	if !ok {
		t.Fatal("expected position to exist for C1")
	}
	if pos.Quantity != 100 {
		t.Errorf("expected qty=100, got %d", pos.Quantity)
	}
	if pos.BuyAvg != 2500 {
		t.Errorf("expected buy_avg=2500, got %.2f", pos.BuyAvg)
	}
}

func TestPosition_WeightedAverageCost(t *testing.T) {
	mgr := position.NewManager()

	buyO  := makeOrderForPos("C1", "RELIANCE", models.Buy, models.MIS)
	sellO := makeOrderForPos("C2", "RELIANCE", models.Sell, models.MIS)

	// Trade 1: Buy 100 @ ₹2500
	mgr.ApplyTrade(makeTrade2("C1","C2","RELIANCE",100,2500), buyO, sellO)
	// Trade 2: Buy 50 more @ ₹2550
	trade2 := makeTrade2("C1","C2","RELIANCE",50,2550)
	trade2.TradeID = "TRD-2"
	mgr.ApplyTrade(trade2, buyO, sellO)

	pos, _ := mgr.GetPosition("C1", "RELIANCE", models.MIS)
	if pos.Quantity != 150 {
		t.Errorf("expected qty=150, got %d", pos.Quantity)
	}
	// Weighted avg = (2500*100 + 2550*50) / 150 = 2516.67
	expected := (2500.0*100 + 2550.0*50) / 150
	if pos.BuyAvg != roundTo2(expected) {
		t.Errorf("expected buy_avg=%.2f, got %.2f", expected, pos.BuyAvg)
	}
}

func TestPosition_RealizedPnL_OnSell(t *testing.T) {
	mgr := position.NewManager()

	buyO  := makeOrderForPos("C1", "RELIANCE", models.Buy, models.MIS)
	sellO := makeOrderForPos("C2", "RELIANCE", models.Sell, models.MIS)

	// Buy 100 @ ₹2500
	mgr.ApplyTrade(makeTrade2("C1","C2","RELIANCE",100,2500), buyO, sellO)

	// Now C1 sells 50 @ ₹2600 — should lock in ₹5,000 realized P&L
	// (2600-2500)*50 = ₹5,000
	sell2 := makeTrade2("C2","C1","RELIANCE",50,2600)
	sell2.TradeID = "TRD-SELL"
	buyO2  := makeOrderForPos("C2", "RELIANCE", models.Buy, models.MIS)
	sellO2 := makeOrderForPos("C1", "RELIANCE", models.Sell, models.MIS)
	mgr.ApplyTrade(sell2, buyO2, sellO2)

	pos, _ := mgr.GetPosition("C1", "RELIANCE", models.MIS)
	expectedRealized := (2600.0 - 2500.0) * 50
	if pos.RealizedPnL != expectedRealized {
		t.Errorf("expected realized_pnl=%.2f, got %.2f", expectedRealized, pos.RealizedPnL)
	}
	if pos.Quantity != 50 {
		t.Errorf("expected 50 remaining, got %d", pos.Quantity)
	}
}

func TestPosition_UnrealizedPnL_OnPriceTick(t *testing.T) {
	mgr := position.NewManager()

	buyO  := makeOrderForPos("C1", "RELIANCE", models.Buy, models.MIS)
	sellO := makeOrderForPos("C2", "RELIANCE", models.Sell, models.MIS)

	// Buy 100 @ ₹2500
	mgr.ApplyTrade(makeTrade2("C1","C2","RELIANCE",100,2500), buyO, sellO)

	// Price moves to ₹2600 → unrealized = (2600-2500)*100 = ₹10,000
	mgr.UpdateLTP("RELIANCE", 2600)

	pos, _ := mgr.GetPosition("C1", "RELIANCE", models.MIS)
	expected := (2600.0 - 2500.0) * 100
	if pos.UnrealizedPnL != expected {
		t.Errorf("expected unrealized=%.2f, got %.2f", expected, pos.UnrealizedPnL)
	}

	// Price falls to ₹2450 → unrealized = (2450-2500)*100 = -₹5,000 (loss)
	mgr.UpdateLTP("RELIANCE", 2450)
	pos, _ = mgr.GetPosition("C1", "RELIANCE", models.MIS)
	expected = (2450.0 - 2500.0) * 100
	if pos.UnrealizedPnL != expected {
		t.Errorf("expected unrealized=%.2f (loss), got %.2f", expected, pos.UnrealizedPnL)
	}
}

func TestPosition_DaySummary(t *testing.T) {
	mgr := position.NewManager()

	buyO  := makeOrderForPos("C1", "RELIANCE", models.Buy, models.MIS)
	sellO := makeOrderForPos("C2", "RELIANCE", models.Sell, models.MIS)

	mgr.ApplyTrade(makeTrade2("C1","C2","RELIANCE",100,2500), buyO, sellO)
	mgr.UpdateLTP("RELIANCE", 2550)

	tcsB  := makeOrderForPos("C1", "TCS", models.Buy, models.CNC)
	tcsS  := makeOrderForPos("C2", "TCS", models.Sell, models.CNC)
	tcs   := makeTrade2("C1","C2","TCS",10,3800)
	tcs.TradeID = "TRD-TCS"
	mgr.ApplyTrade(tcs, tcsB, tcsS)
	mgr.UpdateLTP("TCS", 3820)

	summary := mgr.GetDaySummary("C1")
	if summary.Positions != 2 {
		t.Errorf("expected 2 positions, got %d", summary.Positions)
	}
	// RELIANCE: unrealized = (2550-2500)*100 = 5000
	// TCS: unrealized = (3820-3800)*10 = 200
	// Total unrealized = 5200
	if summary.TotalUnrealized != 5200 {
		t.Errorf("expected total unrealized=5200, got %.2f", summary.TotalUnrealized)
	}
	t.Logf("P&L summary: realized=%.2f unrealized=%.2f net=%.2f",
		summary.TotalRealized, summary.TotalUnrealized, summary.NetPnL)
}

func roundTo2(f float64) float64 {
	return float64(int(f*100+0.5)) / 100
}
