package position

import (
	"fmt"
	"log"
	"math"
	"sync"
	"time"

	"github.com/AastikM/go-oms/internal/models"
)

// This is the in-memory version — also written to Redis and Postgres.
type Position struct {
	ClientID    string
	Symbol      string
	Exchange    models.Exchange
	ProductType models.ProductType

	Quantity int64 // net (positive = long, negative = short)
	BuyQty   int64
	SellQty  int64

	// Average prices (weighted)
	BuyAvg  float64
	SellAvg float64

	RealizedPnL   float64
	UnrealizedPnL float64
	LastPrice     float64

	TotalCharges float64

	UpdatedAt time.Time
}

// NetPnL is realized + unrealized minus charges.
func (p *Position) NetPnL() float64 {
	return math.Round((p.RealizedPnL+p.UnrealizedPnL-p.TotalCharges)*100) / 100
}

func PositionKey(clientID, symbol string, product models.ProductType) string {
	return fmt.Sprintf("%s:%s:%s", clientID, symbol, string(product))
}

type UpdateEvent struct {
	Position *Position
	Trigger  string // "trade" or "price_tick"
}

// Manager maintains all client positions in memory and
// recalculates P&L on every trade and price tick.
type Manager struct {
	mu        sync.RWMutex
	positions map[string]*Position // key → position
	eventCh   chan UpdateEvent
}

// NewManager creates a position manager.
func NewManager() *Manager {
	return &Manager{
		positions: make(map[string]*Position),
		eventCh:   make(chan UpdateEvent, 10000),
	}
}

// Events returns the channel for position update events.
// The OMS listens here to push updates over WebSocket.
func (m *Manager) Events() <-chan UpdateEvent {
	return m.eventCh
}

// ApplyTrade updates the relevant positions when a trade executes.
// Called by the OMS's trade consumer goroutine.
func (m *Manager) ApplyTrade(trade *models.Trade, buyOrder, sellOrder *models.Order) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Update buyer's position
	buyPos := m.getOrCreate(trade.BuyerID, trade.Symbol, trade.Exchange, buyOrder.ProductType)
	m.applyBuy(buyPos, trade.Quantity, trade.Price)
	m.emit(buyPos, "trade")

	// Update seller's position
	sellPos := m.getOrCreate(trade.SellerID, trade.Symbol, trade.Exchange, sellOrder.ProductType)
	m.applyShortSell(sellPos, trade.Quantity, trade.Price, buyOrder.ProductType)
	m.emit(sellPos, "trade")

	log.Printf("[Position] Updated: buyer=%s seller=%s %s %d@%.2f",
		trade.BuyerID, trade.SellerID, trade.Symbol, trade.Quantity, trade.Price)
}

// UpdateLTP recalculates unrealized P&L for all positions in a symbol
// when the price moves. Called on every market data tick.
func (m *Manager) UpdateLTP(symbol string, ltp float64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, pos := range m.positions {
		if pos.Symbol != symbol || pos.Quantity == 0 {
			continue
		}
		pos.LastPrice = ltp

		// Unrealized P&L = (current price - average cost) × net quantity
		// For long:  positive when price > avg cost
		// For short: positive when price < avg sell price
		if pos.Quantity > 0 {
			// Long position
			pos.UnrealizedPnL = math.Round(
				(ltp-pos.BuyAvg)*float64(pos.Quantity)*100) / 100
		} else if pos.Quantity < 0 {
			// Short position
			pos.UnrealizedPnL = math.Round(
				(pos.SellAvg-ltp)*float64(-pos.Quantity)*100) / 100
		}

		pos.UpdatedAt = time.Now()
		m.emit(pos, "price_tick")
	}
}

// GetPosition returns the current position for a client/symbol/product.
func (m *Manager) GetPosition(clientID, symbol string, product models.ProductType) (*Position, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	key := PositionKey(clientID, symbol, product)
	pos, ok := m.positions[key]
	return pos, ok
}

// GetAllPositions returns all positions for a client.
func (m *Manager) GetAllPositions(clientID string) []*Position {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []*Position
	for _, pos := range m.positions {
		if pos.ClientID == clientID {
			result = append(result, pos)
		}
	}
	return result
}

// DaySummary returns total P&L across all positions for a client today.
type DaySummary struct {
	ClientID        string
	TotalRealized   float64
	TotalUnrealized float64
	TotalCharges    float64
	NetPnL          float64
	Positions       int
}

func (m *Manager) GetDaySummary(clientID string) DaySummary {
	m.mu.RLock()
	defer m.mu.RUnlock()

	summary := DaySummary{ClientID: clientID}
	for _, pos := range m.positions {
		if pos.ClientID != clientID {
			continue
		}
		summary.TotalRealized += pos.RealizedPnL
		summary.TotalUnrealized += pos.UnrealizedPnL
		summary.TotalCharges += pos.TotalCharges
		summary.Positions++
	}
	summary.NetPnL = math.Round(
		(summary.TotalRealized+summary.TotalUnrealized-summary.TotalCharges)*100) / 100
	return summary
}

// WEIGHTED AVERAGE COST:
//
//	If you bought 100 @ ₹2500 and now buy 50 @ ₹2600:
//	new_avg = (2500*100 + 2600*50) / 150 = ₹2533.33
func (m *Manager) applyBuy(pos *Position, qty int64, price float64) {
	if pos.Quantity < 0 {
		// Covering a short position — calculate realized P&L on covered portion
		coverQty := qty
		if coverQty > -pos.Quantity {
			coverQty = -pos.Quantity
		}
		// Profit on short = sell price (when we shorted) - buy price (covering now)
		realized := (pos.SellAvg - price) * float64(coverQty)
		pos.RealizedPnL = math.Round((pos.RealizedPnL+realized)*100) / 100
		pos.Quantity += coverQty
		qty -= coverQty
	}

	if qty > 0 {
		// Adding to long position — update weighted average
		totalCost := pos.BuyAvg*float64(pos.BuyQty) + price*float64(qty)
		pos.BuyQty += qty
		pos.Quantity += qty
		if pos.BuyQty > 0 {
			pos.BuyAvg = math.Round(totalCost/float64(pos.BuyQty)*100) / 100
		}
	}
	pos.UpdatedAt = time.Now()
}

// applyShortSell updates a position when the client sells shares.
//
// TWO CASES:
//  1. Selling from existing long (closing/reducing position)
//     → realize P&L = (sell_price - buy_avg) * sold_qty
//  2. Selling short (no existing long, or selling more than held)
//     → open a short position, track sell_avg
func (m *Manager) applyShortSell(pos *Position, qty int64, price float64, _ models.ProductType) {
	if pos.Quantity > 0 {
		// Selling from long position
		sellFromLong := qty
		if sellFromLong > pos.Quantity {
			sellFromLong = pos.Quantity
		}
		realized := (price - pos.BuyAvg) * float64(sellFromLong)
		pos.RealizedPnL = math.Round((pos.RealizedPnL+realized)*100) / 100
		pos.Quantity -= sellFromLong
		pos.SellQty += sellFromLong
		qty -= sellFromLong
	}

	if qty > 0 {
		// Going short — track average sell price
		totalProceeds := pos.SellAvg*float64(pos.SellQty) + price*float64(qty)
		pos.SellQty += qty
		pos.Quantity -= qty // becomes negative
		if pos.SellQty > 0 {
			pos.SellAvg = math.Round(totalProceeds/float64(pos.SellQty)*100) / 100
		}
	}
	pos.UpdatedAt = time.Now()
}

func (m *Manager) getOrCreate(clientID, symbol string, exchange models.Exchange, product models.ProductType) *Position {
	key := PositionKey(clientID, symbol, product)
	if pos, ok := m.positions[key]; ok {
		return pos
	}
	pos := &Position{
		ClientID:    clientID,
		Symbol:      symbol,
		Exchange:    exchange,
		ProductType: product,
		UpdatedAt:   time.Now(),
	}
	m.positions[key] = pos
	return pos
}

func (m *Manager) emit(pos *Position, trigger string) {
	// Copy to avoid races — the consumer reads this after the lock is released
	copy := *pos
	select {
	case m.eventCh <- UpdateEvent{Position: &copy, Trigger: trigger}:
	default:
	}
}

func (p *Position) ToModel() *models.Position {
	return &models.Position{
		ClientID:      p.ClientID,
		Symbol:        p.Symbol,
		Exchange:      p.Exchange,
		ProductType:   p.ProductType,
		Quantity:      p.Quantity,
		BuyQty:        p.BuyQty,
		SellQty:       p.SellQty,
		BuyAvg:        p.BuyAvg,
		SellAvg:       p.SellAvg,
		RealizedPnL:   p.RealizedPnL,
		UnrealizedPnL: p.UnrealizedPnL,
		LastPrice:     p.LastPrice,
		UpdatedAt:     p.UpdatedAt,
	}
}
