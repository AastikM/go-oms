package gtt

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/AastikM/go-oms/internal/models"
)

type TriggerCondition string

const (
	LTPAbove TriggerCondition = "LTP_ABOVE" // fires when LTP >= trigger_price
	LTPBelow TriggerCondition = "LTP_BELOW" // fires when LTP <= trigger_price
)

type GTTStatus string

const (
	GTTActive    GTTStatus = "ACTIVE"
	GTTTriggered GTTStatus = "TRIGGERED"
	GTTExecuted  GTTStatus = "EXECUTED"
	GTTCancelled GTTStatus = "CANCELLED"
	GTTExpired   GTTStatus = "EXPIRED"
)

type GTTOrder struct {
	ID       string          `json:"id"`
	ClientID string          `json:"client_id"`
	Symbol   string          `json:"symbol"`
	Exchange models.Exchange `json:"exchange"`

	Condition    TriggerCondition `json:"condition"`
	TriggerPrice float64          `json:"trigger_price"`

	Side        models.OrderSide   `json:"side"`
	OrderType   models.OrderType   `json:"order_type"`
	ProductType models.ProductType `json:"product_type"`
	Quantity    int64              `json:"quantity"`
	LimitPrice  float64            `json:"limit_price"`

	PairedGTTID string `json:"paired_gtt_id,omitempty"`

	Status    GTTStatus `json:"status"`
	StatusMsg string    `json:"status_msg,omitempty"`

	FiredOrderID string `json:"fired_order_id,omitempty"`

	CreatedAt time.Time  `json:"created_at"`
	UpdatedAt time.Time  `json:"updated_at"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
}

func (g *GTTOrder) IsExpired() bool {
	if g.ExpiresAt == nil {
		return false
	}
	return time.Now().After(*g.ExpiresAt)
}

func (g *GTTOrder) CheckCondition(ltp float64) bool {
	switch g.Condition {
	case LTPAbove:
		return ltp >= g.TriggerPrice
	case LTPBelow:
		return ltp <= g.TriggerPrice
	}
	return false
}

type OrderPlacer interface {
	PlaceOrder(order *models.Order) error
}

type LTPProvider interface {
	GetLTP(symbol string) float64
}

type Manager struct {
	mu      sync.RWMutex
	orders  map[string]*GTTOrder
	placer  OrderPlacer
	prices  LTPProvider
	counter int64
	eventCh chan GTTEvent
}

type GTTEvent struct {
	GTT   *GTTOrder
	Event string
}

func NewManager(placer OrderPlacer, prices LTPProvider) *Manager {
	return &Manager{
		orders:  make(map[string]*GTTOrder),
		placer:  placer,
		prices:  prices,
		eventCh: make(chan GTTEvent, 1000),
	}
}

func (m *Manager) Add(gtt *GTTOrder) error {
	if gtt.TriggerPrice <= 0 {
		return fmt.Errorf("trigger_price must be positive")
	}
	if gtt.Quantity <= 0 {
		return fmt.Errorf("quantity must be positive")
	}
	if gtt.Symbol == "" {
		return fmt.Errorf("symbol required")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.counter++
	gtt.ID = fmt.Sprintf("GTT-%s-%d", gtt.ClientID, m.counter)
	gtt.Status = GTTActive
	gtt.CreatedAt = time.Now()
	gtt.UpdatedAt = time.Now()

	m.orders[gtt.ID] = gtt
	log.Printf("[GTT] Registered %s: %s %s %d@trigger=%.2f (%s)",
		gtt.ID, gtt.Side, gtt.Symbol, gtt.Quantity, gtt.TriggerPrice, gtt.Condition)

	return nil
}

func (m *Manager) Cancel(gttID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	gtt, ok := m.orders[gttID]
	if !ok {
		return fmt.Errorf("GTT %s not found", gttID)
	}
	if gtt.Status != GTTActive {
		return fmt.Errorf("GTT %s is not active (status: %s)", gttID, gtt.Status)
	}

	gtt.Status = GTTCancelled
	gtt.UpdatedAt = time.Now()
	m.eventCh <- GTTEvent{GTT: gtt, Event: "cancelled"}
	return nil
}

func (m *Manager) GetByClient(clientID string) []*GTTOrder {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []*GTTOrder
	for _, gtt := range m.orders {
		if gtt.ClientID == clientID {
			result = append(result, gtt)
		}
	}
	return result
}

func (m *Manager) Events() <-chan GTTEvent {
	return m.eventCh
}

func (m *Manager) Start(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	log.Println("[GTT] Watcher started (polling every 1s)")
	for {
		select {
		case <-ticker.C:
			m.scan()
		case <-ctx.Done():
			log.Println("[GTT] Watcher stopped")
			return
		}
	}
}

func (m *Manager) scan() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, gtt := range m.orders {
		if gtt.Status != GTTActive {
			continue
		}

		if gtt.IsExpired() {
			gtt.Status = GTTExpired
			gtt.UpdatedAt = time.Now()
			m.eventCh <- GTTEvent{GTT: gtt, Event: "expired"}
			log.Printf("[GTT] Expired: %s", gtt.ID)
			continue
		}

		ltp := m.prices.GetLTP(gtt.Symbol)
		if ltp <= 0 {
			continue // no price data yet
		}

		if !gtt.CheckCondition(ltp) {
			continue
		}

		log.Printf("[GTT] TRIGGERED: %s | %s %s %d | LTP=%.2f trigger=%.2f",
			gtt.ID, gtt.Side, gtt.Symbol, gtt.Quantity, ltp, gtt.TriggerPrice)

		gtt.Status = GTTTriggered
		gtt.UpdatedAt = time.Now()

		order := m.buildOrder(gtt, ltp)
		if err := m.placer.PlaceOrder(order); err != nil {
			log.Printf("[GTT] Failed to place triggered order: %v", err)
			gtt.Status = GTTActive
			continue
		}

		gtt.FiredOrderID = order.OrderID
		m.eventCh <- GTTEvent{GTT: gtt, Event: "triggered"}

		// If this GTT has an OCO pair, cancel it
		if gtt.PairedGTTID != "" {
			if paired, ok := m.orders[gtt.PairedGTTID]; ok && paired.Status == GTTActive {
				paired.Status = GTTCancelled
				paired.StatusMsg = fmt.Sprintf("Cancelled by OCO pair %s", gtt.ID)
				paired.UpdatedAt = time.Now()
				m.eventCh <- GTTEvent{GTT: paired, Event: "cancelled"}
				log.Printf("[GTT] OCO: cancelled paired GTT %s", paired.ID)
			}
		}
	}
}

// buildOrder constructs the models.Order to fire when a GTT triggers.
func (m *Manager) buildOrder(gtt *GTTOrder, ltp float64) *models.Order {
	order := models.NewOrder(gtt.ClientID, gtt.Symbol)
	order.Exchange = gtt.Exchange
	order.Side = gtt.Side
	order.ProductType = gtt.ProductType
	order.Validity = models.DAY
	order.Quantity = gtt.Quantity
	order.Tag = fmt.Sprintf("GTT:%s", gtt.ID)

	// If limit price specified, use LIMIT order; else MARKET
	if gtt.LimitPrice > 0 {
		order.OrderType = models.Limit
		order.Price = gtt.LimitPrice
	} else {
		order.OrderType = models.Market
		order.Price = ltp // approximation for margin calculation
	}

	return order
}

// GTTStore is the interface for Redis GTT operations.
// Matches the methods added to RedisStore.
type GTTStore interface {
	GetTriggeredGTTs(symbol string, ltp float64) ([]string, error)
	GetGTTData(gttID string) ([]byte, error)
	RemoveGTT(gttID, symbol, condition string) error
	AddGTT(gttID, symbol, condition string, triggerPrice float64, data interface{}) error
}
