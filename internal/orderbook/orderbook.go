package orderbook

import (
	"fmt"
	"sync"
	"time"

	"github.com/AastikM/go-oms/internal/models"
)

type PriceLevel struct {
	Price  float64
	Orders []*models.Order
	Total  int64
}

// OrderBook is the in memory order book for a single symbol.
// Thread safe — multiple goroutines can place orders concurrently.
type OrderBook struct {
	mu sync.RWMutex // RWMutex: multiple reads OK, write is exclusive

	Symbol   string
	Exchange models.Exchange

	// bids: sorted descending by price (index 0 = best bid = highest price)
	bids []*PriceLevel
	// asks: sorted ascending by price (index 0 = best ask = lowest price)
	asks []*PriceLevel

	// Quick lookup: order_id → order (for cancel/modify)
	orders map[string]*models.Order

	// Channel to emit events — matching engine listens on this
	// Every time a new order arrives, we send it here for matching
	EventCh chan Event
}

// EventType tells the matching engine what happened
type EventType string

const (
	EventNewOrder    EventType = "NEW_ORDER"
	EventCancelOrder EventType = "CANCEL_ORDER"
	EventModifyOrder EventType = "MODIFY_ORDER"
)

// Event is emitted by the order book whenever state changes
type Event struct {
	Type      EventType
	Order     *models.Order
	Timestamp time.Time
}

// NewOrderBook creates an order book for a given symbol.
func NewOrderBook(symbol string, exchange models.Exchange) *OrderBook {
	return &OrderBook{
		Symbol:   symbol,
		Exchange: exchange,
		bids:     make([]*PriceLevel, 0, 10),
		asks:     make([]*PriceLevel, 0, 10),
		orders:   make(map[string]*models.Order),
		EventCh:  make(chan Event, 1000), // buffered so adding doesn't block
	}
}

// Add places a new order into the book.
// Returns error if the order is already in the book.
func (ob *OrderBook) Add(order *models.Order) error {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	if _, exists := ob.orders[order.OrderID]; exists {
		return fmt.Errorf("order %s already exists in book", order.OrderID)
	}

	ob.orders[order.OrderID] = order
	order.UpdateStatus(models.StatusOpen, "Order placed on book")

	if order.Side == models.Buy {
		ob.addToBids(order)
	} else {
		ob.addToAsks(order)
	}

	ob.EventCh <- Event{Type: EventNewOrder, Order: order, Timestamp: time.Now()}
	return nil
}

// addToBids inserts a buy order maintaining descending price order.
// Same price? Append at end (time priority — earlier orders fill first).
func (ob *OrderBook) addToBids(order *models.Order) {
	price := order.Price

	// Find the right price level
	for i, level := range ob.bids {
		if level.Price == price {
			// Price level exists — append (time priority)
			level.Orders = append(level.Orders, order)
			level.Total += order.RemainingQty
			return
		}
		if level.Price < price {
			// Insert before this level (maintain descending order)
			newLevel := &PriceLevel{Price: price, Orders: []*models.Order{order}, Total: order.RemainingQty}
			ob.bids = append(ob.bids[:i], append([]*PriceLevel{newLevel}, ob.bids[i:]...)...)
			return
		}
	}

	// Price is lower than all existing bids - append at end
	ob.bids = append(ob.bids, &PriceLevel{Price: price, Orders: []*models.Order{order}, Total: order.RemainingQty})
}

// addToAsks inserts a sell order maintaining ascending price order.
func (ob *OrderBook) addToAsks(order *models.Order) {
	price := order.Price

	for i, level := range ob.asks {
		if level.Price == price {
			level.Orders = append(level.Orders, order)
			level.Total += order.RemainingQty
			return
		}
		if level.Price > price {
			// Insert before this level (ascending order)
			newLevel := &PriceLevel{Price: price, Orders: []*models.Order{order}, Total: order.RemainingQty}
			ob.asks = append(ob.asks[:i], append([]*PriceLevel{newLevel}, ob.asks[i:]...)...)
			return
		}
	}

	ob.asks = append(ob.asks, &PriceLevel{Price: price, Orders: []*models.Order{order}, Total: order.RemainingQty})
}

// Cancel removes an order from the book.
func (ob *OrderBook) Cancel(orderID string) error {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	order, exists := ob.orders[orderID]
	if !exists {
		return fmt.Errorf("order %s not found in book", orderID)
	}
	if !order.IsActive() {
		return fmt.Errorf("order %s is not active (status: %s)", orderID, order.Status)
	}

	// Remove from price level
	if order.Side == models.Buy {
		ob.removeFromLevels(ob.bids, order)
	} else {
		ob.removeFromLevels(ob.asks, order)
	}

	delete(ob.orders, orderID)
	order.UpdateStatus(models.StatusCancelled, "Cancelled by user")

	ob.EventCh <- Event{Type: EventCancelOrder, Order: order, Timestamp: time.Now()}
	return nil
}

func (ob *OrderBook) removeFromLevels(levels []*PriceLevel, order *models.Order) {
	for i, level := range levels {
		if level.Price == order.Price {
			for j, o := range level.Orders {
				if o.OrderID == order.OrderID {
					level.Orders = append(level.Orders[:j], level.Orders[j+1:]...)
					level.Total -= order.RemainingQty
					// Clean up empty price levels
					if len(level.Orders) == 0 {
						if order.Side == models.Buy {
							ob.bids = append(ob.bids[:i], ob.bids[i+1:]...)
						} else {
							ob.asks = append(ob.asks[:i], ob.asks[i+1:]...)
						}
					}
					return
				}
			}
		}
	}
}

// BestBid returns the highest buy price in the book (nil if empty).
func (ob *OrderBook) BestBid() *PriceLevel {
	ob.mu.RLock()
	defer ob.mu.RUnlock()
	if len(ob.bids) == 0 {
		return nil
	}
	return ob.bids[0]
}

// BestAsk returns the lowest sell price in the book (nil if empty).
func (ob *OrderBook) BestAsk() *PriceLevel {
	ob.mu.RLock()
	defer ob.mu.RUnlock()
	if len(ob.asks) == 0 {
		return nil
	}
	return ob.asks[0]
}

// Spread returns the difference between best ask and best bid.
// A tight spread = liquid market. Wide spread = illiquid.
func (ob *OrderBook) Spread() float64 {
	bid := ob.BestBid()
	ask := ob.BestAsk()
	if bid == nil || ask == nil {
		return 0
	}
	return ask.Price - bid.Price
}

// MarketDepth returns the top N price levels on each side.
func (ob *OrderBook) MarketDepth(levels int) (bids, asks []PriceLevel) {
	ob.mu.RLock()
	defer ob.mu.RUnlock()

	for i, level := range ob.bids {
		if i >= levels {
			break
		}
		bids = append(bids, *level)
	}
	for i, level := range ob.asks {
		if i >= levels {
			break
		}
		asks = append(asks, *level)
	}
	return
}

// GetOrder retrieves an order by ID (for status checks).
func (ob *OrderBook) GetOrder(orderID string) (*models.Order, bool) {
	ob.mu.RLock()
	defer ob.mu.RUnlock()
	o, ok := ob.orders[orderID]
	return o, ok
}

// BidLevels returns a copy of bid price levels (for matching engine).
func (ob *OrderBook) BidLevels() []*PriceLevel {
	ob.mu.RLock()
	defer ob.mu.RUnlock()
	return ob.bids
}

// AskLevels returns a copy of ask price levels (for matching engine).
func (ob *OrderBook) AskLevels() []*PriceLevel {
	ob.mu.RLock()
	defer ob.mu.RUnlock()
	return ob.asks
}

func (ob *OrderBook) Stats() map[string]interface{} {
	ob.mu.RLock()
	defer ob.mu.RUnlock()

	totalBidQty := int64(0)
	totalAskQty := int64(0)
	for _, l := range ob.bids {
		totalBidQty += l.Total
	}
	for _, l := range ob.asks {
		totalAskQty += l.Total
	}

	return map[string]interface{}{
		"symbol":        ob.Symbol,
		"bid_levels":    len(ob.bids),
		"ask_levels":    len(ob.asks),
		"total_bid_qty": totalBidQty,
		"total_ask_qty": totalAskQty,
		"total_orders":  len(ob.orders),
		"spread":        ob.Spread(),
	}
}
