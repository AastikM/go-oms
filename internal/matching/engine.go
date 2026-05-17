package matching

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/AastikM/go-oms/internal/models"
	"github.com/AastikM/go-oms/internal/orderbook"
)

type Engine struct {
	mu      sync.Mutex
	book    *orderbook.OrderBook
	TradeCh chan *models.Trade

	tradeCounter int64
	running      bool
	stopCh       chan struct{}
}

func NewEngine(book *orderbook.OrderBook) *Engine {
	return &Engine{
		book:    book,
		TradeCh: make(chan *models.Trade, 10000),
		stopCh:  make(chan struct{}),
	}
}

func (e *Engine) Start() {
	e.running = true
	go e.matchLoop()
	log.Printf("[MatchingEngine] Started for symbol: %s", e.book.Symbol)
}

func (e *Engine) Stop() {
	close(e.stopCh)
	e.running = false
}

func (e *Engine) matchLoop() {
	for {
		select {
		case event := <-e.book.EventCh:
			if event.Type == orderbook.EventNewOrder {
				e.match(event.Order)
			}
		case <-e.stopCh:
			log.Printf("[MatchingEngine] Stopped for symbol: %s", e.book.Symbol)
			return
		}
	}
}

// This is the core of what every exchange and broker does.
func (e *Engine) match(incoming *models.Order) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if incoming.OrderType == models.Market {
		e.matchMarketOrder(incoming)
	} else {
		e.matchLimitOrder(incoming)
	}
}

func (e *Engine) matchLimitOrder(incoming *models.Order) {
	if incoming.Side == models.Buy {
		for _, level := range e.book.AskLevels() {
			if level.Price > incoming.Price {
				break
			}
			if incoming.RemainingQty == 0 {
				break
			}
			e.fillAgainstLevel(incoming, level)
		}
	} else {

		for _, level := range e.book.BidLevels() {
			if level.Price < incoming.Price {
				break
			}
			if incoming.RemainingQty == 0 {
				break
			}
			e.fillAgainstLevel(incoming, level)
		}
	}
}

func (e *Engine) matchMarketOrder(incoming *models.Order) {
	if incoming.Side == models.Buy {
		for _, level := range e.book.AskLevels() {
			if incoming.RemainingQty == 0 {
				break
			}
			e.fillAgainstLevel(incoming, level)
		}
	} else {
		for _, level := range e.book.BidLevels() {
			if incoming.RemainingQty == 0 {
				break
			}
			e.fillAgainstLevel(incoming, level)
		}
	}

	if incoming.RemainingQty > 0 && incoming.Status != models.StatusFilled {
		incoming.UpdateStatus(models.StatusRejected, "No liquidity available for market order")
	}
}

func (e *Engine) fillAgainstLevel(incoming *models.Order, level *orderbook.PriceLevel) {
	for _, resting := range level.Orders {
		if !resting.IsActive() {
			continue
		}
		if incoming.RemainingQty == 0 {
			break
		}

		fillQty := min64(incoming.RemainingQty, resting.RemainingQty)

		tradePrice := resting.Price
		if resting.OrderType == models.Market {
			tradePrice = level.Price
		}

		trade := e.createTrade(incoming, resting, fillQty, tradePrice)

		incoming.Fill(fillQty, tradePrice)
		resting.Fill(fillQty, tradePrice)

		select {
		case e.TradeCh <- trade:
		default:
			log.Printf("[MatchingEngine] WARNING: TradeCh full, dropping trade %s", trade.TradeID)
		}

		log.Printf("[MatchingEngine] TRADE: %s %d@%.2f | buy:%s sell:%s",
			incoming.Symbol, fillQty, tradePrice, trade.BuyOrderID, trade.SellOrderID)
	}
}

func (e *Engine) createTrade(order1, order2 *models.Order, qty int64, price float64) *models.Trade {
	e.tradeCounter++

	buyOrder, sellOrder := order1, order2
	if order1.Side == models.Sell {
		buyOrder, sellOrder = order2, order1
	}

	return &models.Trade{
		TradeID:     fmt.Sprintf("TRD-%s-%d", e.book.Symbol, e.tradeCounter),
		BuyOrderID:  buyOrder.OrderID,
		SellOrderID: sellOrder.OrderID,
		Symbol:      e.book.Symbol,
		Exchange:    e.book.Exchange,
		Quantity:    qty,
		Price:       price,
		Timestamp:   time.Now(),
		BuyerID:     buyOrder.ClientID,
		SellerID:    sellOrder.ClientID,
	}
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
