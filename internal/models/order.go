package models

import (
	"fmt"
	"time"
)

type OrderSide string

const (
	Buy  OrderSide = "BUY"
	Sell OrderSide = "SELL"
)

type OrderType string

const (
	Market         OrderType = "MARKET"
	Limit          OrderType = "LIMIT"
	StopLoss       OrderType = "SL"
	StopLossMarket OrderType = "SL-M"
)

type ProductType string

const (
	CNC  ProductType = "CNC"
	MIS  ProductType = "MIS"
	NRML ProductType = "NRML"
)

type OrderValidity string

const (
	DAY OrderValidity = "DAY"
	IOC OrderValidity = "IOC"
	GTT OrderValidity = "GTT"
)

type Exchange string

const (
	NSE Exchange = "NSE"
	BSE Exchange = "BSE"
	MCX Exchange = "MCX" // commodity exchange
)

type OrderStatus string

const (
	StatusPending         OrderStatus = "PENDING"          // received, not yet sent to exchange
	StatusOpen            OrderStatus = "OPEN"             // sitting on exchange order book
	StatusPartiallyFilled OrderStatus = "PARTIALLY_FILLED" // some qty filled, rest open
	StatusFilled          OrderStatus = "FILLED"           // fully executed
	StatusCancelled       OrderStatus = "CANCELLED"        // cancelled by user or system
	StatusRejected        OrderStatus = "REJECTED"         // failed pre-trade risk checks
	StatusModified        OrderStatus = "MODIFIED"         // price/qty changed while open
)

type Order struct {
	OrderID   string    `json:"order_id"`
	ClientID  string    `json:"client_id"`
	Timestamp time.Time `json:"timestamp"`

	Symbol   string    `json:"symbol"`
	Exchange Exchange  `json:"exchange"`
	Side     OrderSide `json:"side"`

	OrderType   OrderType     `json:"order_type"`
	ProductType ProductType   `json:"product_type"`
	Validity    OrderValidity `json:"validity"`

	Quantity     int64   `json:"quantity"`
	FilledQty    int64   `json:"filled_qty"`
	RemainingQty int64   `json:"remaining_qty"`
	Price        float64 `json:"price"`
	TriggerPrice float64 `json:"trigger_price"`
	AveragePrice float64 `json:"average_price"`

	DisclosedQty int64 `json:"disclosed_qty"`
	IsAMO        bool  `json:"is_amo"`

	Status    OrderStatus `json:"status"`
	StatusMsg string      `json:"status_msg"`
	UpdatedAt time.Time   `json:"updated_at"`

	ExchangeOrderID string `json:"exchange_order_id"`
	Tag             string `json:"tag"`
}

type Trade struct {
	TradeID     string    `json:"trade_id"`
	BuyOrderID  string    `json:"buy_order_id"`
	SellOrderID string    `json:"sell_order_id"`
	Symbol      string    `json:"symbol"`
	Exchange    Exchange  `json:"exchange"`
	Quantity    int64     `json:"quantity"`
	Price       float64   `json:"price"`
	Timestamp   time.Time `json:"timestamp"`
	BuyerID     string    `json:"buyer_id"`
	SellerID    string    `json:"seller_id"`
}

type Position struct {
	ClientID    string      `json:"client_id"`
	Symbol      string      `json:"symbol"`
	Exchange    Exchange    `json:"exchange"`
	ProductType ProductType `json:"product_type"`

	Quantity int64   `json:"quantity"`
	BuyQty   int64   `json:"buy_qty"`
	SellQty  int64   `json:"sell_qty"`
	BuyAvg   float64 `json:"buy_avg"`
	SellAvg  float64 `json:"sell_avg"`

	// PnL
	RealizedPnL   float64 `json:"realized_pnl"`
	UnrealizedPnL float64 `json:"unrealized_pnl"`
	LastPrice     float64 `json:"last_price"`

	UpdatedAt time.Time `json:"updated_at"`
}

type MarginInfo struct {
	Required  float64 `json:"required"`
	Available float64 `json:"available"`
	Used      float64 `json:"used"`
	Free      float64 `json:"free"`
}

// Sufficient returns true if the user has enough margin for this order.
func (m MarginInfo) Sufficient() bool {
	return m.Free >= m.Required
}

// NewOrder creates a new order with sane defaults.
func NewOrder(clientID, symbol string) *Order {
	now := time.Now()
	return &Order{
		OrderID:     generateOrderID(clientID, now),
		ClientID:    clientID,
		Symbol:      symbol,
		Exchange:    NSE,
		ProductType: MIS,
		Validity:    DAY,
		Status:      StatusPending,
		Timestamp:   now,
		UpdatedAt:   now,
	}
}

func (o *Order) UpdateStatus(status OrderStatus, msg string) {
	o.Status = status
	o.StatusMsg = msg
	o.UpdatedAt = time.Now()
}

func (o *Order) Fill(qty int64, price float64) {
	totalCost := o.AveragePrice*float64(o.FilledQty) + price*float64(qty)
	o.FilledQty += qty
	o.RemainingQty = o.Quantity - o.FilledQty
	if o.FilledQty > 0 {
		o.AveragePrice = totalCost / float64(o.FilledQty)
	}

	if o.RemainingQty == 0 {
		o.UpdateStatus(StatusFilled, "Order fully executed")
	} else {
		o.UpdateStatus(StatusPartiallyFilled, fmt.Sprintf("Filled %d of %d", o.FilledQty, o.Quantity))
	}
}

func (o *Order) IsActive() bool {
	return o.Status == StatusOpen || o.Status == StatusPartiallyFilled
}

func (o *Order) OrderValue(ltp float64) float64 {
	p := o.Price
	if o.OrderType == Market {
		p = ltp
	}
	return p * float64(o.Quantity)
}

// generateOrderID creates a unique, sortable order ID.
// Format: clientID-nanosecond_timestamp
func generateOrderID(clientID string, t time.Time) string {
	return fmt.Sprintf("%s-%d", clientID, t.UnixNano())
}
