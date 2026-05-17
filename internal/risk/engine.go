// Package risk performs in-memory pre-trade validations including margin
// requirements, quantity/price sanity checks, position limits, and exchange 
// price band validations.
package risk

import (
	"fmt"
	"math"
	"sync"

	"github.com/AastikM/go-oms/internal/models"
)

var marginRates = map[models.ProductType]float64{
	models.CNC:  1.00, // Full value required
	models.MIS:  0.20, // Intraday leverage
	models.NRML: 0.15, // F&O overnight
}

// priceBandPercent: max % deviation from LTP allowed in an order.

const priceBandPercent = 0.20

// freezeQuantity: max qty in a single order (exchange mandated).
// For Nifty 50 stocks, NSE freezes orders above this.
const freezeQuantity = int64(500000) // 5 lakh shares

// minOrderValue: minimum order value (broker-level check)
const minOrderValue = float64(1.0)

// Account represents a client's margin balance.
type Account struct {
	ClientID  string
	Balance   float64
	UsedFunds float64
}

// FreeFunds returns how much the user can still use.
func (a *Account) FreeFunds() float64 {
	return a.Balance - a.UsedFunds
}

// Engine performs all pre-trade risk checks.
type Engine struct {
	mu       sync.RWMutex
	accounts map[string]*Account
}

func NewEngine() *Engine {
	return &Engine{
		accounts: make(map[string]*Account),
	}
}

// RegisterAccount adds a new client account (called at user onboarding).
func (e *Engine) RegisterAccount(clientID string, balance float64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.accounts[clientID] = &Account{
		ClientID: clientID,
		Balance:  balance,
	}
}

func (e *Engine) GetAccount(clientID string) (*Account, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	acc, ok := e.accounts[clientID]
	return acc, ok
}

func (e *Engine) Validate(order *models.Order, ltp float64) error {
	// 1. Basic sanity
	if err := e.validateBasics(order); err != nil {
		return err
	}

	// 2. Price band (only for LIMIT orders — MARKET orders are exempt)
	if order.OrderType == models.Limit {
		if err := e.validatePriceBand(order, ltp); err != nil {
			return err
		}
	}

	// 3. Freeze quantity
	if err := e.validateFreezeQty(order); err != nil {
		return err
	}

	// 4. Margin check
	if err := e.validateMargin(order, ltp); err != nil {
		return err
	}

	return nil
}

// ValidateBasics catches obvious bad inputs (exported for use by OMS)
func (e *Engine) ValidateBasics(order *models.Order) error {
	return e.validateBasics(order)
}

// CheckPriceBand validates price band (exported for use by OMS)
func (e *Engine) CheckPriceBand(order *models.Order, ltp float64) error {
	return e.validatePriceBand(order, ltp)
}

// CheckFreezeQty validates freeze quantity (exported for use by OMS)
func (e *Engine) CheckFreezeQty(order *models.Order) error {
	return e.validateFreezeQty(order)
}

// validateBasics catches obvious bad inputs
func (e *Engine) validateBasics(order *models.Order) error {
	if order.Symbol == "" {
		return fmt.Errorf("symbol cannot be empty")
	}
	if order.Quantity <= 0 {
		return fmt.Errorf("quantity must be positive, got %d", order.Quantity)
	}
	if order.OrderType == models.Limit && order.Price <= 0 {
		return fmt.Errorf("limit price must be positive, got %.2f", order.Price)
	}
	if order.OrderType == models.StopLoss && order.TriggerPrice <= 0 {
		return fmt.Errorf("SL orders require a trigger price")
	}
	if order.ClientID == "" {
		return fmt.Errorf("client ID cannot be empty")
	}
	return nil
}

// validatePriceBand ensures the order price falls within the exchange's allowed deviation from LTP.

func (e *Engine) validatePriceBand(order *models.Order, ltp float64) error {
	if ltp <= 0 {
		// If we don't have an LTP (new listing, etc.), skip this check
		return nil
	}

	upperBand := ltp * (1 + priceBandPercent)
	lowerBand := ltp * (1 - priceBandPercent)

	if order.Price > upperBand {
		return fmt.Errorf("price %.2f exceeds upper circuit band %.2f (LTP: %.2f, +%.0f%%)",
			order.Price, upperBand, ltp, priceBandPercent*100)
	}
	if order.Price < lowerBand {
		return fmt.Errorf("price %.2f below lower circuit band %.2f (LTP: %.2f, -%.0f%%)",
			order.Price, lowerBand, ltp, priceBandPercent*100)
	}
	return nil
}

// validateFreezeQty checks if the order exceeds exchange mandated size limits.
func (e *Engine) validateFreezeQty(order *models.Order) error {
	if order.Quantity > freezeQuantity {
		return fmt.Errorf("order quantity %d exceeds freeze quantity limit %d",
			order.Quantity, freezeQuantity)
	}
	return nil
}

func (e *Engine) validateMargin(order *models.Order, ltp float64) error {
	e.mu.RLock()
	acc, ok := e.accounts[order.ClientID]
	e.mu.RUnlock()

	if !ok {
		return fmt.Errorf("client %s not found", order.ClientID)
	}

	// Use LTP for market orders, limit price for limit orders
	price := order.Price
	if order.OrderType == models.Market {
		price = ltp
	}

	orderValue := price * float64(order.Quantity)

	rate, ok := marginRates[order.ProductType]
	if !ok {
		return fmt.Errorf("unknown product type: %s", order.ProductType)
	}

	required := math.Round(orderValue*rate*100) / 100 // round to 2 decimal places

	info := models.MarginInfo{
		Required:  required,
		Available: acc.Balance,
		Used:      acc.UsedFunds,
		Free:      acc.FreeFunds(),
	}

	if !info.Sufficient() {
		return fmt.Errorf("insufficient margin: required ₹%.2f, available ₹%.2f (balance: ₹%.2f, used: ₹%.2f)",
			info.Required, info.Free, info.Available, info.Used)
	}

	return nil
}

// BlockMargin reserves funds for an open order.
// Called after an order is accepted into the book.
func (e *Engine) BlockMargin(clientID string, amount float64) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	acc, ok := e.accounts[clientID]
	if !ok {
		return fmt.Errorf("client %s not found", clientID)
	}
	acc.UsedFunds += amount
	return nil
}

// ReleaseMargin unblocks funds when an order is filled/cancelled.
func (e *Engine) ReleaseMargin(clientID string, amount float64) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	acc, ok := e.accounts[clientID]
	if !ok {
		return fmt.Errorf("client %s not found", clientID)
	}
	acc.UsedFunds -= amount
	if acc.UsedFunds < 0 {
		acc.UsedFunds = 0
	}
	return nil
}

// GetMarginInfo returns the margin breakdown for an order (for display to user).
func (e *Engine) GetMarginInfo(order *models.Order, ltp float64) models.MarginInfo {
	e.mu.RLock()
	acc, ok := e.accounts[order.ClientID]
	e.mu.RUnlock()

	if !ok {
		return models.MarginInfo{}
	}

	price := order.Price
	if order.OrderType == models.Market {
		price = ltp
	}

	rate := marginRates[order.ProductType]
	required := math.Round(price*float64(order.Quantity)*rate*100) / 100

	return models.MarginInfo{
		Required:  required,
		Available: acc.Balance,
		Used:      acc.UsedFunds,
		Free:      acc.FreeFunds(),
	}
}
