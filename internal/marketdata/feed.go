package marketdata

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

type Quote struct {
	Symbol        string    `json:"symbol"`
	LTP           float64   `json:"ltp"`
	Open          float64   `json:"open"`
	High          float64   `json:"high"`
	Low           float64   `json:"low"`
	PreviousClose float64   `json:"previous_close"`
	Change        float64   `json:"change"`
	ChangePercent float64   `json:"change_percent"`
	Volume        int64     `json:"volume"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// TickHandler is a function called whenever a new price tick arrives.
type TickHandler func(quote Quote)

type Feed struct {
	mu         sync.RWMutex
	quotes     map[string]*Quote
	handlers   []TickHandler
	httpClient *http.Client
	simMode    bool // if true, use simulator instead of live data
}

// Set simMode=true for testing without live prices.
func NewFeed(simMode bool) *Feed {
	return &Feed{
		quotes:     make(map[string]*Quote),
		httpClient: &http.Client{Timeout: 5 * time.Second},
		simMode:    simMode,
	}
}

func (f *Feed) Subscribe(h TickHandler) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.handlers = append(f.handlers, h)
}

func (f *Feed) GetLTP(symbol string) float64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if q, ok := f.quotes[symbol]; ok {
		return q.LTP
	}
	return 0
}

func (f *Feed) GetQuote(symbol string) (*Quote, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	q, ok := f.quotes[symbol]
	return q, ok
}

func (f *Feed) StartPolling(symbols []string, interval time.Duration) {
	log.Printf("[MarketData] Starting feed for %d symbols (simMode=%v)", len(symbols), f.simMode)

	for _, sym := range symbols {
		if err := f.Refresh(sym); err != nil {
			log.Printf("[MarketData] Failed to fetch %s: %v", sym, err)
		}
	}

	ticker := time.NewTicker(interval)
	go func() {
		for range ticker.C {
			for _, sym := range symbols {
				if err := f.Refresh(sym); err != nil {
					log.Printf("[MarketData] Poll error for %s: %v", sym, err)
				}
			}
		}
	}()
}

func (f *Feed) Refresh(symbol string) error {
	var quote *Quote
	var err error

	if f.simMode {
		quote = f.simulateQuote(symbol)
	} else {
		quote, err = f.fetchFromYahoo(symbol)
		if err != nil {
			log.Printf("[MarketData] Falling back to sim for %s: %v", symbol, err)
			quote = f.simulateQuote(symbol)
		}
	}

	f.mu.Lock()
	f.quotes[symbol] = quote
	handlers := f.handlers
	f.mu.Unlock()

	for _, h := range handlers {
		h(*quote)
	}
	return nil
}

type yahooResponse struct {
	Chart struct {
		Result []struct {
			Meta struct {
				Symbol               string  `json:"symbol"`
				RegularMarketPrice   float64 `json:"regularMarketPrice"`
				RegularMarketOpen    float64 `json:"regularMarketOpen"`
				RegularMarketDayHigh float64 `json:"regularMarketDayHigh"`
				RegularMarketDayLow  float64 `json:"regularMarketDayLow"`
				PreviousClose        float64 `json:"previousClose"`
				RegularMarketVolume  int64   `json:"regularMarketVolume"`
			} `json:"meta"`
		} `json:"result"`
		Error interface{} `json:"error"`
	} `json:"chart"`
}

// NSE stocks use the .NS suffix: "RELIANCE" → "RELIANCE.NS"
// NSE indices: "NIFTY 50" → "^NSEI"
func (f *Feed) fetchFromYahoo(symbol string) (*Quote, error) {
	// Map our internal symbol names to Yahoo Finance tickers
	ticker := toYahooTicker(symbol)
	url := fmt.Sprintf("https://query1.finance.yahoo.com/v8/finance/chart/%s", ticker)

	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("User-Agent", "Mozilla/5.0")

	resp, err := f.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP error: %w", err)
	}
	defer resp.Body.Close()

	var result yahooResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode error: %w", err)
	}

	if len(result.Chart.Result) == 0 {
		return nil, fmt.Errorf("no data returned for %s", ticker)
	}

	meta := result.Chart.Result[0].Meta
	if meta.RegularMarketPrice == 0 {
		return nil, fmt.Errorf("zero price returned for %s", ticker)
	}

	ltp := meta.RegularMarketPrice
	prev := meta.PreviousClose
	change := ltp - prev
	changePct := 0.0
	if prev > 0 {
		changePct = (change / prev) * 100
	}

	return &Quote{
		Symbol:        symbol,
		LTP:           ltp,
		Open:          meta.RegularMarketOpen,
		High:          meta.RegularMarketDayHigh,
		Low:           meta.RegularMarketDayLow,
		PreviousClose: prev,
		Change:        change,
		ChangePercent: changePct,
		Volume:        meta.RegularMarketVolume,
		UpdatedAt:     time.Now(),
	}, nil
}

func toYahooTicker(symbol string) string {
	mapping := map[string]string{
		"NIFTY":     "^NSEI",
		"BANKNIFTY": "^NSEBANK",
		"SENSEX":    "^BSESN",
	}
	if ticker, ok := mapping[symbol]; ok {
		return ticker
	}
	// Default: NSE equity (append .NS)
	return symbol + ".NS"
}

// PRICE SIMULATOR — for testing without live data

var basePrices = map[string]float64{
	"RELIANCE":  2500.0,
	"TCS":       3800.0,
	"INFY":      1800.0,
	"HDFCBANK":  1600.0,
	"ICICIBANK": 1200.0,
	"WIPRO":     550.0,
	"NIFTY":     22000.0,
	"BANKNIFTY": 47000.0,
	"SBIN":      800.0,
	"TATASTEEL": 150.0,
}

// simulateQuote generates a realistic-looking price tick.
// Uses random walk: price += random(-0.5%, +0.5%)
func (f *Feed) simulateQuote(symbol string) *Quote {
	f.mu.RLock()
	existing, hasExisting := f.quotes[symbol]
	f.mu.RUnlock()

	var currentPrice float64
	if hasExisting {
		currentPrice = existing.LTP
	} else {
		// Use base price or random price for unknown symbols
		if base, ok := basePrices[symbol]; ok {
			currentPrice = base
		} else {
			currentPrice = 100.0 + rand.Float64()*900.0
		}
	}

	// Random walk: ±0.5% per tick - realistic for 5 second intervals
	changePct := (rand.Float64() - 0.5) * 0.01 // -0.5% to +0.5%
	newPrice := currentPrice * (1 + changePct)

	// Keep price above ₹1
	if newPrice < 1.0 {
		newPrice = 1.0
	}

	change := newPrice - currentPrice
	changePctAbs := (change / currentPrice) * 100

	// Simulate volume spike occasionally
	volume := int64(100000 + rand.Intn(500000))

	return &Quote{
		Symbol:        symbol,
		LTP:           newPrice,
		Open:          currentPrice * 0.998,
		High:          newPrice * 1.002,
		Low:           newPrice * 0.998,
		PreviousClose: currentPrice,
		Change:        change,
		ChangePercent: changePctAbs,
		Volume:        volume,
		UpdatedAt:     time.Now(),
	}
}
