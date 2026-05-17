package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/AastikM/go-oms/internal/api"
	"github.com/AastikM/go-oms/internal/db"
	"github.com/AastikM/go-oms/internal/models"
)

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Println("=== Go OMS Starting (Redis + Postgres + WebSocket) ===")

	// Configuration — all overridable via environment variables
	// Defaults work for local dev: go run cmd/server/main.go
	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")
	pgCfg := db.Config{
		Host:     getEnv("PG_HOST", "localhost"),
		Port:     getEnvInt("PG_PORT", 5432),
		User:     getEnv("PG_USER", "oms_user"),
		Password: getEnv("PG_PASS", "oms_pass"),
		DBName:   getEnv("PG_DB", "oms_db"),
		SSLMode:  getEnv("PG_SSLMODE", "disable"),
	}
	simMode := getEnv("SIM_MODE", "true") == "true"
	port := getEnvInt("PORT", 8080)

	log.Printf("Config: redis=%s pg=%s:%d/%s sim=%v port=%d",
		redisAddr, pgCfg.Host, pgCfg.Port, pgCfg.DBName, simMode, port)

	// Boot the OMS
	oms, err := api.NewOMS(redisAddr, pgCfg, simMode)
	if err != nil {
		log.Fatalf("Failed to start OMS: %v", err)
	}

	// Register supported symbols
	for _, sym := range []string{"RELIANCE", "TCS", "INFY", "HDFCBANK", "NIFTY"} {
		oms.AddSymbol(sym, models.NSE)
	}

	// Start market data feed
	oms.StartMarketData([]string{"RELIANCE", "TCS", "INFY", "HDFCBANK", "NIFTY"}, 5*time.Second)

	// Seed test clients (idempotent — safe on restart)
	oms.RegisterClient("CLIENT001", 500000)
	oms.RegisterClient("CLIENT002", 1000000)
	oms.RegisterClient("CLIENT003", 250000)

	// Recover open orders from Redis into in-memory order book
	if err := oms.RecoverFromRedis(); err != nil {
		log.Printf("Warning during recovery: %v", err)
	}

	addr := fmt.Sprintf(":%d", port)
	log.Printf("Server listening on %s", addr)
	log.Println("─── REST endpoints ─────────────────────────────────────")
	log.Println("  POST   /orders                    — Place order")
	log.Println("  GET    /orders/{id}               — Order status")
	log.Println("  DELETE /orders/{id}               — Cancel order")
	log.Println("  GET    /clients/{id}/orders       — Client order history")
	log.Println("  GET    /accounts/{id}             — Balance / margin")
	log.Println("  GET    /depth/{symbol}            — Market depth (L2)")
	log.Println("  GET    /quote/{symbol}            — Live quote / LTP")
	log.Println("  GET    /trades                    — Recent trades (Redis)")
	log.Println("  GET    /reports/trades/{clientID} — Trade history (Postgres)")
	log.Println("  GET    /reports/volume            — Top symbols by volume")
	log.Println("  POST   /admin/eod-settlement      — Run EOD settlement")
	log.Println("─── WebSocket ──────────────────────────────────────────")
	log.Println("  WS     /ws?client_id=CLIENT001    — Real-time push")
	log.Println("  GET    /ws/stats                  — Connection count")
	log.Println("─── Infra ──────────────────────────────────────────────")
	log.Println("  GET    /health                    — Health + Redis stats")

	if err := http.ListenAndServe(addr, oms.Router()); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
