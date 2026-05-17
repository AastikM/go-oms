#!/usr/bin/env python3
"""
Go OMS Load Tester & Integration Tester
========================================
Runs two modes:
  1. LOAD TEST  — Floods the OMS with concurrent orders to test throughput
  2. LIVE TEST  — Fetches real NSE prices via yfinance, places orders based on them

Requirements:
  pip install requests yfinance locust

Usage:
  # Basic integration test (single run)
  python3 test_oms.py --mode integration --url http://localhost:8080

  # Load test: 100 concurrent users, 30 seconds
  python3 test_oms.py --mode load --url http://localhost:8080 --users 100 --duration 30

  # Live market test (during NSE hours, needs internet)
  python3 test_oms.py --mode live --url http://localhost:8080
"""

import argparse
import concurrent.futures
import json
import random
import statistics
import sys
import time
from dataclasses import dataclass, field
from typing import Optional

import requests

# Try importing yfinance (optional — needed for live mode) 
try:
    import yfinance as yf
    YFINANCE_AVAILABLE = True
except ImportError:
    YFINANCE_AVAILABLE = False
    print("[WARN] yfinance not installed. Live mode unavailable. Run: pip install yfinance")

# NSE Symbols we'll test with
NSE_SYMBOLS = ["RELIANCE", "TCS", "INFY", "HDFCBANK", "NIFTY"]

# Test clients registered in our OMS 
TEST_CLIENTS = ["CLIENT001", "CLIENT002", "CLIENT003"]


# RESULT TRACKING
@dataclass
class TestResult:
    total: int = 0
    success: int = 0
    failed: int = 0
    rejected: int = 0  # valid rejections (e.g. margin failure)
    latencies_ms: list = field(default_factory=list)
    errors: list = field(default_factory=list)

    def add(self, success: bool, latency_ms: float, error: Optional[str] = None):
        self.total += 1
        self.latencies_ms.append(latency_ms)
        if success:
            self.success += 1
        else:
            self.failed += 1
            if error:
                self.errors.append(error)

    def summary(self) -> dict:
        if not self.latencies_ms:
            return {}
        return {
            "total":        self.total,
            "success":      self.success,
            "failed":       self.failed,
            "success_rate": f"{self.success/self.total*100:.1f}%",
            "p50_ms":       round(statistics.median(self.latencies_ms), 2),
            "p95_ms":       round(sorted(self.latencies_ms)[int(len(self.latencies_ms) * 0.95)], 2),
            "p99_ms":       round(sorted(self.latencies_ms)[int(len(self.latencies_ms) * 0.99)], 2),
            "max_ms":       round(max(self.latencies_ms), 2),
            "min_ms":       round(min(self.latencies_ms), 2),
            "throughput":   f"{self.total/max(sum(self.latencies_ms)/1000, 0.001):.0f} req/s",
        }


# OMS CLIENT

class OMSClient:
    def __init__(self, base_url: str):
        self.base = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers["Content-Type"] = "application/json"

    def health(self) -> bool:
        try:
            r = self.session.get(f"{self.base}/health", timeout=3)
            return r.status_code == 200
        except Exception:
            return False

    def place_order(self, payload: dict) -> tuple[bool, float, Optional[str]]:
        """Returns (success, latency_ms, error_msg)"""
        start = time.perf_counter()
        try:
            r = self.session.post(f"{self.base}/orders", json=payload, timeout=5)
            latency = (time.perf_counter() - start) * 1000
            if r.status_code in (200, 201):
                return True, latency, None
            else:
                err = r.json().get("error", "unknown")
                return False, latency, err
        except Exception as e:
            latency = (time.perf_counter() - start) * 1000
            return False, latency, str(e)

    def get_order(self, order_id: str) -> Optional[dict]:
        try:
            r = self.session.get(f"{self.base}/orders/{order_id}", timeout=3)
            return r.json() if r.status_code == 200 else None
        except Exception:
            return None

    def get_trades(self) -> dict:
        try:
            r = self.session.get(f"{self.base}/trades", timeout=3)
            return r.json()
        except Exception:
            return {}

    def get_quote(self, symbol: str) -> Optional[dict]:
        try:
            r = self.session.get(f"{self.base}/quote/{symbol}", timeout=3)
            return r.json() if r.status_code == 200 else None
        except Exception:
            return None

    def get_depth(self, symbol: str) -> Optional[dict]:
        try:
            r = self.session.get(f"{self.base}/depth/{symbol}", timeout=3)
            return r.json() if r.status_code == 200 else None
        except Exception:
            return None


# ORDER GENERATORS

def random_order(ltp_map: dict) -> dict:
    """Generates a realistic random order based on current prices."""
    symbol = random.choice(NSE_SYMBOLS)
    client = random.choice(TEST_CLIENTS)
    side = random.choice(["BUY", "SELL"])
    order_type = random.choices(["LIMIT", "MARKET"], weights=[80, 20])[0]
    product = random.choices(["MIS", "CNC"], weights=[70, 30])[0]

    ltp = ltp_map.get(symbol, 2500.0)

    # Generate a realistic price: within ±1% of LTP for limit orders
    # (we want some to match and some to sit in the book)
    spread = ltp * 0.01
    if side == "BUY":
        price = round(ltp - random.uniform(0, spread), 2)
    else:
        price = round(ltp + random.uniform(0, spread), 2)

    qty = random.choice([10, 25, 50, 100, 200])

    payload = {
        "client_id":    client,
        "symbol":       symbol,
        "exchange":     "NSE",
        "side":         side,
        "order_type":   order_type,
        "product_type": product,
        "validity":     "DAY",
        "quantity":     qty,
    }

    if order_type == "LIMIT":
        payload["price"] = price

    return payload


def aggressive_buy_order(symbol: str, ltp: float) -> dict:
    """Creates a BUY order slightly above LTP — likely to match immediately."""
    return {
        "client_id":    "CLIENT001",
        "symbol":       symbol,
        "exchange":     "NSE",
        "side":         "BUY",
        "order_type":   "LIMIT",
        "product_type": "MIS",
        "validity":     "DAY",
        "quantity":     100,
        "price":        round(ltp * 1.001, 2),  # 0.1% above LTP
    }


def passive_sell_order(symbol: str, ltp: float) -> dict:
    """Creates a SELL order slightly above LTP — sits in book as resting order."""
    return {
        "client_id":    "CLIENT002",
        "symbol":       symbol,
        "exchange":     "NSE",
        "side":         "SELL",
        "order_type":   "LIMIT",
        "product_type": "MIS",
        "validity":     "DAY",
        "quantity":     100,
        "price":        round(ltp * 1.001, 2),
    }


# INTEGRATION TEST — verifies correctness

def run_integration_test(oms: OMSClient):
    print("\n" + "="*60)
    print("INTEGRATION TEST")
    print("="*60)

    # 1. Health check
    print("\n[1] Health check...")
    if not oms.health():
        print("  FAIL: OMS is not reachable. Is the server running?")
        sys.exit(1)
    print("  PASS: OMS is healthy")

    # 2. Check quotes
    print("\n[2] Market data check...")
    for sym in NSE_SYMBOLS[:3]:
        quote = oms.get_quote(sym)
        if quote and "ltp" in quote:
            print(f"  {sym}: LTP = ₹{quote['ltp']:.2f} ({quote.get('change_percent', 0):+.2f}%)")
        else:
            print(f"  WARN: No quote for {sym}")

    # 3. Place a valid LIMIT order
    print("\n[3] Placing valid LIMIT SELL order...")
    quote = oms.get_quote("RELIANCE") or {"ltp": 2500.0}
    ltp = quote["ltp"]

    sell_payload = passive_sell_order("RELIANCE", ltp)
    success, latency, err = oms.place_order(sell_payload)
    if success:
        print(f"  PASS: Sell order placed in {latency:.1f}ms")
    else:
        print(f"  FAIL: {err}")

    # 4. Place matching BUY order — should trigger a trade
    print("\n[4] Placing matching BUY order (should trigger trade)...")
    buy_payload = aggressive_buy_order("RELIANCE", ltp)
    success, latency, err = oms.place_order(buy_payload)
    if success:
        print(f"  PASS: Buy order placed in {latency:.1f}ms")
    else:
        print(f"  INFO: {err}")  # might reject due to margin

    # 5. Check trades
    time.sleep(0.1)  # let matching engine process
    print("\n[5] Checking executed trades...")
    trades = oms.get_trades()
    count = trades.get("count", 0)
    print(f"  Total trades executed: {count}")

    # 6. Test rejection — bad price (above circuit breaker)
    print("\n[6] Testing rejection — price above circuit breaker...")
    bad_order = {
        "client_id":    "CLIENT001",
        "symbol":       "RELIANCE",
        "exchange":     "NSE",
        "side":         "BUY",
        "order_type":   "LIMIT",
        "product_type": "MIS",
        "validity":     "DAY",
        "quantity":     10,
        "price":        ltp * 1.50,  # +50% above LTP — will fail ±20% band check
    }
    success, latency, err = oms.place_order(bad_order)
    if not success:
        print(f"  PASS: Correctly rejected ({latency:.1f}ms): {err}")
    else:
        print(f"  FAIL: Should have rejected overpriced order")

    # 7. Test rejection — insufficient margin
    print("\n[7] Testing rejection — insufficient margin (huge qty)...")
    huge_order = {
        "client_id":    "CLIENT003",  # only ₹2.5 lakh balance
        "symbol":       "RELIANCE",
        "exchange":     "NSE",
        "side":         "BUY",
        "order_type":   "LIMIT",
        "product_type": "CNC",  # CNC requires 100% margin
        "validity":     "DAY",
        "quantity":     1000,   # 1000 * 2500 = ₹25 lakh — more than ₹2.5 lakh
        "price":        ltp,
    }
    success, latency, err = oms.place_order(huge_order)
    if not success:
        print(f"  PASS: Correctly rejected ({latency:.1f}ms): {err}")
    else:
        print(f"  FAIL: Should have rejected (insufficient margin)")

    # 8. Market depth
    print("\n[8] Market depth check...")
    depth = oms.get_depth("RELIANCE")
    if depth:
        bids = depth.get("bids", [])
        asks = depth.get("asks", [])
        print(f"  RELIANCE: {len(bids)} bid levels, {len(asks)} ask levels")
    else:
        print("  WARN: No depth data")

    print("\n" + "="*60)
    print("Integration test complete!")
    print("="*60)


# LOAD TEST — tests throughput and latency under concurrency

def run_load_test(oms: OMSClient, num_users: int, duration_secs: int):
    print("\n" + "="*60)
    print(f"LOAD TEST: {num_users} concurrent users, {duration_secs}s duration")
    print("="*60)

    # Get current prices for realistic orders
    ltp_map = {}
    for sym in NSE_SYMBOLS:
        quote = oms.get_quote(sym)
        ltp_map[sym] = quote["ltp"] if quote else 2500.0
    print(f"\nCurrent prices: { {k: f'₹{v:.0f}' for k, v in ltp_map.items()} }")

    result = TestResult()
    start_time = time.time()
    end_time = start_time + duration_secs

    def worker():
        while time.time() < end_time:
            order = random_order(ltp_map)
            success, latency, err = oms.place_order(order)
            result.add(success, latency, err if not success else None)

    print(f"\nRunning load test...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_users) as executor:
        futures = [executor.submit(worker) for _ in range(num_users)]
        concurrent.futures.wait(futures)

    actual_duration = time.time() - start_time
    summary = result.summary()

    print(f"\nResults ({actual_duration:.1f}s):")
    print(f"  Total requests:   {summary['total']}")
    print(f"  Success rate:     {summary['success_rate']}")
    print(f"  Throughput:       {int(summary['total']/actual_duration)} req/s")
    print(f"  Latency p50:      {summary['p50_ms']}ms")
    print(f"  Latency p95:      {summary['p95_ms']}ms")
    print(f"  Latency p99:      {summary['p99_ms']}ms")
    print(f"  Max latency:      {summary['max_ms']}ms")

    # Show unique error types
    if result.errors:
        from collections import Counter
        # Normalize error messages
        error_types = Counter()
        for e in result.errors:
            if "insufficient margin" in e:
                error_types["insufficient_margin"] += 1
            elif "price" in e.lower() and "band" in e.lower():
                error_types["price_band_violation"] += 1
            elif "not found" in e.lower():
                error_types["client_not_found"] += 1
            else:
                error_types[e[:50]] += 1

        print(f"\nRejection breakdown (top 5):")
        for reason, count in error_types.most_common(5):
            print(f"  {reason}: {count}")

    # Final trades check
    trades = oms.get_trades()
    print(f"\nTotal trades executed: {trades.get('count', 0)}")


# LIVE MARKET TEST — uses real NSE prices from yfinance

def run_live_test(oms: OMSClient):
    if not YFINANCE_AVAILABLE:
        print("ERROR: yfinance not available. Install with: pip install yfinance")
        sys.exit(1)

    print("\n" + "="*60)
    print("LIVE MARKET TEST (real NSE prices via Yahoo Finance)")
    print("="*60)

    # Fetch real prices
    print("\nFetching live NSE prices...")
    prices = {}
    for sym in NSE_SYMBOLS:
        ticker_sym = "^NSEI" if sym == "NIFTY" else f"{sym}.NS"
        try:
            ticker = yf.Ticker(ticker_sym)
            info = ticker.fast_info
            ltp = info.last_price
            prices[sym] = ltp
            print(f"  {sym}: ₹{ltp:.2f}")
        except Exception as e:
            print(f"  WARN: Could not fetch {sym}: {e}")
            prices[sym] = 0.0

    # Place orders based on real prices
    print("\nPlacing orders based on real market prices...")
    result = TestResult()

    for sym, ltp in prices.items():
        if ltp <= 0:
            continue

        # Place buy and sell orders at realistic prices
        for side, price_mult in [("BUY", 0.999), ("SELL", 1.001)]:
            payload = {
                "client_id":    random.choice(TEST_CLIENTS),
                "symbol":       sym,
                "exchange":     "NSE",
                "side":         side,
                "order_type":   "LIMIT",
                "product_type": "MIS",
                "validity":     "DAY",
                "quantity":     10,
                "price":        round(ltp * price_mult, 2),
            }
            success, latency, err = oms.place_order(payload)
            result.add(success, latency, err)
            status = "✓" if success else "✗"
            print(f"  {status} {side} {sym} 10@₹{payload['price']:.2f} ({latency:.1f}ms)"
                  + (f" — {err}" if not success else ""))

    # Summary
    print(f"\nSummary: {result.success}/{result.total} orders placed successfully")


# ENTRY POINT

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Go OMS Test Suite")
    parser.add_argument("--mode", choices=["integration", "load", "live"], default="integration",
                        help="Test mode to run")
    parser.add_argument("--url", default="http://localhost:8080", help="OMS server URL")
    parser.add_argument("--users", type=int, default=50, help="Concurrent users for load test")
    parser.add_argument("--duration", type=int, default=30, help="Load test duration in seconds")
    args = parser.parse_args()

    oms = OMSClient(args.url)

    print(f"Target: {args.url}")
    print(f"Mode:   {args.mode}")

    if args.mode == "integration":
        run_integration_test(oms)
    elif args.mode == "load":
        run_load_test(oms, args.users, args.duration)
    elif args.mode == "live":
        run_live_test(oms)
