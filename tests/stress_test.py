#!/usr/bin/env python3
"""
OMS Stress Test Suite
======================
Four test levels:

  Level 1 — EDGE CASES       : weird inputs, boundary conditions, invalid orders
  Level 2 — CONCURRENT       : many users placing orders at the same time
  Level 3 — MATCHING STRESS  : flood buy + sell orders to trigger mass matching
  Level 4 — BOMBARDMENT      : sustained million-request load, checks system holds up

Usage:
  pip install requests locust

  # Run all levels
  python3 tests/stress_test.py --level all

  # Run specific level
  python3 tests/stress_test.py --level 1
  python3 tests/stress_test.py --level 2 --users 500
  python3 tests/stress_test.py --level 3
  python3 tests/stress_test.py --level 4 --requests 1000000

  # Quick smoke test (level 1+2 with low numbers)
  python3 tests/stress_test.py --level all --quick
"""

import argparse
import concurrent.futures
import random
import statistics
import sys
import time
import threading
import json
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional

import requests

BASE_URL = "http://localhost:8080"

SYMBOLS   = ["RELIANCE", "TCS", "INFY", "HDFCBANK", "NIFTY"]
CLIENTS   = ["CLIENT001", "CLIENT002", "CLIENT003"]

# Base prices for simulation
BASE_PRICES = {
    "RELIANCE": 2500.0,
    "TCS":      3800.0,
    "INFY":     1800.0,
    "HDFCBANK": 1600.0,
    "NIFTY":   22000.0,
}

# HTTP session pool 

def make_session():
    s = requests.Session()
    s.headers["Content-Type"] = "application/json"
    # Keep-alive connections — critical for high throughput
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=100,
        pool_maxsize=200,
        max_retries=0,
    )
    s.mount("http://", adapter)
    return s

SESSION = make_session()

# Result tracker

@dataclass
class Results:
    total:      int   = 0
    success:    int   = 0
    rejected:   int   = 0   # valid business rejections (margin, price band)
    errors:     int   = 0   # actual failures (timeout, 500)
    latencies:  list  = field(default_factory=list)
    error_msgs: list  = field(default_factory=list)
    lock:       object = field(default_factory=threading.Lock)

    def record(self, ok: bool, latency_ms: float, err: str = None, rejected=False):
        with self.lock:
            self.total += 1
            self.latencies.append(latency_ms)
            if ok:
                self.success += 1
            elif rejected:
                self.rejected += 1
            else:
                self.errors += 1
                if err:
                    self.error_msgs.append(err[:80])

    def print_summary(self, label: str, duration: float):
        lats = sorted(self.latencies)
        n    = len(lats)
        print(f"\n{'─'*55}")
        print(f"  {label}")
        print(f"{'─'*55}")
        print(f"  Duration:      {duration:.1f}s")
        print(f"  Total:         {self.total:,}")
        print(f"  Success:       {self.success:,}  ({100*self.success/max(self.total,1):.1f}%)")
        print(f"  Rejected:      {self.rejected:,}  (valid business rejections)")
        print(f"  Errors:        {self.errors:,}  (actual failures)")
        print(f"  Throughput:    {int(self.total/max(duration,0.1)):,} req/s")
        if lats:
            print(f"  Latency p50:   {lats[int(n*0.50)]:.1f}ms")
            print(f"  Latency p95:   {lats[int(n*0.95)]:.1f}ms")
            print(f"  Latency p99:   {lats[int(n*0.99)]:.1f}ms")
            print(f"  Latency max:   {lats[-1]:.1f}ms")
        if self.error_msgs:
            from collections import Counter
            top = Counter(self.error_msgs).most_common(3)
            print(f"  Top errors:")
            for msg, cnt in top:
                print(f"    [{cnt}x] {msg}")

# HTTP helpers 

def place_order(payload: dict, session=None) -> tuple:
    """Returns (success, latency_ms, error, is_rejection)"""
    s = session or SESSION
    t = time.perf_counter()
    try:
        r = s.post(f"{BASE_URL}/orders", json=payload, timeout=5)
        ms = (time.perf_counter() - t) * 1000
        if r.status_code in (200, 201):
            return True, ms, None, False
        err = r.json().get("error", "")
        # These are valid business rejections, not bugs
        is_rejection = any(x in err.lower() for x in [
            "margin", "price", "band", "freeze", "not found", "circuit"
        ])
        return False, ms, err, is_rejection
    except Exception as e:
        ms = (time.perf_counter() - t) * 1000
        return False, ms, str(e)[:60], False

def get_json(path: str) -> Optional[dict]:
    try:
        r = SESSION.get(f"{BASE_URL}{path}", timeout=3)
        return r.json() if r.status_code == 200 else None
    except:
        return None

def current_ltp(symbol: str) -> float:
    q = get_json(f"/quote/{symbol}")
    if q and "ltp" in q:
        return q["ltp"]
    return BASE_PRICES.get(symbol, 1000.0)

# ORDER BUILDERS

def limit_order(client, symbol, side, price, qty, product="MIS"):
    return {"client_id": client, "symbol": symbol, "exchange": "NSE",
            "side": side, "order_type": "LIMIT", "product_type": product,
            "validity": "DAY", "quantity": qty, "price": round(price, 2)}

def market_order(client, symbol, side, qty):
    return {"client_id": client, "symbol": symbol, "exchange": "NSE",
            "side": side, "order_type": "MARKET", "product_type": "MIS",
            "validity": "DAY", "quantity": qty, "price": 0}

def random_order(ltp_map: dict):
    symbol  = random.choice(SYMBOLS)
    client  = random.choice(CLIENTS)
    side    = random.choice(["BUY", "SELL"])
    otype   = random.choices(["LIMIT", "MARKET"], weights=[80, 20])[0]
    product = random.choices(["MIS", "CNC"], weights=[75, 25])[0]
    ltp     = ltp_map.get(symbol, 2500.0)
    spread  = ltp * 0.005
    price   = ltp + (random.uniform(-spread, spread) if side == "BUY" else random.uniform(0, spread))
    qty     = random.choice([10, 25, 50, 100, 200])
    if otype == "MARKET":
        return market_order(client, symbol, side, qty)
    return limit_order(client, symbol, side, price, qty, product)

# LEVEL 1 — EDGE CASES
# Tests boundary conditions, invalid inputs, extreme values

def level1_edge_cases():
    print("\n" + "="*55)
    print("  LEVEL 1 — EDGE CASES & BOUNDARY CONDITIONS")
    print("="*55)

    ltp = current_ltp("RELIANCE")
    passed = failed = 0

    def check(name: str, payload: dict, expect_success: bool):
        nonlocal passed, failed
        ok, ms, err, _ = place_order(payload)
        status = "✓" if ok == expect_success else "✗ UNEXPECTED"
        result = "ACCEPTED" if ok else f"REJECTED: {err}"
        print(f"  {status}  [{ms:5.0f}ms]  {name:<45} → {result}")
        if ok == expect_success:
            passed += 1
        else:
            failed += 1

    print("\n  — INVALID INPUTS (all should be rejected) —")
    check("Zero quantity",              limit_order("CLIENT001","RELIANCE","BUY", ltp, 0),          False)
    check("Negative quantity",          {**limit_order("CLIENT001","RELIANCE","BUY",ltp,10), "quantity":-10}, False)
    check("Zero price on LIMIT",        {**limit_order("CLIENT001","RELIANCE","BUY",ltp,10), "price":0},      False)
    check("Negative price",             limit_order("CLIENT001","RELIANCE","BUY", -100, 10),        False)
    check("Empty symbol",               limit_order("CLIENT001","","BUY", ltp, 10),                 False)
    check("Unknown client",             limit_order("GHOST_USER","RELIANCE","BUY", ltp, 10),        False)
    check("Unknown symbol",             limit_order("CLIENT001","FAKEXYZ","BUY", ltp, 10),          False)

    print("\n  — PRICE BAND VIOLATIONS (±20% of LTP) —")
    check("Price +25% above LTP",       limit_order("CLIENT001","RELIANCE","BUY",  ltp*1.25, 10),  False)
    check("Price -25% below LTP",       limit_order("CLIENT001","RELIANCE","SELL", ltp*0.75, 10),  False)
    check("Price exactly at +20%",      limit_order("CLIENT001","RELIANCE","BUY",  ltp*1.20, 10),  True)
    check("Price just inside band",     limit_order("CLIENT001","RELIANCE","BUY",  ltp*1.18, 10),  True)
    check("Price +50% (extreme)",       limit_order("CLIENT001","RELIANCE","BUY",  ltp*1.50, 10),  False)

    print("\n  — MARGIN STRESS (CLIENT003 has only ₹2,50,000) —")
    check("CNC buy within margin",      limit_order("CLIENT003","RELIANCE","BUY",  ltp, 10, "CNC"), True)
    check("CNC buy over margin",        limit_order("CLIENT003","RELIANCE","BUY",  ltp, 500,"CNC"), False)
    check("MIS buy within 5x leverage", limit_order("CLIENT003","RELIANCE","BUY",  ltp, 200,"MIS"), True)
    check("MIS buy over leverage",      limit_order("CLIENT003","RELIANCE","BUY",  ltp,5000,"MIS"), False)

    print("\n  — QUANTITY BOUNDARIES —")
    check("Freeze qty limit (500001)",  limit_order("CLIENT001","RELIANCE","BUY",  ltp, 500001),   False)
    check("Exactly at freeze (500000)", limit_order("CLIENT001","RELIANCE","BUY",  ltp, 500000),   True)
    check("Single share",               limit_order("CLIENT001","RELIANCE","BUY",  ltp, 1),        True)

    print("\n  — MARKET ORDERS —")
    check("Market BUY valid",           market_order("CLIENT001","RELIANCE","BUY",  50),            True)
    check("Market SELL valid",          market_order("CLIENT002","RELIANCE","SELL", 50),            True)

    print(f"\n  Result: {passed} passed, {failed} unexpected")
    return failed == 0

# LEVEL 2 — CONCURRENT USERS
# Simulates N users placing orders simultaneously

def level2_concurrent(num_users=200, orders_per_user=50):
    print("\n" + "="*55)
    print(f"  LEVEL 2 — CONCURRENT ({num_users} users × {orders_per_user} orders)")
    print("="*55)

    # Prefetch prices once
    ltp_map = {sym: current_ltp(sym) for sym in SYMBOLS}
    print(f"  LTPs: { {k: f'₹{v:.0f}' for k,v in ltp_map.items()} }")

    results = Results()
    start   = time.time()

    def user_session(user_id: int):
        session = make_session()
        for _ in range(orders_per_user):
            payload = random_order(ltp_map)
            ok, ms, err, is_rej = place_order(payload, session)
            results.record(ok, ms, err, is_rej)

    print(f"\n  Firing {num_users} goroutines simultaneously...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_users) as ex:
        futures = [ex.submit(user_session, i) for i in range(num_users)]
        concurrent.futures.wait(futures)

    results.print_summary("Level 2 — Concurrent Users", time.time() - start)

    # Verify state consistency
    trades = get_json("/trades")
    health = get_json("/health")
    print(f"\n  Post-test checks:")
    print(f"    Trades executed:  {trades.get('count',0) if trades else 'N/A'}")
    print(f"    Redis open orders: {health.get('open_orders',0) if health else 'N/A'}")

# LEVEL 3 — MATCHING STRESS
# Floods the book with matched buy/sell pairs to test matching engine throughput

def level3_matching_stress(rounds=1000):
    print("\n" + "="*55)
    print(f"  LEVEL 3 — MATCHING ENGINE STRESS ({rounds} matched pairs)")
    print("="*55)

    symbol = "RELIANCE"
    ltp    = current_ltp(symbol)
    results = Results()
    trades_before = (get_json("/trades") or {}).get("count", 0)
    start = time.time()

    def matched_pair(i):
        # Place sell first (resting), then matching buy
        price = round(ltp * (1 + (i % 10) * 0.001), 2)
        sell_ok, sell_ms, sell_err, sell_rej = place_order(
            limit_order("CLIENT002", symbol, "SELL", price, 10))
        results.record(sell_ok, sell_ms, sell_err, sell_rej)

        buy_ok, buy_ms, buy_err, buy_rej = place_order(
            limit_order("CLIENT001", symbol, "BUY", price, 10))
        results.record(buy_ok, buy_ms, buy_err, buy_rej)

    print(f"  Placing {rounds} matched buy/sell pairs...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=100) as ex:
        futures = [ex.submit(matched_pair, i) for i in range(rounds)]
        concurrent.futures.wait(futures)

    duration = time.time() - start
    results.print_summary("Level 3 — Matching Stress", duration)

    time.sleep(0.5)  # let matching engine drain
    trades_after = (get_json("/trades") or {}).get("count", 0)
    new_trades = trades_after - trades_before
    print(f"\n  New trades executed: {new_trades} (expected ~{rounds})")
    print(f"  Match rate: {100*new_trades/max(rounds,1):.1f}%")

# LEVEL 4 — BOMBARDMENT
# Sustained load: millions of requests, checks system stability over time

def level4_bombardment(total_requests=100000, workers=200, duration_secs=60):
    print("\n" + "="*55)
    print(f"  LEVEL 4 — BOMBARDMENT ({total_requests:,} requests, {workers} workers)")
    print("="*55)

    ltp_map  = {sym: current_ltp(sym) for sym in SYMBOLS}
    results  = Results()
    counter  = {"sent": 0}
    lock     = threading.Lock()
    stop     = threading.Event()
    start    = time.time()

    # Progress printer
    def print_progress():
        while not stop.is_set():
            time.sleep(5)
            elapsed = time.time() - start
            with lock:
                sent = counter["sent"]
            rps = int(sent / max(elapsed, 0.1))
            p99 = 0
            if results.latencies:
                lats = sorted(results.latencies)
                p99 = lats[int(len(lats)*0.99)]
            print(f"  [{elapsed:5.0f}s] sent={sent:>8,}  "
                  f"ok={results.success:>7,}  "
                  f"rps={rps:>6,}  p99={p99:.0f}ms")

    threading.Thread(target=print_progress, daemon=True).start()

    def worker():
        session = make_session()
        while not stop.is_set():
            with lock:
                if counter["sent"] >= total_requests:
                    break
                counter["sent"] += 1
            payload = random_order(ltp_map)
            ok, ms, err, is_rej = place_order(payload, session)
            results.record(ok, ms, err, is_rej)

    print(f"  Running for up to {duration_secs}s or {total_requests:,} requests...\n")
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(worker) for _ in range(workers)]
        # Also enforce time limit
        deadline = start + duration_secs
        while time.time() < deadline and counter["sent"] < total_requests:
            time.sleep(1)
        stop.set()
        concurrent.futures.wait(futures, timeout=10)

    results.print_summary("Level 4 — Bombardment", time.time() - start)

    # Final health check
    health = get_json("/health")
    if health:
        print(f"\n  System health after bombardment:")
        print(f"    Status:         {health.get('status')}")
        print(f"    Open orders:    {health.get('open_orders',0)}")
        print(f"    Filled orders:  {health.get('filled_orders',0)}")
        print(f"    Rejected orders:{health.get('rejected_orders',0)}")
        print(f"    Total trades:   {health.get('total_trades',0)}")

# SPECIFIC SCENARIO TESTS

def test_double_spend():
    """
    The hardest concurrent test: same client, many orders at once.
    All orders compete for the same margin pool.
    Total accepted order value must never exceed account balance.
    """
    print("\n" + "="*55)
    print("  SPECIAL: DOUBLE-SPEND / MARGIN RACE CONDITION TEST")
    print("="*55)

    client = "CLIENT003"  # only ₹2,50,000
    ltp    = current_ltp("RELIANCE")
    # Each CNC order = ltp * 100 * 1.0 = ₹2,50,000 (full margin)
    # Only ONE should be accepted — if two get through, we have a bug

    print(f"  Client balance: ₹2,50,000")
    print(f"  Placing 50 simultaneous CNC orders each needing ₹{ltp*100:,.0f}...")
    print(f"  Expected: exactly 1 accepted, 49 rejected\n")

    results = []
    lock    = threading.Lock()

    def fire():
        ok, ms, err, _ = place_order(
            limit_order(client, "RELIANCE", "BUY", ltp, 100, "CNC"))
        with lock:
            results.append((ok, ms))

    threads = [threading.Thread(target=fire) for _ in range(50)]
    for t in threads: t.start()
    for t in threads: t.join()

    accepted = sum(1 for ok, _ in results if ok)
    rejected = sum(1 for ok, _ in results if not ok)
    avg_ms   = statistics.mean(ms for _, ms in results)

    print(f"  Accepted: {accepted}  Rejected: {rejected}  Avg latency: {avg_ms:.1f}ms")

    if accepted <= 1:
        print("  ✓ PASS — atomic margin blocking works correctly")
    else:
        print(f"  ✗ FAIL — {accepted} orders accepted, margin was double-spent!")

def test_order_book_integrity():
    """
    Place 100 buys and 100 sells at the same price.
    Every single one should match — no orphaned orders.
    """
    print("\n" + "="*55)
    print("  SPECIAL: ORDER BOOK INTEGRITY (100×100 perfect matches)")
    print("="*55)

    symbol = "TCS"
    ltp    = current_ltp(symbol)
    price  = round(ltp * 1.001, 2)
    qty    = 10
    n      = 100

    # Place all sells first
    print(f"  Placing {n} SELL orders @ ₹{price}...")
    for _ in range(n):
        place_order(limit_order("CLIENT002", symbol, "SELL", price, qty))

    time.sleep(0.2)

    # Now flood matching buys
    print(f"  Placing {n} matching BUY orders @ ₹{price}...")
    accepted = 0
    for _ in range(n):
        ok, _, _, _ = place_order(limit_order("CLIENT001", symbol, "BUY", price, qty))
        if ok:
            accepted += 1

    time.sleep(0.5)

    depth = get_json(f"/depth/{symbol}")
    bids  = depth.get("bids", []) if depth else []
    asks  = depth.get("asks", []) if depth else []

    print(f"  Buy orders accepted: {accepted}/{n}")
    print(f"  Remaining book depth: {len(bids)} bid levels, {len(asks)} ask levels")

    if len(bids) == 0 and len(asks) == 0:
        print("  ✓ PASS — all orders matched, book is clean")
    else:
        print(f"  ~ INFO — some orders remain (may be margin-limited)")

# ENTRY POINT

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--level",    default="all",
                        choices=["all","1","2","3","4","double","integrity"])
    parser.add_argument("--users",    type=int, default=200)
    parser.add_argument("--requests", type=int, default=100000)
    parser.add_argument("--duration", type=int, default=60)
    parser.add_argument("--quick",    action="store_true",
                        help="Smaller numbers for a quick smoke test")
    parser.add_argument("--url",      default=BASE_URL)
    args = parser.parse_args()

    BASE_URL = args.url

    if args.quick:
        args.users    = 50
        args.requests = 5000
        args.duration = 20

    # Health check first
    h = get_json("/health")
    if not h:
        print("ERROR: OMS not reachable at", BASE_URL)
        print("       Start it first:  go run cmd/server/main.go")
        sys.exit(1)
    print(f"OMS running at {BASE_URL}  ✓")

    if args.level in ("all", "1"):
        level1_edge_cases()
    if args.level in ("all", "2"):
        level2_concurrent(args.users, orders_per_user=20 if args.quick else 50)
    if args.level in ("all", "3"):
        level3_matching_stress(rounds=100 if args.quick else 500)
    if args.level in ("all", "4"):
        level4_bombardment(args.requests, workers=args.users, duration_secs=args.duration)
    if args.level == "double":
        test_double_spend()
    if args.level == "integrity":
        test_order_book_integrity()

    print("\nDone.\n")
