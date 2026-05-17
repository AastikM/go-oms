#!/usr/bin/env python3
"""
OMS WebSocket Test Client
==========================
Connects to the OMS WebSocket, subscribes to topics, places orders via REST,
and watches the live push notifications come in.

Requirements:
  pip install websocket-client requests

Usage:
  # Watch live price ticks for RELIANCE and TCS
  python3 tests/ws_client.py --mode prices

  # Place an order and watch fill notification arrive over WebSocket
  python3 tests/ws_client.py --mode order --client CLIENT001

  # Full demo: subscribe to everything, place matching orders, watch trades
  python3 tests/ws_client.py --mode demo
"""

import argparse
import json
import threading
import time
import requests
import sys

try:
    import websocket
except ImportError:
    print("Install websocket-client:  pip install websocket-client")
    sys.exit(1)


BASE_URL = "http://localhost:8080"
WS_URL   = "ws://localhost:8080/ws"


# colour helpers (makes terminal output readable)
GREEN  = "\033[92m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
RED    = "\033[91m"
RESET  = "\033[0m"
BOLD   = "\033[1m"

def col(c, s): return f"{c}{s}{RESET}"


# REST helpers

def place_order(client_id, symbol, side, order_type, product, qty, price=None):
    payload = {
        "client_id":    client_id,
        "symbol":       symbol,
        "exchange":     "NSE",
        "side":         side,
        "order_type":   order_type,
        "product_type": product,
        "validity":     "DAY",
        "quantity":     qty,
    }
    if price:
        payload["price"] = price

    r = requests.post(f"{BASE_URL}/orders", json=payload, timeout=5)
    return r.json()


def get_quote(symbol):
    r = requests.get(f"{BASE_URL}/quote/{symbol}", timeout=3)
    return r.json() if r.status_code == 200 else {}


def get_account(client_id):
    r = requests.get(f"{BASE_URL}/accounts/{client_id}", timeout=3)
    return r.json() if r.status_code == 200 else {}


# WebSocket client

class WSClient:
    def __init__(self, client_id=None, on_message=None):
        url = WS_URL
        if client_id:
            url += f"?client_id={client_id}"

        self.client_id   = client_id
        self.on_message  = on_message or self._default_handler
        self.ws          = None
        self.connected   = threading.Event()
        self.msg_count   = 0
        self._url        = url

    def connect(self):
        self.ws = websocket.WebSocketApp(
            self._url,
            on_open    = self._on_open,
            on_message = self._on_message,
            on_error   = self._on_error,
            on_close   = self._on_close,
        )
        t = threading.Thread(target=self.ws.run_forever, daemon=True)
        t.start()
        self.connected.wait(timeout=5)
        return self.connected.is_set()

    def subscribe(self, topic):
        self.ws.send(json.dumps({"action": "subscribe", "topic": topic}))

    def unsubscribe(self, topic):
        self.ws.send(json.dumps({"action": "unsubscribe", "topic": topic}))

    def close(self):
        if self.ws:
            self.ws.close()

    def _on_open(self, ws):
        self.connected.set()
        print(col(GREEN, f"[WS] Connected (client_id={self.client_id or 'anonymous'})"))

    def _on_message(self, ws, raw):
        self.msg_count += 1
        try:
            msg = json.loads(raw)
            self.on_message(msg)
        except Exception as e:
            print(f"[WS] Parse error: {e}")

    def _on_error(self, ws, err):
        print(col(RED, f"[WS] Error: {err}"))

    def _on_close(self, ws, code, reason):
        print(col(YELLOW, f"[WS] Disconnected (code={code})"))

    def _default_handler(self, msg):
        msg_type = msg.get("type", "?")
        topic    = msg.get("topic", "")
        payload  = msg.get("payload", {})

        if msg_type == "price_tick":
            sym   = payload.get("symbol", "?")
            ltp   = payload.get("ltp", 0)
            chg   = payload.get("change_percent", 0)
            sign  = "▲" if chg >= 0 else "▼"
            colour = GREEN if chg >= 0 else RED
            print(col(colour, f"  📈 {sym:<12} LTP: ₹{ltp:>10.2f}  {sign} {chg:+.2f}%"))

        elif msg_type == "order_update":
            event = payload.get("event", "?")
            sym   = payload.get("symbol", "?")
            oid   = payload.get("order_id", payload.get("trade_id", "?"))[:20]
            print(col(CYAN, f"  📋 ORDER  [{event:<15}] {sym} | id:{oid}"))

        elif msg_type == "trade_alert":
            sym   = payload.get("symbol", "?")
            qty   = payload.get("quantity", 0)
            price = payload.get("price", 0)
            print(col(YELLOW, f"  ⚡ TRADE  {sym} {qty} shares @ ₹{price:.2f}"))

        elif msg_type == "subscribed":
            print(col(GREEN, f"  ✓ Subscribed to: {msg.get('topic')}"))

        elif msg_type == "pong":
            pass  # silent
        else:
            print(f"  [{msg_type}] {topic}: {payload}")


# Test modes 

def mode_prices(duration=20):
    """Subscribe to all price ticks and watch them arrive."""
    print(col(BOLD, f"\n{'='*55}"))
    print(col(BOLD, "  LIVE PRICE FEED MODE"))
    print(col(BOLD, f"{'='*55}"))
    print(f"  Watching live prices for {duration}s...\n")

    client = WSClient()
    if not client.connect():
        print(col(RED, "Could not connect. Is the server running?"))
        return

    # Subscribe to all price ticks
    client.subscribe("price:*")

    time.sleep(duration)
    print(f"\n  Total price ticks received: {client.msg_count}")
    client.close()


def mode_order(client_id="CLIENT001", duration=15):
    """Place an order and watch the status updates arrive over WebSocket."""
    print(col(BOLD, f"\n{'='*55}"))
    print(col(BOLD, "  ORDER LIFECYCLE OVER WEBSOCKET"))
    print(col(BOLD, f"{'='*55}\n"))

    # Connect with client_id so we auto subscribe to order updates
    client = WSClient(client_id=client_id)
    if not client.connect():
        print(col(RED, "Could not connect."))
        return

    # Also subscribe to price ticks
    client.subscribe("price:RELIANCE")
    time.sleep(0.5)

    # Show account before
    acc = get_account(client_id)
    print(f"  Account balance: ₹{acc.get('balance',0):,.2f}")
    print(f"  Free margin:     ₹{acc.get('free',0):,.2f}\n")

    # Get current LTP
    quote = get_quote("RELIANCE")
    ltp   = quote.get("ltp", 2500)
    print(f"  RELIANCE LTP: ₹{ltp:.2f}")

    # Place a LIMIT SELL first (passive — will sit in book)
    print(f"\n  [1] Placing SELL order at ₹{ltp*1.001:.2f}...")
    sell = place_order("CLIENT002", "RELIANCE", "SELL", "LIMIT", "MIS",
                       100, round(ltp * 1.001, 2))
    print(f"      → {sell}")

    time.sleep(0.3)

    # Place a matching BUY order — should trigger a trade
    print(f"\n  [2] Placing matching BUY order at ₹{ltp*1.001:.2f}...")
    buy = place_order(client_id, "RELIANCE", "BUY", "LIMIT", "MIS",
                      100, round(ltp * 1.001, 2))
    print(f"      → {buy}")

    print(f"\n  Watching for WebSocket events ({duration}s)...\n")
    time.sleep(duration)

    # Show account after
    acc_after = get_account(client_id)
    print(f"\n  Margin used after orders: ₹{acc_after.get('used',0):,.2f}")
    print(f"  Messages received: {client.msg_count}")
    client.close()


def mode_demo(duration=30):
    """Full demo: two clients, subscribe to everything, watch matching happen."""
    print(col(BOLD, f"\n{'='*55}"))
    print(col(BOLD, "  FULL DEMO — LIVE MATCHING OVER WEBSOCKET"))
    print(col(BOLD, f"{'='*55}\n"))

    events = []

    def handler(msg):
        events.append(msg)
        t = msg.get("type")
        p = msg.get("payload", {})

        if t == "price_tick":
            sym = p.get("symbol","?")
            ltp = p.get("ltp", 0)
            print(col(CYAN,   f"  📈  {sym:<10} ₹{ltp:>9.2f}"))
        elif t == "order_update":
            ev  = p.get("event","?")
            sym = p.get("symbol","?")
            print(col(YELLOW, f"  📋  ORDER [{ev}] on {sym}"))
        elif t == "trade_alert":
            sym = p.get("symbol","?")
            qty = p.get("quantity",0)
            px  = p.get("price",0)
            print(col(GREEN,  f"  ⚡  TRADE {sym}  {qty} @ ₹{px:.2f}  ← MATCH!"))
        elif t == "subscribed":
            print(col(GREEN,  f"  ✓   Subscribed: {msg.get('topic')}"))

    # Connect a single observer client
    observer = WSClient(on_message=handler)
    if not observer.connect():
        print(col(RED, "Cannot connect to OMS. Is it running?"))
        return

    # Subscribe to all topics
    for topic in ["price:*", "trades:*", "orders:CLIENT001", "orders:CLIENT002"]:
        observer.subscribe(topic)

    time.sleep(0.5)
    print(f"\n  Subscribed. Placing orders...\n")

    # Place a ladder of orders to generate multiple matches
    quote = get_quote("RELIANCE")
    ltp   = quote.get("ltp", 2500)

    for i in range(3):
        price = round(ltp * (1 + i * 0.001), 2)
        # Sell from CLIENT002
        place_order("CLIENT002", "RELIANCE", "SELL", "LIMIT", "MIS", 50, price)
        time.sleep(0.05)
        # Buy from CLIENT001 at same price — instant match
        place_order("CLIENT001", "RELIANCE", "BUY",  "LIMIT", "MIS", 50, price)
        time.sleep(0.2)

    print(f"\n  Watching live feed for {duration}s...")
    time.sleep(duration)

    # Summary
    type_counts = {}
    for e in events:
        t = e.get("type","?")
        type_counts[t] = type_counts.get(t, 0) + 1

    print(f"\n  {col(BOLD, 'Event Summary:')}")
    for t, count in sorted(type_counts.items()):
        print(f"    {t:<20}: {count}")
    print(f"  Total: {len(events)} events")
    observer.close()


# Entry point

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OMS WebSocket Test Client")
    parser.add_argument("--mode",     choices=["prices","order","demo"], default="demo")
    parser.add_argument("--client",   default="CLIENT001")
    parser.add_argument("--url",      default=BASE_URL)
    parser.add_argument("--duration", type=int, default=20)
    args = parser.parse_args()

    BASE_URL = args.url
    WS_URL   = args.url.replace("http://", "ws://").replace("https://", "wss://") + "/ws"

    if args.mode == "prices":
        mode_prices(args.duration)
    elif args.mode == "order":
        mode_order(args.client, args.duration)
    elif args.mode == "demo":
        mode_demo(args.duration)
