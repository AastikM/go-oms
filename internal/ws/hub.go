package ws

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type MessageType string

const (
	MsgPriceTick   MessageType = "price_tick"
	MsgOrderUpdate MessageType = "order_update"
	MsgTradeAlert  MessageType = "trade_alert"
	MsgSubscribed  MessageType = "subscribed"
	MsgError       MessageType = "error"
	MsgPing        MessageType = "ping"
	MsgPong        MessageType = "pong"
)

type Message struct {
	Type      MessageType `json:"type"`
	Topic     string      `json:"topic,omitempty"` // e.g. "price:RELIANCE"
	Payload   interface{} `json:"payload"`
	Timestamp time.Time   `json:"ts"`
}

// Client represents a single connected WebSocket client.
type Client struct {
	hub      *Hub
	conn     *websocket.Conn
	send     chan Message    // buffered outbound channel
	subs     map[string]bool // topics this client subscribed to
	clientID string          // set if the client authenticated
	mu       sync.RWMutex
}

// Hub is the central broadcaster. It owns all client connections.
type Hub struct {
	// These channels are the ONLY way to interact with the hub's internal state.
	// No mutex needed on clients map — only the run() goroutine touches it.
	register   chan *Client
	unregister chan *Client
	broadcast  chan Message

	clients map[*Client]bool
}

// NewHub creates a new Hub. Call Run() in a goroutine after this.
func NewHub() *Hub {
	return &Hub{
		register:   make(chan *Client, 256),
		unregister: make(chan *Client, 256),
		broadcast:  make(chan Message, 10000), // large buffer for price tick bursts
		clients:    make(map[*Client]bool),
	}
}

// Run is the hub's event loop. Must run in its own goroutine.
// It is the ONLY goroutine that touches h.clients — no locking needed.
func (h *Hub) Run() {
	log.Println("[WS Hub] Started")
	for {
		select {
		case client := <-h.register:
			h.clients[client] = true
			log.Printf("[WS Hub] Client connected (total: %d)", len(h.clients))

		case client := <-h.unregister:
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
				log.Printf("[WS Hub] Client disconnected (total: %d)", len(h.clients))
			}

		case msg := <-h.broadcast:
			// Fan out to every subscribed client
			for client := range h.clients {
				if client.isSubscribed(msg.Topic) {
					select {
					case client.send <- msg:
						// delivered
					default:
						// Client's send buffer is full — it's too slow.
						// Drop the message and disconnect to prevent backpressure.
						delete(h.clients, client)
						close(client.send)
						log.Printf("[WS Hub] Dropped slow client")
					}
				}
			}
		}
	}
}

// Broadcast sends a message to all clients subscribed to the topic.
// Non-blocking — if the hub is busy, the message is queued.
func (h *Hub) Broadcast(msg Message) {
	msg.Timestamp = time.Now()
	select {
	case h.broadcast <- msg:
	default:
		// Hub channel full — very rare, only under extreme load
		log.Println("[WS Hub] WARNING: broadcast channel full, dropping message")
	}
}

// BroadcastPriceTick sends a price update to all "price:{symbol}" subscribers.
func (h *Hub) BroadcastPriceTick(symbol string, payload interface{}) {
	h.Broadcast(Message{
		Type:    MsgPriceTick,
		Topic:   "price:" + symbol,
		Payload: payload,
	})
	// Also broadcast to "price:*" subscribers (those who want all symbols)
	h.Broadcast(Message{
		Type:    MsgPriceTick,
		Topic:   "price:*",
		Payload: payload,
	})
}

// BroadcastOrderUpdate sends an order status change to the order owner.
func (h *Hub) BroadcastOrderUpdate(clientID string, payload interface{}) {
	h.Broadcast(Message{
		Type:    MsgOrderUpdate,
		Topic:   "orders:" + clientID,
		Payload: payload,
	})
}

// BroadcastTrade sends a trade execution alert.
func (h *Hub) BroadcastTrade(payload interface{}) {
	h.Broadcast(Message{
		Type:    MsgTradeAlert,
		Topic:   "trades:*",
		Payload: payload,
	})
}

// ConnectedClients returns current connection count.
func (h *Hub) ConnectedClients() int {
	return len(h.clients)
}

func MsgType(s string) MessageType { return MessageType(s) }

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 4096,
	// Allow all origins in development.
	// In production: check r.Header.Get("Origin") against allowlist.
	CheckOrigin: func(r *http.Request) bool { return true },
}

// ServeWS handles the WebSocket upgrade and registers the client with the hub.
func (h *Hub) ServeWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[WS] Upgrade failed: %v", err)
		return
	}

	clientID := r.URL.Query().Get("client_id")

	client := &Client{
		hub:      h,
		conn:     conn,
		send:     make(chan Message, 256),
		subs:     make(map[string]bool),
		clientID: clientID,
	}

	h.register <- client

	// If client_id provided, auto-subscribe to their order updates
	if clientID != "" {
		client.subscribe("orders:" + clientID)
		client.send <- Message{
			Type:    MsgSubscribed,
			Topic:   "orders:" + clientID,
			Payload: map[string]string{"message": "Auto-subscribed to your orders"},
		}
	}

	// Start read and write pumps in goroutines
	// readPump:  handles incoming messages from client (subscriptions, pings)
	// writePump: sends queued messages to client
	go client.writePump()
	go client.readPump()
}

// subscribe adds the client to a topic.
func (c *Client) subscribe(topic string) {
	c.mu.Lock()
	c.subs[topic] = true
	c.mu.Unlock()
}

// unsubscribe removes the client from a topic.
func (c *Client) unsubscribe(topic string) {
	c.mu.Lock()
	delete(c.subs, topic)
	c.mu.Unlock()
}

// isSubscribed checks if this client wants messages on this topic.
func (c *Client) isSubscribed(topic string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.subs[topic]
}

// readPump reads incoming messages from the client.
// Handles: subscribe, unsubscribe, ping.
// Each client has exactly one readPump goroutine.
func (c *Client) readPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()

	// Configure connection health checks
	c.conn.SetReadLimit(512)
	c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		_, msgBytes, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err,
				websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("[WS] Read error: %v", err)
			}
			break
		}

		// Parse incoming control message
		var incoming struct {
			Action string `json:"action"` // "subscribe" | "unsubscribe" | "ping"
			Topic  string `json:"topic"`  // e.g. "price:RELIANCE"
		}
		if err := json.Unmarshal(msgBytes, &incoming); err != nil {
			c.send <- Message{Type: MsgError, Payload: "invalid message format"}
			continue
		}

		switch incoming.Action {
		case "subscribe":
			c.subscribe(incoming.Topic)
			c.send <- Message{
				Type:    MsgSubscribed,
				Topic:   incoming.Topic,
				Payload: map[string]string{"message": "subscribed to " + incoming.Topic},
			}
		case "unsubscribe":
			c.unsubscribe(incoming.Topic)
		case "ping":
			c.send <- Message{Type: MsgPong, Payload: "pong"}
		}
	}
}

// writePump drains the client's send channel and writes to the WebSocket.
// Each client has exactly one writePump goroutine.
func (c *Client) writePump() {
	// Send a ping every 30 seconds to keep the connection alive
	ticker := time.NewTicker(30 * time.Second)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case msg, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				// Hub closed the channel — send close frame
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			// Write the message as JSON
			if err := c.conn.WriteJSON(msg); err != nil {
				log.Printf("[WS] Write error: %v", err)
				return
			}

		case <-ticker.C:
			// Keepalive ping
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
