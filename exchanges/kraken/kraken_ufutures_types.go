package kraken

import (
	"github.com/thrasher-corp/gocryptotrader/exchanges/stream"
)

type UFuturesOBSnapshot struct {
	Feed      string            `json:"feed"`
	ProductID string            `json:"product_id"`
	Timestamp float64           `json:"timestamp"`
	Seq       int               `json:"seq"`
	TickSize  interface{}       `json:"tickSize"`
	Bids      []UFuturesOBEntry `json:"bids"`
	Asks      []UFuturesOBEntry `json:"asks"`
}

type UFuturesOBEntry struct {
	Price float64 `json:"price"`
	Qty   float64 `json:"qty"`
}

type UFuturesOBUpdate struct {
	Feed      string  `json:"feed"`
	ProductID string  `json:"product_id"`
	Side      string  `json:"side"`
	Seq       int     `json:"seq"`
	Price     float64 `json:"price"`
	Qty       float64 `json:"qty"`
	Timestamp float64 `json:"timestamp"`
}

type WebsocketSubscriptionEventRequestUFutures struct {
	Event      string                       `json:"event"`
	Feed       string                       `json:"feed"`
	ProductIds []string                     `json:"product_ids"`
	Channels   []stream.ChannelSubscription `json:"-"`
}

type WsSubscribedUFutures struct {
	Event      string   `json:"event"`
	Feed       string   `json:"feed"`
	ProductIDs []string `json:"product_ids"`
}