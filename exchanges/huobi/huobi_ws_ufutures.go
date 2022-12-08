package huobi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/thrasher-corp/gocryptotrader/common"
	"github.com/thrasher-corp/gocryptotrader/currency"
	"github.com/thrasher-corp/gocryptotrader/exchanges/account"
	"github.com/thrasher-corp/gocryptotrader/exchanges/asset"
	"github.com/thrasher-corp/gocryptotrader/exchanges/order"
	"github.com/thrasher-corp/gocryptotrader/exchanges/orderbook"
	"github.com/thrasher-corp/gocryptotrader/exchanges/stream"
	"github.com/thrasher-corp/gocryptotrader/exchanges/trade"
	"github.com/thrasher-corp/gocryptotrader/log"
)

const (
	futuresWSURL = "wss://api.hbdm.com/"

	futuresWSMarketURL = futuresWSURL + "linear-swap-ws"
)

var commsF = make(chan WsMessage)

func (h *HUOBI) WsConnectUFutures() error {
	if !h.WebsocketUFutures.IsEnabled() || !h.IsEnabled() {
		return errors.New(stream.WebsocketNotEnabled)
	}
	var dialer websocket.Dialer
	err := h.wsDialUFutures(&dialer)
	if err != nil {
		return err
	}

	h.WebsocketUFutures.Wg.Add(1)
	go h.wsReadDataUFutures()
	return nil
}

func (h *HUOBI) wsDialUFutures(dialer *websocket.Dialer) error {
	err := h.WebsocketUFutures.Conn.Dial(dialer, http.Header{})
	if err != nil {
		return err
	}
	h.WebsocketUFutures.Wg.Add(1)
	go h.wsFunnelConnectionDataUFutures(h.WebsocketUFutures.Conn, futuresWSMarketURL)
	return nil
}

// wsFunnelConnectionData manages data from multiple endpoints and passes it to
// a channel
func (h *HUOBI) wsFunnelConnectionDataUFutures(ws stream.Connection, url string) {
	defer h.WebsocketUFutures.Wg.Done()
	for {
		resp := ws.ReadMessage()
		if resp.Raw == nil {
			return
		}
		commsF <- WsMessage{Raw: resp.Raw, URL: url}
	}
}

// wsReadData receives and passes on websocket messages for processing
func (h *HUOBI) wsReadDataUFutures() {
	defer h.WebsocketUFutures.Wg.Done()
	for {
		select {
		case <-h.WebsocketUFutures.ShutdownC:
			select {
			case resp := <-commsF:
				err := h.wsHandleDataUFutures(resp.Raw)
				if err != nil {
					select {
					case h.WebsocketUFutures.DataHandler <- err:
					default:
						log.Errorf(log.WebsocketMgr,
							"%s websocket handle data error: %v",
							h.Name,
							err)
					}
				}
			default:
			}
			return
		case resp := <-commsF:
			err := h.wsHandleDataUFutures(resp.Raw)
			if err != nil {
				h.WebsocketUFutures.DataHandler <- err
			}
		}
	}
}

func (h *HUOBI) wsHandleDataUFutures(respRaw []byte) error {
	var init WsResponse
	err := json.Unmarshal(respRaw, &init)
	if err != nil {
		return err
	}
	if init.Subscribed != "" ||
		init.UnSubscribed != "" ||
		init.Op == "sub" ||
		init.Op == "unsub" {
		// TODO handle subs
		return nil
	}
	if init.Ping != 0 {
		h.sendPingResponseUFutures(init.Ping)
		return nil
	}

	if init.Op == "ping" {
		authPing := authenticationPing{
			OP: "pong",
			TS: init.TS,
		}
		err := h.WebsocketUFutures.AuthConn.SendJSONMessage(authPing)
		if err != nil {
			log.Error(log.ExchangeSys, err)
		}
		return nil
	}

	if init.ErrorMessage != "" {
		codes, _ := init.ErrorCode.(string)
		return errors.New(h.Name + " Code:" + codes + " Message:" + init.ErrorMessage)
	}

	if init.ClientID > 0 {
		if h.WebsocketUFutures.Match.IncomingWithData(init.ClientID, respRaw) {
			return nil
		}
	}

	switch {
	case strings.Contains(init.Channel, "depth"):
		var depth WsDepth
		err := json.Unmarshal(respRaw, &depth)
		if err != nil {
			return err
		}

		data := strings.Split(depth.Channel, ".")
		err = h.WsProcessOrderbookUFutures(&depth, data[1])
		if err != nil {
			return err
		}
	case strings.Contains(init.Channel, "kline"):
		var kline WsKline
		err := json.Unmarshal(respRaw, &kline)
		if err != nil {
			return err
		}
		data := strings.Split(kline.Channel, ".")
		var p currency.Pair
		var a asset.Item
		p, a, err = h.GetRequestFormattedPairAndAssetType(data[1])
		if err != nil {
			return err
		}
		h.WebsocketUFutures.DataHandler <- stream.KlineData{
			Timestamp:  time.UnixMilli(kline.Timestamp),
			Exchange:   h.Name,
			AssetType:  a,
			Pair:       p,
			OpenPrice:  kline.Tick.Open,
			ClosePrice: kline.Tick.Close,
			HighPrice:  kline.Tick.High,
			LowPrice:   kline.Tick.Low,
			Volume:     kline.Tick.Volume,
			Interval:   data[3],
		}
	case strings.Contains(init.Channel, "trade.detail"):
		if !h.IsSaveTradeDataEnabled() {
			return nil
		}
		var t WsTrade
		err := json.Unmarshal(respRaw, &t)
		if err != nil {
			return err
		}
		data := strings.Split(t.Channel, ".")
		var p currency.Pair
		var a asset.Item
		p, a, err = h.GetRequestFormattedPairAndAssetType(data[1])
		if err != nil {
			return err
		}
		var trades []trade.Data
		for i := range t.Tick.Data {
			side := order.Buy
			if t.Tick.Data[i].Direction != "buy" {
				side = order.Sell
			}
			trades = append(trades, trade.Data{
				Exchange:     h.Name,
				AssetType:    a,
				CurrencyPair: p,
				Timestamp:    time.UnixMilli(t.Tick.Data[i].Timestamp),
				Amount:       t.Tick.Data[i].Amount,
				Price:        t.Tick.Data[i].Price,
				Side:         side,
				TID:          strconv.FormatFloat(t.Tick.Data[i].TradeID, 'f', -1, 64),
			})
		}
		return trade.AddTradesToBuffer(h.Name, trades...)
	default:
		h.WebsocketUFutures.DataHandler <- stream.UnhandledMessageWarning{
			Message: h.Name + stream.UnhandledMessage + string(respRaw),
		}
		return nil
	}
	return nil
}

func (h *HUOBI) sendPingResponseUFutures(pong int64) {
	err := h.WebsocketUFutures.Conn.SendJSONMessage(WsPong{Pong: pong})
	if err != nil {
		log.Error(log.ExchangeSys, err)
	}
}

// WsProcessOrderbook processes new orderbook data
// IMPORTANT: In Huobi the Orderbook Entry sizes are in terms of contract amounts not the underlying asset amount
func (h *HUOBI) WsProcessOrderbookUFutures(update *WsDepth, symbol string) error {
	pairs, err := h.GetEnabledPairs(asset.USDTMarginedFutures)
	if err != nil {
		return err
	}

	format, err := h.GetPairFormat(asset.USDTMarginedFutures, true)
	if err != nil {
		return err
	}

	p, err := currency.NewPairFromFormattedPairs(symbol,
		pairs,
		format)
	if err != nil {
		return err
	}

	bids := make(orderbook.Items, len(update.Tick.Bids))
	for i := range update.Tick.Bids {
		price, ok := update.Tick.Bids[i][0].(float64)
		if !ok {
			return errors.New("unable to type assert bid price")
		}
		amount, ok := update.Tick.Bids[i][1].(float64)
		if !ok {
			return errors.New("unable to type assert bid amount")
		}
		bids[i] = orderbook.Item{
			Price:  price,
			Amount: amount,
		}
	}

	asks := make(orderbook.Items, len(update.Tick.Asks))
	for i := range update.Tick.Asks {
		price, ok := update.Tick.Asks[i][0].(float64)
		if !ok {
			return errors.New("unable to type assert ask price")
		}
		amount, ok := update.Tick.Asks[i][1].(float64)
		if !ok {
			return errors.New("unable to type assert ask amount")
		}
		asks[i] = orderbook.Item{
			Price:  price,
			Amount: amount,
		}
	}
	newOrderBook := orderbook.Base{
		Pair:            p,
		Asset:           asset.USDTMarginedFutures,
		Exchange:        h.Name,
		VerifyOrderbook: h.CanVerifyOrderbook,
		Asks:            asks,
		Bids:            bids,
	}
	return h.WebsocketUFutures.Orderbook.LoadSnapshot(&newOrderBook)
}

// GenerateDefaultSubscriptions Adds default subscriptions to websocket to be handled by ManageSubscriptions()
func (h *HUOBI) GenerateDefaultSubscriptionsUFutures() ([]stream.ChannelSubscription, error) {
	var channels = []string{
		wsMarketDepth}
	var subscriptions []stream.ChannelSubscription
	enabledCurrencies, err := h.GetEnabledPairs(asset.USDTMarginedFutures)
	if err != nil {
		return nil, err
	}
	for i := range channels {
		for j := range enabledCurrencies {
			channel := fmt.Sprintf(channels[i],
				enabledCurrencies[j].String())
			subscriptions = append(subscriptions, stream.ChannelSubscription{
				Channel:  channel,
				Currency: enabledCurrencies[j],
			})
		}
	}
	return subscriptions, nil
}

// Subscribe sends a websocket message to receive data from the channel
func (h *HUOBI) SubscribeUFutures(channelsToSubscribe []stream.ChannelSubscription) error {
	var creds *account.Credentials
	if h.WebsocketUFutures.CanUseAuthenticatedEndpoints() {
		var err error
		creds, err = h.GetCredentials(context.TODO())
		if err != nil {
			return err
		}
	}
	var errs common.Errors
	for i := range channelsToSubscribe {
		if (strings.Contains(channelsToSubscribe[i].Channel, "orders.") ||
			strings.Contains(channelsToSubscribe[i].Channel, "accounts")) && creds != nil {
			err := h.wsAuthenticatedSubscribe(creds,
				"sub",
				wsAccountsOrdersEndPoint+channelsToSubscribe[i].Channel,
				channelsToSubscribe[i].Channel)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			h.WebsocketUFutures.AddSuccessfulSubscriptions(channelsToSubscribe[i])
			continue
		}

		err := h.WebsocketUFutures.Conn.SendJSONMessage(WsRequest{
			Subscribe: channelsToSubscribe[i].Channel,
		})
		if err != nil {
			errs = append(errs, err)
			continue
		}
		h.WebsocketUFutures.AddSuccessfulSubscriptions(channelsToSubscribe[i])
	}
	if errs != nil {
		return errs
	}
	return nil
}

// Unsubscribe sends a websocket message to stop receiving data from the channel
func (h *HUOBI) UnsubscribeUFutures(channelsToUnsubscribe []stream.ChannelSubscription) error {
	var errs common.Errors
	for i := range channelsToUnsubscribe {
		err := h.WebsocketUFutures.Conn.SendJSONMessage(WsRequest{
			Unsubscribe: channelsToUnsubscribe[i].Channel,
		})
		if err != nil {
			errs = append(errs, err)
			continue
		}
		h.WebsocketUFutures.RemoveSuccessfulUnsubscriptions(channelsToUnsubscribe[i])
	}
	if errs != nil {
		return errs
	}
	return nil
}
