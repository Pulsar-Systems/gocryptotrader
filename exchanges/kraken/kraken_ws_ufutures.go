package kraken

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/thrasher-corp/gocryptotrader/common"
	"github.com/thrasher-corp/gocryptotrader/common/convert"
	"github.com/thrasher-corp/gocryptotrader/currency"
	"github.com/thrasher-corp/gocryptotrader/exchanges/asset"
	"github.com/thrasher-corp/gocryptotrader/exchanges/orderbook"
	"github.com/thrasher-corp/gocryptotrader/exchanges/stream"
	"github.com/thrasher-corp/gocryptotrader/log"
)

const (
	krakenUFuturesWSURL = "wss://futures.kraken.com/ws/v1"

	krakenUFuturesSubscribed = "subscribed"
	krakenUFuturesAlert      = "alert"
	krakenUFuturesInfo       = "info"
)

var (
	defaultSubscribedChannelsUFutures = []string{
		krakenWsOrderbook,
	}
)

func (k *Kraken) WsConnectUFutures() error {
	if !k.WebsocketUFutures.IsEnabled() || !k.IsEnabled() {
		return errors.New(stream.WebsocketNotEnabled)
	}

	var dialer websocket.Dialer
	err := k.WebsocketUFutures.Conn.Dial(&dialer, http.Header{})
	if err != nil {
		return err
	}

	comms := make(chan stream.Response)
	k.WebsocketUFutures.Wg.Add(2)
	go k.wsReadDataUFutures(comms)
	go k.wsFunnelConnectionDataUFutures(k.WebsocketUFutures.Conn, comms)

	err = k.wsPingHandlerUFutures()
	if err != nil {
		log.Errorf(log.ExchangeSys,
			"%v - failed setup ping handler. Websocket may disconnect unexpectedly. %v\n",
			k.Name,
			err)
	}
	return nil
}

func (k *Kraken) wsPingHandlerUFutures() error {
	message, err := json.Marshal(pingRequest)
	if err != nil {
		return err
	}
	k.WebsocketUFutures.Conn.SetupPingHandler(stream.PingHandler{
		Message:     message,
		Delay:       krakenWsPingDelay,
		MessageType: websocket.TextMessage,
	})
	return nil
}

// wsFunnelConnectionData funnels both auth and public ws data into one manageable place
func (k *Kraken) wsFunnelConnectionDataUFutures(ws stream.Connection, comms chan stream.Response) {
	defer k.WebsocketUFutures.Wg.Done()
	for {
		resp := ws.ReadMessage()
		if resp.Raw == nil {
			return
		}
		comms <- resp
	}
}

// wsReadData receives and passes on websocket messages for processing
func (k *Kraken) wsReadDataUFutures(comms chan stream.Response) {
	defer k.WebsocketUFutures.Wg.Done()

	for {
		select {
		case <-k.WebsocketUFutures.ShutdownC:
			select {
			case resp := <-comms:
				err := k.wsHandleDataUFutures(resp.Raw)
				if err != nil {
					select {
					case k.WebsocketUFutures.DataHandler <- err:
					default:
						log.Errorf(log.WebsocketMgr,
							"%s websocket handle data error: %v",
							k.Name,
							err)
					}
				}
			default:
			}
			return
		case resp := <-comms:
			err := k.wsHandleDataUFutures(resp.Raw)
			if err != nil {
				k.WebsocketUFutures.DataHandler <- fmt.Errorf("%s - unhandled websocket data: %v",
					k.Name,
					err)
			}
		}
	}
}

func (k *Kraken) wsHandleDataUFutures(respRaw []byte) error {
	var payload map[string]interface{}
	err := json.Unmarshal(respRaw, &payload)
	if err != nil {
		return fmt.Errorf("unable to unmarshal received data, got error: %v", err)
	}
	// First check for events as events have a "feed" field but feeds do not have an "event" field
	if event, ok := payload["event"]; ok {
		switch event {
		case stream.Pong, krakenWsHeartbeat, krakenUFuturesAlert, krakenUFuturesInfo:
			return nil
		case krakenUFuturesSubscribed:
			var sub WsSubscribedUFutures
			err := json.Unmarshal(respRaw, &sub)
			if err != nil {
				return fmt.Errorf("%s - err %s unable to parse subscription response: %s",
					k.Name,
					err,
					respRaw)
			}
			for _, product := range sub.ProductIDs {
				k.Websocket.AddSuccessfulSubscriptions(stream.ChannelSubscription{
					Channel:  sub.Feed,
					Currency: k.GetProductCurrencyPairFromID(product),
				})
			}
		default:
			k.WebsocketUFutures.DataHandler <- stream.UnhandledMessageWarning{
				Message: k.Name + stream.UnhandledMessage + string(respRaw),
			}
		}
		return nil
	}

	if feed, ok := payload["feed"]; ok {
		switch feed {
		case krakenWsOrderbook + "_snapshot":
			var snapshot UFuturesOBSnapshot
			err := json.Unmarshal(respRaw, &snapshot)
			if err != nil {
				return fmt.Errorf("unable to unmarshal orderbook snapshot: %v", err)
			}
			return k.wsHandleOrderbookSnapshotUFutures(snapshot)
		case krakenWsOrderbook:
			var update UFuturesOBUpdate
			err := json.Unmarshal(respRaw, &update)
			if err != nil {
				return fmt.Errorf("unable to unmarshal orderbook snapshot: %v", err)
			}
			return k.wsProcessOrderBookUpdateUFutures(update)
		default:
			return fmt.Errorf("%s received unidentified data for subscription %s: %s",
				k.Name,
				feed,
				respRaw)
		}
	}
	return nil
}

func (k *Kraken) GetProductCurrencyPairFromID(pi string) currency.Pair {
	baseStr := strings.Replace(strings.Replace(pi, "PI_", "", -1), "USD", "", -1)
	return currency.NewPairWithDelimiter(baseStr, "USD", "")
}

func (k *Kraken) wsHandleOrderbookSnapshotUFutures(snapshot UFuturesOBSnapshot) error {
	base := orderbook.Base{
		Exchange:        k.Name,
		Pair:            k.GetProductCurrencyPairFromID(snapshot.ProductID),
		Asset:           asset.USDTMarginedFutures,
		VerifyOrderbook: k.CanVerifyOrderbook,
		LastUpdated:     convert.TimeFromUnixTimestampDecimal(snapshot.Timestamp),
	}
	asks := make(orderbook.Items, len(snapshot.Asks))
	for i, ask := range snapshot.Asks {
		asks[i] = orderbook.Item{
			Price:  ask.Price,
			Amount: ask.Qty,
		}
	}
	bids := make(orderbook.Items, len(snapshot.Bids))
	for i, bid := range snapshot.Bids {
		bids[i] = orderbook.Item{
			Price:  bid.Price,
			Amount: bid.Qty,
		}
	}
	base.Asks = asks
	base.Bids = bids
	return k.WebsocketUFutures.Orderbook.LoadSnapshot(&base)
}

func (k *Kraken) wsProcessOrderBookUpdateUFutures(updatePayload UFuturesOBUpdate) error {
	update := orderbook.Update{
		Asset:      asset.USDTMarginedFutures,
		Pair:       k.GetProductCurrencyPairFromID(updatePayload.ProductID),
		MaxDepth:   krakenWsOrderbookDepth,
		UpdateTime: convert.TimeFromUnixTimestampDecimal(updatePayload.Timestamp),
	}
	itemToUpdate := orderbook.Item{
		Price:  updatePayload.Price,
		Amount: updatePayload.Qty,
	}
	if updatePayload.Side == "buy" {
		update.Bids = orderbook.Items{itemToUpdate}
		update.Asks = make(orderbook.Items, 0)
	}
	if updatePayload.Side == "sell" {
		update.Asks = orderbook.Items{itemToUpdate}
		update.Bids = make(orderbook.Items, 0)
	}
	return k.WebsocketUFutures.Orderbook.Update(&update)
}

// GenerateDefaultSubscriptions Adds default subscriptions to websocket to be handled by ManageSubscriptions()
func (k *Kraken) GenerateDefaultSubscriptionsUFutures() ([]stream.ChannelSubscription, error) {
	enabledCurrencies, err := k.GetEnabledPairs(asset.USDTMarginedFutures)
	if err != nil {
		return nil, err
	}
	var subscriptions []stream.ChannelSubscription
	for i := range defaultSubscribedChannelsUFutures {
		for j := range enabledCurrencies {
			enabledCurrencies[j].Delimiter = ""
			subscriptions = append(subscriptions, stream.ChannelSubscription{
				Channel:  defaultSubscribedChannelsUFutures[i],
				Currency: enabledCurrencies[j],
				Asset:    asset.USDTMarginedFutures,
			})
		}
	}
	return subscriptions, nil
}

// Subscribe sends a websocket message to receive data from the channel
func (k *Kraken) SubscribeUFutures(channelsToSubscribe []stream.ChannelSubscription) error {
	// WebsocketSubscriptionEventRequestUFutures was created with only the depth channel in mind
	// Additional parameters could/should be added for other streams
	var subscriptions = make(map[string]*WebsocketSubscriptionEventRequestUFutures)
	for _, channel := range channelsToSubscribe {
		pairStr := "PI_" + channel.Currency.Format(currency.PairFormat{Delimiter: "", Uppercase: true}).String()
		switch channel.Channel {
		case krakenWsOrderbook:
			req, ok := subscriptions[channel.Channel]
			if !ok {
				subscriptions[channel.Channel] = &WebsocketSubscriptionEventRequestUFutures{
					Event:      "subscribe",
					Feed:       "book",
					ProductIds: []string{pairStr},
					Channels:   []stream.ChannelSubscription{channel},
				}
			} else {
				req.ProductIds = append(req.ProductIds, pairStr)
				req.Channels = append(req.Channels, channel)
			}
		}
	}
	var errs common.Errors
	for subType, sub := range subscriptions {
		if subType == "book" {
			// There is an undocumented subscription limit that is present
			// on websocket orderbooks, to subscribe to the channel while
			// actually receiving the snapshots a rudimentary sleep is
			// imposed and requests are batched in allotments of 20 items.
			time.Sleep(time.Second)
		}
		err := k.WebsocketUFutures.Conn.SendJSONMessage(sub)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		// Commented this out because we can add successful subscriptions after we receive
		// the subscribed event from the websocket
		// k.WebsocketUFutures.AddSuccessfulSubscriptions(sub.Channels...)
	}
	if errs != nil {
		return errs
	}
	return nil
}

func (k *Kraken) UnsubscribeUFutures(channelsToUnsubscribe []stream.ChannelSubscription) error {
	var unsubs []WebsocketSubscriptionEventRequest
channels:
	for x := range channelsToUnsubscribe {
		for y := range unsubs {
			if unsubs[y].Subscription.Name == channelsToUnsubscribe[x].Channel {
				unsubs[y].Pairs = append(unsubs[y].Pairs,
					channelsToUnsubscribe[x].Currency.String())
				unsubs[y].Channels = append(unsubs[y].Channels,
					channelsToUnsubscribe[x])
				continue channels
			}
		}
		var depth int64
		if channelsToUnsubscribe[x].Channel == "book" {
			depth = krakenWsOrderbookDepth
		}

		var id int64
		if common.StringDataContains(authenticatedChannels, channelsToUnsubscribe[x].Channel) {
			id = k.WebsocketUFutures.AuthConn.GenerateMessageID(false)
		} else {
			id = k.WebsocketUFutures.Conn.GenerateMessageID(false)
		}

		unsub := WebsocketSubscriptionEventRequest{
			Event: krakenWsUnsubscribe,
			Pairs: []string{channelsToUnsubscribe[x].Currency.String()},
			Subscription: WebsocketSubscriptionData{
				Name:  channelsToUnsubscribe[x].Channel,
				Depth: depth,
			},
			RequestID: id,
		}
		if common.StringDataContains(authenticatedChannels, channelsToUnsubscribe[x].Channel) {
			unsub.Subscription.Token = authToken
		}
		unsub.Channels = append(unsub.Channels, channelsToUnsubscribe[x])
		unsubs = append(unsubs, unsub)
	}

	var errs common.Errors
	for i := range unsubs {
		if common.StringDataContains(authenticatedChannels, unsubs[i].Subscription.Name) {
			_, err := k.WebsocketUFutures.AuthConn.SendMessageReturnResponse(unsubs[i].RequestID, unsubs[i])
			if err != nil {
				errs = append(errs, err)
				continue
			}
			k.WebsocketUFutures.RemoveSuccessfulUnsubscriptions(unsubs[i].Channels...)
			continue
		}

		_, err := k.WebsocketUFutures.Conn.SendMessageReturnResponse(unsubs[i].RequestID, unsubs[i])
		if err != nil {
			errs = append(errs, err)
			continue
		}
		k.WebsocketUFutures.RemoveSuccessfulUnsubscriptions(unsubs[i].Channels...)
	}
	if errs != nil {
		return errs
	}
	return nil
}
