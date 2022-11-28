package binance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/gorilla/websocket"
	"github.com/thrasher-corp/gocryptotrader/config"
	"github.com/thrasher-corp/gocryptotrader/currency"
	"github.com/thrasher-corp/gocryptotrader/exchanges/asset"
	"github.com/thrasher-corp/gocryptotrader/exchanges/order"
	"github.com/thrasher-corp/gocryptotrader/exchanges/orderbook"
	"github.com/thrasher-corp/gocryptotrader/exchanges/stream"
	"github.com/thrasher-corp/gocryptotrader/exchanges/stream/buffer"
	"github.com/thrasher-corp/gocryptotrader/exchanges/ticker"
	"github.com/thrasher-corp/gocryptotrader/exchanges/trade"
	"github.com/thrasher-corp/gocryptotrader/log"
)

const (
	binanceUFuturesDefaultWebsocketURL = "wss://fstream.binance.com/stream"
)

func (b *Binance) SetupUFuture(exch *config.Exchange) error {
	if !exch.Enabled {
		b.SetEnabled(false)
		return nil
	}

	if b.Config == nil {
		err := b.SetupDefaults(exch)
		if err != nil {
			return err
		}
	}
	err := b.WebsocketUFuture.Setup(&stream.WebsocketSetup{
		ExchangeConfig:        exch,
		DefaultURL:            binanceUFuturesDefaultWebsocketURL,
		RunningURL:            binanceUFuturesDefaultWebsocketURL,
		Connector:             b.UFutureWsConnect,
		Subscriber:            b.SubscribeUFutures,
		Unsubscriber:          b.UnsubscribeUFutures,
		GenerateSubscriptions: b.UFutureGenerateSubscriptions,
		Features:              &b.Features.Supports.WebsocketCapabilities,
		OrderbookBufferConfig: buffer.Config{
			SortBuffer:            true,
			SortBufferByUpdateIDs: true,
		},
		TradeFeed: b.Features.Enabled.TradeFeed,
	})
	if err != nil {
		return err
	}
	return b.WebsocketUFuture.SetupNewConnection(stream.ConnectionSetup{
		ResponseCheckTimeout: exch.WebsocketResponseCheckTimeout,
		ResponseMaxLimit:     exch.WebsocketResponseMaxLimit,
		RateLimit:            wsRateLimitMilliseconds,
	})
}

// Subscribe subscribes to a set of channels
func (b *Binance) SubscribeUFutures(channelsToSubscribe []stream.ChannelSubscription) error {
	payload := WsPayload{
		Method: "SUBSCRIBE",
	}
	for i := range channelsToSubscribe {
		payload.Params = append(payload.Params, channelsToSubscribe[i].Channel)
		if i%50 == 0 && i != 0 {
			err := b.WebsocketUFuture.Conn.SendJSONMessage(payload)
			if err != nil {
				return err
			}
			payload.Params = []string{}
		}
	}
	if len(payload.Params) > 0 {
		err := b.WebsocketUFuture.Conn.SendJSONMessage(payload)
		if err != nil {
			return err
		}
	}
	b.WebsocketUFuture.AddSuccessfulSubscriptions(channelsToSubscribe...)
	return nil
}

// Unsubscribe unsubscribes from a set of channels
func (b *Binance) UnsubscribeUFutures(channelsToUnsubscribe []stream.ChannelSubscription) error {
	payload := WsPayload{
		Method: "UNSUBSCRIBE",
	}
	for i := range channelsToUnsubscribe {
		payload.Params = append(payload.Params, channelsToUnsubscribe[i].Channel)
		if i%50 == 0 && i != 0 {
			err := b.WebsocketUFuture.Conn.SendJSONMessage(payload)
			if err != nil {
				return err
			}
			payload.Params = []string{}
		}
	}
	if len(payload.Params) > 0 {
		err := b.WebsocketUFuture.Conn.SendJSONMessage(payload)
		if err != nil {
			return err
		}
	}
	b.WebsocketUFuture.RemoveSuccessfulUnsubscriptions(channelsToUnsubscribe...)
	return nil
}

// WsConnect initiates a websocket connection
func (b *Binance) UFutureWsConnect() error {
	if !b.WebsocketUFuture.IsEnabled() || !b.IsEnabled() {
		return errors.New(stream.WebsocketNotEnabled)
	}
	var dialer websocket.Dialer
	dialer.HandshakeTimeout = b.Config.HTTPTimeout
	dialer.Proxy = http.ProxyFromEnvironment
	var err error
	if b.WebsocketUFuture.CanUseAuthenticatedEndpoints() {
		listenKey, err = b.GetWsAuthStreamKey(context.TODO())
		if err != nil {
			b.WebsocketUFuture.SetCanUseAuthenticatedEndpoints(false)
			log.Errorf(log.ExchangeSys,
				"%v unable to connect to authenticated UFutures Websocket. Error: %s",
				b.Name,
				err)
		} else {
			// cleans on failed connection
			clean := strings.Split(b.WebsocketUFuture.GetWebsocketURL(), "?streams=")
			authPayload := clean[0] + "?streams=" + listenKey
			err = b.WebsocketUFuture.SetWebsocketURL(authPayload, false, false)
			if err != nil {
				return err
			}
		}
	}

	err = b.WebsocketUFuture.Conn.Dial(&dialer, http.Header{})
	if err != nil {
		return fmt.Errorf("%v - Unable to connect to UFutures Websocket. Error: %s",
			b.Name,
			err)
	}

	if b.WebsocketUFuture.CanUseAuthenticatedEndpoints() {
		go b.KeepAuthKeyAlive()
	}

	b.WebsocketUFuture.Conn.SetupPingHandler(stream.PingHandler{
		UseGorillaHandler: true,
		MessageType:       websocket.PongMessage,
		Delay:             pingDelay,
	})

	b.WebsocketUFuture.Wg.Add(1)
	go b.wsReadDataUFuture()

	b.setupOrderbookManager()
	return nil
}

// processJob fetches and processes orderbook updates
func (b *Binance) processJobUFuture(p currency.Pair) error {
	err := b.SeedLocalCacheUFuture(context.TODO(), p)
	if err != nil {
		return fmt.Errorf("%s %s seeding local cache for orderbook error: %v",
			p, asset.USDTMarginedFutures, err)
	}

	err = b.obm.stopFetchingBook(p, asset.USDTMarginedFutures)
	if err != nil {
		return err
	}

	// Immediately apply the buffer updates so we don't wait for a
	// new update to initiate this.
	err = b.applyBufferUpdateUFuture(p)
	if err != nil {
		b.flushAndCleanupUFutures(p)
		return err
	}
	return nil
}

// flushAndCleanup flushes orderbook and clean local cache
func (b *Binance) flushAndCleanupUFutures(p currency.Pair) {
	errClean := b.WebsocketUFuture.Orderbook.FlushOrderbook(p, asset.USDTMarginedFutures)
	if errClean != nil {
		log.Errorf(log.WebsocketMgr,
			"%s flushing websocket error: %v",
			b.Name,
			errClean)
	}
	errClean = b.obm.cleanup(p, asset.USDTMarginedFutures)
	if errClean != nil {
		log.Errorf(log.WebsocketMgr, "%s cleanup websocket error: %v",
			b.Name,
			errClean)
	}
}

// SeedLocalCache seeds depth data
func (b *Binance) SeedLocalCacheUFuture(ctx context.Context, p currency.Pair) error {
	ob, err := b.UFuturesOrderbook(context.TODO(), p, 1000)
	if err != nil {
		return err
	}
	return b.SeedLocalCacheWithBookUFutures(p, ob)
}

// SeedLocalCacheWithBook seeds the local orderbook cache
func (b *Binance) SeedLocalCacheWithBookUFutures(p currency.Pair, orderbookNew *OrderBook) error {
	newOrderBook := orderbook.Base{
		Pair:            p,
		Asset:           asset.USDTMarginedFutures,
		Exchange:        b.Name,
		LastUpdateID:    orderbookNew.LastUpdateID,
		VerifyOrderbook: b.CanVerifyOrderbook,
		Bids:            make(orderbook.Items, len(orderbookNew.Bids)),
		Asks:            make(orderbook.Items, len(orderbookNew.Asks)),
	}
	for i := range orderbookNew.Bids {
		newOrderBook.Bids[i] = orderbook.Item{
			Amount: orderbookNew.Bids[i].Quantity,
			Price:  orderbookNew.Bids[i].Price,
		}
	}
	for i := range orderbookNew.Asks {
		newOrderBook.Asks[i] = orderbook.Item{
			Amount: orderbookNew.Asks[i].Quantity,
			Price:  orderbookNew.Asks[i].Price,
		}
	}
	return b.WebsocketUFuture.Orderbook.LoadSnapshot(&newOrderBook)
}

func (b *Binance) wsReadDataUFuture() {
	defer b.WebsocketUFuture.Wg.Done()
	for {
		resp := b.WebsocketUFuture.Conn.ReadMessage()
		if resp.Raw == nil {
			return
		}
		err := b.wsHandleDataUFuture(resp.Raw)
		if err != nil {
			b.WebsocketUFuture.DataHandler <- err
		}
	}
}

func (b *Binance) wsHandleDataUFuture(respRaw []byte) error {
	var multiStreamData map[string]interface{}
	err := json.Unmarshal(respRaw, &multiStreamData)
	if err != nil {
		return err
	}

	if r, ok := multiStreamData["result"]; ok {
		if r == nil {
			return nil
		}
	}

	if method, ok := multiStreamData["method"].(string); ok {
		// TODO handle subscription handling
		if strings.EqualFold(method, "subscribe") {
			return nil
		}
		if strings.EqualFold(method, "unsubscribe") {
			return nil
		}
	}
	if newdata, ok := multiStreamData["data"].(map[string]interface{}); ok {
		if e, ok := newdata["e"].(string); ok {
			switch e {
			case "outboundAccountInfo":
				var data wsAccountInfo
				err := json.Unmarshal(respRaw, &data)
				if err != nil {
					return fmt.Errorf("%v - Could not convert to outboundAccountInfo structure %s",
						b.Name,
						err)
				}
				b.WebsocketUFuture.DataHandler <- data
				return nil
			case "outboundAccountPosition":
				var data wsAccountPosition
				err := json.Unmarshal(respRaw, &data)
				if err != nil {
					return fmt.Errorf("%v - Could not convert to outboundAccountPosition structure %s",
						b.Name,
						err)
				}
				b.WebsocketUFuture.DataHandler <- data
				return nil
			case "balanceUpdate":
				var data wsBalanceUpdate
				err := json.Unmarshal(respRaw, &data)
				if err != nil {
					return fmt.Errorf("%v - Could not convert to balanceUpdate structure %s",
						b.Name,
						err)
				}
				b.WebsocketUFuture.DataHandler <- data
				return nil
			case "executionReport":
				var data wsOrderUpdate
				err := json.Unmarshal(respRaw, &data)
				if err != nil {
					return fmt.Errorf("%v - Could not convert to executionReport structure %s",
						b.Name,
						err)
				}
				averagePrice := 0.0
				if data.Data.CumulativeFilledQuantity != 0 {
					averagePrice = data.Data.CumulativeQuoteTransactedQuantity / data.Data.CumulativeFilledQuantity
				}
				remainingAmount := data.Data.Quantity - data.Data.CumulativeFilledQuantity
				pair, assetType, err := b.GetRequestFormattedPairAndAssetType(data.Data.Symbol)
				if err != nil {
					return err
				}
				var feeAsset currency.Code
				if data.Data.CommissionAsset != "" {
					feeAsset = currency.NewCode(data.Data.CommissionAsset)
				}
				orderID := strconv.FormatInt(data.Data.OrderID, 10)
				orderStatus, err := stringToOrderStatus(data.Data.OrderStatus)
				if err != nil {
					b.WebsocketUFuture.DataHandler <- order.ClassificationError{
						Exchange: b.Name,
						OrderID:  orderID,
						Err:      err,
					}
				}
				clientOrderID := data.Data.ClientOrderID
				if orderStatus == order.Cancelled {
					clientOrderID = data.Data.CancelledClientOrderID
				}
				orderType, err := order.StringToOrderType(data.Data.OrderType)
				if err != nil {
					b.WebsocketUFuture.DataHandler <- order.ClassificationError{
						Exchange: b.Name,
						OrderID:  orderID,
						Err:      err,
					}
				}
				orderSide, err := order.StringToOrderSide(data.Data.Side)
				if err != nil {
					b.WebsocketUFuture.DataHandler <- order.ClassificationError{
						Exchange: b.Name,
						OrderID:  orderID,
						Err:      err,
					}
				}
				b.WebsocketUFuture.DataHandler <- &order.Detail{
					Price:                data.Data.Price,
					Amount:               data.Data.Quantity,
					AverageExecutedPrice: averagePrice,
					ExecutedAmount:       data.Data.CumulativeFilledQuantity,
					RemainingAmount:      remainingAmount,
					Cost:                 data.Data.CumulativeQuoteTransactedQuantity,
					CostAsset:            pair.Quote,
					Fee:                  data.Data.Commission,
					FeeAsset:             feeAsset,
					Exchange:             b.Name,
					OrderID:              orderID,
					ClientOrderID:        clientOrderID,
					Type:                 orderType,
					Side:                 orderSide,
					Status:               orderStatus,
					AssetType:            assetType,
					Date:                 data.Data.OrderCreationTime,
					LastUpdated:          data.Data.TransactionTime,
					Pair:                 pair,
				}
				return nil
			case "listStatus":
				var data wsListStatus
				err := json.Unmarshal(respRaw, &data)
				if err != nil {
					return fmt.Errorf("%v - Could not convert to listStatus structure %s",
						b.Name,
						err)
				}
				b.WebsocketUFuture.DataHandler <- data
				return nil
			}
		}
	}
	if wsStream, ok := multiStreamData["stream"].(string); ok {
		streamType := strings.Split(wsStream, "@")
		if len(streamType) > 1 {
			if data, ok := multiStreamData["data"]; ok {
				rawData, err := json.Marshal(data)
				if err != nil {
					return err
				}

				pairs, err := b.GetEnabledPairs(asset.USDTMarginedFutures)
				if err != nil {
					return err
				}

				format, err := b.GetPairFormat(asset.USDTMarginedFutures, true)
				if err != nil {
					return err
				}

				switch streamType[1] {
				case "trade":
					saveTradeData := b.IsSaveTradeDataEnabled()

					if !saveTradeData &&
						!b.IsTradeFeedEnabled() {
						return nil
					}

					var t TradeStream
					err := json.Unmarshal(rawData, &t)
					if err != nil {
						return fmt.Errorf("%v - Could not unmarshal trade data: %s",
							b.Name,
							err)
					}

					price, err := strconv.ParseFloat(t.Price, 64)
					if err != nil {
						return fmt.Errorf("%v - price conversion error: %s",
							b.Name,
							err)
					}

					amount, err := strconv.ParseFloat(t.Quantity, 64)
					if err != nil {
						return fmt.Errorf("%v - amount conversion error: %s",
							b.Name,
							err)
					}

					pair, err := currency.NewPairFromFormattedPairs(t.Symbol, pairs, format)
					if err != nil {
						return err
					}

					return b.WebsocketUFuture.Trade.Update(saveTradeData,
						trade.Data{
							CurrencyPair: pair,
							Timestamp:    t.TimeStamp,
							Price:        price,
							Amount:       amount,
							Exchange:     b.Name,
							AssetType:    asset.USDTMarginedFutures,
							TID:          strconv.FormatInt(t.TradeID, 10),
						})
				case "ticker":
					var t TickerStream
					err := json.Unmarshal(rawData, &t)
					if err != nil {
						return fmt.Errorf("%v - Could not convert to a TickerStream structure %s",
							b.Name,
							err.Error())
					}

					pair, err := currency.NewPairFromFormattedPairs(t.Symbol, pairs, format)
					if err != nil {
						return err
					}

					b.WebsocketUFuture.DataHandler <- &ticker.Price{
						ExchangeName: b.Name,
						Open:         t.OpenPrice,
						Close:        t.ClosePrice,
						Volume:       t.TotalTradedVolume,
						QuoteVolume:  t.TotalTradedQuoteVolume,
						High:         t.HighPrice,
						Low:          t.LowPrice,
						Bid:          t.BestBidPrice,
						Ask:          t.BestAskPrice,
						Last:         t.LastPrice,
						LastUpdated:  t.EventTime,
						AssetType:    asset.USDTMarginedFutures,
						Pair:         pair,
					}
					return nil
				case "kline_1m", "kline_3m", "kline_5m", "kline_15m", "kline_30m", "kline_1h", "kline_2h", "kline_4h",
					"kline_6h", "kline_8h", "kline_12h", "kline_1d", "kline_3d", "kline_1w", "kline_1M":
					var kline KlineStream
					err := json.Unmarshal(rawData, &kline)
					if err != nil {
						return fmt.Errorf("%v - Could not convert to a KlineStream structure %s",
							b.Name,
							err)
					}

					pair, err := currency.NewPairFromFormattedPairs(kline.Symbol, pairs, format)
					if err != nil {
						return err
					}

					b.WebsocketUFuture.DataHandler <- stream.KlineData{
						Timestamp:  kline.EventTime,
						Pair:       pair,
						AssetType:  asset.USDTMarginedFutures,
						Exchange:   b.Name,
						StartTime:  kline.Kline.StartTime,
						CloseTime:  kline.Kline.CloseTime,
						Interval:   kline.Kline.Interval,
						OpenPrice:  kline.Kline.OpenPrice,
						ClosePrice: kline.Kline.ClosePrice,
						HighPrice:  kline.Kline.HighPrice,
						LowPrice:   kline.Kline.LowPrice,
						Volume:     kline.Kline.Volume,
					}
					return nil
				case "depth":
					var depth WebsocketDepthStream
					err := json.Unmarshal(rawData, &depth)
					if err != nil {
						return fmt.Errorf("%v - Could not convert to depthStream structure %s",
							b.Name,
							err)
					}
					init, err := b.UpdateLocalBufferUFutures(&depth)
					if err != nil {
						if init {
							return nil
						}
						return fmt.Errorf("%v - UpdateLocalCache error: %s",
							b.Name,
							err)
					}
					return nil
				default:
					b.WebsocketUFuture.DataHandler <- stream.UnhandledMessageWarning{
						Message: b.Name + stream.UnhandledMessage + string(respRaw),
					}
				}
			}
		}
	}
	return fmt.Errorf("unhandled stream data %s", string(respRaw))
}

// UpdateLocalBuffer updates and returns the most recent iteration of the orderbook
func (b *Binance) UpdateLocalBufferUFutures(wsdp *WebsocketDepthStream) (bool, error) {
	enabledPairs, err := b.GetEnabledPairs(asset.USDTMarginedFutures)
	if err != nil {
		return false, err
	}

	format, err := b.GetPairFormat(asset.USDTMarginedFutures, true)
	if err != nil {
		return false, err
	}

	currencyPair, err := currency.NewPairFromFormattedPairs(wsdp.Pair,
		enabledPairs,
		format)
	if err != nil {
		return false, err
	}

	err = b.obm.stageWsUpdateUFuture(wsdp, currencyPair, asset.USDTMarginedFutures)
	if err != nil {
		init, err2 := b.obm.checkIsInitialSync(currencyPair, asset.USDTMarginedFutures)
		if err2 != nil {
			return false, err2
		}
		return init, err
	}

	err = b.applyBufferUpdateUFuture(currencyPair)
	if err != nil {
		b.flushAndCleanup(currencyPair)
	}

	return false, err
}

func (o *orderbookManager) stageWsUpdateUFuture(u *WebsocketDepthStream, pair currency.Pair, a asset.Item) error {
	o.Lock()
	defer o.Unlock()
	m1, ok := o.state[pair.Base]
	if !ok {
		m1 = make(map[currency.Code]map[asset.Item]*update)
		o.state[pair.Base] = m1
	}

	m2, ok := m1[pair.Quote]
	if !ok {
		m2 = make(map[asset.Item]*update)
		m1[pair.Quote] = m2
	}

	state, ok := m2[a]
	if !ok {
		state = &update{
			// 100ms update assuming we might have up to a 10 second delay.
			// There could be a potential 100 updates for the currency.
			buffer:            make(chan *WebsocketDepthStream, maxWSUpdateBuffer),
			fetchingBook:      false,
			initialSync:       true,
			needsFetchingBook: true,
		}
		m2[a] = state
	}

	if state.lastUpdateID != 0 && u.LastUpdateIDPrevStream != state.lastUpdateID {
		// While listening to the stream, each new event's U should have
		// pu equal to the previous event's u.
		fmt.Println("pu=", u.LastUpdateIDPrevStream, "lastu=", state.lastUpdateID)
		return fmt.Errorf("PU websocket orderbook synchronisation failure for pair %s and asset %s", pair, a)
	}
	// fmt.Println("Setting lastUpdateID:", u.LastUpdateID)
	state.lastUpdateID = u.LastUpdateID

	select {
	// Put update in the channel buffer to be processed
	case state.buffer <- u:
		// fmt.Println("stageWsUpdate put the update to buffer:", a)
		return nil
	default:
		<-state.buffer    // pop one element
		state.buffer <- u // to shift buffer on fail
		return fmt.Errorf("channel blockage for %s, asset %s and connection",
			pair, a)
	}
}

func (b *Binance) applyBufferUpdateUFuture(pair currency.Pair) error {
	fetching, needsFetching, err := b.obm.handleFetchingBook(pair, asset.USDTMarginedFutures)
	if err != nil {
		return err
	}
	if fetching {
		return nil
	}
	if needsFetching {
		if b.Verbose {
			log.Debugf(log.WebsocketMgr, "%s Orderbook: Fetching via REST\n", b.Name)
		}
		return b.obm.fetchBookViaREST(pair, asset.USDTMarginedFutures)
	}

	recent, err := b.WebsocketUFuture.Orderbook.GetOrderbook(pair, asset.USDTMarginedFutures)
	if err != nil {
		log.Errorf(
			log.WebsocketMgr,
			"%s error fetching recent orderbook when applying updates: %s\n",
			b.Name,
			err)
	}

	if recent != nil {
		err = b.obm.checkAndProcessUpdateUFutures(b.ProcessUpdateUFutures, pair, recent)
		if err != nil {
			log.Errorf(
				log.WebsocketMgr,
				"%s error processing update - initiating new orderbook sync via REST: %s\n",
				b.Name,
				err)
			err = b.obm.setNeedsFetchingBook(pair, asset.USDTMarginedFutures)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// ProcessUpdate processes the websocket orderbook update
func (b *Binance) ProcessUpdateUFutures(cp currency.Pair, a asset.Item, ws *WebsocketDepthStream) error {
	updateBid := make([]orderbook.Item, len(ws.UpdateBids))
	for i := range ws.UpdateBids {
		price, ok := ws.UpdateBids[i][0].(string)
		if !ok {
			return errors.New("type assertion failed for bid price")
		}
		p, err := strconv.ParseFloat(price, 64)
		if err != nil {
			return err
		}
		amount, ok := ws.UpdateBids[i][1].(string)
		if !ok {
			return errors.New("type assertion failed for bid amount")
		}
		a, err := strconv.ParseFloat(amount, 64)
		if err != nil {
			return err
		}
		updateBid[i] = orderbook.Item{Price: p, Amount: a}
	}

	updateAsk := make([]orderbook.Item, len(ws.UpdateAsks))
	for i := range ws.UpdateAsks {
		price, ok := ws.UpdateAsks[i][0].(string)
		if !ok {
			return errors.New("type assertion failed for ask price")
		}
		p, err := strconv.ParseFloat(price, 64)
		if err != nil {
			return err
		}
		amount, ok := ws.UpdateAsks[i][1].(string)
		if !ok {
			return errors.New("type assertion failed for ask amount")
		}
		a, err := strconv.ParseFloat(amount, 64)
		if err != nil {
			return err
		}
		updateAsk[i] = orderbook.Item{Price: p, Amount: a}
	}

	return b.WebsocketUFuture.Orderbook.Update(&orderbook.Update{
		Bids:       updateBid,
		Asks:       updateAsk,
		Pair:       cp,
		UpdateID:   ws.LastUpdateID,
		UpdateTime: ws.Timestamp,
		Asset:      a,
	})
}

func (o *orderbookManager) checkAndProcessUpdateUFutures(processor func(currency.Pair, asset.Item, *WebsocketDepthStream) error, pair currency.Pair, recent *orderbook.Base) error {
	o.Lock()
	defer o.Unlock()
	state, ok := o.state[pair.Base][pair.Quote][asset.USDTMarginedFutures]
	if !ok {
		return fmt.Errorf("could not match pair [%s] asset type [%s] in hash table to process websocket orderbook update",
			pair, asset.USDTMarginedFutures)
	}

	// This will continuously remove updates from the buffered channel and
	// apply them to the current orderbook.
buffer:
	for {
		select {
		case d := <-state.buffer:
			process, err := state.validateUFutures(d, recent)
			if err != nil {
				return err
			}
			if process {
				err := processor(pair, asset.USDTMarginedFutures, d)
				if err != nil {
					return fmt.Errorf("%s %s processing update error: %w",
						pair, asset.USDTMarginedFutures, err)
				}
			}
		default:
			break buffer
		}
	}
	return nil
}

func (u *update) validateUFutures(updt *WebsocketDepthStream, recent *orderbook.Base) (bool, error) {
	// https://binance-docs.github.io/apidocs/futures/en/#how-to-manage-a-local-order-book-correctly
	if updt.LastUpdateID < recent.LastUpdateID {
		// Drop any event where u is < lastUpdateId in the snapshot.
		return false, nil
	}
	id := recent.LastUpdateID
	if u.initialSync {
		// The first processed event should have U <= lastUpdateId AND u >= lastUpdateId.
		if updt.FirstUpdateID > id || updt.LastUpdateID < id {
			return false, fmt.Errorf("initial websocket orderbook sync failure for pair %s and asset %s",
				recent.Pair,
				asset.USDTMarginedFutures)
		}
		u.initialSync = false
	} else if recent.LastUpdateID != updt.LastUpdateIDPrevStream {
		// While listening to the stream, each new event's pu should be equal to the previous event's u,
		// otherwise initialize the process from step 3.
		// Raise an error rather than returning false, so that the OrderBook is fetched again
		return false, errors.New("uFuturesValidate: event pu is not equal to previous event's u")
	}
	return true, nil
}

func (b *Binance) UFutureGenerateSubscriptions() ([]stream.ChannelSubscription, error) {
	// For now only depth stream is implemented
	var channels = []string{"@ticker", "@trade", "@kline_1m", "@depth@100ms"}
	var subscriptions []stream.ChannelSubscription
	assets := b.GetAssetTypes(true)
	for x := range assets {
		if assets[x] == asset.USDTMarginedFutures {
			pairs, err := b.GetEnabledPairs(assets[x])
			if err != nil {
				return nil, err
			}

			for y := range pairs {
				for z := range channels {
					lp := pairs[y].Lower()
					lp.Delimiter = ""
					subscriptions = append(subscriptions, stream.ChannelSubscription{
						Channel:  lp.String() + channels[z],
						Currency: pairs[y],
						Asset:    assets[x],
					})
				}
			}
		}
	}
	return subscriptions, nil
}
