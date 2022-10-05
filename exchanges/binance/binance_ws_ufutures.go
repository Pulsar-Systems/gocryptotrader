package binance

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
	"github.com/thrasher-corp/gocryptotrader/currency"
	"github.com/thrasher-corp/gocryptotrader/exchanges/asset"
	"github.com/thrasher-corp/gocryptotrader/exchanges/order"
	"github.com/thrasher-corp/gocryptotrader/exchanges/orderbook"
	"github.com/thrasher-corp/gocryptotrader/exchanges/stream"
	"github.com/thrasher-corp/gocryptotrader/exchanges/ticker"
	"github.com/thrasher-corp/gocryptotrader/exchanges/trade"
	"github.com/thrasher-corp/gocryptotrader/log"
)

// WsConnect initiates a websocket connection
func (b *Binance) WsUFuturesConnect() error {
	if !b.Websocket.IsEnabled() || !b.IsEnabled() {
		return errors.New(stream.WebsocketNotEnabled)
	}

	var dialer websocket.Dialer
	dialer.HandshakeTimeout = b.Config.HTTPTimeout
	dialer.Proxy = http.ProxyFromEnvironment
	var err error
	if b.Websocket.CanUseAuthenticatedEndpoints() {
		listenKey, err = b.GetWsAuthStreamKey(context.TODO())
		if err != nil {
			b.Websocket.SetCanUseAuthenticatedEndpoints(false)
			log.Errorf(log.ExchangeSys,
				"%v unable to connect to authenticated Websocket. Error: %s",
				b.Name,
				err)
		} else {
			// cleans on failed connection
			clean := strings.Split(b.Websocket.GetWebsocketURL(), "?streams=")
			authPayload := clean[0] + "?streams=" + listenKey
			err = b.Websocket.SetWebsocketURL(authPayload, false, false)
			if err != nil {
				return err
			}
		}
	}

	err = b.Websocket.Conn.Dial(&dialer, http.Header{})
	if err != nil {
		return fmt.Errorf("%v - Unable to connect to Websocket. Error: %s",
			b.Name,
			err)
	}

	if b.Websocket.CanUseAuthenticatedEndpoints() {
		go b.KeepAuthKeyAlive()
	}

	b.Websocket.Conn.SetupPingHandler(stream.PingHandler{
		UseGorillaHandler: true,
		MessageType:       websocket.PongMessage,
		Delay:             pingDelay,
	})

	b.Websocket.Wg.Add(1)
	go b.wsReadUFuturesData()

	b.uFuturesSetupOrderbookManager()
	return nil
}

func (b *Binance) uFuturesSetupOrderbookManager() {
	if b.obm == nil {
		b.obm = &orderbookManager{
			state: make(map[currency.Code]map[currency.Code]map[asset.Item]*update),
			jobs:  make(chan job, maxWSOrderbookJobs),
		}
	} else {
		// Change state on reconnect for initial sync.
		for _, m1 := range b.obm.state {
			for _, m2 := range m1 {
				for _, update := range m2 {
					update.initialSync = true
					update.needsFetchingBook = true
					update.lastUpdateID = 0
				}
			}
		}
	}

	for i := 0; i < maxWSOrderbookWorkers; i++ {
		// 10 workers for synchronising book
		b.UFuturesSynchroniseWebsocketOrderbook()
	}
}

// wsReadData receives and passes on websocket messages for processing
func (b *Binance) wsReadUFuturesData() {
	defer b.Websocket.Wg.Done()

	for {
		resp := b.Websocket.Conn.ReadMessage()
		if resp.Raw == nil {
			return
		}
		err := b.wsHandleUFuturesData(resp.Raw)
		if err != nil {
			b.Websocket.DataHandler <- err
		}
	}
}

type UFuturesWebsocketDepthStream struct {
	Event                  string           `json:"e"`
	Timestamp              int64            `json:"E"`
	TransactionTime        int64            `json:"T"`
	LastUpdateIDPrevStream int64            `json:"pu"`
	Pair                   string           `json:"s"`
	FirstUpdateID          int64            `json:"U"`
	LastUpdateID           int64            `json:"u"`
	UpdateBids             [][2]interface{} `json:"a"`
	UpdateAsks             [][2]interface{} `json:"b"`
}

func UFutureDepthStreamToNormal(s UFuturesWebsocketDepthStream) WebsocketDepthStream {
	return WebsocketDepthStream{
		Event:         s.Event,
		Timestamp:     time.Unix(s.Timestamp, 0),
		Pair:          s.Pair,
		FirstUpdateID: s.FirstUpdateID,
		LastUpdateID:  s.LastUpdateID,
		UpdateBids:    s.UpdateBids,
		UpdateAsks:    s.UpdateAsks,
	}
}

func (b *Binance) wsHandleUFuturesData(respRaw []byte) error {
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
				b.Websocket.DataHandler <- data
				return nil
			case "outboundAccountPosition":
				var data wsAccountPosition
				err := json.Unmarshal(respRaw, &data)
				if err != nil {
					return fmt.Errorf("%v - Could not convert to outboundAccountPosition structure %s",
						b.Name,
						err)
				}
				b.Websocket.DataHandler <- data
				return nil
			case "balanceUpdate":
				var data wsBalanceUpdate
				err := json.Unmarshal(respRaw, &data)
				if err != nil {
					return fmt.Errorf("%v - Could not convert to balanceUpdate structure %s",
						b.Name,
						err)
				}
				b.Websocket.DataHandler <- data
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
					b.Websocket.DataHandler <- order.ClassificationError{
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
					b.Websocket.DataHandler <- order.ClassificationError{
						Exchange: b.Name,
						OrderID:  orderID,
						Err:      err,
					}
				}
				orderSide, err := order.StringToOrderSide(data.Data.Side)
				if err != nil {
					b.Websocket.DataHandler <- order.ClassificationError{
						Exchange: b.Name,
						OrderID:  orderID,
						Err:      err,
					}
				}
				b.Websocket.DataHandler <- &order.Detail{
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
				b.Websocket.DataHandler <- data
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

					return b.Websocket.Trade.Update(saveTradeData,
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

					b.Websocket.DataHandler <- &ticker.Price{
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

					b.Websocket.DataHandler <- stream.KlineData{
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
					var udepth UFuturesWebsocketDepthStream
					err := json.Unmarshal(rawData, &udepth)
					if err != nil {
						fmt.Printf("err: %v\n", err)
						return fmt.Errorf("%v - Could not convert to depthStream structure %s",
							b.Name,
							err)
					}
					depth := UFutureDepthStreamToNormal(udepth)
					init, err := b.UFuturesUpdateLocalBuffer(&depth)
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
					b.Websocket.DataHandler <- stream.UnhandledMessageWarning{
						Message: b.Name + stream.UnhandledMessage + string(respRaw),
					}
				}
			}
		}
	}
	return fmt.Errorf("unhandled stream data %s", string(respRaw))
}

// SeedLocalCache seeds depth data
func (b *Binance) UFuturesSeedLocalCache(ctx context.Context, p currency.Pair) error {
	ob, err := b.UFuturesOrderbook(ctx, p, 1000)
	if err != nil {
		return err
	}
	return b.UFuturesSeedLocalCacheWithBook(p, ob)
}

// SeedLocalCacheWithBook seeds the local orderbook cache
func (b *Binance) UFuturesSeedLocalCacheWithBook(p currency.Pair, orderbookNew *OrderBook) error {
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
	return b.Websocket.Orderbook.LoadSnapshot(&newOrderBook)
}

// UpdateLocalBuffer updates and returns the most recent iteration of the orderbook
func (b *Binance) UFuturesUpdateLocalBuffer(wsdp *WebsocketDepthStream) (bool, error) {
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
	err = b.obm.uFuturesStageWsUpdate(wsdp, currencyPair, asset.USDTMarginedFutures)
	if err != nil {
		fmt.Printf("err: %v\n", err)
		init, err2 := b.obm.uFuturesCheckIsInitialSync(currencyPair)
		if err2 != nil {
			fmt.Printf("err2: %v\n", err2)
			return false, err2
		}
		return init, err
	}

	err = b.uFuturesApplyBufferUpdate(currencyPair)
	if err != nil {
		b.uFuturesflushAndCleanup(currencyPair)
	}

	return false, err
}

// stageWsUpdate stages websocket update to roll through updates that need to
// be applied to a fetched orderbook via REST.
func (o *orderbookManager) uFuturesStageWsUpdate(u *WebsocketDepthStream, pair currency.Pair, a asset.Item) error {
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
	state.lastUpdateID = u.LastUpdateID

	select {
	// Put update in the channel buffer to be processed
	case state.buffer <- u:
		return nil
	default:
		<-state.buffer    // pop one element
		state.buffer <- u // to shift buffer on fail
		return fmt.Errorf("channel blockage for %s, asset %s and connection",
			pair, a)
	}
}

// GenerateSubscriptions generates the default subscription set
func (b *Binance) UFuturesGenerateSubscriptions() ([]stream.ChannelSubscription, error) {
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

// applyBufferUpdate applies the buffer to the orderbook or initiates a new
// orderbook sync by the REST protocol which is off handed to go routine.
func (b *Binance) uFuturesApplyBufferUpdate(pair currency.Pair) error {
	fetching, needsFetching, err := b.obm.uFuturesHandleFetchingBook(pair)
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
		return b.obm.uFuturesFetchBookViaREST(pair)
	}

	recent, err := b.Websocket.Orderbook.GetOrderbook(pair, asset.USDTMarginedFutures)
	if err != nil {
		log.Errorf(
			log.WebsocketMgr,
			"%s error fetching recent orderbook when applying updates: %s\n",
			b.Name,
			err)
	}

	if recent != nil {
		err = b.obm.uFuturesCheckAndProcessUpdate(b.ProcessUpdate, pair, recent)
		if err != nil {
			log.Errorf(
				log.WebsocketMgr,
				"%s error processing update - initiating new orderbook sync via REST: %s\n",
				b.Name,
				err)
			err = b.obm.uFuturesSetNeedsFetchingBook(pair)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (o *orderbookManager) uFuturesFetchBookViaREST(pair currency.Pair) error {
	o.Lock()
	defer o.Unlock()

	state, ok := o.state[pair.Base][pair.Quote][asset.USDTMarginedFutures]
	if !ok {
		return fmt.Errorf("fetch book via rest cannot match currency pair %s asset type %s",
			pair,
			asset.USDTMarginedFutures)
	}

	state.initialSync = true
	state.fetchingBook = true

	select {
	case o.jobs <- job{pair}:
		return nil
	default:
		return fmt.Errorf("%s %s book synchronisation channel blocked up",
			pair,
			asset.USDTMarginedFutures)
	}
}

// setNeedsFetchingBook completes the book fetching initiation.
func (o *orderbookManager) uFuturesSetNeedsFetchingBook(pair currency.Pair) error {
	o.Lock()
	defer o.Unlock()
	state, ok := o.state[pair.Base][pair.Quote][asset.USDTMarginedFutures]
	if !ok {
		return fmt.Errorf("could not match pair %s and asset type %s in hash table",
			pair,
			asset.USDTMarginedFutures)
	}
	state.needsFetchingBook = true
	return nil
}

// SynchroniseWebsocketOrderbook synchronises full orderbook for currency pair
// asset
func (b *Binance) UFuturesSynchroniseWebsocketOrderbook() {
	b.Websocket.Wg.Add(1)
	go func() {
		defer b.Websocket.Wg.Done()
		for {
			select {
			case <-b.Websocket.ShutdownC:
				for {
					select {
					case <-b.obm.jobs:
					default:
						return
					}
				}
			case j := <-b.obm.jobs:
				err := b.uFuturesProcessJob(j.Pair)
				if err != nil {
					log.Errorf(log.WebsocketMgr,
						"%s processing websocket orderbook error %v",
						b.Name, err)
				}
			}
		}
	}()
}

// processJob fetches and processes orderbook updates
func (b *Binance) uFuturesProcessJob(p currency.Pair) error {
	err := b.UFuturesSeedLocalCache(context.TODO(), p)
	if err != nil {
		return fmt.Errorf("%s %s seeding local cache for orderbook error: %v",
			p, asset.USDTMarginedFutures, err)
	}

	err = b.obm.uFuturesStopFetchingBook(p)
	if err != nil {
		return err
	}

	// Immediately apply the buffer updates so we don't wait for a
	// new update to initiate this.
	err = b.uFuturesApplyBufferUpdate(p)
	if err != nil {
		b.uFuturesflushAndCleanup(p)
		return err
	}
	return nil
}

func (o *orderbookManager) uFuturesStopFetchingBook(pair currency.Pair) error {
	o.Lock()
	defer o.Unlock()
	state, ok := o.state[pair.Base][pair.Quote][asset.USDTMarginedFutures]
	if !ok {
		return fmt.Errorf("could not match pair %s and asset type %s in hash table",
			pair,
			asset.USDTMarginedFutures)
	}
	if !state.fetchingBook {
		return fmt.Errorf("fetching book already set to false for %s %s",
			pair,
			asset.USDTMarginedFutures)
	}
	state.fetchingBook = false
	return nil
}

// flushAndCleanup flushes orderbook and clean local cache
func (b *Binance) uFuturesflushAndCleanup(p currency.Pair) {
	errClean := b.Websocket.Orderbook.FlushOrderbook(p, asset.USDTMarginedFutures)
	if errClean != nil {
		log.Errorf(log.WebsocketMgr,
			"%s flushing websocket error: %v",
			b.Name,
			errClean)
	}
	errClean = b.obm.cleanup(p)
	if errClean != nil {
		log.Errorf(log.WebsocketMgr, "%s cleanup websocket error: %v",
			b.Name,
			errClean)
	}
}

// handleFetchingBook checks if a full book is being fetched or needs to be
// fetched
func (o *orderbookManager) uFuturesHandleFetchingBook(pair currency.Pair) (fetching, needsFetching bool, err error) {
	o.Lock()
	defer o.Unlock()
	state, ok := o.state[pair.Base][pair.Quote][asset.USDTMarginedFutures]
	if !ok {
		return false,
			false,
			fmt.Errorf("check is fetching book cannot match currency pair %s asset type %s",
				pair,
				asset.USDTMarginedFutures)
	}

	if state.fetchingBook {
		return true, false, nil
	}

	if state.needsFetchingBook {
		state.needsFetchingBook = false
		state.fetchingBook = true
		return false, true, nil
	}
	return false, false, nil
}

// completeInitialSync sets if an asset type has completed its initial sync
func (o *orderbookManager) uFuturesCompleteInitialSync(pair currency.Pair) error {
	o.Lock()
	defer o.Unlock()
	state, ok := o.state[pair.Base][pair.Quote][asset.USDTMarginedFutures]
	if !ok {
		return fmt.Errorf("complete initial sync cannot match currency pair %s asset type %s",
			pair,
			asset.USDTMarginedFutures)
	}
	if !state.initialSync {
		return fmt.Errorf("initital sync already set to false for %s %s",
			pair,
			asset.USDTMarginedFutures)
	}
	state.initialSync = false
	return nil
}

// checkIsInitialSync checks status if the book is Initial Sync being via the REST
// protocol.
func (o *orderbookManager) uFuturesCheckIsInitialSync(pair currency.Pair) (bool, error) {
	o.Lock()
	defer o.Unlock()
	state, ok := o.state[pair.Base][pair.Quote][asset.USDTMarginedFutures]
	if !ok {
		return false,
			fmt.Errorf("checkIsInitialSync of orderbook cannot match currency pair %s asset type %s",
				pair,
				asset.USDTMarginedFutures)
	}
	return state.initialSync, nil
}

func (o *orderbookManager) uFuturesCheckAndProcessUpdate(processor func(currency.Pair, asset.Item, *WebsocketDepthStream) error, pair currency.Pair, recent *orderbook.Base) error {
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
			process, err := state.uFuturesValidate(d, recent)
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

// validate checks for correct update alignment
func (u *update) uFuturesValidate(updt *WebsocketDepthStream, recent *orderbook.Base) (bool, error) {
	if updt.LastUpdateID <= recent.LastUpdateID {
		// Drop any event where u is <= lastUpdateId in the snapshot.
		return false, nil
	}

	id := recent.LastUpdateID + 1
	if u.initialSync {
		// The first processed event should have U <= lastUpdateId+1 AND
		// u >= lastUpdateId+1.
		if updt.FirstUpdateID > id || updt.LastUpdateID < id {
			return false, fmt.Errorf("initial websocket orderbook sync failure for pair %s and asset %s",
				recent.Pair,
				asset.USDTMarginedFutures)
		}
		u.initialSync = false
	}
	return true, nil
}

// cleanup cleans up buffer and reset fetch and init
func (o *orderbookManager) uFuturesCleanup(pair currency.Pair) error {
	o.Lock()
	state, ok := o.state[pair.Base][pair.Quote][asset.USDTMarginedFutures]
	if !ok {
		o.Unlock()
		return fmt.Errorf("cleanup cannot match %s %s to hash table",
			pair,
			asset.USDTMarginedFutures)
	}

bufferEmpty:
	for {
		select {
		case <-state.buffer:
			// bleed and discard buffer
		default:
			break bufferEmpty
		}
	}
	o.Unlock()
	// disable rest orderbook synchronisation
	_ = o.uFuturesStopFetchingBook(pair)
	_ = o.uFuturesCompleteInitialSync(pair)
	_ = o.uFuturesStopNeedsFetchingBook(pair)
	return nil
}

// stopNeedsFetchingBook completes the book fetching initiation.
func (o *orderbookManager) uFuturesStopNeedsFetchingBook(pair currency.Pair) error {
	o.Lock()
	defer o.Unlock()
	state, ok := o.state[pair.Base][pair.Quote][asset.USDTMarginedFutures]
	if !ok {
		return fmt.Errorf("could not match pair %s and asset type %s in hash table",
			pair,
			asset.USDTMarginedFutures)
	}
	if !state.needsFetchingBook {
		return fmt.Errorf("needs fetching book already set to false for %s %s",
			pair,
			asset.USDTMarginedFutures)
	}
	state.needsFetchingBook = false
	return nil
}
