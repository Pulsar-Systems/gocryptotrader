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
	exchange "github.com/thrasher-corp/gocryptotrader/exchanges"
	"github.com/thrasher-corp/gocryptotrader/exchanges/asset"
	"github.com/thrasher-corp/gocryptotrader/exchanges/order"
	"github.com/thrasher-corp/gocryptotrader/exchanges/orderbook"
	"github.com/thrasher-corp/gocryptotrader/exchanges/stream"
	"github.com/thrasher-corp/gocryptotrader/exchanges/ticker"
	"github.com/thrasher-corp/gocryptotrader/exchanges/trade"
	"github.com/thrasher-corp/gocryptotrader/log"
)

const (
	binanceDefaultWebsocketSpotURL    = "wss://stream.binance.com:9443/stream"
	binanceDefaultWebsocketUFutureURL = "wss://fstream.binance.com/stream"
	pingDelay                         = time.Minute * 9
)

var listenKey string

var (
	// maxWSUpdateBuffer defines max websocket updates to apply when an
	// orderbook is initially fetched
	maxWSUpdateBuffer = 150
	// maxWSOrderbookJobs defines max websocket orderbook jobs in queue to fetch
	// an orderbook snapshot via REST
	maxWSOrderbookJobs = 2000
	// maxWSOrderbookWorkers defines a max amount of workers allowed to execute
	// jobs from the job channel
	maxWSOrderbookWorkers = 10
)

func (b *Binance) WsConnectFactory(url exchange.URL) func() error {
	return func() error {
		if !b.IsEnabled() {
			return errors.New(stream.WebsocketNotEnabled)
		}
		bWebsocket, exist := b.Websockets[url]
		if !exist {
			return fmt.Errorf("url of type: %v does not have Websocket", url)
		}
		if !bWebsocket.IsEnabled() {
			return errors.New(stream.WebsocketNotEnabled)
		}

		var dialer websocket.Dialer
		dialer.HandshakeTimeout = b.Config.HTTPTimeout
		dialer.Proxy = http.ProxyFromEnvironment
		var err error
		if bWebsocket.CanUseAuthenticatedEndpoints() {
			listenKey, err = b.GetWsAuthStreamKey(context.TODO())
			if err != nil {
				bWebsocket.SetCanUseAuthenticatedEndpoints(false)
				log.Errorf(log.ExchangeSys, "%v unable to connect to authenticated Websocket. Error: %s", b.Name, err)
			} else {
				// cleans on failed connection
				clean := strings.Split(bWebsocket.GetWebsocketURL(), "?streams=")
				authPayload := clean[0] + "?streams=" + listenKey
				err = bWebsocket.SetWebsocketURL(authPayload, false, false)
				if err != nil {
					return err
				}
			}
		}

		err = bWebsocket.Conn.Dial(&dialer, http.Header{})
		if err != nil {
			return fmt.Errorf("%v - Unable to connect to Websocket. Error: %s",
				b.Name,
				err)
		}

		if bWebsocket.CanUseAuthenticatedEndpoints() {
			go b.KeepAuthKeyAlive(bWebsocket)
		}

		bWebsocket.Conn.SetupPingHandler(stream.PingHandler{
			UseGorillaHandler: true,
			MessageType:       websocket.PongMessage,
			Delay:             pingDelay,
		})

		bWebsocket.Wg.Add(1)
		go b.wsReadData(bWebsocket, url)

		b.setupOrderbookManager(url)
		for i := 0; i < maxWSOrderbookWorkers; i++ {
			// 10 workers for synchronising book
			b.SynchroniseWebsocketOrderbook(url)
		}
		return nil
	}
}

// WsConnect initiates a websocket connection
// func (b *Binance) WsConnect(assetItem asset.Item) error {
// 	bWebsocket := b.Websockets[assetItem]
// 	if !b.IsEnabled() || !bWebsocket.IsEnabled() {
// 		return errors.New(stream.WebsocketNotEnabled)
// 	}

// 	var dialer websocket.Dialer
// 	dialer.HandshakeTimeout = b.Config.HTTPTimeout
// 	dialer.Proxy = http.ProxyFromEnvironment
// 	var err error
// 	if bWebsocket.CanUseAuthenticatedEndpoints() {
// 		listenKey, err = b.GetWsAuthStreamKey(context.TODO())
// 		if err != nil {
// 			bWebsocket.SetCanUseAuthenticatedEndpoints(false)
// 			log.Errorf(log.ExchangeSys,
// 				"%v unable to connect to authenticated Websocket. Error: %s",
// 				b.Name,
// 				err)
// 		} else {
// 			// cleans on failed connection
// 			clean := strings.Split(bWebsocket.GetWebsocketURL(), "?streams=")
// 			authPayload := clean[0] + "?streams=" + listenKey
// 			err = bWebsocket.SetWebsocketURL(authPayload, false, false)
// 			if err != nil {
// 				return err
// 			}
// 		}
// 	}

// 	err = bWebsocket.Conn.Dial(&dialer, http.Header{})
// 	if err != nil {
// 		return fmt.Errorf("%v - Unable to connect to Websocket. Error: %s",
// 			b.Name,
// 			err)
// 	}

// 	if bWebsocket.CanUseAuthenticatedEndpoints() {
// 		go b.KeepAuthKeyAlive()
// 	}

// 	bWebsocket.Conn.SetupPingHandler(stream.PingHandler{
// 		UseGorillaHandler: true,
// 		MessageType:       websocket.PongMessage,
// 		Delay:             pingDelay,
// 	})

// 	bWebsocket.Wg.Add(1)
// 	go b.wsReadData()

// 	b.setupOrderbookManager()
// 	return nil
// }

func (b *Binance) setupOrderbookManager(url exchange.URL) {
	if b.obm == nil {
		b.obm = map[exchange.URL]*orderbookManager{}
		b.obm[url] = &orderbookManager{
			state: make(map[currency.Code]map[currency.Code]map[asset.Item]*update),
			jobs:  make(chan job, maxWSOrderbookJobs),
		}
	} else {
		obm, exist := b.obm[url] // todo err
		if !exist {
			b.obm[url] = &orderbookManager{
				state: make(map[currency.Code]map[currency.Code]map[asset.Item]*update),
				jobs:  make(chan job, maxWSOrderbookJobs),
			}
		} else {
			// urlAssets, err := exchange.GetAssetsFromURLType(url)
			// _ = err // todo
			// Change state on reconnect for initial sync.
			for _, m1 := range obm.state {
				for _, m2 := range m1 {
					for _, update := range m2 {
						// for _, a := range urlAssets {
						// if update, ok := m2[a]; ok {
						update.initialSync = true
						update.needsFetchingBook = true
						update.lastUpdateID = 0
						// }
					}
				}
			}
		}
	}
}

// KeepAuthKeyAlive will continuously send messages to
// keep the WS auth key active
func (b *Binance) KeepAuthKeyAlive(bWebsocket *stream.Websocket) {
	bWebsocket.Wg.Add(1)
	defer bWebsocket.Wg.Done()
	ticks := time.NewTicker(time.Minute * 30)
	for {
		select {
		case <-bWebsocket.ShutdownC:
			ticks.Stop()
			return
		case <-ticks.C:
			err := b.MaintainWsAuthStreamKey(context.TODO())
			if err != nil {
				bWebsocket.DataHandler <- err
				log.Warnf(log.ExchangeSys,
					b.Name+" - Unable to renew auth websocket token, may experience shutdown")
			}
		}
	}
}

// wsReadData receives and passes on websocket messages for processing
func (b *Binance) wsReadData(bWebsocket *stream.Websocket, url exchange.URL) {
	defer bWebsocket.Wg.Done()
	for {
		resp := bWebsocket.Conn.ReadMessage()
		if resp.Raw == nil {
			return
		}
		var err error
		switch url {
		case exchange.WebsocketSpot:
			err = b.wsHandleData(resp.Raw, bWebsocket)
		case exchange.WebsocketUFutures:
			// fmt.Println("wsReadData sending data to wsHandleUFutureData")
			err = b.wsHandleUFutureData(resp.Raw, bWebsocket)
		}
		if err != nil {
			bWebsocket.DataHandler <- err
		}
	}
}

func (b *Binance) wsHandleData(respRaw []byte, bWebsocket *stream.Websocket) error {
	var multiStreamData map[string]interface{}
	err := json.Unmarshal(respRaw, &multiStreamData)
	if err != nil {
		fmt.Println("err wsHandleData")
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
				bWebsocket.DataHandler <- data
				return nil
			case "outboundAccountPosition":
				var data wsAccountPosition
				err := json.Unmarshal(respRaw, &data)
				if err != nil {
					return fmt.Errorf("%v - Could not convert to outboundAccountPosition structure %s",
						b.Name,
						err)
				}
				bWebsocket.DataHandler <- data
				return nil
			case "balanceUpdate":
				var data wsBalanceUpdate
				err := json.Unmarshal(respRaw, &data)
				if err != nil {
					return fmt.Errorf("%v - Could not convert to balanceUpdate structure %s",
						b.Name,
						err)
				}
				bWebsocket.DataHandler <- data
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
					bWebsocket.DataHandler <- order.ClassificationError{
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
					bWebsocket.DataHandler <- order.ClassificationError{
						Exchange: b.Name,
						OrderID:  orderID,
						Err:      err,
					}
				}
				orderSide, err := order.StringToOrderSide(data.Data.Side)
				if err != nil {
					bWebsocket.DataHandler <- order.ClassificationError{
						Exchange: b.Name,
						OrderID:  orderID,
						Err:      err,
					}
				}
				bWebsocket.DataHandler <- &order.Detail{
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
				bWebsocket.DataHandler <- data
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

				pairs, err := b.GetEnabledPairs(asset.Spot)
				if err != nil {
					return err
				}

				format, err := b.GetPairFormat(asset.Spot, true)
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

					return bWebsocket.Trade.Update(saveTradeData,
						trade.Data{
							CurrencyPair: pair,
							Timestamp:    t.TimeStamp,
							Price:        price,
							Amount:       amount,
							Exchange:     b.Name,
							AssetType:    asset.Spot,
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

					bWebsocket.DataHandler <- &ticker.Price{
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
						AssetType:    asset.Spot,
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

					bWebsocket.DataHandler <- stream.KlineData{
						Timestamp:  kline.EventTime,
						Pair:       pair,
						AssetType:  asset.Spot,
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
					// fmt.Printf("New depth update (sum spot): %v\n", len(depth.UpdateAsks)+len(depth.UpdateBids))
					init, err := b.UpdateLocalBuffer(&depth, asset.Spot)
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
					bWebsocket.DataHandler <- stream.UnhandledMessageWarning{
						Message: b.Name + stream.UnhandledMessage + string(respRaw),
					}
				}
			}
		}
	}
	return fmt.Errorf("unhandled stream data %s", string(respRaw))
}

func (b *Binance) wsHandleUFutureData(respRaw []byte, bWebsocket *stream.Websocket) error {
	var multiStreamData map[string]interface{}
	err := json.Unmarshal(respRaw, &multiStreamData)
	if err != nil {
		fmt.Println("err wsHandleData")
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
	// if newdata, ok := multiStreamData["data"].(map[string]interface{}); ok {
	// 	if e, ok := newdata["e"].(string); ok {
	// 		switch e {
	// 		case "outboundAccountInfo":
	// 			var data wsAccountInfo
	// 			err := json.Unmarshal(respRaw, &data)
	// 			if err != nil {
	// 				return fmt.Errorf("%v - Could not convert to outboundAccountInfo structure %s",
	// 					b.Name,
	// 					err)
	// 			}
	// 			bWebsocket.DataHandler <- data
	// 			return nil
	// 		case "outboundAccountPosition":
	// 			var data wsAccountPosition
	// 			err := json.Unmarshal(respRaw, &data)
	// 			if err != nil {
	// 				return fmt.Errorf("%v - Could not convert to outboundAccountPosition structure %s",
	// 					b.Name,
	// 					err)
	// 			}
	// 			bWebsocket.DataHandler <- data
	// 			return nil
	// 		case "balanceUpdate":
	// 			var data wsBalanceUpdate
	// 			err := json.Unmarshal(respRaw, &data)
	// 			if err != nil {
	// 				return fmt.Errorf("%v - Could not convert to balanceUpdate structure %s",
	// 					b.Name,
	// 					err)
	// 			}
	// 			bWebsocket.DataHandler <- data
	// 			return nil
	// 		case "executionReport":
	// 			var data wsOrderUpdate
	// 			err := json.Unmarshal(respRaw, &data)
	// 			if err != nil {
	// 				return fmt.Errorf("%v - Could not convert to executionReport structure %s",
	// 					b.Name,
	// 					err)
	// 			}
	// 			averagePrice := 0.0
	// 			if data.Data.CumulativeFilledQuantity != 0 {
	// 				averagePrice = data.Data.CumulativeQuoteTransactedQuantity / data.Data.CumulativeFilledQuantity
	// 			}
	// 			remainingAmount := data.Data.Quantity - data.Data.CumulativeFilledQuantity
	// 			pair, assetType, err := b.GetRequestFormattedPairAndAssetType(data.Data.Symbol)
	// 			if err != nil {
	// 				return err
	// 			}
	// 			var feeAsset currency.Code
	// 			if data.Data.CommissionAsset != "" {
	// 				feeAsset = currency.NewCode(data.Data.CommissionAsset)
	// 			}
	// 			orderID := strconv.FormatInt(data.Data.OrderID, 10)
	// 			orderStatus, err := stringToOrderStatus(data.Data.OrderStatus)
	// 			if err != nil {
	// 				bWebsocket.DataHandler <- order.ClassificationError{
	// 					Exchange: b.Name,
	// 					OrderID:  orderID,
	// 					Err:      err,
	// 				}
	// 			}
	// 			clientOrderID := data.Data.ClientOrderID
	// 			if orderStatus == order.Cancelled {
	// 				clientOrderID = data.Data.CancelledClientOrderID
	// 			}
	// 			orderType, err := order.StringToOrderType(data.Data.OrderType)
	// 			if err != nil {
	// 				bWebsocket.DataHandler <- order.ClassificationError{
	// 					Exchange: b.Name,
	// 					OrderID:  orderID,
	// 					Err:      err,
	// 				}
	// 			}
	// 			orderSide, err := order.StringToOrderSide(data.Data.Side)
	// 			if err != nil {
	// 				bWebsocket.DataHandler <- order.ClassificationError{
	// 					Exchange: b.Name,
	// 					OrderID:  orderID,
	// 					Err:      err,
	// 				}
	// 			}
	// 			bWebsocket.DataHandler <- &order.Detail{
	// 				Price:                data.Data.Price,
	// 				Amount:               data.Data.Quantity,
	// 				AverageExecutedPrice: averagePrice,
	// 				ExecutedAmount:       data.Data.CumulativeFilledQuantity,
	// 				RemainingAmount:      remainingAmount,
	// 				Cost:                 data.Data.CumulativeQuoteTransactedQuantity,
	// 				CostAsset:            pair.Quote,
	// 				Fee:                  data.Data.Commission,
	// 				FeeAsset:             feeAsset,
	// 				Exchange:             b.Name,
	// 				OrderID:              orderID,
	// 				ClientOrderID:        clientOrderID,
	// 				Type:                 orderType,
	// 				Side:                 orderSide,
	// 				Status:               orderStatus,
	// 				AssetType:            assetType,
	// 				Date:                 data.Data.OrderCreationTime,
	// 				LastUpdated:          data.Data.TransactionTime,
	// 				Pair:                 pair,
	// 			}
	// 			return nil
	// 		case "listStatus":
	// 			var data wsListStatus
	// 			err := json.Unmarshal(respRaw, &data)
	// 			if err != nil {
	// 				return fmt.Errorf("%v - Could not convert to listStatus structure %s",
	// 					b.Name,
	// 					err)
	// 			}
	// 			bWebsocket.DataHandler <- data
	// 			return nil
	// 		}
	// 	}
	// }
	if wsStream, ok := multiStreamData["stream"].(string); ok {
		streamType := strings.Split(wsStream, "@")
		if len(streamType) > 1 {
			assetUsed := asset.USDTMarginedFutures
			if data, ok := multiStreamData["data"]; ok {
				rawData, err := json.Marshal(data)
				if err != nil {
					return err
				}

				pairs, err := b.GetEnabledPairs(assetUsed)
				if err != nil {
					return err
				}

				format, err := b.GetPairFormat(assetUsed, true)
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

					return bWebsocket.Trade.Update(saveTradeData,
						trade.Data{
							CurrencyPair: pair,
							Timestamp:    t.TimeStamp,
							Price:        price,
							Amount:       amount,
							Exchange:     b.Name,
							AssetType:    assetUsed,
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

					bWebsocket.DataHandler <- &ticker.Price{
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
						AssetType:    assetUsed,
						Pair:         pair,
					}
					return nil
				case "kline_1s", "kline_1m", "kline_3m", "kline_5m", "kline_15m", "kline_30m", "kline_1h", "kline_2h", "kline_4h",
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

					bWebsocket.DataHandler <- stream.KlineData{
						Timestamp:  kline.EventTime,
						Pair:       pair,
						AssetType:  assetUsed,
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
					// fmt.Println("PU=", depth.LastUpdateIDPrevStream)
					init, err := b.UpdateLocalBuffer(&depth, assetUsed)
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
					bWebsocket.DataHandler <- stream.UnhandledMessageWarning{
						Message: b.Name + stream.UnhandledMessage + string(respRaw),
					}
				}
			}
		}
	}
	return fmt.Errorf("unhandled stream data %s", string(respRaw))
}

func stringToOrderStatus(status string) (order.Status, error) {
	switch status {
	case "NEW":
		return order.New, nil
	case "PARTIALLY_FILLED":
		return order.PartiallyFilled, nil
	case "FILLED":
		return order.Filled, nil
	case "CANCELED":
		return order.Cancelled, nil
	case "PENDING_CANCEL":
		return order.PendingCancel, nil
	case "REJECTED":
		return order.Rejected, nil
	case "EXPIRED":
		return order.Expired, nil
	default:
		return order.UnknownStatus, errors.New(status + " not recognised as order status")
	}
}

// SeedLocalCache seeds depth data
func (b *Binance) SeedLocalCache(ctx context.Context, p currency.Pair, assetType asset.Item) error {
	var ob *OrderBook
	var err error
	switch assetType {
	case asset.USDTMarginedFutures:
		ob, err = b.UFuturesOrderbook(ctx, p, 1000)
	default:
		ob, err = b.GetOrderBook(ctx,
			OrderBookDataRequestParams{
				Symbol: p,
				Limit:  1000,
			})
	}
	if err != nil {
		return err
	}
	return b.SeedLocalCacheWithBook(p, ob, assetType)
}

// SeedLocalCacheWithBook seeds the local orderbook cache
func (b *Binance) SeedLocalCacheWithBook(p currency.Pair, orderbookNew *OrderBook, assetType asset.Item) error {
	newOrderBook := orderbook.Base{
		Pair:            p,
		Asset:           assetType,
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
	wsURLType, _ := exchange.GetURLTypeFromAsset(assetType) // todo err
	bWebsocket, _ := b.Websockets[wsURLType]                // todo err
	return bWebsocket.Orderbook.LoadSnapshot(&newOrderBook)
}

// UpdateLocalBuffer updates and returns the most recent iteration of the orderbook
func (b *Binance) UpdateLocalBuffer(wsdp *WebsocketDepthStream, assetToUpdate asset.Item) (bool, error) {
	enabledPairs, err := b.GetEnabledPairs(assetToUpdate)
	if err != nil {
		return false, err
	}

	format, err := b.GetPairFormat(assetToUpdate, true)
	if err != nil {
		return false, err
	}

	currencyPair, err := currency.NewPairFromFormattedPairs(wsdp.Pair, enabledPairs, format)
	if err != nil {
		return false, err
	}

	// fmt.Println("UpdateLocalBuffer with asset:", assetToUpdate)
	wsURLType, err := exchange.GetURLTypeFromAsset(assetToUpdate)
	if err != nil {
		return false, err
	}
	obm, exist := b.obm[wsURLType]
	if !exist {
		return false, errors.New("orderbook doesn't exist, wsURL:" + wsURLType.String())
	}
	err = obm.stageWsUpdate(wsdp, currencyPair, assetToUpdate)
	if err != nil {
		init, err2 := obm.checkIsInitialSync(currencyPair, assetToUpdate)
		fmt.Println("stageWsUpdate failed:", assetToUpdate, "Initial sync:", init)
		if err2 != nil {
			return false, err2
		}
		return init, err
	}

	err = b.applyBufferUpdate(currencyPair, assetToUpdate)
	if err != nil {
		b.flushAndCleanup(currencyPair, assetToUpdate)
	}

	return false, err
}

// GenerateSubscriptions generates the default subscription set
func (b *Binance) WsGenerateSubscriptionsFactory(wsURLType exchange.URL) func() ([]stream.ChannelSubscription, error) {
	return func() ([]stream.ChannelSubscription, error) {
		var channels = []string{"@ticker", "@trade", "@kline_1m", "@depth@100ms"}
		var subscriptions []stream.ChannelSubscription
		// assets := b.GetAssetTypes(true)
		assets, _ := exchange.GetAssetsFromURLType(wsURLType) // todo err
		for x := range assets {
			// if assets[x] == asset.Spot {
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
			// }
		}
		return subscriptions, nil
	}
}

// Subscribe subscribes to a set of channels
func (b *Binance) WsSubscribeFactory(wsURLType exchange.URL) func([]stream.ChannelSubscription) error {
	return func(channelsToSubscribe []stream.ChannelSubscription) error {
		payload := WsPayload{
			Method: "SUBSCRIBE",
		}
		bWebsocket := b.Websockets[wsURLType]
		for i := range channelsToSubscribe {
			payload.Params = append(payload.Params, channelsToSubscribe[i].Channel)
			if i%50 == 0 && i != 0 {
				err := bWebsocket.Conn.SendJSONMessage(payload)
				if err != nil {
					return err
				}
				payload.Params = []string{}
			}
		}
		if len(payload.Params) > 0 {
			err := bWebsocket.Conn.SendJSONMessage(payload)
			if err != nil {
				return err
			}
		}
		bWebsocket.AddSuccessfulSubscriptions(channelsToSubscribe...)
		return nil
	}
}

// Unsubscribe unsubscribes from a set of channels
func (b *Binance) WsUnsubscribeFactory(wsURLType exchange.URL) func([]stream.ChannelSubscription) error {
	return func(channelsToUnsubscribe []stream.ChannelSubscription) error {
		payload := WsPayload{
			Method: "UNSUBSCRIBE",
		}
		bWebsocket := b.Websockets[wsURLType]
		for i := range channelsToUnsubscribe {
			payload.Params = append(payload.Params, channelsToUnsubscribe[i].Channel)
			if i%50 == 0 && i != 0 {
				err := bWebsocket.Conn.SendJSONMessage(payload)
				if err != nil {
					return err
				}
				payload.Params = []string{}
			}
		}
		if len(payload.Params) > 0 {
			err := bWebsocket.Conn.SendJSONMessage(payload)
			if err != nil {
				return err
			}
		}
		bWebsocket.RemoveSuccessfulUnsubscriptions(channelsToUnsubscribe...)
		return nil
	}
}

// ProcessUpdate processes the websocket orderbook update
func (b *Binance) ProcessUpdate(cp currency.Pair, a asset.Item, ws *WebsocketDepthStream) error {
	fmt.Println("Calling ProcessUpdate asset:", a)
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

	wsURLType, err := exchange.GetURLTypeFromAsset(a)
	if err != nil {
		return err
	}
	bWebsocket, exist := b.Websockets[wsURLType] // todo
	if !exist {
		return errors.New("does not exist ws with URL:" + wsURLType.String())
	}
	return bWebsocket.Orderbook.Update(&orderbook.Update{
		Bids:       updateBid,
		Asks:       updateAsk,
		Pair:       cp,
		UpdateID:   ws.LastUpdateID,
		UpdateTime: ws.Timestamp,
		Asset:      a,
	})
}

// applyBufferUpdate applies the buffer to the orderbook or initiates a new
// orderbook sync by the REST protocol which is off handed to go routine.
func (b *Binance) applyBufferUpdate(pair currency.Pair, assetToUpdate asset.Item) error {
	// fmt.Println("applyBufferUpdate", assetToUpdate, len(b.obm))
	wsURLType, err := exchange.GetURLTypeFromAsset(assetToUpdate)
	if err != nil {
		return err
	}
	obm, exist := b.obm[wsURLType]
	if !exist {
		return errors.New("orderbook doesn't exist url:" + wsURLType.String())
	}
	fetching, needsFetching, err := obm.handleFetchingBook(pair, assetToUpdate)
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
		return obm.fetchBookViaREST(pair, assetToUpdate)
	}
	bWebsocket := b.Websockets[wsURLType]
	recent, err := bWebsocket.Orderbook.GetOrderbook(pair, assetToUpdate)
	if err != nil {
		fmt.Println("No recent orderbook:", wsURLType)
		log.Errorf(
			log.WebsocketMgr,
			"%s error fetching recent orderbook when applying updates: %s\n",
			b.Name,
			err)
	}

	if recent != nil {
		err = obm.checkAndProcessUpdate(b.ProcessUpdate, pair, recent, assetToUpdate)
		if err != nil {
			log.Errorf(
				log.WebsocketMgr,
				"%s error processing update - initiating new orderbook sync via REST: %s\n",
				b.Name,
				err)
			err = obm.setNeedsFetchingBook(pair, assetToUpdate)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// setNeedsFetchingBook completes the book fetching initiation.
func (o *orderbookManager) setNeedsFetchingBook(pair currency.Pair, asset asset.Item) error {
	o.Lock()
	defer o.Unlock()
	state, ok := o.state[pair.Base][pair.Quote][asset]
	if !ok {
		return fmt.Errorf("could not match pair %s and asset type %s in hash table",
			pair,
			asset)
	}
	state.needsFetchingBook = true
	return nil
}

// SynchroniseWebsocketOrderbook synchronises full orderbook for currency pair
// asset
func (b *Binance) SynchroniseWebsocketOrderbook(wsURLType exchange.URL) {
	bWebsocket, err := b.Websockets[wsURLType]
	obm, err := b.obm[wsURLType]
	_ = err // todo
	bWebsocket.Wg.Add(1)
	go func() {
		defer bWebsocket.Wg.Done()
		for {
			select {
			case <-bWebsocket.ShutdownC:
				for {
					select {
					case <-obm.jobs:
					default:
						return
					}
				}
			case j := <-obm.jobs:
				err := b.processJob(j, wsURLType)
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
func (b *Binance) processJob(j job, wsURLType exchange.URL) error {
	err := b.SeedLocalCache(context.TODO(), j.Pair, j.Asset)
	if err != nil {
		return fmt.Errorf("%s %s seeding local cache for orderbook error: %v", j, wsURLType, err)
	}

	obm, _ := b.obm[wsURLType] // todo err
	err = obm.stopFetchingBook(j.Pair, j.Asset)
	if err != nil {
		return err
	}

	// Immediately apply the buffer updates so we don't wait for a
	// new update to initiate this.
	err = b.applyBufferUpdate(j.Pair, j.Asset)
	if err != nil {
		b.flushAndCleanup(j.Pair, j.Asset)
		return err
	}
	return nil
}

// flushAndCleanup flushes orderbook and clean local cache
func (b *Binance) flushAndCleanup(pair currency.Pair, asset asset.Item) {
	url, err := exchange.GetURLTypeFromAsset(asset)
	_ = err // todo
	bWebsocket := b.Websockets[url]
	errClean := bWebsocket.Orderbook.FlushOrderbook(pair, asset)
	if errClean != nil {
		log.Errorf(log.WebsocketMgr,
			"%s flushing websocket error: %v",
			b.Name,
			errClean)
	}
	obm := b.obm[url]
	errClean = obm.cleanup(pair, asset)
	if errClean != nil {
		log.Errorf(log.WebsocketMgr, "%s cleanup websocket error: %v",
			b.Name,
			errClean)
	}
}

// stageWsUpdate stages websocket update to roll through updates that need to
// be applied to a fetched orderbook via REST.
func (o *orderbookManager) stageWsUpdate(u *WebsocketDepthStream, pair currency.Pair, a asset.Item) error {
	// fmt.Println("Calling stageWsUpdate:", a, u.Timestamp)
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

	if state.lastUpdateID != 0 {
		if a == asset.USDTMarginedFutures {
			if u.LastUpdateIDPrevStream != state.lastUpdateID {
				// While listening to the stream, each new event's U should have
				// pu equal to the previous event's u.
				fmt.Println("pu=", u.LastUpdateIDPrevStream, "lastu=", state.lastUpdateID)
				return fmt.Errorf("PU websocket orderbook synchronisation failure for pair %s and asset %s", pair, a)
			}
		} else if u.FirstUpdateID != state.lastUpdateID+1 {
			// While listening to the stream, each new event's U should be
			// equal to the previous event's u+1.
			return fmt.Errorf("websocket orderbook synchronisation failure for pair %s and asset %s", pair, a)
		}
	}
	fmt.Println("Setting lastUpdateID:", u.LastUpdateID)
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

// handleFetchingBook checks if a full book is being fetched or needs to be
// fetched
func (o *orderbookManager) handleFetchingBook(p currency.Pair, a asset.Item) (fetching, needsFetching bool, err error) {
	o.Lock()
	defer o.Unlock()
	state, ok := o.state[p.Base][p.Quote][a]
	if !ok {
		return false,
			false,
			fmt.Errorf("check is fetching book cannot match currency pair %s asset type %s", p, a)
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

// stopFetchingBook completes the book fetching.
func (o *orderbookManager) stopFetchingBook(pair currency.Pair, assetType asset.Item) error {
	o.Lock()
	defer o.Unlock()
	state, ok := o.state[pair.Base][pair.Quote][assetType]
	if !ok {
		return fmt.Errorf("could not match pair %s and asset type %s in hash table", pair, assetType)
	}
	if !state.fetchingBook {
		return fmt.Errorf("fetching book already set to false for %s %s", pair, assetType)
	}
	state.fetchingBook = false
	return nil
}

// completeInitialSync sets if an asset type has completed its initial sync
func (o *orderbookManager) completeInitialSync(pair currency.Pair, a asset.Item) error {
	o.Lock()
	defer o.Unlock()
	state, ok := o.state[pair.Base][pair.Quote][a]
	if !ok {
		return fmt.Errorf("complete initial sync cannot match currency pair %s asset type %s", pair, a)
	}
	if !state.initialSync {
		return fmt.Errorf("initital sync already set to false for %s %s", pair, a)
	}
	state.initialSync = false
	return nil
}

// checkIsInitialSync checks status if the book is Initial Sync being via the REST
// protocol.
func (o *orderbookManager) checkIsInitialSync(pair currency.Pair, a asset.Item) (bool, error) {
	o.Lock()
	defer o.Unlock()
	state, ok := o.state[pair.Base][pair.Quote][a]
	if !ok {
		return false,
			fmt.Errorf("checkIsInitialSync of orderbook cannot match currency pair %s asset type %s",
				pair,
				a)
	}
	return state.initialSync, nil
}

// fetchBookViaREST pushes a job of fetching the orderbook via the REST protocol
// to get an initial full book that we can apply our buffered updates too.
func (o *orderbookManager) fetchBookViaREST(p currency.Pair, a asset.Item) error {
	o.Lock()
	defer o.Unlock()

	state, ok := o.state[p.Base][p.Quote][a]
	if !ok {
		return fmt.Errorf("fetch book via rest cannot match currency pair %s asset type %s", p, a)
	}

	state.initialSync = true
	state.fetchingBook = true

	select {
	case o.jobs <- job{p, a}:
		return nil
	default:
		return fmt.Errorf("%s %s book synchronisation channel blocked up", p, a)
	}
}

func (o *orderbookManager) checkAndProcessUpdate(processor func(currency.Pair, asset.Item, *WebsocketDepthStream) error, pair currency.Pair, recent *orderbook.Base, assetToUpdate asset.Item) error {
	o.Lock()
	defer o.Unlock()
	state, ok := o.state[pair.Base][pair.Quote][assetToUpdate]
	if !ok {
		return fmt.Errorf("could not match pair [%s] asset type [%s] in hash table to process websocket orderbook update",
			pair, assetToUpdate)
	}

	// This will continuously remove updates from the buffered channel and
	// apply them to the current orderbook.
buffer:
	for {
		select {
		case d := <-state.buffer:
			// fmt.Println("2.checkAndProcessUpdate getting depth from buffer", assetToUpdate)
			var process bool
			var err error
			if assetToUpdate == asset.USDTMarginedFutures {
				// fmt.Println("Calling validate ufuture")
				process, err = state.validateUFutures(d, recent)
			} else {
				// fmt.Println("Calling validate OTHER")
				process, err = state.validate(d, recent)
			}
			if err != nil {
				fmt.Println("depth update NOT VALIDATED", err)
				return err
			}
			if process {
				// fmt.Println("3.checkAndProcessUpdate processing depth", assetToUpdate)
				err := processor(pair, assetToUpdate, d)
				if err != nil {
					return fmt.Errorf("%s %s processing update error: %w", pair, assetToUpdate, err)
				}
			}
		default:
			break buffer
		}
	}
	return nil
}

// validate checks for correct update alignment
func (u *update) validate(updt *WebsocketDepthStream, recent *orderbook.Base) (bool, error) {
	fmt.Println("validate pu:", updt.LastUpdateID, "u:", recent.LastUpdateID)
	if updt.LastUpdateID <= recent.LastUpdateID {
		// Drop any event where u is <= lastUpdateId in the snapshot.
		return false, nil
	}

	// todo: apply different validation in case of futures
	id := recent.LastUpdateID + 1
	if u.initialSync {
		// The first processed event should have U <= lastUpdateId+1 AND
		// u >= lastUpdateId+1.
		if updt.FirstUpdateID > id || updt.LastUpdateID < id {
			return false, fmt.Errorf("initial websocket orderbook sync failure for pair %s and asset %s", recent.Pair, recent.Asset)
		}
		u.initialSync = false
	}
	return true, nil
}

// validate checks for correct update alignment
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

// cleanup cleans up buffer and reset fetch and init
func (o *orderbookManager) cleanup(pair currency.Pair, a asset.Item) error {
	o.Lock()
	state, ok := o.state[pair.Base][pair.Quote][a]
	if !ok {
		o.Unlock()
		return fmt.Errorf("cleanup cannot match %s %s to hash table", pair, a)
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
	_ = o.stopFetchingBook(pair, a)
	_ = o.completeInitialSync(pair, a)
	_ = o.stopNeedsFetchingBook(pair, a)
	return nil
}

// stopNeedsFetchingBook completes the book fetching initiation.
func (o *orderbookManager) stopNeedsFetchingBook(pair currency.Pair, assetType asset.Item) error {
	o.Lock()
	defer o.Unlock()
	state, ok := o.state[pair.Base][pair.Quote][assetType]
	if !ok {
		return fmt.Errorf("could not match pair %s and asset type %s in hash table", pair, assetType)
	}
	if !state.needsFetchingBook {
		return fmt.Errorf("needs fetching book already set to false for %s %s", pair, assetType)
	}
	state.needsFetchingBook = false
	return nil
}
