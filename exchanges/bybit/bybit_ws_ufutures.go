package bybit

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
	"github.com/thrasher-corp/gocryptotrader/common/crypto"
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
	wsUFuturesPublicTopicV2  = "realtime_public"
	wsUFuturesPrivateTopicV2 = "realtime_private"

	wsUSDTKline         = "candle"
	wsTickerUFutures    = "instrument_info.100ms"
	wsTradesUFutures    = "trade" // Same as spot
	wsOrderbookUFutures = "orderBook_200.100ms"
	wsKlinesUFutures    = "candle.1"
)

func (by *Bybit) SetupFuture(exch *config.Exchange) error {
	if !exch.Enabled {
		by.SetEnabled(false)
		return nil
	}

	if by.Config == nil {
		err := by.SetupDefaults(exch)
		if err != nil {
			return err
		}
	}

	// wsRunningEndpoint, err := by.API.Endpoints.GetURL(exchange.WebsocketSpot)
	// if err != nil {
	// 	return err
	// }

	err := by.WebsocketUFuture.Setup(
		&stream.WebsocketSetup{
			ExchangeConfig:        exch,
			DefaultURL:            bybitWSBaseURL + wsUFuturesPublicTopicV2,
			RunningURL:            bybitWSBaseURL + wsUFuturesPublicTopicV2,
			RunningURLAuth:        bybitWSBaseURL + wsUFuturesPrivateTopicV2,
			Connector:             by.WsUSDTConnect,
			Subscriber:            by.SubscribeUSDT,
			Unsubscriber:          by.UnsubscribeUSDT,
			GenerateSubscriptions: by.GenerateDefaultSubscriptionsFactory(asset.USDTMarginedFutures),
			Features:              &by.Features.Supports.WebsocketCapabilities,
			OrderbookBufferConfig: buffer.Config{
				SortBuffer:            true,
				SortBufferByUpdateIDs: true,
			},
			TradeFeed: by.Features.Enabled.TradeFeed,
		})
	if err != nil {
		return err
	}

	err = by.WebsocketUFuture.SetupNewConnection(stream.ConnectionSetup{
		URL:                  by.WebsocketUFuture.GetWebsocketURL(),
		ResponseCheckTimeout: exch.WebsocketResponseCheckTimeout,
		ResponseMaxLimit:     exch.WebsocketResponseMaxLimit,
	})
	if err != nil {
		return err
	}

	return by.WebsocketUFuture.SetupNewConnection(stream.ConnectionSetup{
		URL:                  bybitWSBaseURL + wsUFuturesPrivateTopicV2,
		ResponseCheckTimeout: exch.WebsocketResponseCheckTimeout,
		ResponseMaxLimit:     exch.WebsocketResponseMaxLimit,
		Authenticated:        true,
	})
}

// WsUSDTConnect connects to a USDT websocket feed
func (by *Bybit) WsUSDTConnect() error {
	if !by.WebsocketUFuture.IsEnabled() || !by.IsEnabled() {
		return errors.New(stream.WebsocketNotEnabled)
	}
	var dialer websocket.Dialer
	err := by.WebsocketUFuture.Conn.Dial(&dialer, http.Header{})
	if err != nil {
		return err
	}

	pingMsg, err := json.Marshal(pingRequest)
	if err != nil {
		return err
	}
	by.WebsocketUFuture.Conn.SetupPingHandler(stream.PingHandler{
		Message:     pingMsg,
		MessageType: websocket.PingMessage,
		Delay:       bybitWebsocketTimer,
	})
	if by.Verbose {
		log.Debugf(log.ExchangeSys, "%s Connected to WebsocketUFuture.\n", by.Name)
	}

	go by.wsUSDTReadData()
	if by.IsWebsocketAuthenticationSupported() {
		err = by.WsUSDTAuth(context.TODO())
		if err != nil {
			by.WebsocketUFuture.DataHandler <- err
			by.WebsocketUFuture.SetCanUseAuthenticatedEndpoints(false)
		}
	}

	return nil
}

// WsUSDTAuth sends an authentication message to receive auth data
func (by *Bybit) WsUSDTAuth(ctx context.Context) error {
	creds, err := by.GetCredentials(ctx)
	if err != nil {
		return err
	}

	intNonce := (time.Now().Unix() + 1) * 1000
	strNonce := strconv.FormatInt(intNonce, 10)
	hmac, err := crypto.GetHMAC(
		crypto.HashSHA256,
		[]byte("GET/realtime"+strNonce),
		[]byte(creds.Secret),
	)
	if err != nil {
		return err
	}
	sign := crypto.HexEncodeToString(hmac)
	req := Authenticate{
		Operation: "auth",
		Args:      []interface{}{creds.Key, intNonce, sign},
	}
	return by.WebsocketUFuture.Conn.SendJSONMessage(req)
}

// SubscribeUSDT sends a websocket message to receive data from the channel
func (by *Bybit) SubscribeUSDT(channelsToSubscribe []stream.ChannelSubscription) error {
	var errs common.Errors
	fmt.Println("subscribe usdt:", channelsToSubscribe)
	for i := range channelsToSubscribe {
		var sub WsFuturesReq
		sub.Topic = subscribe

		sub.Args = append(sub.Args, formatArgs(channelsToSubscribe[i].Channel, channelsToSubscribe[i].Params))
		err := by.WebsocketUFuture.Conn.SendJSONMessage(sub)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		by.WebsocketUFuture.AddSuccessfulSubscriptions(channelsToSubscribe[i])
	}
	if errs != nil {
		return errs
	}
	return nil
}

func (by *Bybit) GenerateDefaultSubscriptionsUFutures() ([]stream.ChannelSubscription, error) {
	var subscriptions []stream.ChannelSubscription
	var channels = []string{wsTickerUFutures, wsTradesUFutures, wsOrderbookUFutures, wsKlinesUFutures}
	pairs, err := by.GetEnabledPairs(asset.USDTMarginedFutures)
	if err != nil {
		return nil, err
	}
	for _, p := range pairs {
		pStr := p.Format(currency.PairFormat{Uppercase: true, Delimiter: ""}).String()
		for _, c := range channels {
			subscriptions = append(subscriptions,
				stream.ChannelSubscription{
					Channel:  c,
					Currency: p,
					Asset:    asset.USDTMarginedFutures,
					Params: map[string]interface{}{
						"pair": pStr,
					},
				})
		}
	}
	return subscriptions, nil
}

// UnsubscribeUSDT sends a websocket message to stop receiving data from the channel
func (by *Bybit) UnsubscribeUSDT(channelsToUnsubscribe []stream.ChannelSubscription) error {
	var errs common.Errors

	for i := range channelsToUnsubscribe {
		var unSub WsFuturesReq
		unSub.Topic = unsubscribe

		formattedPair, err := by.FormatExchangeCurrency(channelsToUnsubscribe[i].Currency, asset.USDTMarginedFutures)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		unSub.Args = append(unSub.Args, channelsToUnsubscribe[i].Channel+dot+formattedPair.String())
		err = by.WebsocketUFuture.Conn.SendJSONMessage(unSub)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		by.WebsocketUFuture.RemoveSuccessfulUnsubscriptions(channelsToUnsubscribe[i])
	}
	if errs != nil {
		return errs
	}
	return nil
}

// wsUSDTReadData gets and passes on websocket messages for processing
func (by *Bybit) wsUSDTReadData() {
	by.WebsocketUFuture.Wg.Add(1)
	defer by.WebsocketUFuture.Wg.Done()

	for {
		select {
		case <-by.WebsocketUFuture.ShutdownC:
			return
		default:
			resp := by.WebsocketUFuture.Conn.ReadMessage()
			if resp.Raw == nil {
				return
			}

			err := by.wsUSDTHandleData(resp.Raw)
			if err != nil {
				by.WebsocketUFuture.DataHandler <- err
			}
		}
	}
}

func (by *Bybit) wsUSDTHandleData(respRaw []byte) error {
	var multiStreamData map[string]interface{}
	err := json.Unmarshal(respRaw, &multiStreamData)
	if err != nil {
		return err
	}
	s, ok := multiStreamData["success"].(bool)
	if ok {
		if !s {
			log.Errorf(log.ExchangeSys, "%s WebsocketUFuture connection was unsuccessfull: %v\n", by.Name, multiStreamData)
		}
		return nil
	}

	t, ok := multiStreamData["topic"].(string)
	if !ok {
		log.Errorf(log.ExchangeSys, "%s Received unhandle message on websocket: %v\n", by.Name, multiStreamData)
		return nil
	}

	topics := strings.Split(t, dot)
	if len(topics) < 1 {
		return errors.New(by.Name + " - topic could not be extracted from response")
	}

	switch topics[0] {
	case wsOrder25, wsOrder200:
		// fmt.Println("New handle data: ", multiStreamData)
		if wsType, ok := multiStreamData["type"].(string); ok {
			switch wsType {
			case wsOperationSnapshot:
				// fmt.Println("New handle data: ", multiStreamData, "\n")
				var response WsUSDTOrderbook
				err = json.Unmarshal(respRaw, &response)
				if err != nil {
					return err
				}

				var p currency.Pair
				p, err = by.extractCurrencyPair(response.Data.OBData[0].Symbol, asset.USDTMarginedFutures)
				if err != nil {
					return err
				}

				err = by.processOrderbookUFutures(response.Data.OBData,
					response.Type,
					p,
					asset.USDTMarginedFutures)
				if err != nil {
					return err
				}

			case wsOperationDelta:
				var response WsCoinDeltaOrderbook
				err = json.Unmarshal(respRaw, &response)
				if err != nil {
					return err
				}

				if len(response.OBData.Delete) > 0 {
					var p currency.Pair
					p, err = by.extractCurrencyPair(response.OBData.Delete[0].Symbol, asset.USDTMarginedFutures)
					if err != nil {
						return err
					}

					err = by.processOrderbookUFutures(response.OBData.Delete,
						wsOrderbookActionDelete,
						p,
						asset.USDTMarginedFutures)
					if err != nil {
						return err
					}
				}

				if len(response.OBData.Update) > 0 {
					var p currency.Pair
					p, err = by.extractCurrencyPair(response.OBData.Update[0].Symbol, asset.USDTMarginedFutures)
					if err != nil {
						return err
					}

					err = by.processOrderbookUFutures(response.OBData.Update,
						wsOrderbookActionUpdate,
						p,
						asset.USDTMarginedFutures)
					if err != nil {
						return err
					}
				}

				if len(response.OBData.Insert) > 0 {
					var p currency.Pair
					p, err = by.extractCurrencyPair(response.OBData.Insert[0].Symbol, asset.USDTMarginedFutures)
					if err != nil {
						return err
					}

					err = by.processOrderbookUFutures(response.OBData.Insert,
						wsOrderbookActionInsert,
						p,
						asset.USDTMarginedFutures)
					if err != nil {
						return err
					}
				}
			default:
				by.WebsocketUFuture.DataHandler <- stream.UnhandledMessageWarning{Message: by.Name + stream.UnhandledMessage + "unsupported orderbook operation"}
			}
		}

	case wsTrades:
		if !by.IsSaveTradeDataEnabled() {
			return nil
		}
		var response WsFuturesTrade
		err = json.Unmarshal(respRaw, &response)
		if err != nil {
			return err
		}
		trades := make([]trade.Data, len(response.TradeData))
		for i := range response.TradeData {
			var p currency.Pair
			p, err = by.extractCurrencyPair(response.TradeData[0].Symbol, asset.USDTMarginedFutures)
			if err != nil {
				return err
			}

			var oSide order.Side
			oSide, err = order.StringToOrderSide(response.TradeData[i].Side)
			if err != nil {
				by.WebsocketUFuture.DataHandler <- order.ClassificationError{
					Exchange: by.Name,
					Err:      err,
				}
			}

			trades[i] = trade.Data{
				TID:          response.TradeData[i].ID,
				Exchange:     by.Name,
				CurrencyPair: p,
				AssetType:    asset.USDTMarginedFutures,
				Side:         oSide,
				Price:        response.TradeData[i].Price,
				Amount:       response.TradeData[i].Size,
				Timestamp:    response.TradeData[i].Time,
			}
		}
		return by.AddTradesToBuffer(trades...)

	case wsUSDTKline:
		var response WsFuturesKline
		err = json.Unmarshal(respRaw, &response)
		if err != nil {
			return err
		}

		var p currency.Pair
		p, err = by.extractCurrencyPair(topics[len(topics)-1], asset.USDTMarginedFutures)
		if err != nil {
			return err
		}

		for i := range response.KlineData {
			by.WebsocketUFuture.DataHandler <- stream.KlineData{
				Pair:       p,
				AssetType:  asset.USDTMarginedFutures,
				Exchange:   by.Name,
				OpenPrice:  response.KlineData[i].Open,
				HighPrice:  response.KlineData[i].High,
				LowPrice:   response.KlineData[i].Low,
				ClosePrice: response.KlineData[i].Close,
				Volume:     response.KlineData[i].Volume,
				Timestamp:  response.KlineData[i].Timestamp.Time(),
			}
		}

	case wsInstrument:
		if wsType, ok := multiStreamData["type"].(string); ok {
			switch wsType {
			case wsOperationSnapshot:
				var response WsTicker
				err = json.Unmarshal(respRaw, &response)
				if err != nil {
					return err
				}

				var p currency.Pair
				p, err = by.extractCurrencyPair(response.Ticker.Symbol, asset.USDTMarginedFutures)
				if err != nil {
					return err
				}

				by.WebsocketUFuture.DataHandler <- &ticker.Price{
					ExchangeName: by.Name,
					Last:         response.Ticker.LastPrice,
					High:         response.Ticker.HighPrice24h,
					Low:          response.Ticker.LowPrice24h,
					Bid:          response.Ticker.BidPrice,
					Ask:          response.Ticker.AskPrice,
					Volume:       response.Ticker.Volume24h,
					Close:        response.Ticker.PrevPrice24h,
					LastUpdated:  response.Ticker.UpdateAt,
					AssetType:    asset.USDTMarginedFutures,
					Pair:         p,
				}

			case wsOperationDelta:
				var response WsDeltaTicker
				err = json.Unmarshal(respRaw, &response)
				if err != nil {
					return err
				}

				if len(response.Data.Delete) > 0 {
					for x := range response.Data.Delete {
						var p currency.Pair
						p, err = by.extractCurrencyPair(response.Data.Delete[x].Symbol, asset.USDTMarginedFutures)
						if err != nil {
							return err
						}

						by.WebsocketUFuture.DataHandler <- &ticker.Price{
							ExchangeName: by.Name,
							Last:         response.Data.Delete[x].LastPrice,
							High:         response.Data.Delete[x].HighPrice24h,
							Low:          response.Data.Delete[x].LowPrice24h,
							Bid:          response.Data.Delete[x].BidPrice,
							Ask:          response.Data.Delete[x].AskPrice,
							Volume:       response.Data.Delete[x].Volume24h,
							Close:        response.Data.Delete[x].PrevPrice24h,
							LastUpdated:  response.Data.Delete[x].UpdateAt,
							AssetType:    asset.USDTMarginedFutures,
							Pair:         p,
						}
					}
				}

				if len(response.Data.Update) > 0 {
					for x := range response.Data.Update {
						var p currency.Pair
						p, err = by.extractCurrencyPair(response.Data.Update[x].Symbol, asset.USDTMarginedFutures)
						if err != nil {
							return err
						}

						by.WebsocketUFuture.DataHandler <- &ticker.Price{
							ExchangeName: by.Name,
							Last:         response.Data.Update[x].LastPrice,
							High:         response.Data.Update[x].HighPrice24h,
							Low:          response.Data.Update[x].LowPrice24h,
							Bid:          response.Data.Update[x].BidPrice,
							Ask:          response.Data.Update[x].AskPrice,
							Volume:       response.Data.Update[x].Volume24h,
							Close:        response.Data.Update[x].PrevPrice24h,
							LastUpdated:  response.Data.Update[x].UpdateAt,
							AssetType:    asset.USDTMarginedFutures,
							Pair:         p,
						}
					}
				}

				if len(response.Data.Insert) > 0 {
					for x := range response.Data.Insert {
						var p currency.Pair
						p, err = by.extractCurrencyPair(response.Data.Insert[x].Symbol, asset.USDTMarginedFutures)
						if err != nil {
							return err
						}

						by.WebsocketUFuture.DataHandler <- &ticker.Price{
							ExchangeName: by.Name,
							Last:         response.Data.Insert[x].LastPrice,
							High:         response.Data.Insert[x].HighPrice24h,
							Low:          response.Data.Insert[x].LowPrice24h,
							Bid:          response.Data.Insert[x].BidPrice,
							Ask:          response.Data.Insert[x].AskPrice,
							Volume:       response.Data.Insert[x].Volume24h,
							Close:        response.Data.Insert[x].PrevPrice24h,
							LastUpdated:  response.Data.Insert[x].UpdateAt,
							AssetType:    asset.USDTMarginedFutures,
							Pair:         p,
						}
					}
				}

			default:
				by.WebsocketUFuture.DataHandler <- stream.UnhandledMessageWarning{Message: by.Name + stream.UnhandledMessage + "unsupported ticker operation"}
			}
		}

	case wsLiquidation:
		var response WsFuturesLiquidation
		err = json.Unmarshal(respRaw, &response)
		if err != nil {
			return err
		}
		by.WebsocketUFuture.DataHandler <- response.Data

	case wsPosition:
		var response WsFuturesPosition
		err = json.Unmarshal(respRaw, &response)
		if err != nil {
			return err
		}
		by.WebsocketUFuture.DataHandler <- response.Data

	case wsExecution:
		var response WsFuturesExecution
		err = json.Unmarshal(respRaw, &response)
		if err != nil {
			return err
		}

		for i := range response.Data {
			var p currency.Pair
			p, err = by.extractCurrencyPair(response.Data[i].Symbol, asset.USDTMarginedFutures)
			if err != nil {
				return err
			}

			var oSide order.Side
			oSide, err = order.StringToOrderSide(response.Data[i].Side)
			if err != nil {
				by.WebsocketUFuture.DataHandler <- order.ClassificationError{
					Exchange: by.Name,
					OrderID:  response.Data[i].OrderID,
					Err:      err,
				}
			}

			var oStatus order.Status
			oStatus, err = order.StringToOrderStatus(response.Data[i].ExecutionType)
			if err != nil {
				by.WebsocketUFuture.DataHandler <- order.ClassificationError{
					Exchange: by.Name,
					OrderID:  response.Data[i].OrderID,
					Err:      err,
				}
			}

			by.WebsocketUFuture.DataHandler <- &order.Detail{
				Exchange:  by.Name,
				OrderID:   response.Data[i].OrderID,
				AssetType: asset.USDTMarginedFutures,
				Pair:      p,
				Side:      oSide,
				Status:    oStatus,
				Price:     response.Data[i].Price,
				Amount:    response.Data[i].OrderQty,
				Trades: []order.TradeHistory{
					{
						Price:     response.Data[i].Price,
						Amount:    response.Data[i].OrderQty,
						Exchange:  by.Name,
						Side:      oSide,
						Timestamp: response.Data[i].Time,
					},
				},
			}
		}

	case wsOrder:
		var response WsOrder
		err = json.Unmarshal(respRaw, &response)
		if err != nil {
			return err
		}
		for x := range response.Data {
			var p currency.Pair
			p, err = by.extractCurrencyPair(response.Data[x].Symbol, asset.USDTMarginedFutures)
			if err != nil {
				return err
			}
			var oSide order.Side
			oSide, err = order.StringToOrderSide(response.Data[x].Side)
			if err != nil {
				by.WebsocketUFuture.DataHandler <- order.ClassificationError{
					Exchange: by.Name,
					OrderID:  response.Data[x].OrderID,
					Err:      err,
				}
			}
			var oType order.Type
			oType, err = order.StringToOrderType(response.Data[x].OrderType)
			if err != nil {
				by.WebsocketUFuture.DataHandler <- order.ClassificationError{
					Exchange: by.Name,
					OrderID:  response.Data[x].OrderID,
					Err:      err,
				}
			}
			var oStatus order.Status
			oStatus, err = order.StringToOrderStatus(response.Data[x].OrderStatus)
			if err != nil {
				by.WebsocketUFuture.DataHandler <- order.ClassificationError{
					Exchange: by.Name,
					OrderID:  response.Data[x].OrderID,
					Err:      err,
				}
			}
			by.WebsocketUFuture.DataHandler <- &order.Detail{
				Price:     response.Data[x].Price,
				Amount:    response.Data[x].OrderQty,
				Exchange:  by.Name,
				OrderID:   response.Data[x].OrderID,
				Type:      oType,
				Side:      oSide,
				Status:    oStatus,
				AssetType: asset.USDTMarginedFutures,
				Date:      response.Data[x].CreateTime,
				Pair:      p,
				Trades: []order.TradeHistory{
					{
						Price:     response.Data[x].Price,
						Amount:    response.Data[x].OrderQty,
						Exchange:  by.Name,
						Side:      oSide,
						Timestamp: response.Data[x].Time,
					},
				},
			}
		}

	case wsStopOrder:
		var response WsUSDTFuturesStopOrder
		err = json.Unmarshal(respRaw, &response)
		if err != nil {
			return err
		}
		for x := range response.Data {
			var p currency.Pair
			p, err = by.extractCurrencyPair(response.Data[x].Symbol, asset.USDTMarginedFutures)
			if err != nil {
				return err
			}
			var oSide order.Side
			oSide, err = order.StringToOrderSide(response.Data[x].Side)
			if err != nil {
				by.WebsocketUFuture.DataHandler <- order.ClassificationError{
					Exchange: by.Name,
					OrderID:  response.Data[x].OrderID,
					Err:      err,
				}
			}
			var oType order.Type
			oType, err = order.StringToOrderType(response.Data[x].OrderType)
			if err != nil {
				by.WebsocketUFuture.DataHandler <- order.ClassificationError{
					Exchange: by.Name,
					OrderID:  response.Data[x].OrderID,
					Err:      err,
				}
			}
			var oStatus order.Status
			oStatus, err = order.StringToOrderStatus(response.Data[x].OrderStatus)
			if err != nil {
				by.WebsocketUFuture.DataHandler <- order.ClassificationError{
					Exchange: by.Name,
					OrderID:  response.Data[x].OrderID,
					Err:      err,
				}
			}
			by.WebsocketUFuture.DataHandler <- &order.Detail{
				Price:     response.Data[x].Price,
				Amount:    response.Data[x].OrderQty,
				Exchange:  by.Name,
				OrderID:   response.Data[x].OrderID,
				AccountID: strconv.FormatInt(response.Data[x].UserID, 10),
				Type:      oType,
				Side:      oSide,
				Status:    oStatus,
				AssetType: asset.USDTMarginedFutures,
				Date:      response.Data[x].CreateTime,
				Pair:      p,
				Trades: []order.TradeHistory{
					{
						Price:     response.Data[x].Price,
						Amount:    response.Data[x].OrderQty,
						Exchange:  by.Name,
						Side:      oSide,
						Timestamp: response.Data[x].CreateTime,
					},
				},
			}
		}

	case wsWallet:
		var response WsFuturesWallet
		err = json.Unmarshal(respRaw, &response)
		if err != nil {
			return err
		}
		by.WebsocketUFuture.DataHandler <- response.Data

	default:
		by.WebsocketUFuture.DataHandler <- stream.UnhandledMessageWarning{Message: by.Name + stream.UnhandledMessage + string(respRaw)}
	}

	return nil
}

// processOrderbook processes orderbook updates
func (by *Bybit) processOrderbookUFutures(data []WsFuturesOrderbookData, action string, p currency.Pair, a asset.Item) error {
	if len(data) < 1 {
		return errors.New("no orderbook data")
	}

	switch action {
	case wsOperationSnapshot:
		var book orderbook.Base
		for i := range data {
			item := orderbook.Item{
				Price:  data[i].Price,
				Amount: data[i].Size,
				ID:     data[i].ID,
			}
			switch {
			case strings.EqualFold(data[i].Side, sideSell):
				book.Asks = append(book.Asks, item)
			case strings.EqualFold(data[i].Side, sideBuy):
				book.Bids = append(book.Bids, item)
			default:
				return fmt.Errorf("could not process websocket orderbook update, order side could not be matched for %s",
					data[i].Side)
			}
		}
		// For some reason Bybit sends the orderbook in a single list imitating a spread
		// Therefore the orderbook bids must be reversed
		book.Bids.Reverse()
		book.Asset = a
		book.Pair = p
		book.Exchange = by.Name
		book.VerifyOrderbook = by.CanVerifyOrderbook

		err := by.WebsocketUFuture.Orderbook.LoadSnapshot(&book)
		if err != nil {
			return fmt.Errorf("process orderbook error -  %s", err)
		}
	default:
		updateAction, err := by.GetActionFromString(action)
		if err != nil {
			return err
		}

		var asks, bids []orderbook.Item
		for i := range data {
			item := orderbook.Item{
				Price:  data[i].Price,
				Amount: data[i].Size,
				ID:     data[i].ID,
			}

			switch {
			case strings.EqualFold(data[i].Side, sideSell):
				asks = append(asks, item)
			case strings.EqualFold(data[i].Side, sideBuy):
				bids = append(bids, item)
			default:
				return fmt.Errorf("could not process websocket orderbook update, order side could not be matched for %s",
					data[i].Side)
			}
		}

		err = by.WebsocketUFuture.Orderbook.Update(&orderbook.Update{
			Bids:   bids,
			Asks:   asks,
			Pair:   p,
			Asset:  a,
			Action: updateAction,
		})
		if err != nil {
			return err
		}
	}
	return nil
}
