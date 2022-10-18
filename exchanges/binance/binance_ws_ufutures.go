package binance

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/thrasher-corp/gocryptotrader/currency"
	exchange "github.com/thrasher-corp/gocryptotrader/exchanges"
	"github.com/thrasher-corp/gocryptotrader/exchanges/asset"
	"github.com/thrasher-corp/gocryptotrader/exchanges/orderbook"
	"github.com/thrasher-corp/gocryptotrader/exchanges/stream"
	"github.com/thrasher-corp/gocryptotrader/exchanges/ticker"
	"github.com/thrasher-corp/gocryptotrader/exchanges/trade"
)

func (b *Binance) wsHandleDataUFuture(respRaw []byte, url exchange.URL) error {
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
	b.wsMutex.RLock()
	bWebsocket, exist := b.Websockets[url]
	b.wsMutex.RUnlock()
	if !exist {
		return fmt.Errorf("url of type: %v does not have Websocket", url)
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
