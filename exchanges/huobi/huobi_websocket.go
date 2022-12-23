package huobi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/thrasher-corp/gocryptotrader/common"
	"github.com/thrasher-corp/gocryptotrader/common/crypto"
	"github.com/thrasher-corp/gocryptotrader/currency"
	"github.com/thrasher-corp/gocryptotrader/exchanges/account"
	"github.com/thrasher-corp/gocryptotrader/exchanges/asset"
	"github.com/thrasher-corp/gocryptotrader/exchanges/order"
	"github.com/thrasher-corp/gocryptotrader/exchanges/orderbook"
	"github.com/thrasher-corp/gocryptotrader/exchanges/stream"
	"github.com/thrasher-corp/gocryptotrader/exchanges/ticker"
	"github.com/thrasher-corp/gocryptotrader/exchanges/trade"
	"github.com/thrasher-corp/gocryptotrader/log"
)

const (
	baseWSURL    = "wss://api.huobi.pro"
	futuresWSURL = "wss://api.hbdm.com/"

	wsMarketURL    = baseWSURL + "/ws"
	wsMarketKline  = "market.%s.kline.1min"
	wsMarketDepth  = "market.%s.depth.step0"
	wsMarketTrade  = "market.%s.trade.detail"
	wsMarketTicker = "market.%s.detail"

	wsAccountsOrdersEndPoint = "/ws/v2"
	wsAccountsList           = "accounts.list"
	wsOrdersList             = "orders.list"
	wsOrdersDetail           = "orders.detail"
	wsAccountsOrdersURL      = baseWSURL + wsAccountsOrdersEndPoint
	wsAccountListEndpoint    = wsAccountsOrdersEndPoint + "/" + wsAccountsList
	wsOrdersListEndpoint     = wsAccountsOrdersEndPoint + "/" + wsOrdersList
	wsOrdersDetailEndpoint   = wsAccountsOrdersEndPoint + "/" + wsOrdersDetail

	wsDateTimeFormatting = "2006-01-02T15:04:05"

	signatureMethod  = "HmacSHA256"
	signatureVersion = "2.1"
	requestOp        = "req"
	authOp           = "auth"

	loginDelay = 50 * time.Millisecond
	rateLimit  = 20
)

// Instantiates a communications channel between websocket connections
var comms = make(chan WsMessage)

// WsConnect initiates a new websocket connection
func (h *HUOBI) WsConnect() error {
	if !h.Websocket.IsEnabled() || !h.IsEnabled() {
		return errors.New(stream.WebsocketNotEnabled)
	}
	var dialer websocket.Dialer
	err := h.wsDial(&dialer)
	if err != nil {
		return err
	}
	if h.Websocket.CanUseAuthenticatedEndpoints() {
		err = h.wsAuthenticatedDial(&dialer)
		if err != nil {
			log.Errorf(log.ExchangeSys,
				"%v - authenticated dial failed: %v\n",
				h.Name,
				err)
		}
		err = h.wsLogin(context.TODO())
		if err != nil {
			log.Errorf(log.ExchangeSys,
				"%v - authentication failed: %v\n",
				h.Name,
				err)
			h.Websocket.SetCanUseAuthenticatedEndpoints(false)
		}
	}

	h.Websocket.Wg.Add(1)
	go h.wsReadData()
	return nil
}

func (h *HUOBI) wsDial(dialer *websocket.Dialer) error {
	err := h.Websocket.Conn.Dial(dialer, http.Header{})
	if err != nil {
		return err
	}
	h.Websocket.Wg.Add(1)
	go h.wsFunnelConnectionData(h.Websocket.Conn, wsMarketURL)
	return nil
}

func (h *HUOBI) wsAuthenticatedDial(dialer *websocket.Dialer) error {
	if !h.IsWebsocketAuthenticationSupported() {
		return fmt.Errorf("%v AuthenticatedWebsocketAPISupport not enabled",
			h.Name)
	}
	err := h.Websocket.AuthConn.Dial(dialer, http.Header{})
	if err != nil {
		return err
	}

	h.Websocket.Wg.Add(1)
	go h.wsFunnelConnectionData(h.Websocket.AuthConn, wsAccountsOrdersURL)
	return nil
}

// wsFunnelConnectionData manages data from multiple endpoints and passes it to
// a channel
func (h *HUOBI) wsFunnelConnectionData(ws stream.Connection, url string) {
	defer h.Websocket.Wg.Done()
	for {
		resp := ws.ReadMessage()
		if resp.Raw == nil {
			return
		}
		comms <- WsMessage{Raw: resp.Raw, URL: url}
	}
}

// wsReadData receives and passes on websocket messages for processing
func (h *HUOBI) wsReadData() {
	defer h.Websocket.Wg.Done()
	for {
		select {
		case <-h.Websocket.ShutdownC:
			select {
			case resp := <-comms:
				err := h.wsHandleData(resp.Raw)
				if err != nil {
					select {
					case h.Websocket.DataHandler <- err:
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
		case resp := <-comms:
			err := h.wsHandleData(resp.Raw)
			if err != nil {
				h.Websocket.DataHandler <- err
			}
		}
	}
}

func stringToOrderStatus(status string) (order.Status, error) {
	switch status {
	case "submitted":
		return order.New, nil
	case "canceled":
		return order.Cancelled, nil
	case "partial-filled":
		return order.PartiallyFilled, nil
	case "partial-canceled":
		return order.PartiallyCancelled, nil
	default:
		return order.UnknownStatus,
			errors.New(status + " not recognised as order status")
	}
}

func stringToOrderSide(side string) (order.Side, error) {
	switch {
	case strings.Contains(side, "buy"):
		return order.Buy, nil
	case strings.Contains(side, "sell"):
		return order.Sell, nil
	}

	return order.UnknownSide,
		errors.New(side + " not recognised as order side")
}

func stringToOrderType(oType string) (order.Type, error) {
	switch {
	case strings.Contains(oType, "limit"):
		return order.Limit, nil
	case strings.Contains(oType, "market"):
		return order.Market, nil
	}

	return order.UnknownType,
		errors.New(oType + " not recognised as order type")
}

func (h *HUOBI) wsHandleData(respRaw []byte) error {
	var init WsResponse
	err := json.Unmarshal(respRaw, &init)
	if err != nil {
		return err
	}
	if init.Subscribed != "" ||
		init.UnSubscribed != "" ||
		init.Op == "sub" ||
		init.Op == "unsub" ||
		init.Action == "sub" {
		// TODO handle subs
		return nil
	}
	if init.Ping != 0 {
		h.sendPingResponse(init.Ping)
		return nil
	}

	if init.Action == "ping" {
		var ping authenticationPing
		err := json.Unmarshal(respRaw, &ping)
		if err != nil {
			h.Websocket.DataHandler <- err
			return nil
		}
		ping.Action = "pong"
		err = h.Websocket.AuthConn.SendJSONMessage(ping)
		if err != nil {
			log.Error(log.ExchangeSys, err)
		}
		return nil
	}

	if init.ErrorMessage != "" {
		if init.ErrorMessage == "api-signature-not-valid" {
			h.Websocket.SetCanUseAuthenticatedEndpoints(false)
			return errors.New(h.Name +
				" - invalid credentials. Authenticated requests disabled")
		}

		codes, _ := init.ErrorCode.(string)
		return errors.New(h.Name + " Code:" + codes + " Message:" + init.ErrorMessage)
	}

	if init.ClientID > 0 {
		if h.Websocket.Match.IncomingWithData(init.ClientID, respRaw) {
			return nil
		}
	}

	switch {
	case strings.EqualFold(init.Channel, authOp):
		h.Websocket.SetCanUseAuthenticatedEndpoints(true)
		// Auth captured
		return nil
	case strings.EqualFold(init.Channel, "accounts.update#2"):
		fmt.Println(string(respRaw))
		var response WsNewAuthenticatedAccountsResponse
		err := json.Unmarshal(respRaw, &response)
		if err != nil {
			return err
		}
		h.Websocket.DataHandler <- response.Data
	case strings.Contains(init.Channel, "orders"):
		var response WsNewAuthenticatedOrdersUpdateResponse
		err := json.Unmarshal(respRaw, &response)
		if err != nil {
			return err
		}
		typeSplit := strings.Split(response.Data.Type, "-")
		side, err := stringToOrderSide(typeSplit[0])
		if err != nil {
			return err
		}
		oType, err := stringToOrderType(typeSplit[1])
		if err != nil {
			return err
		}
		status, err := stringToOrderStatus(response.Data.Status)
		if err != nil {
			return err
		}
		p, err := currency.NewPairFromString(response.Data.Symbol)
		if err != nil {
			return err
		}
		var lastUpdated int64
		if response.Data.LastActTime > response.Data.CreateTime {
			lastUpdated = response.Data.LastActTime
		} else {
			lastUpdated = response.Data.CreateTime
		}
		h.Websocket.DataHandler <- &order.Detail{
			Price:           response.Data.Price,
			Amount:          response.Data.Size,
			ExecutedAmount:  response.Data.ExecutedAmount,
			RemainingAmount: response.Data.RemainingAmount,
			Exchange:        h.Name,
			OrderID:         fmt.Sprint(response.Data.OrderID),
			Type:            oType,
			Side:            side,
			Status:          status,
			AssetType:       asset.Spot,
			LastUpdated:     time.UnixMilli(lastUpdated),
			Pair:            p,
		}

	case strings.Contains(init.Topic, "orders"):
		var response WsOldOrderUpdate
		err := json.Unmarshal(respRaw, &response)
		if err != nil {
			return err
		}
		h.Websocket.DataHandler <- response
	case strings.Contains(init.Channel, "depth"):
		var depth WsDepth
		err := json.Unmarshal(respRaw, &depth)
		if err != nil {
			return err
		}

		data := strings.Split(depth.Channel, ".")
		err = h.WsProcessOrderbook(&depth, data[1])
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
		h.Websocket.DataHandler <- stream.KlineData{
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
	case strings.Contains(init.Channel, "detail"),
		strings.Contains(init.Rep, "detail"):
		var wsTicker WsTick
		err := json.Unmarshal(respRaw, &wsTicker)
		if err != nil {
			return err
		}
		var data []string
		if wsTicker.Channel != "" {
			data = strings.Split(wsTicker.Channel, ".")
		}
		if wsTicker.Rep != "" {
			data = strings.Split(wsTicker.Rep, ".")
		}

		var p currency.Pair
		var a asset.Item
		p, a, err = h.GetRequestFormattedPairAndAssetType(data[1])
		if err != nil {
			return err
		}

		h.Websocket.DataHandler <- &ticker.Price{
			ExchangeName: h.Name,
			Open:         wsTicker.Tick.Open,
			Close:        wsTicker.Tick.Close,
			Volume:       wsTicker.Tick.Amount,
			QuoteVolume:  wsTicker.Tick.Volume,
			High:         wsTicker.Tick.High,
			Low:          wsTicker.Tick.Low,
			LastUpdated:  time.UnixMilli(wsTicker.Timestamp),
			AssetType:    a,
			Pair:         p,
		}
	default:
		h.Websocket.DataHandler <- stream.UnhandledMessageWarning{
			Message: h.Name + stream.UnhandledMessage + string(respRaw),
		}
		return nil
	}
	return nil
}

func (h *HUOBI) sendPingResponse(pong int64) {
	err := h.Websocket.Conn.SendJSONMessage(WsPong{Pong: pong})
	if err != nil {
		log.Error(log.ExchangeSys, err)
	}
}

// WsProcessOrderbook processes new orderbook data
func (h *HUOBI) WsProcessOrderbook(update *WsDepth, symbol string) error {
	pairs, err := h.GetEnabledPairs(asset.Spot)
	if err != nil {
		return err
	}

	format, err := h.GetPairFormat(asset.Spot, true)
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

	var newOrderBook orderbook.Base
	newOrderBook.Asks = asks
	newOrderBook.Bids = bids
	newOrderBook.Pair = p
	newOrderBook.Asset = asset.Spot
	newOrderBook.Exchange = h.Name
	newOrderBook.VerifyOrderbook = h.CanVerifyOrderbook

	return h.Websocket.Orderbook.LoadSnapshot(&newOrderBook)
}

// GenerateDefaultSubscriptions Adds default subscriptions to websocket to be handled by ManageSubscriptions()
func (h *HUOBI) GenerateDefaultSubscriptions() ([]stream.ChannelSubscription, error) {
	var channels = []string{}
	// var channels = []string{wsMarketKline,
	// 	wsMarketDepth,
	// 	wsMarketTrade,
	// 	wsMarketTicker}
	var subscriptions []stream.ChannelSubscription
	if h.Websocket.CanUseAuthenticatedEndpoints() {
		channels = append(channels, "orders#%v")
		subscriptions = append(subscriptions, stream.ChannelSubscription{
			Channel: "accounts.update#2",
		})
	}
	enabledCurrencies, err := h.GetEnabledPairs(asset.Spot)
	if err != nil {
		return nil, err
	}
	for i := range channels {
		for j := range enabledCurrencies {
			enabledCurrencies[j].Delimiter = ""
			channel := fmt.Sprintf(channels[i],
				enabledCurrencies[j].Lower().String())
			subscriptions = append(subscriptions, stream.ChannelSubscription{
				Channel:  channel,
				Currency: enabledCurrencies[j],
			})
		}
	}
	return subscriptions, nil
}

// Subscribe sends a websocket message to receive data from the channel
func (h *HUOBI) Subscribe(channelsToSubscribe []stream.ChannelSubscription) error {
	var creds *account.Credentials
	if h.Websocket.CanUseAuthenticatedEndpoints() {
		var err error
		creds, err = h.GetCredentials(context.TODO())
		if err != nil {
			return err
		}
	}
	var errs common.Errors
	for i := range channelsToSubscribe {
		if (strings.Contains(channelsToSubscribe[i].Channel, "orders#") ||
			strings.Contains(channelsToSubscribe[i].Channel, "accounts")) && creds != nil {
			err := h.wsAuthenticatedSubscribe(creds,
				"sub",
				wsAccountsOrdersEndPoint+channelsToSubscribe[i].Channel,
				channelsToSubscribe[i].Channel)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			h.Websocket.AddSuccessfulSubscriptions(channelsToSubscribe[i])
			continue
		}
		err := h.Websocket.Conn.SendJSONMessage(WsRequest{
			Subscribe: channelsToSubscribe[i].Channel,
		})
		if err != nil {
			errs = append(errs, err)
			continue
		}
		h.Websocket.AddSuccessfulSubscriptions(channelsToSubscribe[i])
	}
	if errs != nil {
		return errs
	}
	return nil
}

// Unsubscribe sends a websocket message to stop receiving data from the channel
func (h *HUOBI) Unsubscribe(channelsToUnsubscribe []stream.ChannelSubscription) error {
	var creds *account.Credentials
	if h.Websocket.CanUseAuthenticatedEndpoints() {
		var err error
		creds, err = h.GetCredentials(context.TODO())
		if err != nil {
			return err
		}
	}
	var errs common.Errors
	for i := range channelsToUnsubscribe {
		if (strings.Contains(channelsToUnsubscribe[i].Channel, "orders.") ||
			strings.Contains(channelsToUnsubscribe[i].Channel, "accounts")) && creds != nil {
			err := h.wsAuthenticatedSubscribe(creds,
				"unsub",
				wsAccountsOrdersEndPoint+channelsToUnsubscribe[i].Channel,
				channelsToUnsubscribe[i].Channel)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			h.Websocket.RemoveSuccessfulUnsubscriptions(channelsToUnsubscribe[i])
			continue
		}
		err := h.Websocket.Conn.SendJSONMessage(WsRequest{
			Unsubscribe: channelsToUnsubscribe[i].Channel,
		})
		if err != nil {
			errs = append(errs, err)
			continue
		}
		h.Websocket.RemoveSuccessfulUnsubscriptions(channelsToUnsubscribe[i])
	}
	if errs != nil {
		return errs
	}
	return nil
}

func (h *HUOBI) wsGenerateSignature(creds *account.Credentials, timestamp, endpoint string) ([]byte, error) {
	values := url.Values{}
	values.Set("accessKey", creds.Key)
	values.Set("signatureMethod", signatureMethod)
	values.Set("signatureVersion", signatureVersion)
	values.Set("timestamp", timestamp)
	host := "api.huobi.pro"
	payload := fmt.Sprintf("%s\n%s\n%s\n%s",
		http.MethodGet, host, endpoint, values.Encode())
	return crypto.GetHMAC(crypto.HashSHA256, []byte(payload), []byte(creds.Secret))
}

func (h *HUOBI) wsLogin(ctx context.Context) error {
	if !h.IsWebsocketAuthenticationSupported() {
		return fmt.Errorf("%v AuthenticatedWebsocketAPISupport not enabled", h.Name)
	}
	creds, err := h.GetCredentials(ctx)
	if err != nil {
		return err
	}
	h.Websocket.SetCanUseAuthenticatedEndpoints(true)
	timestamp := time.Now().UTC().Format(wsDateTimeFormatting)
	request := WsNewAuthenticationRequest{
		Action:  requestOp,
		Channel: authOp,
		Params: AuthReqParams{
			AuthType:         "api",
			AccessKey:        creds.Key,
			SignatureMethod:  signatureMethod,
			SignatureVersion: signatureVersion,
			Timestamp:        timestamp,
		},
	}
	hmac, err := h.wsGenerateSignature(creds, timestamp, wsAccountsOrdersEndPoint)
	if err != nil {
		return err
	}
	request.Params.Signature = crypto.Base64Encode(hmac)
	err = h.Websocket.AuthConn.SendJSONMessage(request)
	if err != nil {
		h.Websocket.SetCanUseAuthenticatedEndpoints(false)
		return err
	}

	time.Sleep(loginDelay)
	return nil
}

func (h *HUOBI) wsAuthenticatedSubscribe(creds *account.Credentials, operation, endpoint, topic string) error {
	// timestamp := time.Now().UTC().Format(wsDateTimeFormatting)
	request := WsNewAuthenticatedSubscriptionRequest{
		Action:  operation,
		Channel: topic,
	}
	// hmac, err := h.wsGenerateSignature(creds, timestamp, endpoint)
	// if err != nil {
	// 	return err
	// }
	// request.Signature = crypto.Base64Encode(hmac)
	return h.Websocket.AuthConn.SendJSONMessage(request)
}

func (h *HUOBI) wsGetAccountsList(ctx context.Context) (*WsAuthenticatedAccountsListResponse, error) {
	if !h.Websocket.CanUseAuthenticatedEndpoints() {
		return nil, fmt.Errorf("%v not authenticated cannot get accounts list", h.Name)
	}
	creds, err := h.GetCredentials(ctx)
	if err != nil {
		return nil, err
	}

	timestamp := time.Now().UTC().Format(wsDateTimeFormatting)
	request := WsAuthenticatedAccountsListRequest{
		Op:               requestOp,
		AccessKeyID:      creds.Key,
		SignatureMethod:  signatureMethod,
		SignatureVersion: signatureVersion,
		Timestamp:        timestamp,
		Topic:            wsAccountsList,
	}
	hmac, err := h.wsGenerateSignature(creds, timestamp, wsAccountListEndpoint)
	if err != nil {
		return nil, err
	}
	request.Signature = crypto.Base64Encode(hmac)
	request.ClientID = h.Websocket.AuthConn.GenerateMessageID(true)
	resp, err := h.Websocket.AuthConn.SendMessageReturnResponse(request.ClientID, request)
	if err != nil {
		return nil, err
	}
	var response WsAuthenticatedAccountsListResponse
	err = json.Unmarshal(resp, &response)
	if err != nil {
		return nil, err
	}

	code, _ := response.ErrorCode.(int)
	if code != 0 {
		return nil, errors.New(response.ErrorMessage)
	}
	return &response, nil
}

func (h *HUOBI) wsGetOrdersList(ctx context.Context, accountID int64, pair currency.Pair) (*WsAuthenticatedOrdersResponse, error) {
	if !h.Websocket.CanUseAuthenticatedEndpoints() {
		return nil, fmt.Errorf("%v not authenticated cannot get orders list", h.Name)
	}

	creds, err := h.GetCredentials(ctx)
	if err != nil {
		return nil, err
	}

	fpair, err := h.FormatExchangeCurrency(pair, asset.Spot)
	if err != nil {
		return nil, err
	}

	timestamp := time.Now().UTC().Format(wsDateTimeFormatting)
	request := WsAuthenticatedOrdersListRequest{
		Op:               requestOp,
		AccessKeyID:      creds.Key,
		SignatureMethod:  signatureMethod,
		SignatureVersion: signatureVersion,
		Timestamp:        timestamp,
		Topic:            wsOrdersList,
		AccountID:        accountID,
		Symbol:           fpair.String(),
		States:           "submitted,partial-filled",
	}

	hmac, err := h.wsGenerateSignature(creds, timestamp, wsOrdersListEndpoint)
	if err != nil {
		return nil, err
	}
	request.Signature = crypto.Base64Encode(hmac)
	request.ClientID = h.Websocket.AuthConn.GenerateMessageID(true)

	resp, err := h.Websocket.AuthConn.SendMessageReturnResponse(request.ClientID, request)
	if err != nil {
		return nil, err
	}

	var response WsAuthenticatedOrdersResponse
	err = json.Unmarshal(resp, &response)
	if err != nil {
		return nil, err
	}

	code, _ := response.ErrorCode.(int)
	if code != 0 {
		return nil, errors.New(response.ErrorMessage)
	}
	return &response, nil
}

func (h *HUOBI) wsGetOrderDetails(ctx context.Context, orderID string) (*WsAuthenticatedOrderDetailResponse, error) {
	if !h.Websocket.CanUseAuthenticatedEndpoints() {
		return nil, fmt.Errorf("%v not authenticated cannot get order details", h.Name)
	}
	creds, err := h.GetCredentials(ctx)
	if err != nil {
		return nil, err
	}
	timestamp := time.Now().UTC().Format(wsDateTimeFormatting)
	request := WsAuthenticatedOrderDetailsRequest{
		Op:               requestOp,
		AccessKeyID:      creds.Key,
		SignatureMethod:  signatureMethod,
		SignatureVersion: signatureVersion,
		Timestamp:        timestamp,
		Topic:            wsOrdersDetail,
		OrderID:          orderID,
	}
	hmac, err := h.wsGenerateSignature(creds, timestamp, wsOrdersDetailEndpoint)
	if err != nil {
		return nil, err
	}
	request.Signature = crypto.Base64Encode(hmac)
	request.ClientID = h.Websocket.AuthConn.GenerateMessageID(true)
	resp, err := h.Websocket.AuthConn.SendMessageReturnResponse(request.ClientID, request)
	if err != nil {
		return nil, err
	}
	var response WsAuthenticatedOrderDetailResponse
	err = json.Unmarshal(resp, &response)
	if err != nil {
		return nil, err
	}

	code, _ := response.ErrorCode.(int)
	if code != 0 {
		return nil, errors.New(response.ErrorMessage)
	}
	return &response, nil
}
