package main

import (
	"fmt"
	"os"
	"time"

	"github.com/thrasher-corp/gocryptotrader/config"
	"github.com/thrasher-corp/gocryptotrader/currency"
	"github.com/thrasher-corp/gocryptotrader/exchanges/asset"
	"github.com/thrasher-corp/gocryptotrader/exchanges/binance"
)

func main() {
	c := &config.Config{}
	uhd, err := os.UserHomeDir()
	if err != nil {
		fmt.Printf("os.UserHomeDir: %v\n", err)
	}
	dir := fmt.Sprintf("%s/.gocryptotrader/config.json", uhd)
	err = c.ReadConfigFromFile(dir, false)
	if err != nil {
		fmt.Println("c.ReadConfigFromFile ", err)
	}
	bConfig, err := c.GetExchangeConfig("Binance")
	if err != nil {
		fmt.Println("c.GetExchangeConfig ", err)
	}

	b := new(binance.Binance)
	b.SetDefaults()

	err = b.Setup(bConfig)
	if err != nil {
		fmt.Println("b.Setup ", err)
	}

	err = b.Websocket.Connect()
	if err != nil {
		fmt.Println("b.Websocket.Connect ", err)
	}
	err = b.WebsocketUFuture.Connect()
	if err != nil {
		fmt.Println("b.WebsocketUFuture.Connect ", err)
	}
	p := currency.Pair{
		Quote:     currency.USDT,
		Base:      currency.BTC,
		Delimiter: "_",
	}
	t := time.NewTicker(time.Second * 1)
	for range t.C {
		orbSpot, err := b.Websocket.Orderbook.GetOrderbook(p, asset.Spot)
		if err != nil {
			fmt.Printf("err: %v\n", err)
			continue
		}
		fmt.Printf("orbSpot.Bids[:5]: %v\n", orbSpot.Bids[:5])
		orbFuture, err := b.WebsocketUFuture.Orderbook.GetOrderbook(p, asset.USDTMarginedFutures)
		// fmt.Printf("b.WebsocketUFuture.Conn.GetURL(): %v\n", b.WebsocketUFuture.Conn.GetURL())
		if err != nil {
			fmt.Printf("err: %v\n", err)
			continue
		}
		fmt.Printf("orbFuture.Bids[:5]: %v\n", orbFuture.Bids[:5])
		fmt.Println()
	}
}
