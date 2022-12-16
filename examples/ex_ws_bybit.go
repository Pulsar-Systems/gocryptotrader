package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/thrasher-corp/gocryptotrader/config"
	"github.com/thrasher-corp/gocryptotrader/exchanges/asset"
	"github.com/thrasher-corp/gocryptotrader/exchanges/bybit"
	"github.com/thrasher-corp/gocryptotrader/exchanges/orderbook"
	"github.com/thrasher-corp/gocryptotrader/exchanges/stream"
	"github.com/thrasher-corp/gocryptotrader/exchanges/ticker"
)

func setupBybitAndPrintOrderBook() {
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
	bybitConfig, err := c.GetExchangeConfig("Bybit")
	if err != nil {
		fmt.Println("c.GetExchangeConfig ", err)
	}

	by := new(bybit.Bybit)
	by.SetDefaults()

	bybitConfig.Orderbook.WebsocketBufferEnabled = false
	err = by.Setup(bybitConfig)
	if err != nil {
		fmt.Println("by.Setup ", err)
	}

	err = by.Websocket.Connect()
	if err != nil {
		fmt.Println("by.Websocket.Connect ", err)
	}
	fmt.Printf("by.Websocket.IsConnected(): %v\n", by.Websocket.IsConnected())
	fmt.Printf("by.Websocket.GetWebsocketURL(): %v\n", by.Websocket.GetWebsocketURL())
	fmt.Printf("Asset Spot enabled: %v\n", by.CurrencyPairs.IsAssetEnabled(asset.Spot) == nil)

	err = by.WebsocketUFuture.Connect()
	if err != nil {
		fmt.Println("by.WebsocketUFuture.Connect ", err)
		fmt.Println(!by.WebsocketUFuture.IsEnabled(), !by.IsEnabled(), !by.IsAssetWebsocketSupported(asset.USDTMarginedFutures))
	}
	fmt.Printf("by.WebsocketUFuture.IsConnected(): %v\n", by.WebsocketUFuture.IsConnected())
	fmt.Printf("by.WebsocketUFuture.GetWebsocketURL(): %v\n", by.WebsocketUFuture.GetWebsocketURL())
	fmt.Printf("Asset UFuture enabled: %v\n", by.CurrencyPairs.IsAssetEnabled(asset.USDTMarginedFutures) == nil)

	go func() {
		for payload := range by.WebsocketUFuture.ToRoutine {
			switch data := payload.(type) {
			case error:
				fmt.Printf("error received: %v, of type %T\n", payload, payload)
			case *orderbook.Depth:
				base, err := data.Retrieve()
				if err != nil {
					fmt.Println("unable to retrieve ob base from depth, got error:", err)
				}
				fmt.Printf("data.Asks[:5]: %v\n", base.Asks[:5])
			case *ticker.Price, stream.KlineData:
				continue
			default:
				fmt.Printf("unhandled data received:%T\n", data)
			}
		}
	}()
}

func bybitMain() {
	flag.Parse()
	log.SetFlags(0)

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	go setupBybitAndPrintOrderBook()
	<-interrupt
	fmt.Println("Program interrupted")
	os.Exit(1)
}
