package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/thrasher-corp/gocryptotrader/config"
	"github.com/thrasher-corp/gocryptotrader/currency"
	"github.com/thrasher-corp/gocryptotrader/exchanges/asset"
	"github.com/thrasher-corp/gocryptotrader/exchanges/bybit"
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
	fmt.Println(by.CurrencyPairs.IsAssetEnabled(asset.Spot))
	err = by.WebsocketUFuture.Connect()
	if err != nil {
		fmt.Println("by.WebsocketUFuture.Connect ", err)
	}
	fmt.Printf("by.WebsocketUFuture.IsConnected(): %v\n", by.WebsocketUFuture.IsConnected())
	fmt.Printf("by.WebsocketUFuture.GetWebsocketURL(): %v\n", by.WebsocketUFuture.GetWebsocketURL())

	p := currency.Pair{
		Delimiter: "-",
		Base:      currency.ETH,
		Quote:     currency.USDT,
	}
	// Wait for the Orderbook to be fetched
	ticker := time.NewTicker(time.Second * 1)
	for {
		select {
		case <-ticker.C:
			// orbRest1, err := by.GetOrderBook(context.TODO(), "ETHUSDT", 10)
			// if err != nil {
			// 	fmt.Printf("GetOrderbook REST1: %v\n", err)
			// 	panic(0)
			// }
			// orbRest2, err := by.GetFuturesOrderbook(context.TODO(), p)
			// if err != nil {
			// 	fmt.Printf("GetOrderbook REST2: %v\n", err)
			// }

			// orbWs1, err := by.Websocket.Orderbook.GetOrderbook(p, asset.Spot)
			// orbWs1, err := by.FetchOrderbook(context.TODO(), p, asset.Spot)
			// if err != nil {
			// 	fmt.Printf("GetOrderbook WS1: %v\n", err)
			// 	panic(0)
			// }
			orbWs2, err := by.WebsocketUFuture.Orderbook.GetOrderbook(p, asset.USDTMarginedFutures)
			if err != nil {
				fmt.Printf("GetOrderbook WS2: %v\n", err)
				panic(0)
			}

			// fmt.Printf("orbRest1.LastUpdateID: %v\n", orbRest1.Time)
			fmt.Printf("orbWs1.LastUpdateID  : %v\n", orbWs2.LastUpdated.Unix())
			// fmt.Println("Diff:", orbRest1.Time.UnixMicro()-orbWs1.LastUpdated.UnixMicro())
			// fmt.Printf("orbRest1.Bids: ")
			// for _, b := range orbRest1.Bids[:5] {
			// 	fmt.Printf("{%v %v}, ", b.Price, b.Amount)
			// }
			// fmt.Println()
			fmt.Printf("orbWs1.Bids  : ")
			for _, b := range orbWs2.Bids[:5] {
				fmt.Printf("{%v %v}, ", b.Price, b.Amount)
			}
			fmt.Println()

			// _ = orbWs1
			// _ = orbWs2

			// t, err := by.FetchTicker(context.TODO(), p, a)
			// if err != nil {
			// 	fmt.Printf("FetchTicker: %v\n", err)
			// }
			// Update ticker then print it
			// by.UpdateTicker(context.TODO(), p, a)
			// fmt.Printf("t: %v\n", t)
			// // Check if the orderbook is written to an unexpected asset type
			// if a == asset.USDTMarginedFutures {
			// 	norb, err := by.Websocket.Orderbook.GetOrderbook(p, asset.Spot)
			// 	// Should throw an error
			// 	if err == nil {
			// 		fmt.Println("An orderbook is stored under Spot rather than Futures!", norb)
			// 	}
			// } else {
			// 	norb, err := by.Websocket.Orderbook.GetOrderbook(p, asset.USDTMarginedFutures)
			// 	if err == nil {
			// 		fmt.Println("An orderbook is stored under Futures rather than Spot!", norb)
			// 	}
			// }

		}
	}
}

func bybitMain() {
	flag.Parse()
	log.SetFlags(0)

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	go setupBybitAndPrintOrderBook()
	for {
		select {
		case <-interrupt:
			fmt.Println("Program interrupted")
			os.Exit(1)
			return
		}
	}
}
