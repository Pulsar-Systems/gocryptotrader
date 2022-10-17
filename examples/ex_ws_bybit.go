package main

import (
	"context"
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

func setupBybitAndPrintOrderBook(a asset.Item) {
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
	if a == asset.Spot {
		bybitConfig.Orderbook.WebsocketBufferEnabled = false
		err = by.Setup(bybitConfig)
	} else {
		bybitConfig.Orderbook.WebsocketBufferEnabled = false
		err = by.SetupFuture(bybitConfig)
	}
	if err != nil {
		fmt.Println("by.Setup ", err)
	}
	err = by.Websocket.Connect()
	if err != nil {
		fmt.Println("by.Websocket.Connect ", err)
	}
	fmt.Printf("by.Websocket.IsConnected(): %v\n", by.Websocket.IsConnected())
	fmt.Printf("by.Websocket.GetWebsocketURL(): %v\n", by.Websocket.GetWebsocketURL())
	p := currency.Pair{
		Delimiter: "-",
		Base:      currency.BTC,
		Quote:     currency.USDT,
	}
	// Wait for the Orderbook to be fetched
	ticker := time.NewTicker(time.Second * 1)
	for {
		select {
		case <-ticker.C:
			var orbApi *bybit.Orderbook
			if a == asset.Spot {
				orbApi, err = by.GetOrderBook(context.TODO(), "BTCUSDT", 1000)
			} else {
				orbApi, err = by.GetFuturesOrderbook(context.TODO(), p)
			}
			if err != nil {
				fmt.Printf("GetOrderbook REST: %v\n", err)
			}
			orb, err := by.Websocket.Orderbook.GetOrderbook(p, a)
			if err != nil {
				fmt.Printf("GetOrderbook WS: %v\n", err)
			}
			t, err := by.FetchTicker(context.TODO(), p, a)
			if err != nil {
				fmt.Printf("FetchTicker: %v\n", err)
			}
			// Update ticker then print it
			by.UpdateTicker(context.TODO(), p, a)
			fmt.Printf("t: %v\n", t)
			// Check if the orderbook is written to an unexpected asset type
			if a == asset.USDTMarginedFutures {
				norb, err := by.Websocket.Orderbook.GetOrderbook(p, asset.Spot)
				// Should throw an error
				if err == nil {
					fmt.Println("An orderbook is stored under Spot rather than Futures!", norb)
				}
			} else {
				norb, err := by.Websocket.Orderbook.GetOrderbook(p, asset.USDTMarginedFutures)
				if err == nil {
					fmt.Println("An orderbook is stored under Futures rather than Spot!", norb)
				}
			}

			// If the update differences are too much to compare
			if orb != nil {
				fmt.Println("Asset:", a)
				fmt.Printf("Top 5 Asks:\n\tWS:%v,\n\tREST:%v\n", orb.Asks[:5], orbApi.Asks[:5])
				fmt.Printf("Top 5 Bids:\n\tWS:%v,\n\tREST:%v\n", orb.Bids[:5], orbApi.Bids[:5])
				fmt.Println()
			}
		}
	}
}

func bybitMain() {
	flag.Parse()
	log.SetFlags(0)

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	go setupBybitAndPrintOrderBook(asset.Spot)
	go setupBybitAndPrintOrderBook(asset.USDTMarginedFutures)
	for {
		select {
		case <-interrupt:
			fmt.Println("Program interrupted")
			os.Exit(1)
			return
		}
	}
}
