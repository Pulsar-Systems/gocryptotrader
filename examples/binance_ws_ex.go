package main

import (
	"fmt"
	"os"

	"github.com/thrasher-corp/gocryptotrader/config"
	"github.com/thrasher-corp/gocryptotrader/exchanges/binance"
	"github.com/thrasher-corp/gocryptotrader/exchanges/orderbook"
)

func BinanceWsEx() {
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
		fmt.Println("b.SetupUFuture", err)
	}

	// err = b.Websocket.Connect()
	// if err != nil {
	// 	fmt.Println("b.Websocket.Connect ", err)
	// }
	err = b.WebsocketUFuture.Connect()
	if err != nil {
		fmt.Println("b.WebsocketUFuture.Connect ", err)
	}
	go func() {
		for payload := range b.WebsocketUFuture.ToRoutine {
			fmt.Printf("%T\n", payload)
			switch data := payload.(type) {
			case error:
				fmt.Println("error received", err)
			case *orderbook.Depth:
				base, err := data.Retrieve()
				if err != nil {
					fmt.Println("unable to retrieve ob base from depth, got error:", err)
				}
				fmt.Printf("data.Asks[:5]: %v\n", base.Asks[:5])
			default:
				fmt.Println("unhandled data received:", data)
			}
		}
	}()
}
