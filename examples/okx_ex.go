package main

import (
	"fmt"
	"os"
	"time"

	"github.com/thrasher-corp/gocryptotrader/config"
	"github.com/thrasher-corp/gocryptotrader/exchanges/okx"
	"github.com/thrasher-corp/gocryptotrader/exchanges/orderbook"
)

func OKXWSEx() {
	c := &config.Config{}
	uhd, err := os.UserHomeDir()
	if err != nil {
		fmt.Printf("os.UserHomeDir: %v\n", err)
		return
	}
	dir := fmt.Sprintf("%s/.gocryptotrader/config.json", uhd)
	err = c.ReadConfigFromFile(dir, false)
	if err != nil {
		fmt.Println("c.ReadConfigFromFile ", err)
		return
	}
	okConfig, err := c.GetExchangeConfig("Okx")
	if err != nil {
		fmt.Println("c.GetExchangeConfig ", err)
		return
	}

	ok := new(okx.Okx)
	ok.SetDefaults()

	err = ok.Setup(okConfig)
	if err != nil {
		fmt.Println("ok.Setup ", err)
		return
	}
	fmt.Println("Okx setup successful")

	err = ok.Websocket.Connect()
	if err != nil {
		fmt.Println("ok.Websocket.Connect ", err)
		return
	}
	time.Sleep(time.Second)
	fmt.Println("Websocket connected:", ok.Websocket.IsConnected())
	go func() {
		for payload := range ok.Websocket.ToRoutine {
			switch data := payload.(type) {
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
	// go func() {
	// 	t := time.NewTicker(time.Second)
	// 	p := currency.Pair{
	// 		Base: currency.BTC,
	// 		Quote: currency.USDT,
	// 	}
	// 	for range t.C {
	// 		ob, err := ok.Websocket.Orderbook.GetOrderbook(p, asset.Spot)
	// 		if err != nil {
	// 			fmt.Printf("err: %v\n", err)
	// 			continue
	// 		}
	// 		fmt.Printf("ob.Asks[:5]: %v\n", ob.Asks[:5])
	// 	}
	// }()
}
