package main

import (
	"fmt"
	"os"

	"github.com/thrasher-corp/gocryptotrader/config"
	"github.com/thrasher-corp/gocryptotrader/exchanges/kraken"
	"github.com/thrasher-corp/gocryptotrader/exchanges/orderbook"
)

func KrakenWSEx() {
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
	kConfig, err := c.GetExchangeConfig("Kraken")
	if err != nil {
		fmt.Println("c.GetExchangeConfig ", err)
		return
	}

	k := new(kraken.Kraken)
	k.SetDefaults()

	err = k.SetupUFutures(kConfig)
	// err = k.Setup(kConfig)
	if err != nil {
		fmt.Println("k.Setup ", err)
		return
	}

	// err = k.Websocket.Connect()
	// if err != nil {
	// 	fmt.Println("h.Websocket.Connect ", err)
	// 	return
	// }
	err = k.WebsocketUFutures.Connect()
	if err != nil {
		fmt.Println("k.WebsocketUFutures.Connect ", err)
	}
	err = k.WebsocketUFutures.FlushChannels()
	if err != nil {
		fmt.Println("k.WebsocketUFutures.FlushChannels", err)
	}
	// mu := sync.RWMutex{}
	go func() {
		for payload := range k.WebsocketUFutures.ToRoutine {
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
}
