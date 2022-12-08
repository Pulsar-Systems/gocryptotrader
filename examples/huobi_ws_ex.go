package main

import (
	"fmt"
	"os"
	"sync"

	"github.com/thrasher-corp/gocryptotrader/config"
	"github.com/thrasher-corp/gocryptotrader/exchanges/huobi"
)

func HuobiWSEx() {
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
	hConfig, err := c.GetExchangeConfig("Huobi")
	if err != nil {
		fmt.Println("c.GetExchangeConfig ", err)
	}

	h := new(huobi.HUOBI)
	h.SetDefaults()

	err = h.Setup(hConfig)
	if err != nil {
		fmt.Println("h.Setup ", err)
	}

	// err = h.Websocket.Connect()
	// if err != nil {
	// 	fmt.Println("h.Websocket.Connect ", err)
	// 	return
	// }
	err = h.WebsocketUFutures.Connect()
	if err != nil {
		fmt.Println("h.WebsocketUFutures.Connect ", err)
	}
	err = h.WebsocketUFutures.FlushChannels()
	if err != nil {
		fmt.Println("h.WebsocketUFutures.FlushChannels", err)
	}
	mu := sync.RWMutex{}
	go func() {
		for {
			select {
			case data := <-h.WebsocketUFutures.ToRoutine:
				if data == nil {
					continue
				}
				mu.RLock()
				fmt.Println(data)
				mu.RUnlock()
			}
		}
	}()
}
