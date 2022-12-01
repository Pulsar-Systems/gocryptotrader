package main

import (
	"fmt"
	"os"
	"time"

	"github.com/thrasher-corp/gocryptotrader/config"
	"github.com/thrasher-corp/gocryptotrader/currency"
	"github.com/thrasher-corp/gocryptotrader/exchanges/asset"
	"github.com/thrasher-corp/gocryptotrader/exchanges/huobi"
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
	hConfig, err := c.GetExchangeConfig("Huobi")
	if err != nil {
		fmt.Println("c.GetExchangeConfig ", err)
	}

	h := new(huobi.HUOBI)
	h.SetDefaults()

	err = h.Setup(hConfig)
	if err != nil {
		fmt.Println("b.Setup ", err)
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
	p := currency.Pair{
		Quote:     currency.USDT,
		Base:      currency.ETH,
		Delimiter: "-",
	}
	t := time.NewTicker(time.Second * 1)
	for range t.C {
		// orbSpot, err := h.Websocket.Orderbook.GetOrderbook(p, asset.Spot)
		// if err != nil {
		// 	fmt.Printf("GetOrderbook error received: %v\n", err)
		// 	continue
		// }
		// fmt.Printf("orbSpot.Bids[:5]: %v\n", orbSpot.Bids[:5])

		orbFut, err := h.WebsocketUFutures.Orderbook.GetOrderbook(p, asset.USDTMarginedFutures)
		if err != nil {
			fmt.Printf("GetOrderbook error received: %v\n", err)
			continue
		}
		fmt.Printf("orbFut.Bids[:5]: %v\n", orbFut.Bids[:5])
	}

}
