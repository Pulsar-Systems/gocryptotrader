package main

import (
	"fmt"
	"os"
	"time"

	"github.com/thrasher-corp/gocryptotrader/config"
	"github.com/thrasher-corp/gocryptotrader/exchanges/huobi"
	"github.com/thrasher-corp/gocryptotrader/exchanges/order"
	"github.com/thrasher-corp/gocryptotrader/exchanges/stream"
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
	huobiConfig, err := c.GetExchangeConfig("Huobi")
	if err != nil {
		fmt.Println("c.GetExchangeConfig ", err)
	}

	h := new(huobi.HUOBI)
	h.SetDefaults()

	err = h.Setup(huobiConfig)
	if err != nil {
		fmt.Println("h.Setup ", err)
	}

	err = h.Websocket.Connect()
	if err != nil {
		fmt.Println("h.Websocket.Connect ", err)
	}
	for !h.Websocket.IsConnected() {
		fmt.Println("waiting for ws connect")
		time.Sleep(time.Second)
	}
	for payload := range h.Websocket.ToRoutine {
		switch d := payload.(type) {
		case error:
			fmt.Println("error received", d)
		case stream.UnhandledMessageWarning:
			fmt.Println("unhandled message warning:", d)
		case *order.Detail:
			fmt.Println("order received with price:", d.Price)
		default:
			fmt.Printf("unknown data received of type %T\n", d)
		}
	}
}
