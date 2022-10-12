package main

// import (
// 	"flag"
// 	"fmt"
// 	"log"
// 	"os"
// 	"time"

// 	"github.com/thrasher-corp/gocryptotrader/config"
// 	"github.com/thrasher-corp/gocryptotrader/currency"
// 	"github.com/thrasher-corp/gocryptotrader/exchanges/asset"
// 	"github.com/thrasher-corp/gocryptotrader/exchanges/binance"
// 	"github.com/thrasher-corp/gocryptotrader/signaler"
// )

// func main() {
// 	flag.Parse()
// 	log.SetFlags(0)
// 	interrupt := make(chan struct{})

// 	c := &config.Config{}
// 	home, _ := os.UserHomeDir()
// 	err := c.ReadConfigFromFile(home+"/.gocryptotrader/config.json", false)
// 	if err != nil {
// 		fmt.Println("c.ReadConfigFromFile ", err)
// 	}
// 	binanceConfig, err := c.GetExchangeConfig("Binance")
// 	if err != nil {
// 		fmt.Println("c.GetExchangeConfig ", err)
// 	}

// 	b := new(binance.Binance)
// 	b.SetDefaults()
// 	err = b.SetupFuture(binanceConfig)
// 	if err != nil {
// 		fmt.Println("b.Setup ", err)
// 	}
// 	err = b.Websocket.Connect()
// 	if err != nil {
// 		fmt.Println("b.Websocket.Connect ", err)
// 	}
// 	fmt.Printf("b.Websocket.IsConnected(): %v\n", b.Websocket.IsConnected())
// 	fmt.Printf("b.Websocket.GetWebsocketURL(): %v\n", b.Websocket.GetWebsocketURL())
// 	p := currency.Pair{
// 		Delimiter: "_",
// 		Base:      currency.BTC,
// 		Quote:     currency.USDT,
// 	}
// 	go func() {
// 		for {
// 			orb, err := b.Websocket.Orderbook.GetOrderbook(p, asset.USDTMarginedFutures)
// 			if err != nil {
// 				fmt.Printf("GetOrderbook err: %v\n", err)
// 			} else {
// 				fmt.Printf("orb.LastUpdateID: %v\n", orb.LastUpdateID)
// 				fmt.Printf("orb.Bids: %v\n", orb.Bids[:5])
// 			}
// 			time.Sleep(time.Second * 1)
// 		}
// 	}()

// 	go waitForInteruptw(interrupt)
// 	<-interrupt
// 	b.Websocket.Shutdown()
// 	fmt.Println("Exited.")
// }

// func waitForInteruptw(waiter chan<- struct{}) {
// 	interrupt := signaler.WaitForInterrupt()
// 	fmt.Printf("\nCaptured %v, shutdown requested.\n", interrupt)
// 	waiter <- struct{}{}
// }
