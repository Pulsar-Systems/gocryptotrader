package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/thrasher-corp/gocryptotrader/config"
	"github.com/thrasher-corp/gocryptotrader/currency"
	ex "github.com/thrasher-corp/gocryptotrader/exchanges"
	"github.com/thrasher-corp/gocryptotrader/exchanges/asset"
	"github.com/thrasher-corp/gocryptotrader/exchanges/binance"
	"github.com/thrasher-corp/gocryptotrader/exchanges/stream"
	"github.com/thrasher-corp/gocryptotrader/signaler"
)

func main() {
	flag.Parse()
	log.SetFlags(0)
	interrupt := make(chan struct{})

	c := &config.Config{}
	home, _ := os.UserHomeDir()
	err := c.ReadConfigFromFile(home+"/.gocryptotrader/config.json", false)
	if err != nil {
		fmt.Println("c.ReadConfigFromFile ", err)
	}
	binanceConfig, err := c.GetExchangeConfig("Binance")
	if err != nil {
		fmt.Println("c.GetExchangeConfig ", err)
	}

	b := new(binance.Binance)
	b.SetDefaults()
	err = b.Setup(binanceConfig)
	if err != nil {
		fmt.Println("b.Setup ", err)
	}
	for url, bWebsocket := range b.Websockets {
		err = bWebsocket.Connect()
		if err != nil {
			fmt.Println("b.Websocket.Connect ", err)
		}
		fmt.Printf("bWebsocket[%s].IsConnected(): %v\n", url.String(), bWebsocket.IsConnected())
		fmt.Printf("bWebsocket[%s].GetWebsocketURL(): %v\n", url.String(), bWebsocket.GetWebsocketURL())

		p := currency.Pair{
			Delimiter: "_", //todo put delimit of eash asset
			Base:      currency.BTC,
			Quote:     currency.USDT,
		}
		// mu mutex
		go func(u ex.URL, bws *stream.Websocket) {
			for {
				fmt.Printf("#########################\nGetOrderbook: %s\n", u.String())
				assets, _ := ex.GetAssetsFromURLType(u)
				for _, a := range assets {
					var orb2 *binance.OrderBook
					var err2 error
					switch a {
					case asset.Spot:
						orb2, err2 = b.GetOrderBook(context.TODO(), binance.OrderBookDataRequestParams{Symbol: p, Limit: 1000})
					case asset.USDTMarginedFutures:
						orb2, err2 = b.UFuturesOrderbook(context.TODO(), p, 1000)
					}

					orb, err := bws.Orderbook.GetOrderbook(p, a)

					if err != nil {
						fmt.Printf("GetOrderbook err: %v\n", err)
					} else if err2 != nil {
						fmt.Printf("GetOrderbook2222 err: %v\n", err2)
					} else {
						fmt.Printf("orb1.LastUpdateID: %v\n", orb.LastUpdateID)
						fmt.Printf("orb2.LastUpdateID: %v\n", orb2.LastUpdateID)
						fmt.Println("Diff:", orb.LastUpdateID-orb2.LastUpdateID)
						fmt.Printf("orb1.Bids: %v\n", orb.Bids[:5])
						fmt.Printf("orb2.Bids: %v\n", orb2.Bids[:5])
					}
					time.Sleep(time.Second * 2)
				}
			}
		}(url, bWebsocket)
	}
	go waitForInteruptw(interrupt)
	<-interrupt
	for _, bWebsocket := range b.Websockets {
		bWebsocket.Shutdown()
	}
	fmt.Println("Exited.")
}

func waitForInteruptw(waiter chan<- struct{}) {
	interrupt := signaler.WaitForInterrupt()
	fmt.Printf("\nCaptured %v, shutdown requested.\n", interrupt)
	waiter <- struct{}{}
}
