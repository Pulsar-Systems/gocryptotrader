package main

import (
	"fmt"
	"os"
	"os/signal"
)

func main() {
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	KrakenWSEx()
	<-interrupt
	fmt.Println("Program interrupted")
	os.Exit(1)
}