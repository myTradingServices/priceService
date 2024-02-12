package main

import (
	"context"
	"fmt"

	"github.com/mmfshirokan/PriceService/internal/config"
	"github.com/mmfshirokan/PriceService/internal/consumer"
	"github.com/mmfshirokan/PriceService/internal/model"
)

func main() {
	conf := config.New()
	ctx := context.Background()
	mainChan := make(chan model.Price)

	cons := consumer.New(conf.KafkaURL, conf.KafkaTopic, mainChan)
	go cons.Read(ctx)

	var count uint = 0
	for {
		pr := <-mainChan
		fmt.Printf("Num: %v, Symbol: %v, Bid: %v, Ask: %v, Time: %v\n", count, pr.Symbol, pr.Bid, pr.Ask, pr.Date)
		count++
	}

}
