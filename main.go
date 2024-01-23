package main

import (
	"context"

	"github.com/mmfshirokan/PriceService/internal/config"
	"github.com/mmfshirokan/PriceService/internal/consumer"
)

func main() {
	conf := config.New()
	cons := consumer.New(conf.KafkaURL, conf.KafkaTopic)
	ctx := context.Background()
	forever := make(chan struct{})

	go cons.Read(ctx)

	<-forever
}
