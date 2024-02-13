package main

import (
	"context"
	"net"

	"github.com/mmfshirokan/PriceService/internal/config"
	"github.com/mmfshirokan/PriceService/internal/consumer"
	"github.com/mmfshirokan/PriceService/internal/model"
	"github.com/mmfshirokan/PriceService/internal/rpc"
	"github.com/mmfshirokan/PriceService/proto/pb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

func main() {
	conf := config.New()
	ctx := context.Background()

	mainChan := make(chan model.Price)
	foreverChan := make(chan struct{})

	cons := consumer.New(conf.KafkaURL, conf.KafkaTopic, mainChan)
	go cons.Read(ctx)

	go rpcServerStart(mainChan)

	<-foreverChan
}

func rpcServerStart(ch chan model.Price) {
	lis, err := net.Listen("tcp", "localhost:9091")
	if err != nil {
		log.Errorf("failed to listen: %v", err)
	}

	rpcServer := grpc.NewServer()
	rpcConsumer := rpc.NewConsumerServer(ch)

	pb.RegisterConsumerServer(rpcServer, rpcConsumer)

	err = rpcServer.Serve(lis)
	if err != nil {
		log.Error("rpc error: Server can't start")
	}
}

// var count uint = 0
// for {
// 	pr := <-mainChan
// 	fmt.Printf("Num: %v, Symbol: %v, Bid: %v, Ask: %v, Time: %v\n", count, pr.Symbol, pr.Bid, pr.Ask, pr.Date)
// 	count++
// }
