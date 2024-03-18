package main

import (
	"context"
	"net"
	"strings"

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

	chanelsMap := map[string]chan model.Price{
		getPort(conf.RpcChartPort): make(chan model.Price),
		getPort(conf.RpcPosPort):   make(chan model.Price),
		// ADD more chanelse if nessasry
	}
	foreverChan := make(chan struct{})

	cons := consumer.New(conf.KafkaURL, conf.KafkaTopic, chanelsMap)
	go cons.Read(ctx)
	go rpcServerStart(chanelsMap)

	<-foreverChan
}

func rpcServerStart(chMap map[string]chan model.Price) {

	for port, chanel := range chMap {
		go func(prt string, ch chan model.Price) {
			lis, err := net.Listen("tcp", "localhost:"+prt)
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
		}(port, chanel)
	}
}

func getPort(url string) string {
	_, port, found := strings.Cut(url, ":")
	if !found {
		log.Error("Empty port-env, using default env for map-key 7070")
		return "7070"
	}

	return port
}
